//! Offline VPIN parameter sweep: replay a captured trade tape (see
//! `trade_ingest::capture`) through `vpin-engine` at every combination of
//! `bucket_volume` x `window` you give it, and print descriptive stats
//! for each so a human can pick a reasonable pair.
//!
//! Also reports how often BVC's probabilistic buy/sell call actually
//! matches the real taker side each exchange adapter captures in
//! `NormalizedTrade::taker_side` (`bvc_accuracy`/`bvc_mae` columns).
//! `vpin-engine` never looks at this field by design (BVC exists
//! specifically to avoid needing it), this is what lets you check how
//! much that probabilistic classification actually costs you on your
//! own instrument instead of taking it on faith.
//!
//! This is NOT `realistic-mm-backtester`: there's no fill simulation, no
//! PnL, no order queue, this only measures the VPIN estimator itself
//! (does it warm up in a reasonable fraction of the tape, what's its
//! mean/spread, how often does a bucket actually close in wall-clock
//! time at this volume). Testing whether a *strategy* that reacts to
//! this signal makes money is `mmbt`'s job, not this tool's.
//!
//! What this deliberately does NOT do: validate VPIN spikes against
//! known toxic/informed-trading episodes. That needs labeled ground
//! truth (timestamps of real flash crashes, known toxic prints, etc.)
//! for your specific instrument, which isn't something this tool can
//! invent or ship a default for. If you have that data, cross-reference
//! it against this tool's output yourself, or extend `calibrate_one`
//! below to take a list of labeled timestamps and report where they
//! land in the VPIN/CDF distribution.
//!
//! Usage:
//!   calibrate <capture-file> --bucket-volumes 10,25,50,100 --windows 20,50,100 [--cdf-window 250]
//!
//! One pitfall worth knowing about when reading the output: if
//! `non_warmup_reads` stays near zero no matter how large the tape is,
//! that's not necessarily still-warming-up, it can mean every bucket's
//! price change is landing on an identical (or near-identical) value,
//! which drives BVC's sigma to zero, and `vpin-engine` correctly refuses
//! to classify against a zero-variance estimate rather than divide by
//! it. A `bucket_volume` that happens to be an exact multiple of your
//! typical per-print size on a very regular tape can produce exactly
//! this, worth trying a slightly different `bucket_volume` if you see it.

use std::env;
use std::process::ExitCode;

use trade_ingest::capture::TradeReader;
use trade_ingest::TakerSide;
use vpin_engine::{classify, RollingSigma, VolumeBucketer, VpinEngine, VpinEngineConfig, VpinError};

struct Trade {
    price: f64,
    volume: f64,
    ts_ns: u64,
    taker_side: TakerSide,
}

struct Args {
    file: String,
    bucket_volumes: Vec<f64>,
    windows: Vec<usize>,
    cdf_window: Option<usize>,
}

const USAGE: &str =
    "usage: calibrate <capture-file> --bucket-volumes V1,V2,... --windows N1,N2,... [--cdf-window N]";

fn parse_args<I: Iterator<Item = String>>(mut args: I) -> Result<Args, String> {
    let file = args.next().ok_or(USAGE)?;

    let mut bucket_volumes = None;
    let mut windows = None;
    let mut cdf_window = None;

    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--bucket-volumes" => bucket_volumes = Some(parse_csv(&value, "--bucket-volumes")?),
            "--windows" => windows = Some(parse_csv(&value, "--windows")?),
            "--cdf-window" => {
                cdf_window = Some(value.parse::<usize>().map_err(|e| format!("bad --cdf-window {value:?}: {e}"))?)
            }
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
    }

    Ok(Args {
        file,
        bucket_volumes: bucket_volumes.ok_or_else(|| format!("missing --bucket-volumes\n{USAGE}"))?,
        windows: windows.ok_or_else(|| format!("missing --windows\n{USAGE}"))?,
        cdf_window,
    })
}

fn parse_csv<T: std::str::FromStr>(s: &str, flag: &str) -> Result<Vec<T>, String>
where
    T::Err: std::fmt::Display,
{
    s.split(',')
        .map(|x| x.trim().parse::<T>().map_err(|e| format!("bad value {x:?} for {flag}: {e}")))
        .collect()
}

struct Stats {
    bucket_volume: f64,
    window: usize,
    total_buckets: u64,
    non_warmup_readings: u64,
    vpin_mean: f64,
    vpin_min: f64,
    vpin_max: f64,
    vpin_stddev: f64,
    /// Mean wall-clock gap between consecutive bucket closes, using the
    /// trades' own exchange timestamps. This is the number that answers
    /// "if I pick this bucket_volume, how often do I actually get a
    /// fresh reading" independent of what VPIN's value comes out to.
    avg_bucket_interval_ms: f64,
    /// Fraction of buckets where BVC's majority-side call (buy_fraction
    /// > 0.5) agreed with the real taker-side majority (by volume) for
    /// that bucket, using the ground truth every exchange adapter
    /// captures in `NormalizedTrade::taker_side` but `vpin-engine` never
    /// looks at. `NaN` if sigma never warmed up for this config.
    bvc_accuracy: f64,
    /// Mean |BVC's buy_fraction - true volume-weighted buy_fraction|
    /// across compared buckets. Complements `bvc_accuracy`: two configs
    /// can have the same majority-call accuracy while one is
    /// systematically closer to the true split and the other isn't.
    bvc_mean_abs_error: f64,
}

fn calibrate_one(trades: &[Trade], bucket_volume: f64, window: usize, cdf_window: Option<usize>) -> Result<Stats, VpinError> {
    let mut engine = VpinEngine::new(VpinEngineConfig {
        bucket_volume,
        sigma_window: window,
        vpin_window: window,
        cdf_window,
    })?;

    let mut total_buckets: u64 = 0;
    let mut vpins = Vec::new();
    let mut close_times = Vec::new();

    for t in trades {
        if let Some(reading) = engine.push_trade(t.price, t.volume, t.ts_ns) {
            total_buckets += 1;
            close_times.push(reading.ts_close_ns);
            if let Some(v) = reading.vpin {
                vpins.push(v);
            }
        }
    }

    let (vpin_mean, vpin_min, vpin_max, vpin_stddev) = summarize(&vpins);
    let (bvc_accuracy, bvc_mean_abs_error) = bvc_accuracy_stats(trades, bucket_volume, window)?;

    Ok(Stats {
        bucket_volume,
        window,
        total_buckets,
        non_warmup_readings: vpins.len() as u64,
        vpin_mean,
        vpin_min,
        vpin_max,
        vpin_stddev,
        avg_bucket_interval_ms: mean_interval_ms(&close_times),
        bvc_accuracy,
        bvc_mean_abs_error,
    })
}

/// Replays the same trades through the lower-level pieces
/// (`VolumeBucketer` + `RollingSigma` + `bvc::classify`) that
/// `VpinEngine` itself uses internally, so BVC's per-bucket call can be
/// compared against the real taker-side volume split for that bucket.
/// `VpinEngine`'s own public API only exposes the aggregated VPIN score,
/// not a per-bucket buy_fraction, so this can't be read off the first
/// pass above, it needs its own pass over the same data. Same sigma
/// discipline as `VpinEngine::push_trade`: classify with sigma from
/// before this bucket, advance sigma with this bucket's own delta_p
/// only after.
fn bvc_accuracy_stats(trades: &[Trade], bucket_volume: f64, sigma_window: usize) -> Result<(f64, f64), VpinError> {
    let mut bucketer = VolumeBucketer::new(bucket_volume)?;
    let mut sigma = RollingSigma::new(sigma_window)?;

    let mut buy_vol = 0.0;
    let mut sell_vol = 0.0;
    let mut correct = 0u64;
    let mut compared = 0u64;
    let mut abs_errors = Vec::new();

    for t in trades {
        match t.taker_side {
            TakerSide::Buy => buy_vol += t.volume,
            TakerSide::Sell => sell_vol += t.volume,
        }

        if let Some(closed) = bucketer.push(t.price, t.volume, t.ts_ns) {
            if let Some(sigma_before) = sigma.current() {
                if let Some((bvc_buy, _)) = classify(closed.delta_p, sigma_before, closed.volume) {
                    let bvc_frac = bvc_buy / closed.volume;
                    let true_frac = buy_vol / (buy_vol + sell_vol);

                    if (bvc_frac > 0.5) == (true_frac > 0.5) {
                        correct += 1;
                    }
                    abs_errors.push((bvc_frac - true_frac).abs());
                    compared += 1;
                }
            }
            sigma.push(closed.delta_p);
            buy_vol = 0.0;
            sell_vol = 0.0;
        }
    }

    let accuracy = if compared > 0 { correct as f64 / compared as f64 } else { f64::NAN };
    let mae = if abs_errors.is_empty() { f64::NAN } else { abs_errors.iter().sum::<f64>() / abs_errors.len() as f64 };
    Ok((accuracy, mae))
}

fn summarize(values: &[f64]) -> (f64, f64, f64, f64) {
    if values.is_empty() {
        return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let stddev = if values.len() > 1 {
        (values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64).sqrt()
    } else {
        0.0
    };
    (mean, min, max, stddev)
}

fn mean_interval_ms(close_times_ns: &[u64]) -> f64 {
    if close_times_ns.len() < 2 {
        return f64::NAN;
    }
    let span_ns = close_times_ns[close_times_ns.len() - 1].saturating_sub(close_times_ns[0]);
    (span_ns as f64 / (close_times_ns.len() - 1) as f64) / 1e6
}

fn load_trades(path: &str) -> Result<Vec<Trade>, String> {
    let reader = TradeReader::open(path).map_err(|e| format!("opening {path}: {e}"))?;
    reader
        .map(|r| {
            r.map(|t| Trade {
                price: t.price.raw() as f64 / 1e8,
                volume: t.qty.raw() as f64 / 1e8,
                ts_ns: t.ts_exchange_ns,
                taker_side: t.taker_side,
            })
        })
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| format!("reading {path}: {e}"))
}

fn print_report(trades_len: usize, file: &str, bucket_volumes: &[f64], windows: &[usize], cdf_window: Option<usize>, trades: &[Trade]) {
    println!("loaded {trades_len} trades from {file}");
    println!();
    println!(
        "{:>14} {:>8} {:>14} {:>16} {:>10} {:>10} {:>10} {:>10} {:>18} {:>12} {:>10}",
        "bucket_volume",
        "window",
        "total_buckets",
        "non_warmup_reads",
        "vpin_mean",
        "vpin_min",
        "vpin_max",
        "vpin_std",
        "avg_interval_ms",
        "bvc_accuracy",
        "bvc_mae"
    );

    for &bucket_volume in bucket_volumes {
        for &window in windows {
            match calibrate_one(trades, bucket_volume, window, cdf_window) {
                Ok(s) => println!(
                    "{:>14.4} {:>8} {:>14} {:>16} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>18.1} {:>12.4} {:>10.4}",
                    s.bucket_volume,
                    s.window,
                    s.total_buckets,
                    s.non_warmup_readings,
                    s.vpin_mean,
                    s.vpin_min,
                    s.vpin_max,
                    s.vpin_stddev,
                    s.avg_bucket_interval_ms,
                    s.bvc_accuracy,
                    s.bvc_mean_abs_error,
                ),
                Err(e) => println!("{bucket_volume:>14.4} {window:>8}  invalid config: {e}"),
            }
        }
    }
    println!();
    println!(
        "bvc_accuracy: fraction of buckets where BVC's buy/sell majority call matched the real taker-side majority (by volume)."
    );
    println!("bvc_mae: mean |BVC's buy_fraction - true buy_fraction| across compared buckets. Both NaN if sigma never warmed up.");
}

fn main() -> ExitCode {
    let args = match parse_args(env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let trades = match load_trades(&args.file) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    if trades.is_empty() {
        eprintln!("error: {} has zero trades, nothing to calibrate against", args.file);
        return ExitCode::FAILURE;
    }

    print_report(trades.len(), &args.file, &args.bucket_volumes, &args.windows, args.cdf_window, &trades);
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_args() {
        let raw = ["trades.cap", "--bucket-volumes", "10,25,50", "--windows", "20,50", "--cdf-window", "100"]
            .map(String::from);
        let args = parse_args(raw.into_iter()).unwrap();
        assert_eq!(args.file, "trades.cap");
        assert_eq!(args.bucket_volumes, vec![10.0, 25.0, 50.0]);
        assert_eq!(args.windows, vec![20, 50]);
        assert_eq!(args.cdf_window, Some(100));
    }

    #[test]
    fn cdf_window_is_optional() {
        let raw = ["trades.cap", "--bucket-volumes", "10", "--windows", "20"].map(String::from);
        let args = parse_args(raw.into_iter()).unwrap();
        assert_eq!(args.cdf_window, None);
    }

    #[test]
    fn missing_required_flag_is_an_error() {
        let raw = ["trades.cap", "--windows", "20"].map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn unknown_flag_is_an_error() {
        let raw = ["trades.cap", "--bogus", "1"].map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn malformed_number_in_csv_is_an_error() {
        let raw = ["trades.cap", "--bucket-volumes", "10,abc,50", "--windows", "20"].map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn summarize_empty_is_all_nan() {
        let (mean, min, max, std) = summarize(&[]);
        assert!(mean.is_nan() && min.is_nan() && max.is_nan() && std.is_nan());
    }

    #[test]
    fn summarize_matches_known_values() {
        let (mean, min, max, std) = summarize(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert!((mean - 5.0).abs() < 1e-9);
        assert_eq!(min, 2.0);
        assert_eq!(max, 9.0);
        // same worked example verified against Python's statistics.stdev
        // when this exact data set was used for vpin-engine's RollingSigma
        assert!((std - 2.138_089_935_299_395).abs() < 1e-9);
    }

    #[test]
    fn mean_interval_computes_average_gap() {
        let interval = mean_interval_ms(&[0, 1_000_000, 3_000_000, 6_000_000]); // ns
        // gaps: 1ms, 2ms, 3ms -> mean 2ms
        assert!((interval - 2.0).abs() < 1e-9);
    }

    #[test]
    fn mean_interval_needs_at_least_two_points() {
        assert!(mean_interval_ms(&[]).is_nan());
        assert!(mean_interval_ms(&[42]).is_nan());
    }

    #[test]
    fn calibrate_one_reports_invalid_config_as_error_not_panic() {
        let trades = vec![Trade { price: 100.0, volume: 1.0, ts_ns: 0, taker_side: TakerSide::Buy }];
        let result = calibrate_one(&trades, -5.0, 10, None);
        assert!(result.is_err());
    }

    #[test]
    fn calibrate_one_runs_end_to_end_on_synthetic_trades() {
        // One trade per bucket (bucket_volume == trade volume), same
        // shape as vpin-engine's own warmup test. Pairing 2+ trades per
        // bucket with a perfectly periodic zigzag makes every bucket's
        // delta_p identical, collapsing sigma to zero and making BVC
        // correctly refuse to classify anything, that's a real trap for
        // synthetic test data, not a bug in calibrate_one.
        let mut trades = Vec::new();
        let mut price = 100.0;
        for i in 0..500u64 {
            price += if i % 2 == 0 { 0.4 } else { -0.25 };
            let taker_side = if i % 2 == 0 { TakerSide::Buy } else { TakerSide::Sell };
            trades.push(Trade { price, volume: 10.0, ts_ns: i * 1_000_000, taker_side });
        }
        let stats = calibrate_one(&trades, 10.0, 10, Some(20)).unwrap();
        assert!(stats.total_buckets > 0);
        assert!(stats.non_warmup_readings > 0);
        assert!(stats.vpin_mean >= 0.0 && stats.vpin_mean <= 1.0);
        assert!(!stats.avg_bucket_interval_ms.is_nan());
        assert!(!stats.bvc_accuracy.is_nan(), "expected BVC accuracy to be computed once sigma warms up");
        assert!((0.0..=1.0).contains(&stats.bvc_accuracy));
        assert!(stats.bvc_mean_abs_error >= 0.0);
    }

    #[test]
    fn bvc_accuracy_is_nan_before_sigma_warms_up() {
        // window=1000 on a 5-trade tape: sigma never fills, nothing to
        // compare, this must report NaN rather than a misleading 0% or
        // panicking on an empty average.
        let trades = (0..5u64)
            .map(|i| Trade { price: 100.0 + i as f64, volume: 10.0, ts_ns: i, taker_side: TakerSide::Buy })
            .collect::<Vec<_>>();
        let (accuracy, mae) = bvc_accuracy_stats(&trades, 10.0, 1000).unwrap();
        assert!(accuracy.is_nan());
        assert!(mae.is_nan());
    }

    #[test]
    fn bvc_accuracy_is_perfect_when_every_trade_pushes_price_with_its_own_side() {
        // Construct a tape where every single trade is BOTH the entire
        // bucket (bucket_volume == trade volume) AND its price move is
        // signed exactly the way its taker_side says: buys push price
        // up, sells push it down. BVC classifies buckets by delta_p, so
        // this is the case where BVC's signal and the ground truth are,
        // by construction, perfectly aligned, accuracy has to be 1.0.
        let mut trades = Vec::new();
        let mut price = 1000.0;
        let mut state: u64 = 42;
        for i in 0..300u64 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let is_buy = (state >> 63) == 1;
            price += if is_buy { 1.0 } else { -1.0 };
            trades.push(Trade {
                price,
                volume: 10.0,
                ts_ns: i,
                taker_side: if is_buy { TakerSide::Buy } else { TakerSide::Sell },
            });
        }
        let (accuracy, _mae) = bvc_accuracy_stats(&trades, 10.0, 20).unwrap();
        assert!(!accuracy.is_nan());
        assert!(accuracy > 0.95, "expected near-perfect accuracy on a tape constructed to align, got {accuracy}");
    }
}
