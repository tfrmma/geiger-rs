//! Offline VPIN parameter sweep: replay a captured trade tape (see
//! `trade_ingest::capture`) through `vpin-engine` at every combination of
//! `bucket_volume` x `sigma_window` x `vpin_window` you give it, and
//! report descriptive stats for each so a human can pick a reasonable
//! combination. `sigma_window` and `vpin_window` sweep independently
//! (they don't have to be the same value, even though `VpinEngineConfig`
//! happens to accept them separately too): BVC's classification quality
//! depends only on `sigma_window`, while `vpin_window` only controls how
//! many classified buckets get averaged into one VPIN score, they're
//! answering different questions and coupling them in a sweep hides that.
//!
//! Also reports how often BVC's probabilistic buy/sell call actually
//! matches the real taker side each exchange adapter captures in
//! `NormalizedTrade::taker_side` (`bvc_accuracy`/`bvc_mae` columns), plus
//! a `bvc_p_value` comparing each config's accuracy against the sweep's
//! best one, see `two_proportion_p_value`. `vpin-engine` never looks at
//! `taker_side` by design (BVC exists specifically to avoid needing it),
//! this is what lets you check how much that probabilistic classification
//! actually costs you on your own instrument instead of taking it on
//! faith.
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
//!   calibrate <capture-file> --bucket-volumes 10,25,50,100 \
//!       --sigma-windows 20,50,100 --vpin-windows 20,50,100 \
//!       [--cdf-window 250] [--format table|csv|json]
//!
//! `--format` defaults to `table` (human-readable, stdout). `csv` and
//! `json` are for piping into a notebook/spreadsheet, both go to stdout
//! too, redirect with shell `>` if you want a file.
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
use vpin_engine::{
    classify, standard_normal_cdf, RollingSigma, VolumeBucketer, VpinEngine, VpinEngineConfig,
    VpinError,
};

struct Trade {
    price: f64,
    volume: f64,
    ts_ns: u64,
    taker_side: TakerSide,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "table" => Ok(OutputFormat::Table),
            "csv" => Ok(OutputFormat::Csv),
            "json" => Ok(OutputFormat::Json),
            other => Err(format!(
                "unknown --format {other:?}, expected table, csv, or json"
            )),
        }
    }
}

struct Args {
    file: String,
    bucket_volumes: Vec<f64>,
    sigma_windows: Vec<usize>,
    vpin_windows: Vec<usize>,
    cdf_window: Option<usize>,
    format: OutputFormat,
}

const USAGE: &str = "usage: calibrate <capture-file> --bucket-volumes V1,V2,... --sigma-windows N1,N2,... --vpin-windows N1,N2,... [--cdf-window N] [--format table|csv|json]";

fn parse_args<I: Iterator<Item = String>>(mut args: I) -> Result<Args, String> {
    let file = args.next().ok_or(USAGE)?;

    let mut bucket_volumes = None;
    let mut sigma_windows = None;
    let mut vpin_windows = None;
    let mut cdf_window = None;
    let mut format = OutputFormat::Table;

    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--bucket-volumes" => bucket_volumes = Some(parse_csv(&value, "--bucket-volumes")?),
            "--sigma-windows" => sigma_windows = Some(parse_csv(&value, "--sigma-windows")?),
            "--vpin-windows" => vpin_windows = Some(parse_csv(&value, "--vpin-windows")?),
            "--cdf-window" => {
                cdf_window = Some(
                    value
                        .parse::<usize>()
                        .map_err(|e| format!("bad --cdf-window {value:?}: {e}"))?,
                )
            }
            "--format" => format = value.parse()?,
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
    }

    Ok(Args {
        file,
        bucket_volumes: bucket_volumes
            .ok_or_else(|| format!("missing --bucket-volumes\n{USAGE}"))?,
        sigma_windows: sigma_windows.ok_or_else(|| format!("missing --sigma-windows\n{USAGE}"))?,
        vpin_windows: vpin_windows.ok_or_else(|| format!("missing --vpin-windows\n{USAGE}"))?,
        cdf_window,
        format,
    })
}

fn parse_csv<T: std::str::FromStr>(s: &str, flag: &str) -> Result<Vec<T>, String>
where
    T::Err: std::fmt::Display,
{
    s.split(',')
        .map(|x| {
            x.trim()
                .parse::<T>()
                .map_err(|e| format!("bad value {x:?} for {flag}: {e}"))
        })
        .collect()
}

#[derive(Debug, Clone)]
struct Stats {
    bucket_volume: f64,
    sigma_window: usize,
    vpin_window: usize,
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
    /// Fraction of buckets where BVC's majority-side call (buy_fraction >
    /// 0.5) agreed with the real taker-side majority (by volume) for that
    /// bucket, using the ground truth every exchange adapter captures in
    /// `NormalizedTrade::taker_side` but `vpin-engine` never looks at.
    /// `NaN` if sigma never warmed up for this config.
    bvc_accuracy: f64,
    /// Mean |BVC's buy_fraction - true volume-weighted buy_fraction|
    /// across compared buckets. Complements `bvc_accuracy`: two configs
    /// can have the same majority-call accuracy while one is
    /// systematically closer to the true split and the other isn't.
    bvc_mean_abs_error: f64,
    /// Number of buckets `bvc_accuracy`/`bvc_mean_abs_error` are actually
    /// computed over (sigma warmed up). This is `n` for
    /// `bvc_accuracy_p_value`'s significance test, not just informational.
    bvc_compared: u64,
    /// Two-tailed p-value from a two-proportion z-test comparing this
    /// config's `bvc_accuracy` against the sweep's single best
    /// `bvc_accuracy`. `NaN` for the best config itself (nothing to
    /// compare it against) or when either side has too few compared
    /// buckets, see `two_proportion_p_value`. A small value (conventionally
    /// < 0.05) means this config's accuracy is unlikely to just be noise
    /// around the best one's; a large value means the difference could
    /// easily be sampling noise from a finite tape, not necessarily a
    /// real difference in classification quality.
    bvc_accuracy_p_value: f64,
}

#[derive(Debug, Clone, Copy)]
struct BvcStats {
    accuracy: f64,
    mean_abs_error: f64,
    compared: u64,
}

fn calibrate_one(
    trades: &[Trade],
    bucket_volume: f64,
    sigma_window: usize,
    vpin_window: usize,
    cdf_window: Option<usize>,
    bvc: BvcStats,
) -> Result<Stats, VpinError> {
    let mut engine = VpinEngine::new(VpinEngineConfig {
        bucket_volume,
        sigma_window,
        vpin_window,
        cdf_window,
        confidence_interval: None,
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

    Ok(Stats {
        bucket_volume,
        sigma_window,
        vpin_window,
        total_buckets,
        non_warmup_readings: vpins.len() as u64,
        vpin_mean,
        vpin_min,
        vpin_max,
        vpin_stddev,
        avg_bucket_interval_ms: mean_interval_ms(&close_times),
        bvc_accuracy: bvc.accuracy,
        bvc_mean_abs_error: bvc.mean_abs_error,
        bvc_compared: bvc.compared,
        bvc_accuracy_p_value: f64::NAN,
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
///
/// Depends only on `bucket_volume` and `sigma_window`, not
/// `vpin_window`: `run_sweep` below relies on that to call this once per
/// `(bucket_volume, sigma_window)` pair and reuse the result across
/// every `vpin_window` in the sweep, rather than replaying the whole
/// tape redundantly once per `vpin_window` for an identical answer.
fn bvc_accuracy_stats(
    trades: &[Trade],
    bucket_volume: f64,
    sigma_window: usize,
) -> Result<BvcStats, VpinError> {
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

    let accuracy = if compared > 0 {
        correct as f64 / compared as f64
    } else {
        f64::NAN
    };
    let mae = if abs_errors.is_empty() {
        f64::NAN
    } else {
        abs_errors.iter().sum::<f64>() / abs_errors.len() as f64
    };
    Ok(BvcStats {
        accuracy,
        mean_abs_error: mae,
        compared,
    })
}

/// Two-proportion z-test (Wald, unpooled variance): is `p1` (over `n1`
/// compared buckets) actually different from `p2` (over `n2`), or is the
/// gap explainable by finite-sample noise alone? `bvc_accuracy` is a
/// plain proportion (successes/trials), which has a well-known closed
/// form for its sampling variance, unlike VPIN itself — a ratio of sums,
/// not a simple proportion — which needed a bootstrap instead (see
/// `vpin_engine::ConfidenceIntervalConfig`'s doc comment for why). No
/// bootstrap needed here, the closed form is exact enough for this.
///
/// Returns the two-tailed p-value, or `NaN` if either side has fewer
/// than `MIN_COMPARED` compared buckets (too few for the normal
/// approximation this test relies on to be trustworthy) or either
/// proportion is itself `NaN`.
fn two_proportion_p_value(p1: f64, n1: u64, p2: f64, n2: u64) -> f64 {
    const MIN_COMPARED: u64 = 30;
    if n1 < MIN_COMPARED || n2 < MIN_COMPARED || p1.is_nan() || p2.is_nan() {
        return f64::NAN;
    }
    let n1 = n1 as f64;
    let n2 = n2 as f64;
    let se = (p1 * (1.0 - p1) / n1 + p2 * (1.0 - p2) / n2).sqrt();
    if se == 0.0 {
        // The only way se==0 with both n's above MIN_COMPARED is p1==p2
        // exactly, nothing to distinguish, not a degenerate error case.
        return 1.0;
    }
    let z = (p1 - p2) / se;
    2.0 * (1.0 - standard_normal_cdf(z.abs()))
}

struct SweepResult {
    bucket_volume: f64,
    sigma_window: usize,
    vpin_window: usize,
    stats: Result<Stats, VpinError>,
}

/// Runs the full `bucket_volumes x sigma_windows x vpin_windows`
/// cartesian product and fills in `bvc_accuracy_p_value` on every
/// successful result by comparing it against the sweep's single best
/// (highest, non-`NaN`) `bvc_accuracy`.
fn run_sweep(
    trades: &[Trade],
    bucket_volumes: &[f64],
    sigma_windows: &[usize],
    vpin_windows: &[usize],
    cdf_window: Option<usize>,
) -> Vec<SweepResult> {
    let mut out =
        Vec::with_capacity(bucket_volumes.len() * sigma_windows.len() * vpin_windows.len());

    for &bv in bucket_volumes {
        for &sw in sigma_windows {
            let bvc = bvc_accuracy_stats(trades, bv, sw);

            for &vw in vpin_windows {
                let stats = match &bvc {
                    Ok(bvc_stats) => calibrate_one(trades, bv, sw, vw, cdf_window, *bvc_stats),
                    Err(e) => Err(*e),
                };
                out.push(SweepResult {
                    bucket_volume: bv,
                    sigma_window: sw,
                    vpin_window: vw,
                    stats,
                });
            }
        }
    }

    let best_idx = out
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.stats.as_ref().ok().map(|s| (i, s.bvc_accuracy)))
        .filter(|(_, acc)| !acc.is_nan())
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(i, _)| i);

    if let Some(best_idx) = best_idx {
        let (best_accuracy, best_n) = {
            let s = out[best_idx].stats.as_ref().unwrap();
            (s.bvc_accuracy, s.bvc_compared)
        };
        for (i, r) in out.iter_mut().enumerate() {
            if i == best_idx {
                continue; // nothing to meaningfully compare the best against but itself
            }
            if let Ok(s) = &mut r.stats {
                s.bvc_accuracy_p_value =
                    two_proportion_p_value(s.bvc_accuracy, s.bvc_compared, best_accuracy, best_n);
            }
        }
    }

    out
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

fn format_p_value(p: f64) -> String {
    if p.is_nan() {
        "-".to_string()
    } else {
        format!("{p:.4}")
    }
}

fn print_table(trades_len: usize, file: &str, results: &[SweepResult]) {
    println!("loaded {trades_len} trades from {file}");
    println!();
    println!(
        "{:>14} {:>12} {:>11} {:>14} {:>16} {:>10} {:>10} {:>10} {:>10} {:>18} {:>12} {:>10} {:>8} {:>12}",
        "bucket_volume",
        "sigma_window",
        "vpin_window",
        "total_buckets",
        "non_warmup_reads",
        "vpin_mean",
        "vpin_min",
        "vpin_max",
        "vpin_std",
        "avg_interval_ms",
        "bvc_accuracy",
        "bvc_mae",
        "bvc_n",
        "bvc_p_value",
    );

    for r in results {
        match &r.stats {
            Ok(s) => println!(
                "{:>14.4} {:>12} {:>11} {:>14} {:>16} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>18.1} {:>12.4} {:>10.4} {:>8} {:>12}",
                s.bucket_volume,
                s.sigma_window,
                s.vpin_window,
                s.total_buckets,
                s.non_warmup_readings,
                s.vpin_mean,
                s.vpin_min,
                s.vpin_max,
                s.vpin_stddev,
                s.avg_bucket_interval_ms,
                s.bvc_accuracy,
                s.bvc_mean_abs_error,
                s.bvc_compared,
                format_p_value(s.bvc_accuracy_p_value),
            ),
            Err(e) => println!(
                "{:>14.4} {:>12} {:>11}  invalid config: {e}",
                r.bucket_volume, r.sigma_window, r.vpin_window
            ),
        }
    }
    println!();
    println!(
        "bvc_accuracy: fraction of buckets where BVC's buy/sell majority call matched the real taker-side majority (by volume)."
    );
    println!(
        "bvc_mae: mean |BVC's buy_fraction - true buy_fraction| across compared buckets. Both NaN if sigma never warmed up."
    );
    println!(
        "bvc_p_value: two-proportion z-test p-value vs. the sweep's best bvc_accuracy. \"-\" for the best config itself, or under 30 compared buckets on either side."
    );
}

/// RFC 4126-style minimal CSV quoting: wraps in double quotes (doubling
/// any internal quote) only if the field actually needs it. The only
/// field here that ever needs it is `error` (a `VpinError`'s `Display`
/// text can contain a comma, e.g. "window size must be at least 2, got
/// 0"), every numeric column is comma/quote-free by construction.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn csv_f64(x: f64) -> String {
    if x.is_nan() {
        "NaN".to_string()
    } else {
        format!("{x}")
    }
}

const CSV_HEADER: &str = "bucket_volume,sigma_window,vpin_window,total_buckets,non_warmup_readings,vpin_mean,vpin_min,vpin_max,vpin_stddev,avg_bucket_interval_ms,bvc_accuracy,bvc_mean_abs_error,bvc_compared,bvc_accuracy_p_value,error";

fn csv_row(r: &SweepResult) -> Vec<String> {
    match &r.stats {
        Ok(s) => vec![
            s.bucket_volume.to_string(),
            s.sigma_window.to_string(),
            s.vpin_window.to_string(),
            s.total_buckets.to_string(),
            s.non_warmup_readings.to_string(),
            csv_f64(s.vpin_mean),
            csv_f64(s.vpin_min),
            csv_f64(s.vpin_max),
            csv_f64(s.vpin_stddev),
            csv_f64(s.avg_bucket_interval_ms),
            csv_f64(s.bvc_accuracy),
            csv_f64(s.bvc_mean_abs_error),
            s.bvc_compared.to_string(),
            csv_f64(s.bvc_accuracy_p_value),
            String::new(),
        ],
        Err(e) => {
            let mut fields = vec![
                r.bucket_volume.to_string(),
                r.sigma_window.to_string(),
                r.vpin_window.to_string(),
            ];
            // The 11 numeric columns between vpin_window and error
            // (total_buckets .. bvc_accuracy_p_value): nothing to report,
            // the config itself was rejected before any of them could be
            // computed.
            fields.extend(std::iter::repeat_n(String::new(), 11));
            fields.push(csv_escape(&e.to_string()));
            fields
        }
    }
}

fn print_csv(results: &[SweepResult]) {
    println!("{CSV_HEADER}");
    for r in results {
        println!("{}", csv_row(r).join(","));
    }
}

fn json_f64(x: f64) -> serde_json::Value {
    if x.is_finite() {
        serde_json::json!(x)
    } else {
        serde_json::Value::Null // JSON has no NaN/Infinity literal
    }
}

fn stats_to_json(r: &SweepResult) -> serde_json::Value {
    match &r.stats {
        Ok(s) => serde_json::json!({
            "bucket_volume": s.bucket_volume,
            "sigma_window": s.sigma_window,
            "vpin_window": s.vpin_window,
            "total_buckets": s.total_buckets,
            "non_warmup_readings": s.non_warmup_readings,
            "vpin_mean": json_f64(s.vpin_mean),
            "vpin_min": json_f64(s.vpin_min),
            "vpin_max": json_f64(s.vpin_max),
            "vpin_stddev": json_f64(s.vpin_stddev),
            "avg_bucket_interval_ms": json_f64(s.avg_bucket_interval_ms),
            "bvc_accuracy": json_f64(s.bvc_accuracy),
            "bvc_mean_abs_error": json_f64(s.bvc_mean_abs_error),
            "bvc_compared": s.bvc_compared,
            "bvc_accuracy_p_value": json_f64(s.bvc_accuracy_p_value),
            "error": null,
        }),
        Err(e) => serde_json::json!({
            "bucket_volume": r.bucket_volume,
            "sigma_window": r.sigma_window,
            "vpin_window": r.vpin_window,
            "error": e.to_string(),
        }),
    }
}

fn print_json(results: &[SweepResult]) {
    let rows: Vec<serde_json::Value> = results.iter().map(stats_to_json).collect();
    match serde_json::to_string_pretty(&rows) {
        Ok(text) => println!("{text}"),
        Err(e) => eprintln!("error: failed to serialize results as JSON: {e}"),
    }
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
        eprintln!(
            "error: {} has zero trades, nothing to calibrate against",
            args.file
        );
        return ExitCode::FAILURE;
    }

    let results = run_sweep(
        &trades,
        &args.bucket_volumes,
        &args.sigma_windows,
        &args.vpin_windows,
        args.cdf_window,
    );

    match args.format {
        OutputFormat::Table => print_table(trades.len(), &args.file, &results),
        OutputFormat::Csv => print_csv(&results),
        OutputFormat::Json => print_json(&results),
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_args() {
        let raw = [
            "trades.cap",
            "--bucket-volumes",
            "10,25,50",
            "--sigma-windows",
            "20,50",
            "--vpin-windows",
            "30,60",
            "--cdf-window",
            "100",
            "--format",
            "csv",
        ]
        .map(String::from);
        let args = parse_args(raw.into_iter()).unwrap();
        assert_eq!(args.file, "trades.cap");
        assert_eq!(args.bucket_volumes, vec![10.0, 25.0, 50.0]);
        assert_eq!(args.sigma_windows, vec![20, 50]);
        assert_eq!(args.vpin_windows, vec![30, 60]);
        assert_eq!(args.cdf_window, Some(100));
        assert_eq!(args.format, OutputFormat::Csv);
    }

    #[test]
    fn cdf_window_and_format_are_optional_with_sane_defaults() {
        let raw = [
            "trades.cap",
            "--bucket-volumes",
            "10",
            "--sigma-windows",
            "20",
            "--vpin-windows",
            "20",
        ]
        .map(String::from);
        let args = parse_args(raw.into_iter()).unwrap();
        assert_eq!(args.cdf_window, None);
        assert_eq!(args.format, OutputFormat::Table);
    }

    #[test]
    fn missing_required_flag_is_an_error() {
        let raw = [
            "trades.cap",
            "--sigma-windows",
            "20",
            "--vpin-windows",
            "20",
        ]
        .map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn missing_vpin_windows_is_an_error() {
        // sigma_window and vpin_window sweep independently now, both are
        // required flags, neither silently falls back to the other.
        let raw = [
            "trades.cap",
            "--bucket-volumes",
            "10",
            "--sigma-windows",
            "20",
        ]
        .map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn unknown_flag_is_an_error() {
        let raw = ["trades.cap", "--bogus", "1"].map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn unknown_format_is_an_error() {
        let raw = [
            "trades.cap",
            "--bucket-volumes",
            "10",
            "--sigma-windows",
            "20",
            "--vpin-windows",
            "20",
            "--format",
            "xml",
        ]
        .map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn malformed_number_in_csv_is_an_error() {
        let raw = [
            "trades.cap",
            "--bucket-volumes",
            "10,abc,50",
            "--sigma-windows",
            "20",
            "--vpin-windows",
            "20",
        ]
        .map(String::from);
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
        let trades = vec![Trade {
            price: 100.0,
            volume: 1.0,
            ts_ns: 0,
            taker_side: TakerSide::Buy,
        }];
        let result = calibrate_one(
            &trades,
            -5.0,
            10,
            10,
            None,
            BvcStats {
                accuracy: f64::NAN,
                mean_abs_error: f64::NAN,
                compared: 0,
            },
        );
        assert!(result.is_err());
    }

    fn synthetic_zigzag_trades(n: u64) -> Vec<Trade> {
        // One trade per bucket (bucket_volume == trade volume), same
        // shape as vpin-engine's own warmup test. Pairing 2+ trades per
        // bucket with a perfectly periodic zigzag makes every bucket's
        // delta_p identical, collapsing sigma to zero and making BVC
        // correctly refuse to classify anything, that's a real trap for
        // synthetic test data, not a bug in calibrate_one.
        let mut trades = Vec::new();
        let mut price = 100.0;
        for i in 0..n {
            price += if i % 2 == 0 { 0.4 } else { -0.25 };
            let taker_side = if i % 2 == 0 {
                TakerSide::Buy
            } else {
                TakerSide::Sell
            };
            trades.push(Trade {
                price,
                volume: 10.0,
                ts_ns: i * 1_000_000,
                taker_side,
            });
        }
        trades
    }

    #[test]
    fn calibrate_one_runs_end_to_end_on_synthetic_trades() {
        let trades = synthetic_zigzag_trades(500);
        let bvc = bvc_accuracy_stats(&trades, 10.0, 10).unwrap();
        let stats = calibrate_one(&trades, 10.0, 10, 10, Some(20), bvc).unwrap();
        assert!(stats.total_buckets > 0);
        assert!(stats.non_warmup_readings > 0);
        assert!(stats.vpin_mean >= 0.0 && stats.vpin_mean <= 1.0);
        assert!(!stats.avg_bucket_interval_ms.is_nan());
        assert!(
            !stats.bvc_accuracy.is_nan(),
            "expected BVC accuracy to be computed once sigma warms up"
        );
        assert!((0.0..=1.0).contains(&stats.bvc_accuracy));
        assert!(stats.bvc_mean_abs_error >= 0.0);
    }

    #[test]
    fn bvc_accuracy_is_nan_before_sigma_warms_up() {
        // window=1000 on a 5-trade tape: sigma never fills, nothing to
        // compare, this must report NaN rather than a misleading 0% or
        // panicking on an empty average.
        let trades = (0..5u64)
            .map(|i| Trade {
                price: 100.0 + i as f64,
                volume: 10.0,
                ts_ns: i,
                taker_side: TakerSide::Buy,
            })
            .collect::<Vec<_>>();
        let bvc = bvc_accuracy_stats(&trades, 10.0, 1000).unwrap();
        assert!(bvc.accuracy.is_nan());
        assert!(bvc.mean_abs_error.is_nan());
        assert_eq!(bvc.compared, 0);
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
                taker_side: if is_buy {
                    TakerSide::Buy
                } else {
                    TakerSide::Sell
                },
            });
        }
        let bvc = bvc_accuracy_stats(&trades, 10.0, 20).unwrap();
        assert!(!bvc.accuracy.is_nan());
        assert!(
            bvc.accuracy > 0.95,
            "expected near-perfect accuracy on a tape constructed to align, got {}",
            bvc.accuracy
        );
    }

    #[test]
    fn two_proportion_p_value_is_one_for_identical_large_samples() {
        let p = two_proportion_p_value(0.8, 200, 0.8, 200);
        assert!((p - 1.0).abs() < 1e-9);
    }

    #[test]
    fn two_proportion_p_value_is_small_for_a_clear_difference() {
        // 95% vs 60% accuracy, 200 compared on each side: not a subtle
        // difference, should come back very significant.
        let p = two_proportion_p_value(0.95, 200, 0.60, 200);
        assert!(p < 0.001, "expected a tiny p-value, got {p}");
    }

    #[test]
    fn two_proportion_p_value_is_nan_below_min_compared() {
        assert!(two_proportion_p_value(0.9, 10, 0.5, 200).is_nan());
        assert!(two_proportion_p_value(0.9, 200, 0.5, 10).is_nan());
    }

    #[test]
    fn two_proportion_p_value_is_nan_for_nan_inputs() {
        assert!(two_proportion_p_value(f64::NAN, 200, 0.5, 200).is_nan());
    }

    #[test]
    fn run_sweep_covers_the_full_cartesian_product() {
        let trades = synthetic_zigzag_trades(500);
        let results = run_sweep(&trades, &[10.0, 20.0], &[10, 20], &[10, 15], None);
        assert_eq!(results.len(), 2 * 2 * 2);
    }

    #[test]
    fn run_sweep_reuses_bvc_stats_across_vpin_windows() {
        // bvc_accuracy depends only on (bucket_volume, sigma_window),
        // not vpin_window, see bvc_accuracy_stats's doc comment. If
        // run_sweep is actually reusing that computation rather than
        // silently recomputing it differently per vpin_window, every
        // vpin_window at a fixed (bucket_volume, sigma_window) must
        // report the exact same bvc_accuracy and bvc_compared.
        let trades = synthetic_zigzag_trades(500);
        let results = run_sweep(&trades, &[10.0], &[10], &[5, 10, 20, 50], None);
        let accuracies: Vec<f64> = results
            .iter()
            .map(|r| r.stats.as_ref().unwrap().bvc_accuracy)
            .collect();
        assert!(
            accuracies.windows(2).all(|w| w[0] == w[1]),
            "expected identical bvc_accuracy across vpin_windows, got {accuracies:?}"
        );
    }

    #[test]
    fn run_sweep_gives_the_best_config_a_nan_p_value_and_others_a_real_one() {
        let trades = synthetic_zigzag_trades(500);
        let results = run_sweep(&trades, &[10.0, 20.0, 30.0], &[10, 15], &[10], None);

        let best_idx = results
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.stats.as_ref().ok().map(|s| (i, s.bvc_accuracy)))
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .map(|(i, _)| i)
            .unwrap();

        assert!(results[best_idx]
            .stats
            .as_ref()
            .unwrap()
            .bvc_accuracy_p_value
            .is_nan());

        let any_other_has_a_real_p_value = results
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != best_idx)
            .filter_map(|(_, r)| r.stats.as_ref().ok())
            .any(|s| !s.bvc_accuracy_p_value.is_nan());
        assert!(
            any_other_has_a_real_p_value,
            "expected at least one non-best config to get a real p-value"
        );
    }

    #[test]
    fn csv_escape_only_quotes_when_needed() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape("has,comma"), "\"has,comma\"");
        assert_eq!(csv_escape("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn csv_rows_have_the_same_field_count_as_the_header() {
        let trades = synthetic_zigzag_trades(200);
        let results = run_sweep(&trades, &[10.0, -5.0], &[10], &[10], None); // -5.0 forces an Err row too
        let header_fields = CSV_HEADER.split(',').count();
        for r in &results {
            let row = csv_row(r);
            assert_eq!(
                row.len(),
                header_fields,
                "row {:?} has {} fields, header has {header_fields}",
                row,
                row.len()
            );
        }
    }

    #[test]
    fn json_output_is_valid_and_has_the_expected_shape() {
        let trades = synthetic_zigzag_trades(200);
        let results = run_sweep(&trades, &[10.0, -5.0], &[10], &[10], None);
        let rows: Vec<serde_json::Value> = results.iter().map(stats_to_json).collect();
        let text = serde_json::to_string(&rows).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr[0].get("bvc_accuracy").is_some());
        // the bucket_volume=-5.0 row is the Err case, no bvc_accuracy key at all
        assert!(arr[1].get("error").is_some());
        assert!(arr[1].get("bvc_accuracy").is_none());
    }
}
