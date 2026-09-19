//! Standalone live capture: connects to one exchange/symbol stream and
//! writes every trade to a `trade_ingest::capture` file (see that
//! module for the on-disk format), so building a tape for
//! `tools/calibrate` — or anything else that reads one back with
//! `TradeReader` — doesn't mean writing throwaway code that wires up an
//! adapter and a `TradeRecorder` by hand each time.
//!
//! Usage:
//!   capture <binance|bybit|hyperliquid> <symbol> <output-file> [--flush-every N]
//!
//! Runs until Ctrl-C (SIGINT), flushing on exit. Also flushes every
//! `--flush-every` trades (default 100) and every 5 seconds regardless
//! of trade count, so a quiet symbol doesn't leave a long stretch of
//! captured trades sitting unflushed in memory if the process gets
//! killed rather than Ctrl-C'd.
//!
//! `TradeRecorder::create` appends to an existing file (validating its
//! format header first, see `capture.rs`), so re-running this against
//! the same output file resumes the same tape rather than overwriting
//! it — handy for a capture session split across multiple runs, but
//! worth knowing if you expected a fresh file each time.

use std::process::ExitCode;
use std::time::Duration;

use tokio::sync::mpsc;

use backoff::BackoffConfig;
use feedhandler::Exchange;
use trade_ingest::capture::TradeRecorder;

const USAGE: &str =
    "usage: capture <binance|bybit|hyperliquid> <symbol> <output-file> [--flush-every N]";

/// How often to flush regardless of trade count, so a quiet symbol
/// doesn't leave captured trades unflushed indefinitely.
const PERIODIC_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

struct Args {
    exchange: Exchange,
    symbol: String,
    output: String,
    flush_every: u64,
}

fn parse_exchange(s: &str) -> Option<Exchange> {
    match s.to_ascii_lowercase().as_str() {
        "binance" => Some(Exchange::Binance),
        "bybit" => Some(Exchange::Bybit),
        "hyperliquid" => Some(Exchange::Hyperliquid),
        _ => None,
    }
}

fn parse_args<I: Iterator<Item = String>>(mut args: I) -> Result<Args, String> {
    let exchange_raw = args.next().ok_or(USAGE)?;
    let symbol = args.next().ok_or(USAGE)?;
    let output = args.next().ok_or(USAGE)?;
    let exchange = parse_exchange(&exchange_raw)
        .ok_or_else(|| format!("unknown exchange: {exchange_raw:?}\n{USAGE}"))?;

    let mut flush_every = 100u64;
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--flush-every" => {
                flush_every = value
                    .parse()
                    .map_err(|e| format!("bad --flush-every {value:?}: {e}"))?;
            }
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
    }

    Ok(Args {
        exchange,
        symbol,
        output,
        flush_every,
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = match parse_args(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let mut recorder = match TradeRecorder::create(&args.output) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: opening {}: {e}", args.output);
            return ExitCode::FAILURE;
        }
    };

    let (trade_tx, mut trade_rx) = mpsc::unbounded_channel();
    let symbol = args.symbol.clone();

    match args.exchange {
        Exchange::Binance => {
            tokio::spawn(trade_ingest::binance::run(
                symbol,
                trade_tx,
                BackoffConfig::default(),
            ));
        }
        Exchange::Bybit => {
            tokio::spawn(trade_ingest::bybit::run(
                symbol,
                trade_tx,
                BackoffConfig::default(),
            ));
        }
        Exchange::Hyperliquid => {
            tokio::spawn(trade_ingest::hyperliquid::run(
                symbol,
                trade_tx,
                BackoffConfig::default(),
            ));
        }
    }

    tracing::info!(
        exchange = ?args.exchange,
        symbol = %args.symbol,
        output = %args.output,
        "capture started, Ctrl-C to stop"
    );

    let mut recorded: u64 = 0;
    let mut since_flush: u64 = 0;
    let mut periodic_flush = tokio::time::interval(PERIODIC_FLUSH_INTERVAL);
    periodic_flush.tick().await; // first tick fires immediately, skip it

    loop {
        tokio::select! {
            trade = trade_rx.recv() => {
                let Some(trade) = trade else {
                    // The adapters reconnect forever on their own; this
                    // channel only closes if the sender task itself
                    // panicked, not a transient disconnect. Whatever's
                    // been flushed so far is still a valid, readable
                    // capture file, only what's unflushed since is lost.
                    eprintln!("error: trade-ingest task ended unexpectedly, stopping");
                    let _ = recorder.flush();
                    return ExitCode::FAILURE;
                };
                if let Err(e) = recorder.record(&trade) {
                    eprintln!("error: writing to {}: {e}", args.output);
                    return ExitCode::FAILURE;
                }
                recorded += 1;
                since_flush += 1;
                if since_flush >= args.flush_every {
                    if let Err(e) = recorder.flush() {
                        eprintln!("error: flushing {}: {e}", args.output);
                        return ExitCode::FAILURE;
                    }
                    since_flush = 0;
                }
            }
            _ = periodic_flush.tick() => {
                if since_flush > 0 {
                    if let Err(e) = recorder.flush() {
                        eprintln!("error: flushing {}: {e}", args.output);
                        return ExitCode::FAILURE;
                    }
                    since_flush = 0;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!(recorded, "Ctrl-C received, flushing and exiting");
                break;
            }
        }
    }

    if let Err(e) = recorder.flush() {
        eprintln!("error: final flush of {}: {e}", args.output);
        return ExitCode::FAILURE;
    }

    eprintln!("wrote {recorded} trades to {}", args.output);
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_args_with_default_flush_every() {
        let raw = ["binance", "BTCUSDT", "out.cap"].map(String::from);
        let args = parse_args(raw.into_iter()).unwrap();
        assert_eq!(args.exchange, Exchange::Binance);
        assert_eq!(args.symbol, "BTCUSDT");
        assert_eq!(args.output, "out.cap");
        assert_eq!(args.flush_every, 100);
    }

    #[test]
    fn parses_flush_every_override() {
        let raw = ["bybit", "ETHUSDT", "out.cap", "--flush-every", "10"].map(String::from);
        let args = parse_args(raw.into_iter()).unwrap();
        assert_eq!(args.exchange, Exchange::Bybit);
        assert_eq!(args.flush_every, 10);
    }

    #[test]
    fn exchange_names_are_case_insensitive() {
        let raw = ["HyperLiquid", "BTC", "out.cap"].map(String::from);
        let args = parse_args(raw.into_iter()).unwrap();
        assert_eq!(args.exchange, Exchange::Hyperliquid);
    }

    #[test]
    fn rejects_unknown_exchange() {
        let raw = ["okx", "BTCUSDT", "out.cap"].map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn rejects_missing_required_args() {
        assert!(parse_args(std::iter::empty()).is_err());
        assert!(parse_args(["binance".to_string()].into_iter()).is_err());
    }

    #[test]
    fn rejects_unknown_flag() {
        let raw = ["binance", "BTCUSDT", "out.cap", "--bogus", "1"].map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn rejects_flag_missing_its_value() {
        let raw = ["binance", "BTCUSDT", "out.cap", "--flush-every"].map(String::from);
        assert!(parse_args(raw.into_iter()).is_err());
    }
}
