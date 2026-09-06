//! Same convention as `boros-mm`'s `arb-bot`/`mm-bot`: simple settings
//! from env vars, structured per-stream config from a JSON file. No
//! `toml` dependency, `serde_json` already does the job and is already a
//! dependency everywhere else in this workspace.

use std::time::Duration;

use serde::Deserialize;

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required env var: {name}"))
}

fn optional_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn optional_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// One (exchange, symbol) pair to track, and the VPIN parameters for it.
/// Bucket volume in particular is instrument-specific (BTC's liquid
/// volume profile and a mid-cap alt's aren't remotely comparable), no
/// sane global default, this is deliberately not optional.
#[derive(Debug, Clone, Deserialize)]
pub struct StreamConfig {
    pub exchange: String,
    pub symbol: String,
    pub bucket_volume: f64,
    pub sigma_window: usize,
    pub vpin_window: usize,
    pub cdf_window: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct StreamsFile {
    streams: Vec<StreamConfig>,
}

pub struct ServiceConfig {
    pub bind_addr: String,
    pub streams: Vec<StreamConfig>,
    pub heartbeat_interval: Duration,
}

impl ServiceConfig {
    pub fn from_env() -> Self {
        let streams_path = required("GEIGER_STREAMS_FILE");
        let raw = std::fs::read_to_string(&streams_path)
            .unwrap_or_else(|e| panic!("failed to read GEIGER_STREAMS_FILE ({streams_path}): {e}"));
        let file: StreamsFile = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("failed to parse GEIGER_STREAMS_FILE ({streams_path}): {e}"));

        if file.streams.is_empty() {
            panic!("GEIGER_STREAMS_FILE ({streams_path}) has zero streams configured, nothing to run");
        }

        ServiceConfig {
            bind_addr: optional_string("GEIGER_BIND_ADDR", "0.0.0.0:9700"),
            streams: file.streams,
            heartbeat_interval: Duration::from_secs(optional_u64("GEIGER_HEARTBEAT_SECS", 5)),
        }
    }
}

/// Case-insensitive, matches the casing conventions each venue happens to
/// use in its own docs ("Binance", "bybit", whatever an operator types).
pub fn parse_exchange(s: &str) -> Option<feedhandler::Exchange> {
    match s.to_ascii_lowercase().as_str() {
        "binance" => Some(feedhandler::Exchange::Binance),
        "bybit" => Some(feedhandler::Exchange::Bybit),
        "hyperliquid" => Some(feedhandler::Exchange::Hyperliquid),
        _ => None,
    }
}

/// Registry key for a (exchange, symbol) pair. Plain string rather than a
/// `(Exchange, Symbol)` tuple, this is only ever used as a `HashMap` key
/// for wiring up subscriptions, not on any hot path, a string compare
/// isn't worth avoiding here.
pub fn stream_key(exchange: &str, symbol: &str) -> String {
    format!("{}:{}", exchange.to_ascii_lowercase(), symbol.to_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_exchanges_case_insensitively() {
        assert_eq!(parse_exchange("Binance"), Some(feedhandler::Exchange::Binance));
        assert_eq!(parse_exchange("BYBIT"), Some(feedhandler::Exchange::Bybit));
        assert_eq!(parse_exchange("hyperliquid"), Some(feedhandler::Exchange::Hyperliquid));
    }

    #[test]
    fn rejects_unknown_exchange() {
        assert_eq!(parse_exchange("okx"), None);
    }

    #[test]
    fn stream_key_normalizes_casing() {
        assert_eq!(stream_key("Binance", "btcusdt"), "binance:BTCUSDT");
        assert_eq!(stream_key("BYBIT", "BTCUSDT"), "bybit:BTCUSDT");
    }

    #[test]
    fn parses_streams_file_json() {
        let json = r#"{
            "streams": [
                {"exchange":"binance","symbol":"BTCUSDT","bucket_volume":50.0,"sigma_window":50,"vpin_window":50,"cdf_window":100}
            ]
        }"#;
        let file: StreamsFile = serde_json::from_str(json).unwrap();
        assert_eq!(file.streams.len(), 1);
        assert_eq!(file.streams[0].exchange, "binance");
        assert_eq!(file.streams[0].cdf_window, Some(100));
    }

    #[test]
    fn omitted_cdf_window_defaults_to_none() {
        // serde defaults an absent Option<T> field to None automatically,
        // no #[serde(default)] needed. Worth knowing: this means an
        // operator who forgets cdf_window in their streams file silently
        // gets "CDF transform disabled" instead of a config error, a
        // real footgun this field's type doesn't rule out on its own.
        let json = r#"{"streams":[{"exchange":"bybit","symbol":"ETHUSDT","bucket_volume":10.0,"sigma_window":20,"vpin_window":20}]}"#;
        let file: StreamsFile = serde_json::from_str(json).unwrap();
        assert_eq!(file.streams[0].cdf_window, None);
    }
}
