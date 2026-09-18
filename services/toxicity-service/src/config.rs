//! Same convention as `boros-mm`'s `arb-bot`/`mm-bot`: simple settings
//! from env vars, structured per-stream config from a JSON file. No
//! `toml` dependency, `serde_json` already does the job and is already a
//! dependency everywhere else in this workspace.

use std::time::Duration;

use backoff::BackoffConfig;
use serde::Deserialize;

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required env var: {name}"))
}

fn optional_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn optional_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
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
    /// Reconnect backoff override for this stream. `None` for both
    /// fields (the common case, and what every entry in
    /// `streams.example.json` uses) falls back to `BackoffConfig`'s
    /// default, see that type's doc comment for why it's a reasonable
    /// starting point for all three venues. Override per-stream if
    /// you're running many symbols on the same exchange from one IP and
    /// want to spread out reconnect attempts, or you have your own
    /// observed rate-limit behavior that suggests something more
    /// conservative.
    #[serde(default)]
    pub backoff_base_ms: Option<u64>,
    #[serde(default)]
    pub backoff_max_secs: Option<u64>,
}

impl StreamConfig {
    pub fn backoff_config(&self) -> BackoffConfig {
        let default = BackoffConfig::default();
        BackoffConfig {
            base: self
                .backoff_base_ms
                .map(Duration::from_millis)
                .unwrap_or(default.base),
            max: self
                .backoff_max_secs
                .map(Duration::from_secs)
                .unwrap_or(default.max),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct StreamsFile {
    streams: Vec<StreamConfig>,
}

pub struct ServiceConfig {
    pub bind_addr: String,
    /// Separate port from `bind_addr`: `/health` and `/metrics` are
    /// plain HTTP, not WebSocket, and keeping them off the WS port means
    /// a monitoring scrape can't be mistaken for (or interfere with) a
    /// subscriber handshake.
    pub health_bind_addr: String,
    /// Kept (not just consumed into `streams`) so `main.rs` can re-read
    /// the same file later if config hot-reload is enabled.
    pub streams_file: String,
    pub streams: Vec<StreamConfig>,
    pub heartbeat_interval: Duration,
    /// `None` if `GEIGER_AUTH_TOKEN` isn't set, meaning no auth is
    /// enforced on the WS endpoint. Fine for localhost/VPN-only
    /// deployments, not fine for anything with a public-reachable bind
    /// address.
    pub auth_token: Option<String>,
    /// `None` if `GEIGER_CONFIG_POLL_SECS` is unset or `0`: hot-reload
    /// disabled, `streams_file` is only ever read once, at startup.
    /// `Some(interval)` otherwise: `main.rs` re-reads it on this
    /// interval and starts any new stream it finds. See
    /// `main::config_reload_loop` for why streams that drop out of the
    /// file are deliberately never removed.
    pub config_poll_interval: Option<Duration>,
}

/// Reads and parses a streams file. Returns `Result` rather than
/// panicking like the rest of this module's `from_env` path: this is
/// also called by `main::config_reload_loop` on a running process,
/// which needs to log a bad file and keep going, not take the whole
/// service down over an operator's typo mid-session — the one thing
/// that's actually fine, even correct, to do at startup instead.
///
/// # Errors
/// The file can't be read, isn't valid JSON for this schema, or has
/// zero streams in it.
pub fn load_streams(path: &str) -> Result<Vec<StreamConfig>, String> {
    let raw = std::fs::read_to_string(path).map_err(|e| format!("failed to read {path}: {e}"))?;
    let file: StreamsFile =
        serde_json::from_str(&raw).map_err(|e| format!("failed to parse {path}: {e}"))?;
    if file.streams.is_empty() {
        return Err(format!(
            "{path} has zero streams configured, nothing to run"
        ));
    }
    Ok(file.streams)
}

impl ServiceConfig {
    pub fn from_env() -> Self {
        let streams_path = required("GEIGER_STREAMS_FILE");
        let streams = load_streams(&streams_path).unwrap_or_else(|e| panic!("{e}"));

        let poll_secs = optional_u64("GEIGER_CONFIG_POLL_SECS", 0);

        ServiceConfig {
            bind_addr: optional_string("GEIGER_BIND_ADDR", "0.0.0.0:9700"),
            health_bind_addr: optional_string("GEIGER_HEALTH_ADDR", "0.0.0.0:9701"),
            streams_file: streams_path,
            streams,
            heartbeat_interval: Duration::from_secs(optional_u64("GEIGER_HEARTBEAT_SECS", 5)),
            auth_token: std::env::var("GEIGER_AUTH_TOKEN")
                .ok()
                .filter(|s| !s.is_empty()),
            config_poll_interval: (poll_secs > 0).then(|| Duration::from_secs(poll_secs)),
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
    format!(
        "{}:{}",
        exchange.to_ascii_lowercase(),
        symbol.to_ascii_uppercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_exchanges_case_insensitively() {
        assert_eq!(
            parse_exchange("Binance"),
            Some(feedhandler::Exchange::Binance)
        );
        assert_eq!(parse_exchange("BYBIT"), Some(feedhandler::Exchange::Bybit));
        assert_eq!(
            parse_exchange("hyperliquid"),
            Some(feedhandler::Exchange::Hyperliquid)
        );
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

    #[test]
    fn backoff_config_falls_back_to_default_when_unset() {
        let json = r#"{"exchange":"binance","symbol":"BTCUSDT","bucket_volume":50.0,"sigma_window":50,"vpin_window":50}"#;
        let cfg: StreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.backoff_config(), BackoffConfig::default());
    }

    #[test]
    fn backoff_config_uses_per_stream_override_when_set() {
        let json = r#"{"exchange":"binance","symbol":"BTCUSDT","bucket_volume":50.0,"sigma_window":50,"vpin_window":50,"backoff_base_ms":500,"backoff_max_secs":60}"#;
        let cfg: StreamConfig = serde_json::from_str(json).unwrap();
        let resolved = cfg.backoff_config();
        assert_eq!(resolved.base, Duration::from_millis(500));
        assert_eq!(resolved.max, Duration::from_secs(60));
    }

    #[test]
    fn backoff_config_allows_overriding_only_one_field() {
        let json = r#"{"exchange":"binance","symbol":"BTCUSDT","bucket_volume":50.0,"sigma_window":50,"vpin_window":50,"backoff_base_ms":500}"#;
        let cfg: StreamConfig = serde_json::from_str(json).unwrap();
        let resolved = cfg.backoff_config();
        assert_eq!(resolved.base, Duration::from_millis(500));
        assert_eq!(resolved.max, BackoffConfig::default().max); // untouched field keeps the default
    }

    fn temp_streams_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("ts-config-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("streams.json")
    }

    #[test]
    fn load_streams_reads_a_valid_file() {
        let path = temp_streams_path("valid");
        std::fs::write(
            &path,
            r#"{"streams":[{"exchange":"binance","symbol":"BTCUSDT","bucket_volume":50.0,"sigma_window":50,"vpin_window":50}]}"#,
        )
        .unwrap();

        let streams = load_streams(path.to_str().unwrap()).unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].symbol, "BTCUSDT");
    }

    #[test]
    fn load_streams_errors_on_missing_file() {
        // config_reload_loop depends on this being an `Err`, not a
        // panic: a streams file that briefly doesn't exist mid-edit
        // (some editors write via a temp file + rename) shouldn't take
        // the whole service down.
        let err = load_streams("/nonexistent/path/does-not-exist.json").unwrap_err();
        assert!(err.contains("failed to read"));
    }

    #[test]
    fn load_streams_errors_on_invalid_json() {
        let path = temp_streams_path("bad-json");
        std::fs::write(&path, "not valid json at all").unwrap();
        let err = load_streams(path.to_str().unwrap()).unwrap_err();
        assert!(err.contains("failed to parse"));
    }

    #[test]
    fn load_streams_errors_on_zero_streams() {
        let path = temp_streams_path("empty");
        std::fs::write(&path, r#"{"streams":[]}"#).unwrap();
        let err = load_streams(path.to_str().unwrap()).unwrap_err();
        assert!(err.contains("zero streams"));
    }
}
