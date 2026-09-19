//! Wires config -> per-stream trade-ingest tasks -> per-stream VpinEngine
//! workers -> a WS server that fans out readings to subscribers.
//! See `config.rs` for how streams are configured (`GEIGER_STREAMS_FILE`)
//! and `lib.rs`'s `protocol` module for the wire format subscribers speak
//! (public so `toxicity-client-rs` can depend on the same types instead
//! of redefining them).

mod config;
mod error;
mod health;
mod registry;
mod server;
mod worker;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, watch, RwLock};

use config::{parse_exchange, stream_key, ServiceConfig, StreamConfig};
use feedhandler::Exchange;
use registry::StreamEntry;
use vpin_engine::VpinEngineConfig;

pub use error::ServiceError;

const BROADCAST_CAPACITY: usize = 1024;

/// Resolves once on SIGTERM or Ctrl-C (SIGINT). Without this, the OS's
/// default handling for either signal just kills the process
/// immediately, no log line, no chance for `server::run` to stop
/// accepting new connections cleanly.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl-C"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }
}

/// Validates one stream's exchange name and VPIN parameters. Shared
/// between the startup path (where an invalid entry is fatal, see
/// `main`) and the hot-reload poller (where it isn't, see
/// `config_reload_loop`) so the two can't drift on what "valid" means.
fn resolve_stream(s: &StreamConfig) -> Result<(Exchange, VpinEngineConfig), String> {
    let exchange =
        parse_exchange(&s.exchange).ok_or_else(|| format!("unknown exchange: {:?}", s.exchange))?;
    let engine_cfg = VpinEngineConfig {
        bucket_volume: s.bucket_volume,
        sigma_window: s.sigma_window,
        vpin_window: s.vpin_window,
        cdf_window: s.cdf_window,
        confidence_interval: None,
    };
    // Constructing (and dropping) one is the validation, `VpinEngine::new`
    // is the single source of truth for what "valid" means here, this
    // doesn't duplicate that logic.
    vpin_engine::VpinEngine::new(engine_cfg).map_err(|e| format!("invalid VPIN config: {e}"))?;
    Ok((exchange, engine_cfg))
}

/// Registers `stream_cfg` in `registry` and spawns its trade-ingest and
/// worker tasks. The only place either kind of task gets spawned, used
/// both for the initial batch at startup and by `config_reload_loop` for
/// streams that show up in `GEIGER_STREAMS_FILE` later, so the two can't
/// drift on how a stream gets wired up.
async fn spawn_stream(
    exchange: Exchange,
    stream_cfg: StreamConfig,
    engine_cfg: VpinEngineConfig,
    heartbeat_interval: Duration,
    registry: &registry::Registry,
) {
    let key = stream_key(&stream_cfg.exchange, &stream_cfg.symbol);
    let (out_tx, _out_rx) = broadcast::channel(BROADCAST_CAPACITY);
    let entry = Arc::new(StreamEntry::new(
        stream_cfg.exchange.clone(),
        stream_cfg.symbol.clone(),
        out_tx,
    ));
    registry.write().await.insert(key, entry.clone());

    let (trade_tx, trade_rx) = mpsc::unbounded_channel();
    let symbol = stream_cfg.symbol.clone();
    let backoff_cfg = stream_cfg.backoff_config();

    match exchange {
        Exchange::Binance => {
            tokio::spawn(trade_ingest::binance::run(
                symbol.clone(),
                trade_tx,
                backoff_cfg,
            ));
        }
        Exchange::Bybit => {
            tokio::spawn(trade_ingest::bybit::run(
                symbol.clone(),
                trade_tx,
                backoff_cfg,
            ));
        }
        Exchange::Hyperliquid => {
            tokio::spawn(trade_ingest::hyperliquid::run(
                symbol.clone(),
                trade_tx,
                backoff_cfg,
            ));
        }
    }

    tokio::spawn(worker::run(
        stream_cfg.exchange.clone(),
        symbol,
        engine_cfg,
        trade_rx,
        entry,
        heartbeat_interval,
    ));

    tracing::info!(exchange = %stream_cfg.exchange, symbol = %stream_cfg.symbol, "stream started");
}

/// Re-reads `streams_file` every `interval` and starts any stream in it
/// that isn't already in `registry`. Deliberately never removes a
/// stream whose entry disappears from the file: there's no clean way to
/// tell an already-connected subscriber "this stream is gone" short of
/// dropping its `broadcast::Sender` and having every subscriber's
/// `recv()` surface a `Closed` error, and doing that automatically on
/// every reload cycle over what might just be an operator's typo would
/// be worse than a stale stream lingering. Restart the process if you
/// need to actually stop tracking a symbol.
async fn config_reload_loop(
    streams_file: String,
    interval: Duration,
    heartbeat_interval: Duration,
    registry: registry::Registry,
) {
    tracing::info!(
        streams_file,
        poll_secs = interval.as_secs(),
        "config hot-reload enabled"
    );

    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // first tick fires immediately; the initial
                         // batch in `main` already covers the file's
                         // contents as of startup, skip re-reading it
                         // a second time right away

    loop {
        ticker.tick().await;

        let streams = match config::load_streams(&streams_file) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(streams_file, error = %e, "config hot-reload: failed to read streams file, keeping current streams");
                continue;
            }
        };

        for s in &streams {
            let key = stream_key(&s.exchange, &s.symbol);
            if registry.read().await.contains_key(&key) {
                continue;
            }
            match resolve_stream(s) {
                Ok((exchange, engine_cfg)) => {
                    tracing::info!(exchange = %s.exchange, symbol = %s.symbol, "config hot-reload: starting new stream");
                    spawn_stream(
                        exchange,
                        s.clone(),
                        engine_cfg,
                        heartbeat_interval,
                        &registry,
                    )
                    .await;
                }
                Err(e) => {
                    tracing::error!(exchange = %s.exchange, symbol = %s.symbol, error = %e, "config hot-reload: skipping invalid stream");
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // rustls 0.23+ builds no TLS config until a crypto provider is
    // installed process-wide, and won't auto-pick one unless exactly one
    // of ring/aws-lc-rs is in the dependency closure unambiguously.
    // Skipping this panics on the first wss:// connect attempt, not at
    // startup, this has bitten multiple real, unrelated projects the
    // same way (snapview/tokio-tungstenite#336, #339, #353). Must run
    // before any exchange adapter tries to connect.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let cfg = ServiceConfig::from_env();

    // Validate every stream's exchange name and VpinEngineConfig up
    // front, before spawning anything. An operator typo in one stream
    // shouldn't take down streams that were configured correctly, but it
    // also shouldn't silently run with 5 of 6 streams and no indication
    // the 6th never started, fail the whole process at boot instead.
    // Hot-reload (`config_reload_loop`) uses the same `resolve_stream`
    // but logs-and-skips instead, that's the deliberate difference
    // between a startup error and a mid-session one.
    let mut resolved = Vec::with_capacity(cfg.streams.len());
    for s in &cfg.streams {
        match resolve_stream(s) {
            Ok((exchange, engine_cfg)) => resolved.push((exchange, s.clone(), engine_cfg)),
            Err(e) => panic!("invalid stream {}:{}: {e}", s.exchange, s.symbol),
        }
    }

    let registry: registry::Registry =
        Arc::new(RwLock::new(HashMap::with_capacity(resolved.len())));
    for (exchange, stream_cfg, engine_cfg) in resolved {
        spawn_stream(
            exchange,
            stream_cfg,
            engine_cfg,
            cfg.heartbeat_interval,
            &registry,
        )
        .await;
    }

    let health_addr = cfg.health_bind_addr.clone();
    let health_registry = registry.clone();
    tokio::spawn(async move {
        health::run(&health_addr, health_registry).await;
    });

    if let Some(interval) = cfg.config_poll_interval {
        let poll_registry = registry.clone();
        let streams_file = cfg.streams_file.clone();
        let heartbeat_interval = cfg.heartbeat_interval;
        tokio::spawn(config_reload_loop(
            streams_file,
            interval,
            heartbeat_interval,
            poll_registry,
        ));
    } else {
        tracing::info!("GEIGER_CONFIG_POLL_SECS not set, config hot-reload disabled");
    }

    let auth_token: server::AuthToken = cfg.auth_token.map(Arc::from);
    if auth_token.is_none() {
        tracing::warn!("GEIGER_AUTH_TOKEN not set, the WS endpoint has no auth, fine for localhost/VPN-only, not fine otherwise");
    }
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::select! {
        result = server::run(&cfg.bind_addr, registry, auth_token, shutdown_rx) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "server exited");
                std::process::exit(1);
            }
        }
        () = shutdown_signal() => {
            tracing::info!("shutting down");
            let _ = shutdown_tx.send(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_stream() -> StreamConfig {
        StreamConfig {
            exchange: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
            bucket_volume: 50.0,
            sigma_window: 50,
            vpin_window: 50,
            cdf_window: None,
            backoff_base_ms: None,
            backoff_max_secs: None,
        }
    }

    #[test]
    fn resolve_stream_accepts_a_valid_config() {
        let (exchange, engine_cfg) = resolve_stream(&valid_stream()).unwrap();
        assert_eq!(exchange, Exchange::Binance);
        assert_eq!(engine_cfg.bucket_volume, 50.0);
    }

    #[test]
    fn resolve_stream_rejects_unknown_exchange() {
        let mut s = valid_stream();
        s.exchange = "okx".to_string();
        let err = resolve_stream(&s).unwrap_err();
        assert!(err.contains("unknown exchange"));
    }

    #[test]
    fn resolve_stream_rejects_invalid_vpin_config() {
        // Same validation `VpinEngine::new` itself enforces, this is
        // just checking `resolve_stream` actually surfaces it as an
        // `Err` instead of, say, only catching it via a later panic
        // inside `worker::run`.
        let mut s = valid_stream();
        s.bucket_volume = -1.0;
        let err = resolve_stream(&s).unwrap_err();
        assert!(err.contains("invalid VPIN config"));
    }
}
