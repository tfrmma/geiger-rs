//! Wires config -> per-stream trade-ingest tasks -> per-stream VpinEngine
//! workers -> a WS server that fans out readings to subscribers.
//! See `config.rs` for how streams are configured (`GEIGER_STREAMS_FILE`)
//! and `lib.rs`'s `protocol` module for the wire format subscribers speak
//! (public so `toxicity-client-rs` can depend on the same types instead
//! of redefining them).

mod config;
mod error;
mod server;
mod worker;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};

use config::{parse_exchange, stream_key, ServiceConfig};
use feedhandler::Exchange;
use vpin_engine::VpinEngineConfig;

pub use error::ServiceError;

const BROADCAST_CAPACITY: usize = 1024;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cfg = ServiceConfig::from_env();

    // Validate every stream's exchange name and VpinEngineConfig up
    // front, before spawning anything. An operator typo in one stream
    // shouldn't take down streams that were configured correctly, but it
    // also shouldn't silently run with 5 of 6 streams and no indication
    // the 6th never started, fail the whole process at boot instead.
    let mut resolved = Vec::with_capacity(cfg.streams.len());
    for s in &cfg.streams {
        let exchange = parse_exchange(&s.exchange)
            .unwrap_or_else(|| panic!("unknown exchange in GEIGER_STREAMS_FILE: {:?}", s.exchange));
        let engine_cfg = VpinEngineConfig {
            bucket_volume: s.bucket_volume,
            sigma_window: s.sigma_window,
            vpin_window: s.vpin_window,
            cdf_window: s.cdf_window,
        };
        // Constructing (and dropping) one up front is the validation,
        // `VpinEngine::new` is the single source of truth for what
        // "valid" means here, this doesn't duplicate that logic.
        if let Err(e) = vpin_engine::VpinEngine::new(engine_cfg) {
            panic!("invalid VPIN config for {}:{}: {e}", s.exchange, s.symbol);
        }
        resolved.push((exchange, s.clone(), engine_cfg));
    }

    let mut registry = HashMap::with_capacity(resolved.len());

    for (exchange, stream_cfg, engine_cfg) in resolved {
        let key = stream_key(&stream_cfg.exchange, &stream_cfg.symbol);
        let (out_tx, _out_rx) = broadcast::channel(BROADCAST_CAPACITY);
        registry.insert(key, out_tx.clone());

        let (trade_tx, trade_rx) = mpsc::unbounded_channel();
        let symbol = stream_cfg.symbol.clone();

        match exchange {
            Exchange::Binance => {
                tokio::spawn(trade_ingest::binance::run(symbol.clone(), trade_tx));
            }
            Exchange::Bybit => {
                tokio::spawn(trade_ingest::bybit::run(symbol.clone(), trade_tx));
            }
            Exchange::Hyperliquid => {
                tokio::spawn(trade_ingest::hyperliquid::run(symbol.clone(), trade_tx));
            }
        }

        tokio::spawn(worker::run(
            stream_cfg.exchange.clone(),
            symbol,
            engine_cfg,
            trade_rx,
            out_tx,
            cfg.heartbeat_interval,
        ));

        tracing::info!(exchange = %stream_cfg.exchange, symbol = %stream_cfg.symbol, "stream started");
    }

    let registry: server::Registry = Arc::new(registry);
    if let Err(e) = server::run(&cfg.bind_addr, registry).await {
        tracing::error!(error = %e, "server exited");
        std::process::exit(1);
    }
}
