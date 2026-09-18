//! Shared per-stream state. `Registry` used to be a plain
//! `Arc<HashMap<String, broadcast::Sender<ServerMessage>>>`, fixed at
//! startup; the `RwLock` around the map is here so config hot-reload can
//! insert streams into a running registry later without rebuilding it.
//! Each entry also carries a health snapshot for the `/health` and
//! `/metrics` endpoints in `health.rs`, and the last message sent, for
//! catching up a client that subscribes mid-session instead of leaving
//! it waiting for the next bucket close or heartbeat.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

use toxicity_service::protocol::ServerMessage;

/// Point-in-time counters for one stream, updated by `worker::run` as
/// trades and readings flow through it. Not the same thing as
/// `ServerMessage::Reading`/`Heartbeat`: those are wire messages pushed
/// to subscribers, this is state `/health` and `/metrics` read on
/// demand, and it tracks running totals those messages don't carry.
#[derive(Debug, Clone, Default)]
pub struct StreamHealth {
    pub trades_processed: u64,
    pub readings_emitted: u64,
    pub last_trade_ts_ns: Option<u64>,
    pub last_heartbeat_ts_ns: Option<u64>,
    /// Flipped to `false` right before `worker::run` returns, i.e. its
    /// trade-ingest channel closed. A stream that never started at all
    /// just isn't in the registry, see `main.rs`, so this only ever
    /// distinguishes "running" from "started, then died".
    pub worker_alive: bool,
}

pub struct StreamEntry {
    pub exchange: String,
    pub symbol: String,
    pub sender: broadcast::Sender<ServerMessage>,
    /// Most recent message sent on `sender`. Read on subscribe (see
    /// `server.rs`) to catch a client up immediately instead of leaving
    /// it waiting for the next bucket close or heartbeat.
    pub last_message: RwLock<Option<ServerMessage>>,
    pub health: RwLock<StreamHealth>,
}

impl StreamEntry {
    pub fn new(exchange: String, symbol: String, sender: broadcast::Sender<ServerMessage>) -> Self {
        StreamEntry {
            exchange,
            symbol,
            sender,
            last_message: RwLock::new(None),
            health: RwLock::new(StreamHealth::default()),
        }
    }
}

/// Keyed the same way the old plain-map registry was, see
/// `config::stream_key`. `RwLock` wraps the map itself, not just its
/// values: every WS subscribe and every `/health`/`/metrics` poll only
/// takes a read lock, so none of those contend with each other, only a
/// future hot-reload insert briefly blocks them.
pub type Registry = Arc<RwLock<HashMap<String, Arc<StreamEntry>>>>;
