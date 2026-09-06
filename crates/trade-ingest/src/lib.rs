//! Normalized perpetual-futures trade feeds for Binance, Bybit, and
//! Hyperliquid, feeding `vpin-engine`.
//!
//! Every adapter here runs forever, reconnecting with backoff, and is
//! meant to be spawned as its own task per (symbol, venue):
//!
//! ```no_run
//! # use tokio::sync::mpsc;
//! # #[tokio::main] async fn main() {
//! let (tx, mut rx) = mpsc::unbounded_channel();
//! tokio::spawn(trade_ingest::binance::run("BTCUSDT".to_string(), tx));
//! while let Some(trade) = rx.recv().await {
//!     // feed trade.price / trade.qty / trade.ts_exchange_ns into
//!     // vpin_engine::VpinEngine::push_trade
//! }
//! # }
//! ```
//!
//! None of these adapters have been run against a live exchange
//! connection. Wire parsing is unit-tested against literal examples
//! pulled from each venue's own docs (see each module's tests), the
//! connect/reconnect/ping loops are not live-tested. Exercise those
//! against testnet or a throwaway symbol before trusting this with real
//! capital.

pub mod binance;
pub mod bybit;
pub mod capture;
mod error;
pub mod hyperliquid;
mod types;

pub use error::IngestError;
pub use types::{parse_fixed8, parse_price, parse_qty, NormalizedTrade, TakerSide};
