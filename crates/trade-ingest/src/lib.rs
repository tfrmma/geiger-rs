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
//! tokio::spawn(trade_ingest::binance::run(
//!     "BTCUSDT".to_string(), tx, backoff::BackoffConfig::default(),
//! ));
//! while let Some(trade) = rx.recv().await {
//!     // feed trade.price / trade.qty / trade.ts_exchange_ns into
//!     // vpin_engine::VpinEngine::push_trade
//! }
//! # }
//! ```
//!

pub mod binance;
pub mod bybit;
pub mod capture;
mod error;
pub mod hyperliquid;
mod types;

pub use error::IngestError;
pub use types::{parse_fixed8, parse_price, parse_qty, NormalizedTrade, TakerSide};
