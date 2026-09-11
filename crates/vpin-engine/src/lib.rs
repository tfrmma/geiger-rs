//! Streaming VPIN (Volume-Synchronized Probability of Informed Trading)
//! on a volume clock, via Bulk Volume Classification.
//!
//! Easley, Lopez de Prado & O'Hara, "Flow Toxicity and Liquidity in a
//! High-Frequency World", Review of Financial Studies 25(5), 2012.
//!
//! No I/O, no exchange-specific anything, this crate takes
//! (price, volume, timestamp) trade tuples and produces VPIN readings.
//! Wiring it up to a real trade feed lives in `trade-ingest` /
//! `toxicity-service`.
//!
//! Known limitation worth having in your head before trusting this in
//! production: Andersen & Bondarenko (2014, Journal of Financial Markets)
//! found VPIN's predictive power for short-term volatility is largely a
//! mechanical byproduct of trading intensity, and that it peaked AFTER,
//! not before, the 2010 Flash Crash. `VpinReading::trades_in_bucket` is
//! exposed specifically so a consumer can control for trading intensity
//! instead of reacting to raw VPIN. `IntensityAdjustedCdf` is a
//! reference implementation of one way to do that (rank a reading
//! against similarly-busy buckets rather than all buckets), not the only
//! correct one, real policy decisions here belong downstream.

mod bucket;
mod bvc;
mod error;
mod intensity;
mod vpin;

pub use bucket::{ClosedBucket, VolumeBucketer};
pub use bvc::{buy_fraction, classify};
pub use error::VpinError;
pub use intensity::IntensityAdjustedCdf;
pub use vpin::{EmpiricalCdf, RollingSigma, VpinEngine, VpinEngineConfig, VpinReading};
