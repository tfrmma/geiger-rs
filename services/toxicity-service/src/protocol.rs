//! Wire protocol between `toxicity-service` and its subscribers
//! (`boros-mm`'s `toxicity-client-rs`, `gamma-scalper` via `py-vpin`,
//! whatever else subscribes later). Plain JSON over a WebSocket, one
//! connection per (exchange, symbol) subscription: connect, send a
//! `SubscribeRequest`, then receive a stream of `ServerMessage`.
//!
//! `Reading.vpin`/`vpin_cdf` are the raw continuous score and its CDF
//! transform, not a discretized tier: each consumer sets its own
//! thresholds rather than this service imposing one. `trades_in_bucket`
//! is included specifically so a consumer can control for trading
//! intensity if it wants to (see Andersen & Bondarenko's critique, noted
//! in `vpin-engine`'s crate docs), that's a downstream policy choice
//! this service doesn't make for you.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    pub exchange: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "reading")]
    Reading {
        exchange: String,
        symbol: String,
        bucket_id: u64,
        ts_close_ns: u64,
        trades_in_bucket: u32,
        /// `None` during warmup, see `vpin_engine::VpinReading`.
        vpin: Option<f64>,
        vpin_cdf: Option<f64>,
    },
    /// Sent on a fixed interval regardless of trade flow, independent of
    /// `Reading` messages. This is what a consumer's own staleness check
    /// should key off, not the absence of `Reading`s, a quiet market
    /// with genuinely low toxicity looks identical to a dead feed if all
    /// you're watching for is "did a Reading arrive recently."
    #[serde(rename = "heartbeat")]
    Heartbeat { ts_ns: u64 },
    #[serde(rename = "error")]
    Error { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_request_decodes() {
        let json = r#"{"exchange":"binance","symbol":"BTCUSDT"}"#;
        let req: SubscribeRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.exchange, "binance");
        assert_eq!(req.symbol, "BTCUSDT");
    }

    #[test]
    fn reading_serializes_with_tagged_type() {
        let msg = ServerMessage::Reading {
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
            bucket_id: 42,
            ts_close_ns: 1_700_000_000_000_000_000,
            trades_in_bucket: 17,
            vpin: Some(0.32),
            vpin_cdf: Some(0.81),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["type"], "reading");
        assert_eq!(value["bucket_id"], 42);
        assert_eq!(value["vpin"], 0.32);
    }

    #[test]
    fn warmup_reading_has_null_vpin_not_a_missing_field() {
        let msg = ServerMessage::Reading {
            exchange: "bybit".into(),
            symbol: "ETHUSDT".into(),
            bucket_id: 0,
            ts_close_ns: 0,
            trades_in_bucket: 3,
            vpin: None,
            vpin_cdf: None,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        // a consumer distinguishing "warmup" from "field absent due to a
        // bug" needs this to be explicit null, not an absent key
        assert!(value.get("vpin").is_some());
        assert!(value["vpin"].is_null());
    }

    #[test]
    fn heartbeat_serializes_with_tagged_type() {
        let msg = ServerMessage::Heartbeat { ts_ns: 123 };
        let json = serde_json::to_string(&msg).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["type"], "heartbeat");
        assert_eq!(value["ts_ns"], 123);
    }
}
