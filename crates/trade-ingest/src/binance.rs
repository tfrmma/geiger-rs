//! Binance USDS-M Futures, Aggregate Trade Stream.
//!
//! There is no per-fill trade stream for USDS-M futures, only spot has
//! that (`@trade`). The raw futures `@trade` stream exists in practice
//! but is undocumented and has been reported to deliver events out of
//! timestamp order on Binance's own developer forum, not something to
//! build a production toxicity signal on. `@aggTrade` is the documented,
//! supported one: fills from a single taker order at the same price get
//! merged into one event. That's a fine unit for BVC, it doesn't need
//! individual fills, just (price, volume, direction) per print.
//!
//! As of this writing Binance requires routed WebSocket endpoints,
//! `@aggTrade` is a Market-category stream, so this connects to
//! `/market/stream`, not the old unrouted `/stream`. The legacy unrouted
//! URLs were decommissioned 2026-04-23, connecting to the old path will
//! not silently degrade, it will just not work.
//! https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use feedhandler::{Exchange, Symbol};

use backoff::ExponentialBackoff;
use crate::types::{parse_price, parse_qty, NormalizedTrade, TakerSide};
use crate::IngestError;

const BASE_URL: &str = "wss://fstream.binance.com/market/stream";

#[derive(Deserialize)]
struct StreamEnvelope {
    #[allow(dead_code)]
    stream: String,
    data: AggTrade,
}

/// Field names match the wire format exactly, see the module doc link.
#[derive(Deserialize)]
struct AggTrade {
    #[serde(rename = "T")]
    trade_time_ms: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
    /// True if the buyer is the market maker, i.e. the seller crossed the
    /// spread and was the taker.
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

fn build_url(symbol_lower: &str) -> String {
    format!("{BASE_URL}?streams={symbol_lower}@aggTrade")
}

/// Runs forever, reconnecting with backoff on any disconnect or error.
/// Lowercases `symbol` itself since Binance stream names are
/// case-sensitive-lowercase, the caller's own casing doesn't matter.
/// Takes an owned `String` rather than `&str`: this gets moved into a
/// `tokio::spawn`'d task by every caller we have, which needs `'static`,
/// a borrow from a caller's loop-local config can't satisfy that.
pub async fn run(symbol: String, tx: mpsc::UnboundedSender<NormalizedTrade>) {
    let symbol_lower = symbol.to_ascii_lowercase();
    let normalized_symbol = Symbol::from_bytes(symbol.to_ascii_uppercase().as_bytes());
    let url = build_url(&symbol_lower);
    let mut backoff = ExponentialBackoff::default();
    let mut sequence: u64 = 0;

    loop {
        match run_once(&url, normalized_symbol, &tx, &mut sequence, &mut backoff).await {
            Ok(()) => {
                // session ended cleanly (server closed), still reconnect,
                // "clean" doesn't mean "stop caring about this feed"
                tracing::warn!(venue = "binance", symbol, "session closed, reconnecting");
            }
            Err(e) => {
                tracing::warn!(venue = "binance", symbol, error = %e, "session error, reconnecting");
            }
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

async fn run_once(
    url: &str,
    symbol: Symbol,
    tx: &mpsc::UnboundedSender<NormalizedTrade>,
    sequence: &mut u64,
    backoff: &mut ExponentialBackoff,
) -> Result<(), IngestError> {
    let (ws, _resp) = tokio_tungstenite::connect_async(url).await?;
    tracing::info!(venue = "binance", url, "connected");
    // Only reset here, not on every message, this marks "we successfully
    // established a session," not "we're still receiving data." Resetting
    // on connect_async failure would defeat the point of backing off.
    backoff.reset();
    let (mut write, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        let msg = msg?;
        match msg {
            Message::Text(text) => {
                let trade = decode(&text, symbol, sequence)?;
                // receiver gone means the whole service is shutting down,
                // not this adapter's problem to handle, just stop
                if tx.send(trade).is_err() {
                    return Ok(());
                }
            }
            // tokio-tungstenite surfaces Ping/Pong as regular messages
            // rather than auto-replying underneath us, reply explicitly
            // instead of assuming the library does it, that assumption
            // wasn't verifiable without a live connection to check
            // against.
            Message::Ping(payload) => {
                write.send(Message::Pong(payload)).await?;
            }
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }

    Ok(())
}

fn decode(text: &str, symbol: Symbol, sequence: &mut u64) -> Result<NormalizedTrade, IngestError> {
    let envelope: StreamEnvelope = serde_json::from_str(text).map_err(|e| {
        tracing::debug!(venue = "binance", raw = text, "decode failure");
        IngestError::from(e)
    })?;

    let trade = envelope.data;
    let seq = *sequence;
    *sequence += 1;

    Ok(NormalizedTrade {
        price: parse_price(&trade.price)?,
        qty: parse_qty(&trade.qty)?,
        // Binance gives millisecond resolution, padded to ns units here,
        // this is NOT genuine nanosecond precision, don't treat it as
        // more accurate than it is.
        ts_exchange_ns: trade.trade_time_ms.saturating_mul(1_000_000),
        ts_recv_ns: feedhandler::timer::now_ns(),
        symbol,
        exchange: Exchange::Binance,
        taker_side: if trade.buyer_is_maker { TakerSide::Sell } else { TakerSide::Buy },
        sequence: seq,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Straight from the official Binance aggTrade docs example, this is
    // what actually verifies the field mapping instead of trusting it by
    // inspection.
    const SAMPLE: &str = r#"{
        "stream": "bnbusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 1672515782136,
            "s": "BNBUSDT",
            "a": 12345,
            "p": "0.001",
            "q": "100",
            "f": 100,
            "l": 105,
            "T": 1672515782136,
            "m": true
        }
    }"#;

    #[test]
    fn decodes_sample_envelope() {
        let symbol = Symbol::from_bytes(b"BNBUSDT");
        let mut seq = 0u64;
        let trade = decode(SAMPLE, symbol, &mut seq).unwrap();
        assert_eq!(trade.price.raw(), 100_000); // 0.001 * 1e8
        assert_eq!(trade.qty.raw(), 100 * 100_000_000); // 100 * 1e8
        assert_eq!(trade.ts_exchange_ns, 1_672_515_782_136_000_000);
        assert_eq!(trade.taker_side, TakerSide::Sell); // m=true -> buyer is maker -> seller took
        assert_eq!(trade.exchange, Exchange::Binance);
        assert_eq!(trade.sequence, 0);
    }

    #[test]
    fn sequence_increments_across_calls() {
        let symbol = Symbol::from_bytes(b"BNBUSDT");
        let mut seq = 0u64;
        let a = decode(SAMPLE, symbol, &mut seq).unwrap();
        let b = decode(SAMPLE, symbol, &mut seq).unwrap();
        assert_eq!(a.sequence, 0);
        assert_eq!(b.sequence, 1);
    }

    #[test]
    fn buyer_is_taker_when_m_is_false() {
        let flipped = SAMPLE.replace("\"m\": true", "\"m\": false");
        let symbol = Symbol::from_bytes(b"BNBUSDT");
        let mut seq = 0u64;
        let trade = decode(&flipped, symbol, &mut seq).unwrap();
        assert_eq!(trade.taker_side, TakerSide::Buy);
    }

    #[test]
    fn url_is_lowercased_and_market_routed() {
        let url = build_url("bnbusdt");
        assert_eq!(url, "wss://fstream.binance.com/market/stream?streams=bnbusdt@aggTrade");
    }

    #[test]
    fn rejects_malformed_json() {
        let symbol = Symbol::from_bytes(b"BNBUSDT");
        let mut seq = 0u64;
        assert!(decode("not json", symbol, &mut seq).is_err());
    }
}
