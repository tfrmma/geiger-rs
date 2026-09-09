//! Bybit v5, linear category (USDT/USDC perpetuals + USDT futures),
//! `publicTrade.<symbol>` topic.
//! https://bybit-exchange.github.io/docs/v5/websocket/public/trade
//! https://bybit-exchange.github.io/docs/v5/ws/connect
//!
//! Two things this venue needs that Binance doesn't:
//!
//! - An explicit subscribe message after connecting, the URL alone
//!   doesn't pick a topic.
//! - An application-level `{"op":"ping"}` text frame at least every 20s.
//!   Bybit's docs are explicit that this is what keeps the connection
//!   alive, not a protocol-level WS ping, so replying to protocol Pings
//!   alone (which this adapter also does, same as Binance) isn't enough
//!   here, both matter.
//!
//! A single `publicTrade` push can carry up to 1024 trades in `data`,
//! per Bybit's own docs, so every entry in that array gets emitted, not
//! just the first.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use feedhandler::{Exchange, Symbol};

use backoff::{BackoffConfig, ExponentialBackoff};
use crate::types::{parse_price, parse_qty, NormalizedTrade, TakerSide};
use crate::IngestError;

const URL: &str = "wss://stream.bybit.com/v5/public/linear";
const APP_PING_INTERVAL: Duration = Duration::from_secs(15); // Bybit asks for <=20s

#[derive(Deserialize)]
struct TradeEnvelope {
    topic: String,
    data: Vec<BybitTrade>,
}

/// Field names match the wire format exactly, see the module doc links.
#[derive(Deserialize)]
struct BybitTrade {
    #[serde(rename = "T")]
    time_ms: u64,
    #[serde(rename = "S")]
    side: String, // "Buy" or "Sell", the taker's side, straight from Bybit
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "v")]
    qty: String,
}

/// See `binance::run`'s doc comment for why this takes an owned `String`.
pub async fn run(symbol: String, tx: mpsc::UnboundedSender<NormalizedTrade>, backoff_cfg: BackoffConfig) {
    let normalized_symbol = Symbol::from_bytes(symbol.as_bytes());
    let mut backoff = backoff_cfg.build();
    let mut sequence: u64 = 0;

    loop {
        match run_once(&symbol, normalized_symbol, &tx, &mut sequence, &mut backoff).await {
            Ok(()) => tracing::warn!(venue = "bybit", symbol, "session closed, reconnecting"),
            Err(e) => tracing::warn!(venue = "bybit", symbol, error = %e, "session error, reconnecting"),
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

async fn run_once(
    symbol: &str,
    normalized_symbol: Symbol,
    tx: &mpsc::UnboundedSender<NormalizedTrade>,
    sequence: &mut u64,
    backoff: &mut ExponentialBackoff,
) -> Result<(), IngestError> {
    let (ws, _resp) = tokio_tungstenite::connect_async(URL).await?;
    tracing::info!(venue = "bybit", url = URL, "connected");
    backoff.reset();
    let (mut write, mut read) = ws.split();

    let sub = serde_json::json!({
        "op": "subscribe",
        "args": [format!("publicTrade.{symbol}")],
    });
    write.send(Message::Text(sub.to_string())).await?;

    let mut ping_tick = tokio::time::interval(APP_PING_INTERVAL);
    ping_tick.tick().await; // first tick is immediate, we just connected, skip it

    loop {
        tokio::select! {
            _ = ping_tick.tick() => {
                write.send(Message::Text(r#"{"op":"ping"}"#.to_string())).await?;
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        for trade in decode(&text, normalized_symbol, sequence)? {
                            if tx.send(trade).is_err() {
                                return Ok(()); // receiver gone, service shutting down
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    _ => {}
                }
            }
        }
    }
}

/// Bybit multiplexes control messages (subscribe acks, pong replies) and
/// data pushes on the same connection, distinguished by the presence of
/// a `"topic"` field. Anything without one is a control message, ignored
/// here rather than treated as an error, it's expected traffic.
fn decode(text: &str, symbol: Symbol, sequence: &mut u64) -> Result<Vec<NormalizedTrade>, IngestError> {
    let value: Value = serde_json::from_str(text)?;
    if value.get("topic").is_none() {
        return Ok(Vec::new());
    }

    let envelope: TradeEnvelope = serde_json::from_value(value)?;
    if !envelope.topic.starts_with("publicTrade.") {
        return Ok(Vec::new());
    }

    envelope
        .data
        .into_iter()
        .map(|t| {
            let seq = *sequence;
            *sequence += 1;
            Ok(NormalizedTrade {
                price: parse_price(&t.price)?,
                qty: parse_qty(&t.qty)?,
                ts_exchange_ns: t.time_ms.saturating_mul(1_000_000),
                ts_recv_ns: feedhandler::timer::now_ns(),
                symbol,
                exchange: Exchange::Bybit,
                taker_side: match t.side.as_str() {
                    "Buy" => TakerSide::Buy,
                    _ => TakerSide::Sell, // "Sell" is the only other documented value
                },
                sequence: seq,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Straight from the official Bybit v5 public trade docs example.
    const SAMPLE: &str = r#"{
        "topic": "publicTrade.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486868,
        "data": [
            {
                "T": 1672304486865,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.001",
                "p": "16578.50",
                "L": "PlusTick",
                "i": "20f43950-d8dd-5b31-9112-a178eb6023af",
                "BT": false,
                "seq": 1783284617
            }
        ]
    }"#;

    #[test]
    fn decodes_sample_envelope() {
        let symbol = Symbol::from_bytes(b"BTCUSDT");
        let mut seq = 0u64;
        let trades = decode(SAMPLE, symbol, &mut seq).unwrap();
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.price.raw(), 1_657_850_000_000);
        assert_eq!(t.qty.raw(), 100_000); // 0.001 * 1e8
        assert_eq!(t.taker_side, TakerSide::Buy);
        assert_eq!(t.exchange, Exchange::Bybit);
        assert_eq!(t.ts_exchange_ns, 1_672_304_486_865_000_000);
    }

    #[test]
    fn ignores_control_messages_without_topic() {
        let symbol = Symbol::from_bytes(b"BTCUSDT");
        let mut seq = 0u64;
        let pong = r#"{"success":true,"ret_msg":"pong","conn_id":"abc","op":"ping"}"#;
        let trades = decode(pong, symbol, &mut seq).unwrap();
        assert!(trades.is_empty());
        assert_eq!(seq, 0); // control messages don't advance the sequence

        let sub_ack = r#"{"success":true,"ret_msg":"subscribe","op":"subscribe"}"#;
        let trades = decode(sub_ack, symbol, &mut seq).unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn handles_multiple_trades_in_one_push() {
        let multi = SAMPLE.replace(
            "\"data\": [",
            "\"data\": [{\"T\":1,\"s\":\"BTCUSDT\",\"S\":\"Sell\",\"v\":\"0.5\",\"p\":\"100\",\"L\":\"PlusTick\",\"i\":\"x\",\"BT\":false,\"seq\":1},",
        );
        let symbol = Symbol::from_bytes(b"BTCUSDT");
        let mut seq = 0u64;
        let trades = decode(&multi, symbol, &mut seq).unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].sequence, 0);
        assert_eq!(trades[1].sequence, 1);
        assert_eq!(trades[0].taker_side, TakerSide::Sell);
    }
}
