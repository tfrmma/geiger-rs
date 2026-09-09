//! Hyperliquid perpetuals, `trades` subscription.
//! https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
//! https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats
//!
//! `coin` is the bare perp symbol ("BTC", "ETH"), no suffix, that's the
//! perps convention, spot markets use a different `@{index}` scheme this
//! adapter doesn't handle.
//!
//! Server closes the connection after 60s with no message sent to it, a
//! quiet perp could plausibly go that long between trades, so this pings
//! on a fixed interval regardless of trade flow rather than only when
//! idle, simpler to reason about than tracking last-message-sent time.
//!
//! One thing worth flagging: Hyperliquid's own docs (fetched directly for
//! this adapter) give `WsTrade`'s shape but don't spell out what `side`
//! "A" vs "B" means semantically, that came from a third-party API
//! reference (Dwellir), not Hyperliquid's primary docs: "B" (Bid) is the
//! taker buying from the ask, "A" (Ask) is the taker selling into the
//! bid. Corroborated, not primary-sourced, worth an eye if fills ever
//! look inverted in practice.

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

const URL: &str = "wss://api.hyperliquid.xyz/ws";
const APP_PING_INTERVAL: Duration = Duration::from_secs(30); // well under the 60s timeout

#[derive(Deserialize)]
struct ChannelEnvelope {
    channel: String,
    #[serde(default)]
    data: Value,
}

/// Field names match the wire format exactly, see the module doc link.
#[derive(Deserialize)]
struct WsTrade {
    side: String, // "A" or "B", see module doc comment
    px: String,
    sz: String,
    time: u64, // ms
}

/// See `binance::run`'s doc comment for why this takes an owned `String`.
pub async fn run(coin: String, tx: mpsc::UnboundedSender<NormalizedTrade>, backoff_cfg: BackoffConfig) {
    let normalized_symbol = Symbol::from_bytes(coin.as_bytes());
    let mut backoff = backoff_cfg.build();
    let mut sequence: u64 = 0;

    loop {
        match run_once(&coin, normalized_symbol, &tx, &mut sequence, &mut backoff).await {
            Ok(()) => tracing::warn!(venue = "hyperliquid", coin, "session closed, reconnecting"),
            Err(e) => tracing::warn!(venue = "hyperliquid", coin, error = %e, "session error, reconnecting"),
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

async fn run_once(
    coin: &str,
    normalized_symbol: Symbol,
    tx: &mpsc::UnboundedSender<NormalizedTrade>,
    sequence: &mut u64,
    backoff: &mut ExponentialBackoff,
) -> Result<(), IngestError> {
    let (ws, _resp) = tokio_tungstenite::connect_async(URL).await?;
    tracing::info!(venue = "hyperliquid", url = URL, "connected");
    backoff.reset();
    let (mut write, mut read) = ws.split();

    let sub = serde_json::json!({
        "method": "subscribe",
        "subscription": { "type": "trades", "coin": coin },
    });
    write.send(Message::Text(sub.to_string())).await?;

    let mut ping_tick = tokio::time::interval(APP_PING_INTERVAL);
    ping_tick.tick().await; // first tick is immediate, we just connected, skip it

    loop {
        tokio::select! {
            _ = ping_tick.tick() => {
                write.send(Message::Text(r#"{"method":"ping"}"#.to_string())).await?;
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

/// Hyperliquid multiplexes `subscriptionResponse`, `pong`, and `trades`
/// on the same connection, distinguished by `"channel"`. Anything that
/// isn't `"trades"` is expected control traffic, not an error.
fn decode(text: &str, symbol: Symbol, sequence: &mut u64) -> Result<Vec<NormalizedTrade>, IngestError> {
    let envelope: ChannelEnvelope = serde_json::from_str(text)?;
    if envelope.channel != "trades" {
        return Ok(Vec::new());
    }

    let trades: Vec<WsTrade> = serde_json::from_value(envelope.data)?;

    trades
        .into_iter()
        .map(|t| {
            let seq = *sequence;
            *sequence += 1;
            Ok(NormalizedTrade {
                price: parse_price(&t.px)?,
                qty: parse_qty(&t.sz)?,
                ts_exchange_ns: t.time.saturating_mul(1_000_000),
                ts_recv_ns: feedhandler::timer::now_ns(),
                symbol,
                exchange: Exchange::Hyperliquid,
                taker_side: match t.side.as_str() {
                    "B" => TakerSide::Buy,
                    _ => TakerSide::Sell, // "A" is the only other documented value
                },
                sequence: seq,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Shape matches Hyperliquid's documented WsTrade type, values are
    // representative, not copied from a captured live payload (the
    // primary docs don't publish a full worked JSON example the way
    // Binance and Bybit do).
    const SAMPLE: &str = r#"{
        "channel": "trades",
        "data": [
            {
                "coin": "BTC",
                "side": "B",
                "px": "43250.5",
                "sz": "0.25",
                "hash": "0xabc123",
                "time": 1700000000000,
                "tid": 123456789,
                "users": ["0x1111", "0x2222"]
            }
        ]
    }"#;

    #[test]
    fn decodes_sample_envelope() {
        let symbol = Symbol::from_bytes(b"BTC");
        let mut seq = 0u64;
        let trades = decode(SAMPLE, symbol, &mut seq).unwrap();
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.price.raw(), 4_325_050_000_000);
        assert_eq!(t.qty.raw(), 25_000_000); // 0.25 * 1e8
        assert_eq!(t.taker_side, TakerSide::Buy);
        assert_eq!(t.exchange, Exchange::Hyperliquid);
        assert_eq!(t.ts_exchange_ns, 1_700_000_000_000_000_000);
    }

    #[test]
    fn ignores_non_trade_channels() {
        let symbol = Symbol::from_bytes(b"BTC");
        let mut seq = 0u64;
        let sub_response = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe"}}"#;
        assert!(decode(sub_response, symbol, &mut seq).unwrap().is_empty());

        let pong = r#"{"channel":"pong"}"#;
        assert!(decode(pong, symbol, &mut seq).unwrap().is_empty());
        assert_eq!(seq, 0);
    }

    #[test]
    fn side_a_is_sell() {
        let flipped = SAMPLE.replace("\"side\": \"B\"", "\"side\": \"A\"");
        let symbol = Symbol::from_bytes(b"BTC");
        let mut seq = 0u64;
        let trades = decode(&flipped, symbol, &mut seq).unwrap();
        assert_eq!(trades[0].taker_side, TakerSide::Sell);
    }
}
