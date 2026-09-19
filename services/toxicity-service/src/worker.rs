use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use trade_ingest::NormalizedTrade;
use vpin_engine::{VpinEngine, VpinEngineConfig};

use toxicity_service::protocol::ServerMessage;

use crate::registry::StreamEntry;

/// Sends `msg` on `entry.sender`, and records it as the entry's
/// `last_message` first so a subscriber that catches up right after this
/// broadcast still sees it (the reverse order would let a subscriber
/// arrive between the send and the record and miss it from both paths).
async fn publish(entry: &StreamEntry, msg: ServerMessage) {
    *entry.last_message.write().await = Some(msg.clone());
    // Err here only means zero subscribers are currently connected, not
    // a failure, nothing to log.
    let _ = entry.sender.send(msg);
}

/// Owns one `VpinEngine` for one (exchange, symbol) stream. Consumes
/// `NormalizedTrade`s off `trade_rx`, feeds them into the engine, and
/// broadcasts a `ServerMessage::Reading` whenever a bucket closes plus a
/// `ServerMessage::Heartbeat` on a fixed interval regardless of trade
/// flow. Runs until `trade_rx` closes (the corresponding trade-ingest
/// task died, which given trade-ingest's own reconnect-forever loop
/// should only happen if the whole process is shutting down).
pub async fn run(
    exchange: String,
    symbol: String,
    engine_cfg: VpinEngineConfig,
    mut trade_rx: mpsc::UnboundedReceiver<NormalizedTrade>,
    entry: Arc<StreamEntry>,
    heartbeat_interval: Duration,
) {
    let mut engine = match VpinEngine::new(engine_cfg) {
        Ok(e) => e,
        Err(err) => {
            // config is validated at startup in main.rs before any
            // worker is spawned, reaching this means that validation has
            // a gap, loud and immediate is right here, not a silent
            // never-produces-readings worker.
            panic!("invalid VPIN engine config for {exchange}:{symbol}: {err}");
        }
    };

    entry.health.write().await.worker_alive = true;

    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.tick().await; // first tick fires immediately, skip it

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let ts_ns = feedhandler::timer::now_ns();
                publish(&entry, ServerMessage::Heartbeat { ts_ns }).await;
                entry.health.write().await.last_heartbeat_ts_ns = Some(ts_ns);
            }
            trade = trade_rx.recv() => {
                let Some(trade) = trade else {
                    tracing::error!(exchange, symbol, "trade-ingest channel closed, worker exiting");
                    entry.health.write().await.worker_alive = false;
                    return;
                };

                {
                    let mut health = entry.health.write().await;
                    health.trades_processed += 1;
                    health.last_trade_ts_ns = Some(trade.ts_exchange_ns);
                }

                let price = trade.price.raw() as f64 / 1e8;
                let qty = trade.qty.raw() as f64 / 1e8;

                if let Some(reading) = engine.push_trade(price, qty, trade.ts_exchange_ns) {
                    let msg = ServerMessage::Reading {
                        exchange: exchange.clone(),
                        symbol: symbol.clone(),
                        bucket_id: reading.bucket_id,
                        ts_close_ns: reading.ts_close_ns,
                        trades_in_bucket: reading.trades_in_bucket,
                        vpin: reading.vpin,
                        vpin_cdf: reading.vpin_cdf,
                    };
                    publish(&entry, msg).await;
                    entry.health.write().await.readings_emitted += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feedhandler::{Exchange, Price, Qty, Symbol};

    fn make_trade(price: f64, qty: f64, ts_ns: u64, seq: u64) -> NormalizedTrade {
        NormalizedTrade {
            price: Price::new((price * 1e8) as u64),
            qty: Qty::new((qty * 1e8) as u64),
            ts_exchange_ns: ts_ns,
            ts_recv_ns: ts_ns,
            symbol: Symbol::from_bytes(b"BTCUSDT"),
            exchange: Exchange::Binance,
            taker_side: trade_ingest::TakerSide::Buy,
            sequence: seq,
        }
    }

    fn make_entry(exchange: &str, symbol: &str, capacity: usize) -> Arc<StreamEntry> {
        let (sender, _rx) = tokio::sync::broadcast::channel(capacity);
        Arc::new(StreamEntry::new(
            exchange.to_string(),
            symbol.to_string(),
            sender,
        ))
    }

    #[tokio::test]
    async fn emits_readings_and_heartbeats() {
        let (trade_tx, trade_rx) = mpsc::unbounded_channel();
        let entry = make_entry("binance", "BTCUSDT", 256);
        let mut out_rx = entry.sender.subscribe();

        let cfg = VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 3,
            cdf_window: None,
            confidence_interval: None,
        };

        let handle = tokio::spawn(run(
            "binance".to_string(),
            "BTCUSDT".to_string(),
            cfg,
            trade_rx,
            entry.clone(),
            Duration::from_secs(3600), // long enough not to fire during this test
        ));

        let mut price = 100.0;
        for i in 0..100u64 {
            price += if i % 2 == 0 { 0.7 } else { -0.4 };
            trade_tx.send(make_trade(price, 10.0, i, i)).unwrap();
        }

        // Wait for actual messages rather than sleeping a fixed duration
        // and checking whatever showed up: a fixed sleep is racy under
        // CPU contention when many tests run concurrently, each with its
        // own tokio runtime, timeout-bounded waiting for the specific
        // condition isn't.
        let mut saw_reading = false;
        for _ in 0..100 {
            let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_secs(5), out_rx.recv()).await
            else {
                break;
            };
            if let ServerMessage::Reading {
                vpin,
                trades_in_bucket,
                ..
            } = msg
            {
                assert!(trades_in_bucket > 0);
                if vpin.is_some() {
                    saw_reading = true;
                    break;
                }
            }
        }
        assert!(
            saw_reading,
            "expected at least one non-warmup reading over 100 trades"
        );

        // The loop above breaks as soon as one non-warmup reading shows
        // up, which can happen well before the worker has drained every
        // queued trade off `trade_rx` (it's a separate task, scheduled
        // cooperatively). Dropping the sender and joining the worker
        // guarantees it processes every already-sent trade before
        // `trade_rx.recv()` returns `None` and it exits, unbounded mpsc
        // semantics, not a race, so the health counts below are exact.
        drop(trade_tx);
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("worker should drain and exit promptly once the trade channel closes")
            .unwrap();

        let health = entry.health.read().await;
        assert!(
            !health.worker_alive,
            "worker exited once its channel drained and closed"
        );
        assert_eq!(health.trades_processed, 100);
        assert!(health.readings_emitted > 0);
        assert!(health.last_trade_ts_ns.is_some());

        let last_message = entry.last_message.read().await;
        assert!(
            last_message.is_some(),
            "expected last_message to be populated after activity"
        );
    }

    #[tokio::test]
    async fn heartbeat_fires_even_with_no_trades() {
        let (_trade_tx, trade_rx) = mpsc::unbounded_channel::<NormalizedTrade>();
        let entry = make_entry("bybit", "ETHUSDT", 8);
        let mut out_rx = entry.sender.subscribe();

        let cfg = VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 3,
            cdf_window: None,
            confidence_interval: None,
        };
        tokio::spawn(run(
            "bybit".to_string(),
            "ETHUSDT".to_string(),
            cfg,
            trade_rx,
            entry.clone(),
            Duration::from_millis(30),
        ));

        // Same reasoning as `emits_readings_and_heartbeats`: wait for
        // actual messages with a generous per-message timeout instead of
        // a fixed sleep, immune to CPU contention from other tests
        // running concurrently.
        let mut heartbeats = 0;
        for _ in 0..10 {
            let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_secs(5), out_rx.recv()).await
            else {
                break;
            };
            if matches!(msg, ServerMessage::Heartbeat { .. }) {
                heartbeats += 1;
                if heartbeats >= 2 {
                    break;
                }
            }
        }
        assert!(
            heartbeats >= 2,
            "expected at least 2 heartbeats at a 30ms interval, got {heartbeats}"
        );
        assert!(entry.health.read().await.last_heartbeat_ts_ns.is_some());
    }

    #[tokio::test]
    async fn worker_exits_when_trade_channel_closes() {
        let (trade_tx, trade_rx) = mpsc::unbounded_channel();
        let entry = make_entry("hyperliquid", "BTC", 8);
        let cfg = VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 3,
            cdf_window: None,
            confidence_interval: None,
        };

        let handle = tokio::spawn(run(
            "hyperliquid".to_string(),
            "BTC".to_string(),
            cfg,
            trade_rx,
            entry.clone(),
            Duration::from_secs(3600),
        ));

        drop(trade_tx); // closes the channel
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("worker should exit promptly once its trade channel closes")
            .unwrap();

        assert!(
            !entry.health.read().await.worker_alive,
            "worker_alive should flip to false once the worker exits"
        );
    }
}
