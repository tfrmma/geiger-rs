use std::time::Duration;

use tokio::sync::{broadcast, mpsc};

use trade_ingest::NormalizedTrade;
use vpin_engine::{VpinEngine, VpinEngineConfig};

use toxicity_service::protocol::ServerMessage;

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
    out_tx: broadcast::Sender<ServerMessage>,
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

    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.tick().await; // first tick fires immediately, skip it

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                // Err here only means zero subscribers are currently
                // connected, not a failure, nothing to log.
                let _ = out_tx.send(ServerMessage::Heartbeat { ts_ns: feedhandler::timer::now_ns() });
            }
            trade = trade_rx.recv() => {
                let Some(trade) = trade else {
                    tracing::error!(exchange, symbol, "trade-ingest channel closed, worker exiting");
                    return;
                };

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
                    let _ = out_tx.send(msg);
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

    #[tokio::test]
    async fn emits_readings_and_heartbeats() {
        let (trade_tx, trade_rx) = mpsc::unbounded_channel();
        let (out_tx, mut out_rx) = broadcast::channel(256);

        let cfg = VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 3,
            cdf_window: None,
        };

        tokio::spawn(run(
            "binance".to_string(),
            "BTCUSDT".to_string(),
            cfg,
            trade_rx,
            out_tx,
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
            let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_secs(5), out_rx.recv()).await else {
                break;
            };
            if let ServerMessage::Reading { vpin, trades_in_bucket, .. } = msg {
                assert!(trades_in_bucket > 0);
                if vpin.is_some() {
                    saw_reading = true;
                    break;
                }
            }
        }
        assert!(saw_reading, "expected at least one non-warmup reading over 100 trades");
    }

    #[tokio::test]
    async fn heartbeat_fires_even_with_no_trades() {
        let (_trade_tx, trade_rx) = mpsc::unbounded_channel::<NormalizedTrade>();
        let (out_tx, mut out_rx) = broadcast::channel(8);

        let cfg = VpinEngineConfig { bucket_volume: 10.0, sigma_window: 3, vpin_window: 3, cdf_window: None };
        tokio::spawn(run("bybit".to_string(), "ETHUSDT".to_string(), cfg, trade_rx, out_tx, Duration::from_millis(30)));

        // Same reasoning as `emits_readings_and_heartbeats`: wait for
        // actual messages with a generous per-message timeout instead of
        // a fixed sleep, immune to CPU contention from other tests
        // running concurrently.
        let mut heartbeats = 0;
        for _ in 0..10 {
            let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_secs(5), out_rx.recv()).await else {
                break;
            };
            if matches!(msg, ServerMessage::Heartbeat { .. }) {
                heartbeats += 1;
                if heartbeats >= 2 {
                    break;
                }
            }
        }
        assert!(heartbeats >= 2, "expected at least 2 heartbeats at a 30ms interval, got {heartbeats}");
    }

    #[tokio::test]
    async fn worker_exits_when_trade_channel_closes() {
        let (trade_tx, trade_rx) = mpsc::unbounded_channel();
        let (out_tx, _out_rx) = broadcast::channel(8);
        let cfg = VpinEngineConfig { bucket_volume: 10.0, sigma_window: 3, vpin_window: 3, cdf_window: None };

        let handle = tokio::spawn(run(
            "hyperliquid".to_string(),
            "BTC".to_string(),
            cfg,
            trade_rx,
            out_tx,
            Duration::from_secs(3600),
        ));

        drop(trade_tx); // closes the channel
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("worker should exit promptly once its trade channel closes")
            .unwrap();
    }
}
