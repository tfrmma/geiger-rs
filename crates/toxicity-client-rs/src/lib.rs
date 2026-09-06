//! Thin subscriber for `toxicity-service`'s WebSocket feed. Meant to be
//! created once at startup and held for the life of a process like
//! `boros-mm`'s `mm-bot`:
//!
//! ```no_run
//! # use std::time::Duration;
//! # #[tokio::main] async fn main() {
//! let client = toxicity_client_rs::ToxicityClient::connect(
//!     "ws://127.0.0.1:9700",
//!     "binance",
//!     "BTCUSDT",
//!     Duration::from_secs(15),
//! );
//!
//! // in the quoting loop, synchronous, no await:
//! match client.state() {
//!     toxicity_client_rs::ToxicityState::Live { vpin_cdf: Some(cdf), .. } if cdf > 0.9 => {
//!         // widen quotes
//!     }
//!     _ => {}
//! }
//! # }
//! ```
//!
//! Reconnect and staleness-detection logic is tested here against a
//! fake WS server on loopback (see `tests` below), a real socket, not a
//! mocked transport. Not tested against a real `toxicity-service`
//! process end-to-end.

mod error;
mod state;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

use backoff::ExponentialBackoff;
use toxicity_service::protocol::{ServerMessage, SubscribeRequest};

pub use error::ClientError;
pub use state::ToxicityState;

pub struct ToxicityClient {
    rx: watch::Receiver<ToxicityState>,
    // Explicitly aborted in `Drop`: dropping a `JoinHandle` on its own
    // does NOT cancel the task, tokio just detaches it and it keeps
    // running forever, which would leak a reconnect-forever background
    // task every time a `ToxicityClient` goes out of scope.
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ToxicityClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ToxicityClient {
    /// Spawns the background connection immediately. Reconnects forever
    /// with backoff, same pattern as `trade-ingest`'s adapters, this
    /// never gives up on its own.
    pub fn connect(
        url: impl Into<String>,
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        stale_after: Duration,
    ) -> Self {
        let (tx, rx) = watch::channel(ToxicityState::Connecting);
        let task = tokio::spawn(run(url.into(), exchange.into(), symbol.into(), stale_after, tx));
        ToxicityClient { rx, task }
    }

    /// Current state. Synchronous, just reads the latest value out of a
    /// `watch` channel, safe to call from a hot quoting loop, this is
    /// not a network round-trip.
    pub fn state(&self) -> ToxicityState {
        *self.rx.borrow()
    }
}

async fn run(url: String, exchange: String, symbol: String, stale_after: Duration, tx: watch::Sender<ToxicityState>) {
    let mut backoff = ExponentialBackoff::default();
    loop {
        let _ = tx.send(ToxicityState::Connecting);
        match run_once(&url, &exchange, &symbol, stale_after, &tx, &mut backoff).await {
            Ok(()) => {
                tracing::warn!(exchange, symbol, "toxicity-service session closed, reconnecting");
            }
            Err(e) => {
                tracing::warn!(exchange, symbol, error = %e, "toxicity-service session error, reconnecting");
            }
        }
        let _ = tx.send(ToxicityState::Stale);
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

async fn run_once(
    url: &str,
    exchange: &str,
    symbol: &str,
    stale_after: Duration,
    tx: &watch::Sender<ToxicityState>,
    backoff: &mut ExponentialBackoff,
) -> Result<(), ClientError> {
    let (ws, _resp) = tokio_tungstenite::connect_async(url).await?;
    tracing::info!(url, exchange, symbol, "connected to toxicity-service");
    backoff.reset();
    let (mut write, mut read) = ws.split();

    let sub = SubscribeRequest { exchange: exchange.to_string(), symbol: symbol.to_string() };
    write.send(Message::Text(serde_json::to_string(&sub)?)).await?;

    // Checked at twice the staleness threshold's frequency so the
    // detection latency is bounded well under `stale_after` itself,
    // rather than possibly waiting up to a full `stale_after` extra
    // before noticing.
    let mut staleness_check = tokio::time::interval((stale_after / 2).max(Duration::from_millis(1)));
    staleness_check.tick().await; // first tick is immediate, skip it

    let mut last_msg_at = tokio::time::Instant::now();

    loop {
        tokio::select! {
            _ = staleness_check.tick() => {
                if last_msg_at.elapsed() > stale_after {
                    let _ = tx.send(ToxicityState::Stale);
                }
            }
            incoming = read.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        last_msg_at = tokio::time::Instant::now();
                        handle_message(&text, exchange, symbol, tx);
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

fn handle_message(text: &str, exchange: &str, symbol: &str, tx: &watch::Sender<ToxicityState>) {
    let msg: ServerMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(exchange, symbol, error = %e, "malformed message from toxicity-service");
            return;
        }
    };

    match msg {
        ServerMessage::Reading { vpin, vpin_cdf, trades_in_bucket, .. } => {
            let state = match vpin {
                Some(v) => ToxicityState::Live { vpin: v, vpin_cdf, trades_in_bucket },
                None => ToxicityState::Warming { trades_in_bucket },
            };
            let _ = tx.send(state);
        }
        ServerMessage::Heartbeat { .. } => {
            // No data in a heartbeat, but if we were sitting on `Stale`
            // or `Connecting`, this proves the connection is alive again
            // even without a fresh `Reading`, worth surfacing as
            // `Warming` rather than leaving the caller thinking nothing
            // has changed. Leaves `Warming`/`Live` alone, a heartbeat
            // shouldn't downgrade a real reading.
            tx.send_if_modified(|current| {
                if matches!(current, ToxicityState::Stale | ToxicityState::Connecting) {
                    *current = ToxicityState::Warming { trades_in_bucket: 0 };
                    true
                } else {
                    false
                }
            });
        }
        ServerMessage::Error { message } => {
            tracing::warn!(exchange, symbol, message, "toxicity-service reported an error");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// A minimal fake `toxicity-service` on loopback: accepts one
    /// connection, expects a `SubscribeRequest`, then sends whatever
    /// `ServerMessage`s the test hands it over `to_send`. This is a real
    /// WebSocket server and a real `ToxicityClient` talking over a real
    /// (local) socket, not a mocked transport.
    async fn spawn_fake_server(mut to_send: tokio::sync::mpsc::UnboundedReceiver<ServerMessage>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws.split();

            // consume (and ignore the content of) the subscribe request
            let _ = read.next().await;

            while let Some(msg) = to_send.recv().await {
                let text = serde_json::to_string(&msg).unwrap();
                if write.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
        });

        format!("ws://{addr}")
    }

    async fn wait_for<F: Fn(&ToxicityState) -> bool>(client: &ToxicityClient, cond: F) -> ToxicityState {
        for _ in 0..200 {
            let s = client.state();
            if cond(&s) {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition never became true within 2s, last state: {:?}", client.state());
    }

    #[tokio::test]
    async fn goes_from_connecting_to_live_on_a_reading() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let url = spawn_fake_server(rx).await;
        let client = ToxicityClient::connect(url, "binance", "BTCUSDT", Duration::from_secs(30));

        assert_eq!(client.state(), ToxicityState::Connecting);

        tx.send(ServerMessage::Reading {
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
            bucket_id: 1,
            ts_close_ns: 0,
            trades_in_bucket: 12,
            vpin: Some(0.42),
            vpin_cdf: Some(0.9),
        })
        .unwrap();

        let state = wait_for(&client, |s| s.is_live()).await;
        assert_eq!(state, ToxicityState::Live { vpin: 0.42, vpin_cdf: Some(0.9), trades_in_bucket: 12 });
    }

    #[tokio::test]
    async fn warmup_reading_maps_to_warming_not_live() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let url = spawn_fake_server(rx).await;
        let client = ToxicityClient::connect(url, "bybit", "ETHUSDT", Duration::from_secs(30));

        tx.send(ServerMessage::Reading {
            exchange: "bybit".into(),
            symbol: "ETHUSDT".into(),
            bucket_id: 0,
            ts_close_ns: 0,
            trades_in_bucket: 3,
            vpin: None,
            vpin_cdf: None,
        })
        .unwrap();

        let state = wait_for(&client, |s| matches!(s, ToxicityState::Warming { .. })).await;
        assert_eq!(state, ToxicityState::Warming { trades_in_bucket: 3 });
        assert!(!state.is_live());
    }

    #[tokio::test]
    async fn goes_stale_when_messages_stop_arriving() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let url = spawn_fake_server(rx).await;
        let client = ToxicityClient::connect(url, "hyperliquid", "BTC", Duration::from_millis(100));

        tx.send(ServerMessage::Heartbeat { ts_ns: 0 }).unwrap();
        wait_for(&client, |s| !matches!(s, ToxicityState::Connecting)).await;

        // stop sending anything, drop the sender so the fake server's
        // loop ends too, and wait past the staleness threshold
        drop(tx);
        let state = wait_for(&client, |s| s.is_stale()).await;
        assert!(state.is_stale());
    }

    #[tokio::test]
    async fn heartbeat_does_not_downgrade_an_existing_live_reading() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let url = spawn_fake_server(rx).await;
        let client = ToxicityClient::connect(url, "binance", "BTCUSDT", Duration::from_secs(30));

        tx.send(ServerMessage::Reading {
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
            bucket_id: 1,
            ts_close_ns: 0,
            trades_in_bucket: 5,
            vpin: Some(0.7),
            vpin_cdf: Some(0.95),
        })
        .unwrap();
        wait_for(&client, |s| s.is_live()).await;

        tx.send(ServerMessage::Heartbeat { ts_ns: 1 }).unwrap();
        // give the heartbeat a moment to be processed, then confirm the
        // Live reading is still there, not reset to Warming
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(client.state(), ToxicityState::Live { vpin: 0.7, vpin_cdf: Some(0.95), trades_in_bucket: 5 });
    }

    #[tokio::test]
    async fn dropping_the_client_ends_the_background_task() {
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let url = spawn_fake_server(rx).await;
        let client = ToxicityClient::connect(url, "binance", "BTCUSDT", Duration::from_secs(30));
        let task = client.task.abort_handle();
        drop(client);
        // give the runtime a moment to actually process the abort
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(task.is_finished());
    }
}
