use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

use crate::config::stream_key;
use crate::ServiceError;
use toxicity_service::protocol::{ServerMessage, SubscribeRequest};

pub type Registry = Arc<HashMap<String, broadcast::Sender<ServerMessage>>>;

/// Accepts connections forever. The set of streams is fixed at startup
/// (config-driven, see `config.rs`), so `Registry` is a plain `Arc`, no
/// lock, there's nothing to mutate after construction.
pub async fn run(bind_addr: &str, registry: Registry) -> Result<(), ServiceError> {
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!(bind_addr, "toxicity-service listening");

    loop {
        let (stream, peer) = listener.accept().await?;
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, peer, registry).await {
                tracing::debug!(%peer, error = %e, "connection ended");
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, peer: SocketAddr, registry: Registry) -> Result<(), ServiceError> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws.split();

    let sub = match read.next().await {
        Some(Ok(Message::Text(text))) => serde_json::from_str::<SubscribeRequest>(&text)?,
        _ => return Err(ServiceError::NoSubscribeRequest),
    };

    let key = stream_key(&sub.exchange, &sub.symbol);
    let Some(sender) = registry.get(&key) else {
        let err = ServerMessage::Error { message: format!("unknown stream: {key}") };
        write.send(Message::Text(serde_json::to_string(&err)?)).await?;
        return Err(ServiceError::UnknownStream(key));
    };
    let mut sub_rx = sender.subscribe();
    tracing::info!(%peer, key, "subscribed");

    loop {
        tokio::select! {
            broadcast_msg = sub_rx.recv() => {
                match broadcast_msg {
                    Ok(msg) => {
                        write.send(Message::Text(serde_json::to_string(&msg)?)).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // slow client missed n messages, that's on them,
                        // not a reason to drop a connection that might
                        // otherwise be fine, just note it and carry on
                        tracing::warn!(%peer, key, lagged = n, "subscriber too slow, dropped messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            incoming = read.next() => {
                match incoming {
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    _ => {} // ignore any other client-sent frames post-subscribe
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::connect_async;

    async fn spawn_test_server() -> (String, Registry) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // release the port, run() below rebinds it, small
                         // race in theory, fine for a local test server

        let mut map = HashMap::new();
        let (tx, _rx) = broadcast::channel(64);
        map.insert(stream_key("binance", "BTCUSDT"), tx.clone());
        let registry: Registry = Arc::new(map);

        let addr_string = addr.to_string();
        let bind_addr = addr_string.clone();
        let reg = registry.clone();
        tokio::spawn(async move {
            run(&bind_addr, reg).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        (addr_string, registry)
    }

    #[tokio::test]
    async fn subscribes_and_receives_broadcast_reading() {
        let (addr, registry) = spawn_test_server().await;

        let (mut ws, _) = connect_async(format!("ws://{addr}/")).await.unwrap();
        ws.send(Message::Text(r#"{"exchange":"binance","symbol":"BTCUSDT"}"#.to_string()))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        let sender = registry.get(&stream_key("binance", "BTCUSDT")).unwrap();
        let msg = ServerMessage::Reading {
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
            bucket_id: 1,
            ts_close_ns: 42,
            trades_in_bucket: 5,
            vpin: Some(0.5),
            vpin_cdf: Some(0.9),
        };
        sender.send(msg.clone()).unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), ws.next())
            .await
            .expect("timed out waiting for broadcast message")
            .unwrap()
            .unwrap();

        let Message::Text(text) = received else { panic!("expected a text frame") };
        let decoded: ServerMessage = serde_json::from_str(&text).unwrap();
        assert_eq!(decoded, msg);
    }

    #[tokio::test]
    async fn unknown_stream_gets_an_error_and_disconnect() {
        let (addr, _registry) = spawn_test_server().await;

        let (mut ws, _) = connect_async(format!("ws://{addr}/")).await.unwrap();
        ws.send(Message::Text(r#"{"exchange":"binance","symbol":"DOGEUSDT"}"#.to_string()))
            .await
            .unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), ws.next())
            .await
            .expect("timed out waiting for error message")
            .unwrap()
            .unwrap();

        let Message::Text(text) = received else { panic!("expected a text frame") };
        let decoded: ServerMessage = serde_json::from_str(&text).unwrap();
        assert!(matches!(decoded, ServerMessage::Error { .. }));
    }
}
