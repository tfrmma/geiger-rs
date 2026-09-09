use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message;

use crate::config::stream_key;
use crate::ServiceError;
use toxicity_service::protocol::{ServerMessage, SubscribeRequest};

pub type Registry = Arc<HashMap<String, broadcast::Sender<ServerMessage>>>;

/// `None` means no auth is enforced, anyone who can reach the port can
/// subscribe. Fine for pure localhost/VPN deployments, not fine for
/// anything with a public-reachable bind address, set `GEIGER_AUTH_TOKEN`
/// in that case.
pub type AuthToken = Option<Arc<str>>;

/// Accepts connections forever, until `shutdown` fires. The set of
/// streams is fixed at startup (config-driven, see `config.rs`), so
/// `Registry` is a plain `Arc`, no lock, there's nothing to mutate after
/// construction.
///
/// On shutdown this stops accepting new connections and returns
/// promptly, it does not wait for existing subscribers to disconnect
/// cleanly, every subscriber (`toxicity-client-rs`, `py-vpin`'s WS
/// client if one exists) is already built to reconnect on its own, a
/// hard disconnect here is a normal, expected condition for them, not a
/// failure to design around.
pub async fn run(bind_addr: &str, registry: Registry, auth_token: AuthToken, mut shutdown: watch::Receiver<bool>) -> Result<(), ServiceError> {
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!(bind_addr, auth_enabled = auth_token.is_some(), "toxicity-service listening");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let registry = registry.clone();
                let auth_token = auth_token.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, peer, registry, auth_token).await {
                        tracing::debug!(%peer, error = %e, "connection ended");
                    }
                });
            }
            _ = shutdown.changed() => {
                tracing::info!("shutdown signal received, no longer accepting new connections");
                return Ok(());
            }
        }
    }
}

/// Checked during the WS handshake itself, before any WebSocket frame is
/// ever exchanged, rather than as a field on the first `SubscribeRequest`
/// message: rejecting at the HTTP layer means an unauthorized client
/// never gets a completed WS connection at all, not even a "here's an
/// error message" one.
fn check_auth(req: &Request, auth_token: &AuthToken) -> Result<(), ErrorResponse> {
    let Some(expected) = auth_token else { return Ok(()) }; // auth disabled

    let provided = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    if provided == Some(expected.as_ref()) {
        return Ok(());
    }

    // Returning `Ok(response-with-error-status)` rather than `Err(...)`
    // from the handshake callback: the latter has a history of panicking
    // inside tungstenite (github.com/snapview/tokio-tungstenite/issues/205),
    // this is the pattern actually used in the wild to reject a handshake
    // cleanly.
    let body = if provided.is_none() { "missing bearer token" } else { "invalid bearer token" };
    let status = if provided.is_none() { StatusCode::UNAUTHORIZED } else { StatusCode::FORBIDDEN };
    let response = Response::builder().status(status).body(Some(body.to_string())).expect("valid static response");
    Err(response)
}

async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    registry: Registry,
    auth_token: AuthToken,
) -> Result<(), ServiceError> {
    let ws = tokio_tungstenite::accept_hdr_async(stream, |req: &Request, res: Response| -> Result<Response, ErrorResponse> {
        match check_auth(req, &auth_token) {
            Ok(()) => Ok(res),
            Err(rejection) => {
                tracing::warn!(%peer, "rejected unauthorized connection attempt");
                Err(rejection)
            }
        }
    })
    .await?;
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
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    async fn spawn_test_server(auth_token: AuthToken) -> (String, Registry, watch::Sender<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // release the port, run() below rebinds it, small
                         // race in theory, fine for a local test server

        let mut map = HashMap::new();
        let (tx, _rx) = broadcast::channel(64);
        map.insert(stream_key("binance", "BTCUSDT"), tx.clone());
        let registry: Registry = Arc::new(map);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let addr_string = addr.to_string();
        let bind_addr = addr_string.clone();
        let reg = registry.clone();
        tokio::spawn(async move {
            run(&bind_addr, reg, auth_token, shutdown_rx).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        (addr_string, registry, shutdown_tx)
    }

    #[tokio::test]
    async fn subscribes_and_receives_broadcast_reading() {
        let (addr, registry, _shutdown_tx) = spawn_test_server(None).await;

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
        let (addr, _registry, _shutdown_tx) = spawn_test_server(None).await;

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

    #[tokio::test]
    async fn connects_fine_with_no_auth_configured_and_no_header_sent() {
        // auth disabled entirely (AuthToken::None): a plain connect with
        // no Authorization header at all still has to work, this is the
        // default localhost/dev deployment.
        let (addr, _registry, _shutdown_tx) = spawn_test_server(None).await;
        let result = connect_async(format!("ws://{addr}/")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn rejects_connection_with_missing_bearer_token() {
        let (addr, _registry, _shutdown_tx) = spawn_test_server(Some(Arc::from("secret-token"))).await;
        let result = connect_async(format!("ws://{addr}/")).await;
        assert!(result.is_err(), "expected the handshake itself to fail, not just the subscribe to be refused later");
    }

    #[tokio::test]
    async fn rejects_connection_with_wrong_bearer_token() {
        let (addr, _registry, _shutdown_tx) = spawn_test_server(Some(Arc::from("secret-token"))).await;
        let mut req = format!("ws://{addr}/").into_client_request().unwrap();
        req.headers_mut().insert("Authorization", "Bearer wrong-token".parse().unwrap());
        let result = connect_async(req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn accepts_connection_with_correct_bearer_token() {
        let (addr, _registry, _shutdown_tx) = spawn_test_server(Some(Arc::from("secret-token"))).await;
        let mut req = format!("ws://{addr}/").into_client_request().unwrap();
        req.headers_mut().insert("Authorization", "Bearer secret-token".parse().unwrap());
        let result = connect_async(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn stops_accepting_new_connections_after_shutdown_signal() {
        let (addr, _registry, shutdown_tx) = spawn_test_server(None).await;

        // connects fine before shutdown
        let before = connect_async(format!("ws://{addr}/")).await;
        assert!(before.is_ok());

        shutdown_tx.send(true).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let the accept loop notice and exit

        // the listening socket itself is gone once run() returns, so a
        // fresh connection attempt has to fail, not just get ignored
        let after = connect_async(format!("ws://{addr}/")).await;
        assert!(after.is_err(), "expected connect to fail once the server has shut down");
    }
}
