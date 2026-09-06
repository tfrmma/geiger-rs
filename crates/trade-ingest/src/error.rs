use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("malformed decimal string: {0:?}")]
    BadDecimal(String),

    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("json decode error: {0}")]
    Json(#[from] serde_json::Error),
}
