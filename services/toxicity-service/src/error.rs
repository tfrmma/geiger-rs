use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("client didn't send a subscribe request before the first message")]
    NoSubscribeRequest,

    #[error("unknown stream: {0} (check GEIGER_STREAMS_FILE)")]
    UnknownStream(String),
}
