//! Bare-bones HTTP server for operational visibility, no framework
//! same pattern as `boros-mm`'s `risk-monitor::serve_health`: a raw
//! `TcpListener`, read the request into a buffer, hand-format the
//! response. Two routes here instead of risk-monitor's one, so unlike
//! that service this one actually has to look at the request line
//! rather than answering every connection the same way.
//!
//! `/health` is a JSON snapshot meant for a human or an uptime check.
//! `/metrics` is Prometheus exposition format, hand-written the same
//! way as `feedhandler-core-rs::metrics`: no metrics client library, no
//! background thread, just string formatting against state that already
//! exists (`registry::StreamHealth`).

use std::fmt::Write as _;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::registry::Registry;

/// Binds `addr` and serves `/health` and `/metrics` forever. Logs and
/// returns (rather than panicking) if the bind fails, same reasoning as
/// `risk-monitor::serve_health`: a bad `GEIGER_HEALTH_ADDR` shouldn't
/// take down the WS server this runs alongside, it should just mean no
/// health endpoint this run, loudly logged.
pub async fn run(addr: &str, registry: Registry) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(addr, error = %e, "failed to bind health endpoint");
            return;
        }
    };
    tracing::info!(addr, "health endpoint listening");

    loop {
        let Ok((socket, _)) = listener.accept().await else {
            continue;
        };
        let registry = registry.clone();
        tokio::spawn(handle_connection(socket, registry));
    }
}

async fn handle_connection(mut socket: TcpStream, registry: Registry) {
    let mut buf = [0u8; 1024];
    let n = match socket.read(&mut buf).await {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    // Only the request line matters, headers and body (there shouldn't
    // be one, both routes are GETs) are ignored, same "don't bother
    // parsing what you don't need" approach as risk-monitor, which
    // ignores the whole request. One route there didn't need a path at
    // all; two here does.
    let request = String::from_utf8_lossy(&buf[..n]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, content_type, body) = match path {
        "/health" => ("200 OK", "application/json", render_health(&registry).await),
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4",
            render_metrics(&registry).await,
        ),
        _ => ("404 Not Found", "text/plain", "not found".to_string()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

async fn render_health(registry: &Registry) -> String {
    let map = registry.read().await;
    let mut streams = serde_json::Map::with_capacity(map.len());
    for (key, entry) in map.iter() {
        let health = entry.health.read().await;
        streams.insert(
            key.clone(),
            serde_json::json!({
                "exchange": entry.exchange,
                "symbol": entry.symbol,
                "alive": health.worker_alive,
                "trades_processed": health.trades_processed,
                "readings_emitted": health.readings_emitted,
                "last_trade_ts_ns": health.last_trade_ts_ns,
                "last_heartbeat_ts_ns": health.last_heartbeat_ts_ns,
            }),
        );
    }
    let body = serde_json::json!({
        "status": "ok",
        "stream_count": map.len(),
        "streams": streams,
    });
    body.to_string()
}

const METRICS: &[(&str, &str, &str)] = &[
    (
        "geiger_trades_processed_total",
        "counter",
        "Trades processed for this stream since the worker started",
    ),
    (
        "geiger_readings_emitted_total",
        "counter",
        "VPIN readings emitted for this stream since the worker started",
    ),
    (
        "geiger_stream_alive",
        "gauge",
        "1 if the stream's worker is still running, 0 if its trade-ingest channel closed",
    ),
    (
        "geiger_last_trade_timestamp_ns",
        "gauge",
        "Exchange timestamp (ns) of the last trade processed for this stream",
    ),
    (
        "geiger_last_heartbeat_timestamp_ns",
        "gauge",
        "Timestamp (ns) of the last heartbeat sent for this stream",
    ),
];

/// TYPE/HELP lines are metric-level metadata, Prometheus wants them
/// declared once, not once per stream, same convention as
/// `feedhandler-core-rs::metrics::write_metrics_header`.
fn write_metrics_header(out: &mut String) {
    for (name, kind, help) in METRICS {
        let _ = writeln!(out, "# HELP {name} {help}.");
        let _ = writeln!(out, "# TYPE {name} {kind}");
    }
}

async fn render_metrics(registry: &Registry) -> String {
    let mut out = String::new();
    write_metrics_header(&mut out);

    let map = registry.read().await;
    for entry in map.values() {
        let health = entry.health.read().await;
        let exchange = &entry.exchange;
        let symbol = &entry.symbol;
        let _ = writeln!(
            out,
            r#"geiger_trades_processed_total{{exchange="{exchange}",symbol="{symbol}"}} {}"#,
            health.trades_processed
        );
        let _ = writeln!(
            out,
            r#"geiger_readings_emitted_total{{exchange="{exchange}",symbol="{symbol}"}} {}"#,
            health.readings_emitted
        );
        let _ = writeln!(
            out,
            r#"geiger_stream_alive{{exchange="{exchange}",symbol="{symbol}"}} {}"#,
            u8::from(health.worker_alive)
        );
        if let Some(ts) = health.last_trade_ts_ns {
            let _ = writeln!(
                out,
                r#"geiger_last_trade_timestamp_ns{{exchange="{exchange}",symbol="{symbol}"}} {ts}"#
            );
        }
        if let Some(ts) = health.last_heartbeat_ts_ns {
            let _ = writeln!(
                out,
                r#"geiger_last_heartbeat_timestamp_ns{{exchange="{exchange}",symbol="{symbol}"}} {ts}"#
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::StreamEntry;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    async fn test_registry() -> Registry {
        let mut map = HashMap::new();

        let (tx, _rx) = tokio::sync::broadcast::channel(8);
        let entry = StreamEntry::new("binance".to_string(), "BTCUSDT".to_string(), tx);
        {
            let mut health = entry.health.write().await;
            health.worker_alive = true;
            health.trades_processed = 42;
            health.readings_emitted = 3;
            health.last_trade_ts_ns = Some(1_700_000_000_000_000_000);
            health.last_heartbeat_ts_ns = Some(1_700_000_000_500_000_000);
        }
        map.insert("binance:BTCUSDT".to_string(), Arc::new(entry));

        let (tx2, _rx2) = tokio::sync::broadcast::channel(8);
        let dead_entry = StreamEntry::new("bybit".to_string(), "ETHUSDT".to_string(), tx2);
        // worker_alive stays false (Default), simulating a stream whose
        // trade-ingest channel closed.
        map.insert("bybit:ETHUSDT".to_string(), Arc::new(dead_entry));

        Arc::new(RwLock::new(map))
    }

    #[tokio::test]
    async fn render_health_reports_status_and_per_stream_counters() {
        let registry = test_registry().await;
        let body = render_health(&registry).await;
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(value["status"], "ok");
        assert_eq!(value["stream_count"], 2);
        assert_eq!(value["streams"]["binance:BTCUSDT"]["alive"], true);
        assert_eq!(value["streams"]["binance:BTCUSDT"]["trades_processed"], 42);
        assert_eq!(value["streams"]["bybit:ETHUSDT"]["alive"], false);
    }

    #[tokio::test]
    async fn render_metrics_declares_each_metric_once_and_reflects_counters() {
        let registry = test_registry().await;
        let out = render_metrics(&registry).await;

        for (name, _, _) in METRICS {
            assert_eq!(
                out.matches(&format!("# TYPE {name}")).count(),
                1,
                "{name} TYPE line missing or duplicated"
            );
            assert_eq!(
                out.matches(&format!("# HELP {name}")).count(),
                1,
                "{name} HELP line missing or duplicated"
            );
        }

        assert!(out
            .contains(r#"geiger_trades_processed_total{exchange="binance",symbol="BTCUSDT"} 42"#));
        assert!(
            out.contains(r#"geiger_readings_emitted_total{exchange="binance",symbol="BTCUSDT"} 3"#)
        );
        assert!(out.contains(r#"geiger_stream_alive{exchange="binance",symbol="BTCUSDT"} 1"#));
        assert!(out.contains(r#"geiger_stream_alive{exchange="bybit",symbol="ETHUSDT"} 0"#));
        // dead_entry never processed a trade, its ts gauges are absent
        // rather than printed as some placeholder zero, a real ts of 0
        // would be indistinguishable from "no trade yet".
        assert!(!out.contains(r#"geiger_last_trade_timestamp_ns{exchange="bybit""#));
    }

    #[tokio::test]
    async fn serves_health_and_metrics_and_404s_anything_else() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // release the port, run() below rebinds it

        let registry = test_registry().await;
        let addr_string = addr.to_string();
        let bind_addr = addr_string.clone();
        tokio::spawn(async move {
            run(&bind_addr, registry).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        let (status, body) = raw_get(&addr_string, "/health").await;
        assert!(status.contains("200"));
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["status"], "ok");

        let (status, body) = raw_get(&addr_string, "/metrics").await;
        assert!(status.contains("200"));
        assert!(body.contains("geiger_trades_processed_total"));

        let (status, _body) = raw_get(&addr_string, "/nope").await;
        assert!(status.contains("404"));
    }

    /// Minimal raw HTTP/1.0 GET over a plain `TcpStream`, no client
    /// library, this is testing a hand-rolled server, a hand-rolled
    /// client for it keeps the test honest about the exact bytes on the
    /// wire instead of a library normalizing around any bug.
    async fn raw_get(addr: &str, path: &str) -> (String, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.0\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8_lossy(&response).to_string();
        let mut parts = response.splitn(2, "\r\n\r\n");
        let head = parts.next().unwrap_or_default();
        let body = parts.next().unwrap_or_default().to_string();
        let status_line = head.lines().next().unwrap_or_default().to_string();
        (status_line, body)
    }
}
