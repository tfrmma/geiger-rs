# geiger-rs

Real-time order-flow toxicity for perpetual futures: VPIN on a volume
clock, via Bulk Volume Classification, streamed as a continuous score
for market makers to widen or pull quotes on.

Companion to [`feedhandler-core-rs`](https://github.com/tfrmma/feedhandler-core-rs)
and [`boros-mm`](https://github.com/tfrmma/boros-mm), but a standalone
repo: it depends on `feedhandler-core-rs` for shared types, it doesn't
modify it.

## What it does

Easley, Lopez de Prado & O'Hara's VPIN (*"Flow Toxicity and Liquidity in
a High-Frequency World"*, Review of Financial Studies, 2012) buckets
trades by volume instead of wall-clock time, classifies each bucket's
buy/sell split probabilistically from its price change (Bulk Volume
Classification), and tracks the resulting order imbalance over a
rolling window. `geiger-rs` computes this in real time off live
Binance/Bybit/Hyperliquid perpetual futures trade feeds and streams the
score to any consumer over a WebSocket.

This is a signal service, not a strategy backtester. Testing whether a
market maker that reacts to this signal is actually profitable belongs
in a proper backtester with fill simulation; `tools/calibrate` here only
tunes the estimator itself (bucket size, window length) against a
captured trade tape.

## Architecture

```
Binance / Bybit / Hyperliquid  (perpetual futures trade streams)
            │
            ▼
   crates/trade-ingest          normalized trades, WS adapters, capture/replay
            │
            ▼
   crates/vpin-engine           volume-clock bucketer, BVC, rolling VPIN + CDF
            │
            ▼
 services/toxicity-service      per (exchange, symbol) worker, WebSocket fan-out
            │
    ┌───────┼────────────┬──────────────┐
    ▼       ▼            ▼              ▼
 crates/  bindings/   services/    tools/calibrate
 toxicity- py-vpin    dashboard    (offline parameter
 client-rs (Python,   (planned)    sweep against a
 (Rust)    pyo3)                   captured tape)
```

`crates/backoff` is a small reconnect-with-backoff utility shared by the
exchange adapters and the Rust client.

Only trades feed the VPIN calculation, not the order book, VPIN needs
executed volume classified by direction, which is a different data
stream from L2 book depth.

## Method notes

VPIN's absolute level isn't comparable across instruments (different
bucket sizes, different volume profiles), what's actually meant to be
consumed is its CDF (percentile rank within its own recent history),
which `vpin-engine` computes and exposes alongside the raw score.

Worth knowing before wiring this into anything that moves real spreads:
Andersen & Bondarenko (2014, *Journal of Financial Markets*) found VPIN's
predictive power for short-term volatility is largely a mechanical
byproduct of trading intensity rather than genuine informed-trading
detection. Every reading includes `trades_in_bucket` specifically so a
consumer can control for intensity before reacting to the raw score.
`IntensityAdjustedCdf` is a reference implementation of one way to do
that: it ranks a reading against similarly-busy buckets instead of all
buckets, so a spike that's unremarkable *for how busy things are right
now* doesn't get treated the same as one that's genuinely unusual.

Quantities from all three exchanges are in base-asset units (Binance
USDⓈ-M futures, Bybit linear, Hyperliquid perps all confirmed against
each venue's own contract-specification docs, not assumed), so
`bucket_volume` means the same thing across venues, this isn't
COIN-margined-style fixed-notional contract counts.

## Components

| Path | What it is |
|---|---|
| `crates/vpin-engine` | Pure compute: volume-clock bucketer, BVC classification, rolling sigma, rolling VPIN, CDF transform, plus `IntensityAdjustedCdf` (a reference intensity correction). No I/O, no exchange-specific code. |
| `crates/trade-ingest` | WS adapters: Binance USDⓈ-M futures (`aggTrade`), Bybit v5 linear (`publicTrade`), Hyperliquid perps (`trades`). Includes `capture`, a bit-stable recorder/reader for building trade tapes offline. |
| `crates/backoff` | Exponential backoff shared by the ingest adapters and the Rust client. |
| `crates/toxicity-client-rs` | Rust subscriber. `ToxicityClient::connect(...)` runs in the background; `.state()` is synchronous (backed by a `watch` channel), safe to call from a hot quoting loop. |
| `services/toxicity-service` | The service binary. Wires trade streams into per-(exchange, symbol) `vpin-engine` instances and fans out readings over a WebSocket as JSON: continuous score and CDF, not a discretized tier, that policy is left to each consumer. |
| `bindings/py-vpin` | Python bindings (`pyo3`/`maturin`) directly over `vpin-engine`, for Python-based consumers. |
| `tools/calibrate` | Offline: replay a captured trade tape through `vpin-engine` across a grid of `(bucket_volume, window)` values, printing descriptive stats plus how often BVC's classification actually matches the real taker side (`bvc_accuracy`/`bvc_mae`), to help pick parameters for a given instrument. |
| `services/dashboard` | Planned, not yet implemented. |

## Getting started

Requires Rust 1.85+ (`feedhandler-core-rs` uses newer language features
that set the floor).

```bash
cargo build --workspace
cargo test --workspace
```

### Running the service

Settings come from environment variables; the list of streams to track
comes from a JSON file:

```bash
export GEIGER_STREAMS_FILE=services/toxicity-service/streams.example.json
export GEIGER_BIND_ADDR=0.0.0.0:9700   # default shown
export GEIGER_HEARTBEAT_SECS=5          # default shown
export GEIGER_AUTH_TOKEN=some-shared-secret  # optional, unset = no auth
cargo run -p toxicity-service
```

The values in `streams.example.json` are illustrative placeholders, not
a calibration. `bucket_volume` in particular is instrument-specific, run
`tools/calibrate` against real captured data before using this live.

### Consuming from Rust

```rust
let client = toxicity_client_rs::ToxicityClient::connect(
    "ws://127.0.0.1:9700", "binance", "BTCUSDT",
    Duration::from_secs(15), backoff::BackoffConfig::default(),
);

match client.state() {
    ToxicityState::Live { vpin_cdf: Some(cdf), .. } if cdf > 0.9 => { /* widen */ }
    ToxicityState::Stale => { /* feed is down, assume the worst */ }
    _ => {}
}
```

`Stale` and `Live` are distinct on purpose: a dead feed and a genuinely
low toxicity reading should never be indistinguishable to a caller.

### Consuming from Python

```bash
cd bindings/py-vpin
maturin develop   # or `maturin build` and install the wheel
```

```python
from vpin import VpinEngine

engine = VpinEngine(bucket_volume=50.0, sigma_window=50, vpin_window=50, cdf_window=250)
reading = engine.push_trade(price=100.5, volume=2.3, ts_ns=...)
if reading is not None and reading.vpin is not None:
    print(reading.vpin, reading.vpin_cdf)
```

### Calibrating

```bash
cargo run -p calibrate -- trades.cap \
  --bucket-volumes 10,25,50,100 --windows 20,50,100 --cdf-window 250
```

Expects a capture file written with `trade_ingest::capture::TradeRecorder`.
`calibrate` reports descriptive statistics (mean/spread of VPIN, warmup
fraction, average time between bucket closes) plus `bvc_accuracy` and
`bvc_mae`: how often BVC's probabilistic buy/sell call agrees with the
real taker side each exchange adapter captures but `vpin-engine` never
looks at, and how far off its buy fraction runs on average. It does not
validate against labeled historical toxic events, that needs ground
truth specific to your instrument.

## Operational notes

- `GEIGER_AUTH_TOKEN` is unset by default, meaning the WS endpoint has no
  auth and anyone who can reach the bind address can subscribe. Set it
  for anything that isn't purely localhost/VPN-only; the server checks
  it as a `Authorization: Bearer <token>` header during the WS handshake
  itself, an unauthorized client never completes the connection.
- The exchange adapters have not been run against live exchange
  connections as part of this repo's own test suite; wire parsing is
  tested against literal examples from each venue's documentation.
  Verify against testnet or a throwaway symbol before running against
  real markets.
- Reconnect backoff (`BackoffConfig`: base delay, max delay, doubling
  in between) defaults to 250ms/30s, checked against each venue's
  documented per-IP connection-attempt limits. Override per-stream via
  `backoff_base_ms`/`backoff_max_secs` in `GEIGER_STREAMS_FILE` if you're
  running many symbols on one exchange from one IP; the default doesn't
  coordinate reconnect timing across streams, so a shared outage can
  still cause a burst of simultaneous reconnect attempts.
- Hyperliquid's `side` field semantics (`"A"`/`"B"`) are documented here
  based on third-party API references rather than Hyperliquid's own
  docs, which type the field without explaining it. Worth confirming
  independently if fill direction ever looks wrong.
- Binance USDⓈ-M futures requires the routed WebSocket endpoints
  (`/market/...`); the legacy unrouted URLs some older examples online
  still reference are decommissioned.

## License

MIT, see `LICENSE`.
