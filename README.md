# Nexus Market Observer

An unofficial fault-tolerant Rust service for ingesting, analyzing, persisting, and monitoring Nexus Exchange market data.

Rust · Tokio · Axum · PostgreSQL · SQLx · Prometheus · Grafana

## Disclaimer

This is an independent portfolio and training project. It is **unofficial, not affiliated with Nexus, and not endorsed by Nexus**. The live mode uses only public Nexus Exchange Testnet market data and never requests credentials. Replay mode is explicitly labeled and never presented as live exchange data.

## Quick start

The deterministic demo is the recommended first experience. It exercises the complete pipeline even when the public Nexus Exchange Testnet is unavailable.

```bash
cp .env.example .env
make demo
make smoke
```

Open:

- API: <http://localhost:8080>
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000> (`admin` / `admin` by default)

Stop the demo with `make down`.

## Overview

Nexus Market Observer tracks `BTC-USDX-PERP`, validates order books, calculates decimal-safe market-quality indicators, stores periodic snapshots, and exposes current and historical state through a read-only API. It deliberately treats stale, disconnected, and incomplete market data as reliability states rather than silently manufacturing valid-looking values.

There are two explicit data sources:

| Mode | Purpose | Upstream behavior |
|---|---|---|
| `replay` | Deterministic portfolio demo and CI smoke test | Never contacts Nexus; reports network `demo` |
| `nexus_live` | Official Nexus SDK and public Exchange Testnet integration | Uses real public data or remains degraded |

The replay fixture is not claimed to be recorded history. Its initial shape is based on the sanitized official REST example documented in the tests; subsequent frames and failures are a deterministic demonstration scenario.

## Problem

Exchange market data is transient and failure-prone. A useful backend must distinguish a current, consistent book from stale or incomplete data, preserve financial precision, remain observable while dependencies recover, and avoid turning a high-frequency feed into unnecessary database growth.

The public Nexus Exchange Testnet currently makes a live portfolio demonstration unreliable: REST availability has been intermittent and official SDK 0.9.1 exposes no usable public Testnet WebSocket origin. Replay mode makes the internal system reproducible without weakening the honesty of live mode.

## Architecture

```mermaid
flowchart TD
    Live["Nexus Exchange Testnet\nOfficial SDK · REST · WebSocket events"]
    Replay["Deterministic replay fixture\nsnapshot · updates · gap · disconnect"]
    Select{"MARKET_DATA_SOURCE"}
    Collector["Tokio collector\nvalidation · resync · cancellation"]
    State["Arc&lt;RwLock&lt;MarketState&gt;&gt;\nDecimal order book analytics"]
    API["Axum read-only API"]
    DB[("PostgreSQL\nsource-isolated history")]
    Prom["Prometheus"]
    Grafana["Grafana"]

    Live --> Select
    Replay --> Select
    Select --> Collector
    Collector --> State
    State --> API
    State -->|periodic deduplicated snapshot| DB
    DB -->|source-filtered history| API
    API -->|/metrics| Prom
    Prom --> Grafana
```

One crate produces one service. Collection, persistence, stale detection, and HTTP serving are bounded Tokio tasks sharing one in-memory state. No state lock is held during Nexus I/O, SQL, or message waits.

## Features

- Official `nexus-exchange` SDK integration pinned to `0.9.1`.
- Explicit replay and Testnet-live modes with provenance in API, PostgreSQL, metrics, logs, and isolated Compose volumes.
- Deterministic replay of snapshot, updates, sequence gap, stale state, REST-style resync, disconnect, reconnect, and recovery.
- `Starting`, `Connecting`, `Resyncing`, `Healthy`, `Stale`, `Reconnecting`, `Degraded`, and `ShuttingDown` states.
- Best bid/ask, midpoint, spread, spread bps, top-ten depth, and order-book imbalance.
- `rust_decimal::Decimal` through the financial domain, PostgreSQL `NUMERIC`, and JSON strings.
- Periodic duplicate-resistant persistence and newest-first bounded history.
- Database failure recovery, stale detection, Prometheus metrics, Grafana, Docker Compose, CI, and coordinated shutdown.

## Technology stack

- Rust 1.88.0, Tokio, Axum, Serde, thiserror, tower-http, and tracing.
- Official `nexus-exchange` Rust SDK **0.9.1** and Nexus Exchange API **v0.8.1**.
- SQLx and PostgreSQL 17.
- rust_decimal, metrics, Prometheus 3, and Grafana 12.
- Docker, Docker Compose, and GitHub Actions.

### Verified Nexus contract

The integration was inspected on 2026-08-20 against the official [nexus-xyz organization](https://github.com/nexus-xyz), [`nexus-exchange-rs`](https://github.com/nexus-xyz/nexus-exchange-rs), and [`nexus-exchange-api`](https://github.com/nexus-xyz/nexus-exchange-api):

- SDK release commit: `0e797a3984e8886e1cf5ab018c2c3589681813c7`.
- API revision: `19f808110f80027009b43fbc0768702a7315a658`; OpenAPI version `0.8.1`.
- SDK minimum Rust version `1.86`; the locked application graph uses Rust `1.88.0`.
- Public channel name `book`; envelopes include `subscribed`, `event`, and `out_of_sync` with stream sequences.
- The payload is opaque and may represent snapshots or deltas, so the live adapter uses valid events as change notifications and performs a debounced REST refresh rather than inventing a reconstruction algorithm.
- SDK 0.9.1 reports no usable Testnet WebSocket origin (`ENG-3398`). Live mode retries the SDK subscription and uses rate-limited REST refresh while exposing degraded connection health.

## Running the live integration

Do not run demo and live simultaneously because they publish the same local ports.

```bash
make down
make live
make smoke-live
```

`make smoke-live` is intentionally allowed to fail when Nexus does not provide a usable snapshot. That external check does not block CI. Stop it with `make down-live`.

On Linux hosts whose firewall blocks outbound traffic from Docker bridges, use the supplied host-network override:

```bash
COMPOSE_FILE=docker-compose.yml:docker-compose.host-network.yml make live
```

## Configuration

| Variable | Default/example | Validation |
|---|---|---|
| `APP_ENV` | `development` | `development`, `production`, or `test`; production logs JSON |
| `HTTP_HOST` | `0.0.0.0` | Valid IP address |
| `HTTP_PORT` | `8080` | Valid TCP port |
| `DATABASE_URL` | `postgres://nexus:nexus@postgres:5432/nexus_market` | Required |
| `MARKET_DATA_SOURCE` | `replay` | `replay` or `nexus_live` |
| `NEXUS_NETWORK` | `testnet` | Only `testnet`; mainnet fails startup |
| `NEXUS_MARKET` | `BTC-USDX-PERP` | Non-empty; replay provides only this fixture |
| `SNAPSHOT_INTERVAL_SECONDS` | `5` | Greater than zero |
| `STALE_AFTER_SECONDS` | `15` | Greater than the snapshot interval |
| `REST_TIMEOUT_SECONDS` | `10` | Greater than zero |
| `RUST_LOG` | `info,nexus_market_observer=debug` | tracing-subscriber filter |
| `GRAFANA_ADMIN_USER` | `admin` | Local Compose default |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Change outside local development |
| `GRAFANA_PORT` | `3000` | Published Grafana port |

Application environment access is centralized in `Config`. There are no API-key, secret-key, private-key, mainnet, wallet, or trading settings.

## API

```bash
curl -s http://localhost:8080/health | jq
curl -s http://localhost:8080/markets | jq
curl -s http://localhost:8080/markets/BTC-USDX-PERP | jq
curl -s "http://localhost:8080/markets/BTC-USDX-PERP/history?limit=10" | jq
curl -s http://localhost:8080/metrics
```

| Endpoint | Purpose |
|---|---|
| `GET /health` | Process, source, freshness, connection, and PostgreSQL status |
| `GET /markets` | Known catalog and tracked-market marker |
| `GET /markets/:symbol` | Current decimal analytics for the configured market |
| `GET /markets/:symbol/history?limit=100` | Newest-first source-isolated history; range 1–500 |
| `GET /metrics` | Prometheus text exposition |

Every JSON market response contains `data_source`. Health distinguishes replay from live without pretending the demo is Nexus Testnet:

```json
{
  "status": "healthy",
  "service": "nexus-market-observer",
  "data_source": {"mode": "replay", "network": "demo"},
  "market_data": {
    "connection": "connected",
    "market": "BTC-USDX-PERP",
    "stale": false
  },
  "database": {"status": "connected"}
}
```

Financial values are strings such as `"73807.25"`, never binary floating-point JSON numbers. Errors use a consistent envelope with a stable code and safe message.

## Data model

`markets` stores Nexus-compatible identifiers and symbols. `market_snapshots` stores validated analytics, source provenance, nonce, source timestamp, and capture timestamp. Financial columns are `NUMERIC`; imbalance is nullable. The `(market_id, data_source, captured_at DESC)` index supports newest-first source-isolated history.

Compose project names create separate PostgreSQL, Prometheus, and Grafana volumes for demo and live. Database filtering provides a second isolation boundary.

## Reliability model

- PostgreSQL is required for startup migrations with approximately 30 seconds of retry.
- A temporary database failure after startup does not stop collection; the persistence worker records the error and recovers through the SQLx pool.
- Nexus is not required for HTTP process liveness. Live collection retries and reports unavailable data honestly.
- Books require both sides, valid prices and quantities, and `best_bid < best_ask`.
- Lag, gaps, disconnects, and `out_of_sync` invalidate confidence and require a full resync.
- Stale detection retains old values and exposes their age instead of replacing them with zero.
- SIGINT/SIGTERM cancels collectors, persistence and stale detection, drains HTTP, closes the SDK subscription and PostgreSQL pool, and enforces a bounded worker timeout.

## Observability

Production logs are JSON; development logs are compact. Prometheus exposes source-labeled connection, reconnect, lag, update, resync, data-age, midpoint, spread, imbalance, and persistence series plus database and HTTP metrics. The provisioned `Nexus Market Observer` dashboard contains ten operational panels and works with replay or live data.

Prometheus necessarily accepts floating samples. Conversion happens only at the telemetry boundary; financial reception, computation, storage, and API serialization remain decimal-safe.

## Testing

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo build --release --locked
make demo
make smoke
```

Tests cover analytics precision and invalid books, status transitions, controlled stale detection, official SDK REST and stream fixtures, replay parsing and recovery, in-memory Axum routes, migrations, PostgreSQL precision, source-isolated history, container monitoring, and graceful recovery. CI never contacts Nexus.

See [docs/demo-script.md](docs/demo-script.md) for a concise portfolio walkthrough.

## Technical decisions

Rust makes ownership, cancellation and failure paths explicit. The official SDK avoids inventing an upstream contract. PostgreSQL provides durable `NUMERIC` history. Periodic persistence bounds write volume. Full resync is safer than applying a delta to an untrusted book. One service remains sufficient for one market and keeps the code explainable.

See [docs/decisions.md](docs/decisions.md) for the ADRs.

## Trade-offs

- Replay provides reproducibility but is not evidence of current Testnet availability.
- REST refresh is less timely than a fully documented delta stream but avoids guessed financial state.
- Periodic snapshots intentionally omit individual microstructure events.
- One configured market avoids premature multi-market coordination.
- PostgreSQL is a startup dependency because schema correctness is critical.
- The official SDK's JSON-number adapter remains an upstream precision boundary before values enter this service's `Decimal` domain.

## Limitations

- Nexus SDK 0.9.1 has no usable public Testnet WebSocket origin.
- The public Exchange Testnet REST endpoint has shown intermittent availability.
- The published book event payload is not sufficiently specified for safe local delta reconstruction.
- Only one market is tracked per process.
- Replay frames are deterministic demonstration fixtures, not historical market data.

## Future improvements

- Enable the official Testnet WebSocket when Nexus publishes a usable origin.
- Apply typed deltas only after a complete official payload contract exists.
- Add retention and downsampling after measuring long-running storage growth.
- Consider multi-market tracking only after measuring REST budget, memory and metric cardinality.

## License

MIT. See [LICENSE](LICENSE).
