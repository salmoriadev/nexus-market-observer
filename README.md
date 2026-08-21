# Nexus Market Intelligence

Reliable Rust service for streaming, analyzing, persisting, and observing Nexus Exchange market data.

## Disclaimer

This is an independent portfolio and training project. It is **unofficial, not affiliated with Nexus, and not endorsed by Nexus**. It uses public Nexus Exchange Testnet market data only. Testnet funds have no monetary value.

## Overview

Nexus Market Intelligence discovers public Testnet markets, tracks one configured perpetual market, validates REST order-book snapshots, calculates market-quality indicators with decimal arithmetic, stores periodic observations in PostgreSQL, and exposes the current state through an Axum API. Prometheus and a provisioned Grafana dashboard make the collector, database, and API behavior visible.

The default tracked market is `BTC-USDX-PERP`. The service has no wallet, credentials, authenticated routes, order submission, or mainnet support.

## Problem

Exchange market data is transient and failure-prone. A useful backend must distinguish a current, internally consistent order book from stale or incomplete data; preserve financial precision; remain queryable while upstream systems recover; and avoid turning a high-frequency feed into an unnecessarily large database.

This service demonstrates that reliability boundary in one explainable Rust process.

## Architecture

```mermaid
flowchart TD
    Nexus["Nexus Exchange Testnet\nOfficial Rust SDK\nREST + WebSocket events"]
    Collector["Nexus adapter and collector\nTokio tasks\nresync + stale detection"]
    State["Arc<RwLock<MarketState>>\nvalidated in-memory order book\nDecimal analytics"]
    API["Axum read-only API"]
    DB[("PostgreSQL\nSQLx migrations + history")]
    Prom["Prometheus"]
    Grafana["Grafana dashboard"]

    Nexus --> Collector
    Collector --> State
    State --> API
    State -->|periodic deduplicated snapshot| DB
    DB -->|history| API
    API -->|/metrics| Prom
    Prom --> Grafana
```

There is one crate, one deployable service, and three bounded background responsibilities: market collection, periodic persistence, and stale detection. No lock is held across Nexus I/O or SQL awaits.

## Features

- Public Nexus Exchange Testnet catalog and order-book snapshots through the official Rust SDK.
- Explicit `Starting`, `Connecting`, `Resyncing`, `Healthy`, `Stale`, `Reconnecting`, `Degraded`, and `ShuttingDown` states.
- Pure best-price, midpoint, spread, spread-bps, top-ten depth, and imbalance calculations.
- `rust_decimal::Decimal` throughout the financial domain, PostgreSQL `NUMERIC`, and JSON decimal strings.
- REST resynchronization after connection, disconnect, lag, sequence gap, or `out_of_sync`.
- Rate-limited REST refresh fallback when the current public Testnet WebSocket cannot be used.
- Periodic, duplicate-resistant PostgreSQL snapshots rather than one insert per stream frame.
- Read-only Axum API, central error envelope, bounded history pagination, and graceful shutdown.
- Structured logs, Prometheus metrics, a provisioned Grafana dashboard, Docker Compose, CI, and smoke tests.

## Technology stack

- Rust 1.88.0, Tokio, Axum, Serde, thiserror, and tower-http. The SDK itself declares Rust 1.86 as its minimum; the locked current dependency graph requires Rust 1.88.
- Official `nexus-exchange` Rust SDK **0.9.1**, pinned exactly in `Cargo.toml` and transitively pinned in `Cargo.lock`.
- Nexus Exchange API specification **v0.8.1**.
- SQLx and PostgreSQL 17.
- rust_decimal, tracing, metrics, Prometheus 3, and Grafana 12.
- Docker, Docker Compose, and GitHub Actions.

### Nexus source verification

The integration was re-verified on 2026-08-20 against the official [nexus-xyz organization](https://github.com/nexus-xyz), [`nexus-exchange-rs`](https://github.com/nexus-xyz/nexus-exchange-rs), and [`nexus-exchange-api`](https://github.com/nexus-xyz/nexus-exchange-api):

- SDK release: `nexus-exchange` 0.9.1, release commit `0e797a3984e8886e1cf5ab018c2c3589681813c7`.
- SDK API pin: `v0.8.1`; SDK minimum Rust version: 1.86.
- API repository revision inspected: `19f808110f80027009b43fbc0768702a7315a658`; OpenAPI version: 0.8.1.
- The official REST examples returned four markets and a live `BTC-USDX-PERP` order book with a nonce.
- The current typed public channel is named `book` (not the older `orderbook` selector). Server frames use `op` envelopes: `subscribed` with `seq_at_join`, `event` with monotonic `seq` and opaque `payload`, and `out_of_sync` when a resume cursor falls outside the replay buffer.

The SDK states that book payloads may be snapshots or deltas but deliberately forwards them as raw JSON; the API specification does not define enough book-payload fields to reconstruct levels safely. More importantly, SDK 0.9.1 currently reports no usable Testnet WebSocket origin (`ENG-3398`). Its official WebSocket example exits with that diagnostic. The adapter therefore treats future valid `book` frames as change notifications, detects stream-sequence gaps, and performs debounced REST resynchronization. Today it retries the SDK subscription every 30 seconds and refreshes real REST snapshots at `SNAPSHOT_INTERVAL_SECONDS`; connection health remains `degraded` instead of pretending a stream exists. No synthetic market data is ever emitted.

## Quick start

Requirements: Docker Engine with Compose and outbound HTTPS access to the Nexus Exchange Testnet.

```bash
cp .env.example .env
docker compose up --build
```

In another terminal:

```bash
make smoke
```

Services are available at:

- Backend: <http://localhost:8080>
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000> (development default `admin` / `admin`)
- PostgreSQL: `localhost:5432`

The Grafana credential is intentionally a local development default. Change it in `.env` outside local use.

The default Compose file uses its private bridge network. On Linux hosts where local firewall policy blocks outbound traffic from Docker bridges, set `COMPOSE_FILE=docker-compose.yml:docker-compose.host-network.yml`; the optional override keeps the same service ports while routing external Testnet traffic through the host network.

For a native Rust run with PostgreSQL already listening locally:

```bash
cp .env.example .env
DATABASE_URL_LOCAL=postgres://nexus:nexus@localhost:5432/nexus_market make run
```

## Configuration

| Variable | Default/example | Validation |
|---|---|---|
| `APP_ENV` | `development` | `development`, `production`, or `test`; production logs JSON |
| `HTTP_HOST` | `0.0.0.0` | Valid IP address |
| `HTTP_PORT` | `8080` | Valid TCP port |
| `DATABASE_URL` | `postgres://nexus:nexus@postgres:5432/nexus_market` | Required |
| `NEXUS_NETWORK` | `testnet` | Only `testnet` is accepted; any other value fails startup |
| `NEXUS_MARKET` | `BTC-USDX-PERP` | Non-empty and must exist in the live catalog |
| `SNAPSHOT_INTERVAL_SECONDS` | `5` | Greater than zero; controls REST fallback and DB capture cadence |
| `STALE_AFTER_SECONDS` | `15` | Must be greater than the snapshot interval |
| `REST_TIMEOUT_SECONDS` | `10` | Greater than zero |
| `RUST_LOG` | `info,nexus_market_intelligence=debug` | tracing-subscriber filter |
| `GRAFANA_ADMIN_USER` | `admin` | Compose/Grafana only |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Compose/Grafana only; change outside local development |
| `GRAFANA_PORT` | `3000` | Published Grafana port; change if the host port is occupied |

All application environment access is centralized in `Config`. The service has no API-key, secret-key, or private-key setting.

## API

The API is read-only.

```bash
curl -s http://localhost:8080/health | jq
curl -s http://localhost:8080/markets | jq
curl -s http://localhost:8080/markets/BTC-USDX-PERP | jq
curl -s "http://localhost:8080/markets/BTC-USDX-PERP/history?limit=10" | jq
curl -s http://localhost:8080/metrics
```

| Endpoint | Purpose |
|---|---|
| `GET /health` | Process, Nexus freshness/connection, and PostgreSQL status. Returns 503 only before a usable snapshot exists. |
| `GET /markets` | Last known public catalog and tracked-market marker. |
| `GET /markets/:symbol` | Current tracked-market analytics; unknown or untracked symbols return 404. |
| `GET /markets/:symbol/history?limit=100` | Newest-first persisted history. Limit range is 1–500. |
| `GET /metrics` | Prometheus text exposition. |

Errors use one shape:

```json
{
  "error": {
    "code": "MARKET_NOT_FOUND",
    "message": "Market BTC-INVALID was not found"
  }
}
```

Financial values in API JSON are strings, for example `"117233.40"`, never binary floating-point JSON numbers.

## Data model

`markets` stores the stable Nexus market identifier and unique symbol. `market_snapshots` stores validated analytics, source nonce and source timestamp, plus the database capture timestamp. Financial columns are PostgreSQL `NUMERIC`; `imbalance` is nullable when total top-N depth is zero. The `(market_id, captured_at DESC)` index supports newest-first history.

Migrations are embedded in the binary and also copied into the runtime image for inspection.

## Reliability model

- PostgreSQL is required for startup migrations. Connection attempts are bounded to approximately 30 seconds so an orchestrator can restart a failed container.
- Nexus is not required for the HTTP listener. Collector failures update state and retry without terminating the process.
- A valid REST snapshot requires both sides, positive prices, non-negative quantities, and `best_bid < best_ask`.
- Connected, lagged, sequence-gap, and out-of-sync events invalidate local confidence and cause a fresh catalog plus REST snapshot before returning to healthy.
- The current WebSocket limitation leaves the service degraded while real REST snapshots remain usable and persistible.
- Stale detection retains old values and marks their age; it never replaces missing data with zero.
- Persistence copies the state before SQL, skips the same source update, records failures, and relies on the SQLx pool to recover automatically when PostgreSQL returns.
- SIGINT/SIGTERM marks `shutting_down`, cancels background work, closes the SDK subscription, gracefully drains HTTP, waits with a bounded timeout, and closes the pool.

## Observability

Development logs are compact; production logs are JSON. High-frequency updates are debug events and metrics rather than info-log noise.

Prometheus collects connection state, reconnects, lag, updates, resyncs, data age, midpoint, spread bps, imbalance, persistence, database errors, request counts, and request latency. The provisioned `Nexus Market Intelligence` Grafana dashboard contains panels for all ten requested operational views.

Prometheus gauges require floating-point samples by protocol/library design. That conversion occurs only at the observability boundary; all received, computed, persisted, and API-serialized financial values remain `Decimal`/`NUMERIC`/strings.

## Testing

```bash
make fmt
make lint
make test
make build
```

The suite covers analytics edge cases and precision, state transitions and controlled-time stale detection, official Nexus snapshot/envelope fixtures, connection/lag/out-of-sync classification, API responses and errors through an in-memory Router, and PostgreSQL migration/persistence/history behavior. PostgreSQL integration tests run when `DATABASE_URL` is available; CI always provides it. No CI test contacts Nexus.

Full container smoke test:

```bash
make up
make smoke
make down
```

## Technical decisions

Rust makes ownership, cancellation, bounded concurrency, and explicit error paths visible while delivering predictable performance. The official SDK prevents a locally invented API contract and already provides typed Decimal REST models, retry behavior, and bounded WebSocket channels. PostgreSQL supplies durable, queryable `NUMERIC` history and mature recovery behavior. Decimal arithmetic avoids binary floating-point errors in financial state.

Periodic persistence captures useful time-series history without a row per high-frequency frame. Resynchronization after disconnect or lag is simpler and safer than applying data to a book whose completeness is unknown. One service keeps the portfolio project operable and explainable; its workload does not justify distributed coordination.

See [docs/decisions.md](docs/decisions.md) for the ADRs.

## Trade-offs

- REST refresh is less timely and uses more HTTP requests than a fully specified live delta feed, but it is correct under the currently published contract and unavailable Testnet WebSocket.
- In-memory state supports one configured market, intentionally avoiding multi-market coordination.
- Periodic snapshots omit individual microstructure events in exchange for bounded storage and simpler history.
- PostgreSQL is a startup dependency because schema correctness is considered critical; it becomes non-fatal after startup.
- The official SDK parses current REST book number fields into `Decimal` through its documented float adapter. The domain does not introduce `f32`/`f64`, but wire-number precision is ultimately bounded by the upstream SDK and the exchange tick/lot grid.

## Limitations

- Nexus SDK 0.9.1 has no usable public Testnet WebSocket origin, so the deployed service currently runs the documented REST refresh fallback and reports the connection as degraded.
- The published stream contract leaves the mixed snapshot/delta book payload opaque; the service deliberately does not reconstruct it from guessed fields.
- Only one market is tracked at a time.
- History begins when this service starts; Nexus does not provide this service's derived historical series.

## Future improvements

- Enable the SDK `Channel::book` subscription when Nexus publishes a usable Testnet WebSocket origin.
- Replace REST invalidation refreshes with typed snapshot/delta application only after Nexus publishes a complete book-payload schema.
- Add configurable multi-market tracking after measuring memory, REST budget, metric cardinality, and database load.
- Add retention/downsampling policies for long-running deployments.

## License

MIT. See [LICENSE](LICENSE).
