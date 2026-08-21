# Architecture decisions

## ADR-001: Single-service architecture

**Context.** Collection, analytics, persistence, and read-only serving share one small market state and one deployment lifecycle.

**Decision.** Implement one Rust crate and one process with bounded Tokio tasks.

**Positive consequences.** Deployment, local development, failure analysis, and pair-programming explanations remain simple. Shared state requires no network serialization or distributed consistency mechanism.

**Limitations.** Components cannot scale independently, and a process restart temporarily affects all responsibilities. The current one-market workload does not justify that operational cost.

## ADR-002: Decimal financial values

**Context.** Binary floating point cannot exactly represent many decimal prices and quantities.

**Decision.** Use `rust_decimal::Decimal` in the adapter boundary, analytics, application state, SQLx bindings, and API models; store values as PostgreSQL `NUMERIC` and serialize them as JSON strings.

**Positive consequences.** Formulas and persisted values retain deterministic decimal semantics, including null imbalance when total depth is zero.

**Limitations.** Prometheus accepts floating samples, so a lossy projection is made only after domain calculations for visualization. The official SDK's documented JSON-number adapter remains an upstream precision boundary.

## ADR-003: Periodic snapshot persistence

**Context.** An order-book feed may emit many frames per second, while the product needs operational history rather than an event archive.

**Decision.** Persist at `SNAPSHOT_INTERVAL_SECONDS` and skip a source timestamp that was already stored.

**Positive consequences.** Database growth and write load are bounded, temporary database failures do not block collection, and history queries remain straightforward.

**Limitations.** Microstructure changes between captures are not retained, so this database is not a replay log.

## ADR-004: REST resynchronization after stream gaps

**Context.** Disconnects, lag, sequence gaps, or `out_of_sync` mean the local stream view may be incomplete. The current SDK forwards mixed snapshot/delta book payloads without a complete published reconstruction schema.

**Decision.** Discard confidence, refresh the catalog, and replace the local book with a validated REST snapshot. Valid future book events act as debounced change notifications. While the Testnet WebSocket origin remains unavailable, a rate-limited periodic REST refresh supplies real data and keeps connection health degraded.

**Positive consequences.** No delta is applied to an incomplete book, and no unverified wire algorithm is invented.

**Limitations.** REST refresh adds latency and request load compared with applying fully specified deltas.

## ADR-005: Testnet-only and public endpoints

**Context.** The project is a market-intelligence portfolio service, not a trading system. Mainnet involves real collateral and the SDK is pre-1.0.

**Decision.** Accept only `NEXUS_NETWORK=testnet`, construct `Network::Testnet`, expose no credential configuration, and call only public market endpoints.

**Positive consequences.** The service cannot accidentally trade, sign, or move real assets. Local setup needs no secrets.

**Limitations.** Mainnet data, private account state, and authenticated trading are intentionally unavailable.

## ADR-006: No broker or cache in the MVP

**Context.** One process tracks one market, PostgreSQL stores periodic history, and the API reads one in-memory state object.

**Decision.** Do not add Kafka, Redis, or another infrastructure service.

**Positive consequences.** Fewer failure modes, lower operating cost, and direct ownership of backpressure and recovery behavior.

**Limitations.** There is no cross-process event replay or shared cache. Those capabilities should be added only with a demonstrated multi-consumer or horizontal-scaling requirement.
