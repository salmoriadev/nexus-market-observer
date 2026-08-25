# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow semantic versioning while the project remains pre-1.0.

## Unreleased

## 0.1.1 - 2026-08-24

### Added

- Open-source contribution, security, issue, pull-request, and dependency-update guidance.
- RustSec dependency audit with guarded exceptions for inactive optional dependencies.

### Changed

- Snapshot identity is enforced durably in PostgreSQL across process restarts.
- Compose publishes development services on localhost only.
- CI actions use immutable revisions and read-only token permissions.
- Smoke checks validate exact health, Prometheus, and Grafana responses.
- Host-network mode preserves custom database URLs and applies alternate backend ports consistently.
- Compose contracts are checked automatically in CI.
- External PostgreSQL deployments can disable the bundled database with a dedicated Compose override.
- The official Nexus Exchange SDK is pinned to `0.10.0` while retaining API `v0.8.1`.
- Prometheus exporter, Tower HTTP, and the checkout action use their current validated releases.
- Dependabot groups only Cargo patch updates; Rust toolchain changes remain an intentional cross-file upgrade.

### Fixed

- Live market-data age no longer reports a false zero after REST refresh.
- Asymmetric books now calculate both depth sides over the same effective level count.
- HTTP shutdown and container health checks now honor explicit bounded/configured behavior.
- Security-exception reachability checks now resolve the same all-features graph used by CI.
- Live smoke validation requires a fresh, timestamped market snapshot.
- Native and integration-test Make targets honor an explicitly supplied `DATABASE_URL`.

## 0.1.0 - 2026-08-21

### Added

- Official Nexus Exchange SDK integration for public Testnet REST and stream events.
- Deterministic replay mode covering update, gap, stale, reconnect, and resync behavior.
- Decimal-safe analytics, PostgreSQL history, Axum API, Prometheus metrics, and Grafana dashboard.
- Docker Compose demo/live workflows, smoke tests, CI, ADRs, and graceful shutdown.
