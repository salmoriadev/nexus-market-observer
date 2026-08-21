# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow semantic versioning while the project remains pre-1.0.

## Unreleased

### Added

- Open-source contribution, security, issue, pull-request, and dependency-update guidance.
- RustSec dependency audit with guarded exceptions for inactive optional dependencies.

### Changed

- Snapshot identity is enforced durably in PostgreSQL across process restarts.
- Compose publishes development services on localhost only.
- CI actions use immutable revisions and read-only token permissions.
- Smoke checks validate exact health, Prometheus, and Grafana responses.

### Fixed

- Live market-data age no longer reports a false zero after REST refresh.
- Asymmetric books now calculate both depth sides over the same effective level count.
- HTTP shutdown and container health checks now honor explicit bounded/configured behavior.

## 0.1.0 - 2026-08-21

### Added

- Official Nexus Exchange SDK integration for public Testnet REST and stream events.
- Deterministic replay mode covering update, gap, stale, reconnect, and resync behavior.
- Decimal-safe analytics, PostgreSQL history, Axum API, Prometheus metrics, and Grafana dashboard.
- Docker Compose demo/live workflows, smoke tests, CI, ADRs, and graceful shutdown.
