# Contributing

Thank you for considering a contribution to Nexus Market Observer. This is an unofficial portfolio project with a deliberately small scope: one Rust service, one configured market, public Testnet integration, and an explicit deterministic replay mode.

## Before opening a change

- Use English for code, documentation, commit messages, logs, and API errors.
- Do not add credentials, authenticated trading, mainnet, wallets, Kafka, Redis, Kubernetes, or a frontend.
- Never present replay fixtures as live Nexus data.
- Keep financial values as `rust_decimal::Decimal` until the Prometheus boundary.
- Open an issue before a large architectural change so the scope can be discussed first.

## Local setup

Requirements: Docker with Compose, `curl`, `jq`, and the Rust toolchain selected by `rust-toolchain.toml`.

```bash
cp .env.example .env
make demo
make smoke
```

Run deterministic checks without PostgreSQL:

```bash
make fmt
make lint
make test
cargo build --release --locked
```

Run the PostgreSQL integration test while the demo database is available:

```bash
make test-integration
```

Run the dependency audit after installing its pinned tool version:

```bash
cargo install cargo-audit --version 0.22.2 --locked
make audit
make validate-compose
```

## Pull requests

Keep pull requests focused and explain the observable behavior they change. Add regression tests for bug fixes. Update the README, ADRs, migrations, fixtures, dashboard, or smoke test when their public contract changes.

Before submitting, run the same locked checks used by CI and confirm `git diff --check` is clean. Live Nexus availability is never required for CI; use `make smoke-live` only as an explicit external integration check.

The live smoke check accepts degraded connection status only while its REST market snapshot remains present and fresh. Retained stale data is never a successful live validation.

## Reporting security issues

Do not disclose vulnerability details in a public issue. Follow [SECURITY.md](SECURITY.md).
