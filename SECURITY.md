# Security policy

## Supported versions

Security fixes are applied to the latest code on `main` and the latest `0.1.x` release. Older development snapshots are not supported.

## Reporting a vulnerability

After this repository is published on GitHub, use **Security → Report a vulnerability** to submit details privately. Include the affected revision, impact, reproduction steps, and any suggested mitigation.

If private vulnerability reporting is not available, open a minimal issue asking the maintainer to establish a private channel. Do not include exploit details, credentials, private keys, or sensitive logs in that issue.

You should receive an acknowledgement within seven days. The maintainer will validate the report, coordinate a fix and disclosure timeline, and credit the reporter when requested and appropriate.

## Scope and security boundaries

- The service is Testnet-only and exposes no authenticated trading or wallet operations.
- Replay data is deterministic demo data and is labeled `replay` / `demo` in every public boundary.
- Docker Compose is a localhost development topology, not a production deployment.
- Development credentials in `.env.example` are intentionally non-secret and must not be reused outside local development.

`cargo audit` scans the lockfile in CI. Two advisories are documented in `.cargo/audit.toml` because Cargo records inactive optional dependencies: `rkyv` is optional serialization support and `rsa` belongs to SQLx's disabled MySQL driver. `scripts/audit.sh` resolves all features and fails if either crate becomes active in the normal, build, or development graph.
