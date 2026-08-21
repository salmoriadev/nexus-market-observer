## Summary

Describe the behavior changed and why it belongs in this project's scope.

## Verification

List the exact commands and manual checks you ran. Do not claim live Nexus behavior unless you verified it during this change.

## Checklist

- [ ] Code, documentation, logs, and errors are in English.
- [ ] Financial values remain decimal-safe outside the Prometheus boundary.
- [ ] Replay data remains explicitly labeled and isolated from live history.
- [ ] No credentials, authenticated trading, wallets, or mainnet support were added.
- [ ] Tests cover the changed behavior and use no real Nexus dependency in CI.
- [ ] `cargo fmt`, Clippy, locked tests, and the release build pass.
- [ ] Documentation, migrations, monitoring, and smoke checks were updated when applicable.
