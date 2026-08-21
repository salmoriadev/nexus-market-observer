# Portfolio demo script

This walkthrough is designed for a 60–90 second screen recording. Replay data must remain visibly labeled throughout the recording.

1. Run `cp .env.example .env && make demo` and show the four healthy containers with `docker compose -p nexus-market-observer-demo ps`.
2. Open `/health` and point out `data_source.mode = replay`, `network = demo`, the explicit service status, source connection, update age, and database status.
3. Open `/markets/BTC-USDX-PERP` and briefly explain decimal strings, spread bps, top-ten depth, and imbalance.
4. Open `/history?limit=10` and show periodic PostgreSQL snapshots rather than one row per update.
5. Open the Grafana dashboard and show changing midpoint, spread, imbalance, persistence, API traffic, the injected sequence gap, and reconnect counter.
6. Explain that the replay collector cycles through healthy, stale, resyncing, reconnecting, and healthy without presenting fixture data as live Nexus data.
7. End with `make smoke`, then mention that `make live` preserves the official SDK integration and correctly becomes degraded when the public Testnet is unavailable.

Suggested closing sentence:

> The demo proves the deterministic internal pipeline; live mode separately proves the official Nexus integration without hiding upstream outages or fabricating market data.
