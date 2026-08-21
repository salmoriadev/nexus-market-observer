CREATE TABLE IF NOT EXISTS markets (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    id BIGSERIAL PRIMARY KEY,
    market_id TEXT NOT NULL REFERENCES markets(id),
    best_bid NUMERIC NOT NULL,
    best_ask NUMERIC NOT NULL,
    mid_price NUMERIC NOT NULL,
    spread NUMERIC NOT NULL,
    spread_bps NUMERIC NOT NULL,
    bid_depth NUMERIC NOT NULL,
    ask_depth NUMERIC NOT NULL,
    imbalance NUMERIC,
    depth_levels INTEGER NOT NULL,
    source_nonce BIGINT,
    source_updated_at TIMESTAMPTZ NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_market_time
ON market_snapshots (market_id, captured_at DESC);

