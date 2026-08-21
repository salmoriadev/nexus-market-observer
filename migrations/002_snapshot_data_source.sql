ALTER TABLE market_snapshots
ADD COLUMN data_source TEXT NOT NULL DEFAULT 'nexus_live';

ALTER TABLE market_snapshots
ADD CONSTRAINT market_snapshots_data_source_check
CHECK (data_source IN ('replay', 'nexus_live'));

DROP INDEX IF EXISTS idx_market_snapshots_market_time;

CREATE INDEX idx_market_snapshots_market_source_time
ON market_snapshots (market_id, data_source, captured_at DESC);
