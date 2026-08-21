-- Existing installations may contain process-restart duplicates created
-- before durable snapshot identity was enforced. Keep the newest copy.
DELETE FROM market_snapshots AS older
USING market_snapshots AS newer
WHERE older.id < newer.id
  AND older.market_id = newer.market_id
  AND older.data_source = newer.data_source
  AND older.source_updated_at = newer.source_updated_at
  AND older.source_nonce IS NOT DISTINCT FROM newer.source_nonce;

CREATE UNIQUE INDEX idx_market_snapshots_unique_source_update
ON market_snapshots (market_id, data_source, source_updated_at, source_nonce)
NULLS NOT DISTINCT;

-- The default was needed only to backfill rows created before provenance was
-- introduced. New writes must always identify their source explicitly.
ALTER TABLE market_snapshots
ALTER COLUMN data_source DROP DEFAULT;
