//! Stable JSON response models for the public API.
//!
//! Financial values are represented as decimal strings. This preserves their exact scale and
//! avoids silently converting them through JSON's floating-point number model.

use serde::Serialize;

/// Process, source, market-data, and database health summary.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    /// Current service lifecycle status.
    pub status: String,
    /// Stable service identifier.
    pub service: &'static str,
    /// Build-time Cargo package version.
    pub version: &'static str,
    /// Monotonic process uptime.
    pub uptime_seconds: u64,
    /// Explicit live or replay provenance.
    pub data_source: DataSourceResponse,
    /// Source connection and market-data freshness.
    pub market_data: MarketDataHealth,
    /// Last observed persistence connectivity.
    pub database: DatabaseHealth,
    /// Sanitized current failure context, when present.
    pub last_error: Option<String>,
}

/// Provenance of the values returned by the service.
#[derive(Debug, Serialize)]
pub struct DataSourceResponse {
    /// `replay` or `nexus_live`.
    pub mode: &'static str,
    /// `demo` for replay or `testnet` for live data.
    pub network: &'static str,
}

/// Connection and freshness details for the tracked market.
#[derive(Debug, Serialize)]
pub struct MarketDataHealth {
    /// Transport-level source connection state.
    pub connection: &'static str,
    /// Configured market symbol.
    pub market: String,
    /// Timestamp supplied by the source for the latest valid snapshot.
    pub last_update_at: Option<String>,
    /// Age of that source timestamp at response time.
    pub last_update_age_ms: Option<u64>,
    /// Time at which the service last completed a resynchronization.
    pub last_resync_at: Option<String>,
    /// Whether market values exceed the configured freshness threshold.
    pub stale: bool,
}

/// PostgreSQL health as observed by the persistence worker.
#[derive(Debug, Serialize)]
pub struct DatabaseHealth {
    /// `connected` or `disconnected`.
    pub status: &'static str,
}

/// Catalog collection with source provenance and update metadata.
#[derive(Debug, Serialize)]
pub struct MarketsResponse<T> {
    /// Source responsible for this catalog.
    pub data_source: &'static str,
    /// Reduced public market records.
    pub data: Vec<T>,
    /// Number of records in `data`.
    pub count: usize,
    /// Time the in-memory catalog was last replaced.
    pub catalog_updated_at: Option<String>,
}

/// Reduced market metadata exposed from the source catalog.
#[derive(Debug, Serialize)]
pub struct MarketCatalogItem {
    /// Stable source identifier.
    pub market_id: String,
    /// Public API symbol.
    pub symbol: String,
    /// Base asset code.
    pub base_asset: String,
    /// Quote asset code.
    pub quote_asset: String,
    /// Whether the collector is configured for this market.
    pub tracked: bool,
}

/// Latest validated analytics for the single tracked market.
#[derive(Debug, Serialize)]
pub struct MarketResponse {
    /// Explicit source provenance.
    pub data_source: &'static str,
    /// Tracked market symbol.
    pub symbol: String,
    /// Current data-confidence lifecycle.
    pub status: String,
    /// Highest bid as an exact decimal string.
    pub best_bid: String,
    /// Lowest ask as an exact decimal string.
    pub best_ask: String,
    /// Arithmetic top-of-book midpoint as an exact decimal string.
    pub mid_price: String,
    /// Absolute spread as an exact decimal string.
    pub spread: String,
    /// Relative spread in basis points as an exact decimal string.
    pub spread_bps: String,
    /// Bid quantity across the effective top-N depth.
    pub bid_depth_top_n: String,
    /// Ask quantity across the effective top-N depth.
    pub ask_depth_top_n: String,
    /// Normalized depth difference, absent when total depth is zero.
    pub imbalance: Option<String>,
    /// Equal number of bid and ask levels included in depth metrics.
    pub depth_levels: usize,
    /// REST nonce or replay sequence identifying the snapshot.
    pub source_nonce: Option<i64>,
    /// Source-provided timestamp of the snapshot.
    pub updated_at: String,
}

/// Newest-first persisted analytics for one market and source.
#[derive(Debug, Serialize)]
pub struct HistoryResponse {
    /// Source partition used for the query.
    pub data_source: &'static str,
    /// Requested market symbol.
    pub symbol: String,
    /// Newest-first snapshot projection.
    pub data: Vec<HistoryItem>,
    /// Number of returned snapshots.
    pub count: usize,
}

/// Historical projection returned to API clients.
#[derive(Debug, Serialize)]
pub struct HistoryItem {
    /// Best bid as an exact decimal string.
    pub best_bid: String,
    /// Best ask as an exact decimal string.
    pub best_ask: String,
    /// Mid-price as an exact decimal string.
    pub mid_price: String,
    /// Spread in basis points as an exact decimal string.
    pub spread_bps: String,
    /// Depth imbalance, absent when total depth was zero.
    pub imbalance: Option<String>,
    /// Database capture timestamp.
    pub captured_at: String,
}
