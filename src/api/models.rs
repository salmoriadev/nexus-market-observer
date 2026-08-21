use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub service: &'static str,
    pub version: &'static str,
    pub uptime_seconds: u64,
    pub nexus: NexusHealth,
    pub database: DatabaseHealth,
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct NexusHealth {
    pub network: &'static str,
    pub connection: &'static str,
    pub market: String,
    pub last_update_at: Option<String>,
    pub last_update_age_ms: Option<u64>,
    pub last_resync_at: Option<String>,
    pub stale: bool,
}

#[derive(Debug, Serialize)]
pub struct DatabaseHealth {
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
pub struct MarketsResponse<T> {
    pub data: Vec<T>,
    pub count: usize,
    pub catalog_updated_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MarketCatalogItem {
    pub market_id: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tracked: bool,
}

#[derive(Debug, Serialize)]
pub struct MarketResponse {
    pub symbol: String,
    pub status: String,
    pub best_bid: String,
    pub best_ask: String,
    pub mid_price: String,
    pub spread: String,
    pub spread_bps: String,
    pub bid_depth_top_n: String,
    pub ask_depth_top_n: String,
    pub imbalance: Option<String>,
    pub depth_levels: usize,
    pub source_nonce: Option<i64>,
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct HistoryResponse {
    pub symbol: String,
    pub data: Vec<HistoryItem>,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct HistoryItem {
    pub best_bid: String,
    pub best_ask: String,
    pub mid_price: String,
    pub spread_bps: String,
    pub imbalance: Option<String>,
    pub captured_at: String,
}
