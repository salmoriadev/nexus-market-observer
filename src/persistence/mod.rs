//! Periodic snapshot persistence and read-side abstraction.
//!
//! Persistence is deliberately decoupled from update frequency: the worker samples the latest
//! validated in-memory state on a configured interval instead of writing every stream frame. It
//! copies values while holding a read lock, releases the lock, and only then performs SQL. Temporary
//! PostgreSQL failures update health and metrics but never stop the market-data collector.

pub mod postgres;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::error::AppError;
use crate::state::SharedMarketState;

pub use postgres::{connect_with_retry, PostgresRepository};

/// Historical projection returned by the repository.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HistoryPoint {
    /// Best bid at capture time.
    pub best_bid: Decimal,
    /// Best ask at capture time.
    pub best_ask: Decimal,
    /// Mid-price at capture time.
    pub mid_price: Decimal,
    /// Spread in basis points at capture time.
    pub spread_bps: Decimal,
    /// Depth imbalance, absent when both sampled depths were zero.
    pub imbalance: Option<Decimal>,
    /// Database capture timestamp.
    pub captured_at: DateTime<Utc>,
}

#[async_trait]
/// Read-only history boundary used by HTTP handlers and in-memory API tests.
pub trait HistoryReader: Send + Sync {
    /// Returns at most `limit` snapshots for one symbol and source, newest first.
    async fn history(
        &self,
        symbol: &str,
        data_source: &str,
        limit: i64,
    ) -> Result<Vec<HistoryPoint>, AppError>;
}

/// Complete validated snapshot copied from shared state and ready for SQL binding.
///
/// Source provenance is mandatory so replay rows can never be mistaken for live Nexus data.
#[derive(Debug, Clone)]
pub struct PersistableSnapshot {
    /// Explicit `replay` or `nexus_live` provenance.
    pub data_source: String,
    /// Stable market foreign-key value.
    pub market_id: String,
    /// Public market symbol.
    pub symbol: String,
    /// Best bid at the source update.
    pub best_bid: Decimal,
    /// Best ask at the source update.
    pub best_ask: Decimal,
    /// Mid-price at the source update.
    pub mid_price: Decimal,
    /// Absolute spread at the source update.
    pub spread: Decimal,
    /// Relative spread in basis points.
    pub spread_bps: Decimal,
    /// Bid quantity across the effective top-N levels.
    pub bid_depth: Decimal,
    /// Ask quantity across the effective top-N levels.
    pub ask_depth: Decimal,
    /// Normalized depth difference, if total depth was nonzero.
    pub imbalance: Option<Decimal>,
    /// Equal count of bid and ask levels used by analytics.
    pub depth_levels: i32,
    /// Source identity paired with `source_updated_at` for durable deduplication.
    pub source_nonce: Option<i64>,
    /// Timestamp supplied by the source rather than local capture time.
    pub source_updated_at: DateTime<Utc>,
}

/// Periodically persists new validated source updates until cancellation.
///
/// In-process identity tracking avoids needless queries between unchanged updates. PostgreSQL also
/// enforces the source identity uniquely, which makes deduplication durable across process restarts.
/// When no new snapshot exists, a lightweight ping still detects database recovery or failure.
pub async fn run_persistence_worker(
    repository: PostgresRepository,
    state: SharedMarketState,
    database_connected: Arc<AtomicBool>,
    interval: Duration,
    data_source: &'static str,
    cancellation: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_persisted_identity = None;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                tracing::info!("snapshot persistence worker stopped");
                break;
            }
            _ = ticker.tick() => {
                // This helper only holds the RwLock long enough to copy owned values. SQL starts
                // after it returns, with no state guard alive.
                let snapshot = snapshot_for_persistence(&state, last_persisted_identity, data_source).await;
                if let Some(snapshot) = snapshot {
                    let identity = (snapshot.source_updated_at, snapshot.source_nonce);
                    match repository.persist_snapshot(&snapshot).await {
                        Ok(inserted) => {
                            if !database_connected.swap(true, Ordering::Relaxed) {
                                tracing::info!("PostgreSQL persistence recovered");
                            }
                            last_persisted_identity = Some(identity);
                            if inserted {
                                metrics::counter!("nexus_snapshots_persisted_total", "market" => snapshot.symbol, "source" => data_source).increment(1);
                            } else {
                                tracing::debug!(
                                    market = snapshot.symbol,
                                    source = data_source,
                                    "skipped a snapshot already present in PostgreSQL"
                                );
                            }
                        }
                        Err(error) => {
                            database_connected.store(false, Ordering::Relaxed);
                            metrics::counter!("nexus_database_errors_total", "operation" => "persist_snapshot").increment(1);
                            tracing::error!(error = %error, "failed to persist market snapshot; worker will retry");
                        }
                    }
                } else {
                    match repository.ping().await {
                        Ok(()) => {
                            if !database_connected.swap(true, Ordering::Relaxed) {
                                tracing::info!("PostgreSQL connectivity recovered");
                            }
                        }
                        Err(error) => {
                            database_connected.store(false, Ordering::Relaxed);
                            metrics::counter!("nexus_database_errors_total", "operation" => "ping").increment(1);
                            tracing::error!(error = %error, "PostgreSQL connectivity check failed");
                        }
                    }
                }
            }
        }
    }
}

async fn snapshot_for_persistence(
    state: &SharedMarketState,
    last_persisted_identity: Option<(DateTime<Utc>, Option<i64>)>,
    data_source: &str,
) -> Option<PersistableSnapshot> {
    let state = state.read().await;
    let source_updated_at = state.last_update_at?;
    let identity = (source_updated_at, state.source_nonce_or_sequence);
    if last_persisted_identity.is_some_and(|last| last == identity) {
        return None;
    }

    Some(PersistableSnapshot {
        data_source: data_source.to_string(),
        market_id: state.market_id.clone(),
        symbol: state.symbol.clone(),
        best_bid: state.best_bid?,
        best_ask: state.best_ask?,
        mid_price: state.mid_price?,
        spread: state.spread?,
        spread_bps: state.spread_bps?,
        bid_depth: state.bid_depth?,
        ask_depth: state.ask_depth?,
        imbalance: state.imbalance,
        depth_levels: i32::try_from(state.depth_levels).ok()?,
        source_nonce: state.source_nonce_or_sequence,
        source_updated_at,
    })
}
