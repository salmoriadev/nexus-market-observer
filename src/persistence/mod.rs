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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HistoryPoint {
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub mid_price: Decimal,
    pub spread_bps: Decimal,
    pub imbalance: Option<Decimal>,
    pub captured_at: DateTime<Utc>,
}

#[async_trait]
pub trait HistoryReader: Send + Sync {
    async fn history(
        &self,
        symbol: &str,
        data_source: &str,
        limit: i64,
    ) -> Result<Vec<HistoryPoint>, AppError>;
}

#[derive(Debug, Clone)]
pub struct PersistableSnapshot {
    pub data_source: String,
    pub market_id: String,
    pub symbol: String,
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub mid_price: Decimal,
    pub spread: Decimal,
    pub spread_bps: Decimal,
    pub bid_depth: Decimal,
    pub ask_depth: Decimal,
    pub imbalance: Option<Decimal>,
    pub depth_levels: i32,
    pub source_nonce: Option<i64>,
    pub source_updated_at: DateTime<Utc>,
}

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
