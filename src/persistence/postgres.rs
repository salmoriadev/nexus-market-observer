//! PostgreSQL implementation for snapshots and historical reads.
//!
//! Queries are parameterized and financial values bind directly as PostgreSQL `NUMERIC`. Embedded
//! migrations own the schema contract. Startup connection retry is bounded because migrations are
//! mandatory, while runtime query failures are handled by the persistence worker without bringing
//! down collection or HTTP serving.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::error::AppError;

use super::{HistoryPoint, HistoryReader, PersistableSnapshot};

const STARTUP_DATABASE_RETRY_WINDOW: Duration = Duration::from_secs(30);
const DATABASE_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Cloneable SQLx repository backed by a shared PostgreSQL pool.
#[derive(Debug, Clone)]
pub struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    /// Wraps an initialized pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Applies all embedded migrations before workers and HTTP serving begin.
    pub async fn migrate(&self) -> Result<(), AppError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Checks whether the pool can execute a minimal query.
    pub async fn ping(&self) -> Result<(), AppError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Inserts or refreshes the stable market identity used by snapshot foreign keys.
    pub async fn upsert_market(&self, market_id: &str, symbol: &str) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO markets (id, symbol)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE
            SET symbol = EXCLUDED.symbol, updated_at = NOW()
            "#,
        )
        .bind(market_id)
        .bind(symbol)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Persists one validated periodic snapshot.
    ///
    /// Returns `true` when a row was inserted and `false` when the database uniqueness constraint
    /// recognized the same source update from an earlier interval or process run.
    pub async fn persist_snapshot(&self, snapshot: &PersistableSnapshot) -> Result<bool, AppError> {
        self.upsert_market(&snapshot.market_id, &snapshot.symbol)
            .await?;
        let result = sqlx::query(
            r#"
            INSERT INTO market_snapshots (
                market_id, data_source, best_bid, best_ask, mid_price, spread, spread_bps,
                bid_depth, ask_depth, imbalance, depth_levels, source_nonce,
                source_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (market_id, data_source, source_updated_at, source_nonce) DO NOTHING
            "#,
        )
        .bind(&snapshot.market_id)
        .bind(&snapshot.data_source)
        .bind(snapshot.best_bid)
        .bind(snapshot.best_ask)
        .bind(snapshot.mid_price)
        .bind(snapshot.spread)
        .bind(snapshot.spread_bps)
        .bind(snapshot.bid_depth)
        .bind(snapshot.ask_depth)
        .bind(snapshot.imbalance)
        .bind(snapshot.depth_levels)
        .bind(snapshot.source_nonce)
        .bind(snapshot.source_updated_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Exposes the pool for integration-test assertions and controlled lifecycle operations.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl HistoryReader for PostgresRepository {
    async fn history(
        &self,
        symbol: &str,
        data_source: &str,
        limit: i64,
    ) -> Result<Vec<HistoryPoint>, AppError> {
        let rows = sqlx::query_as::<_, HistoryPoint>(
            r#"
            SELECT
                snapshot.best_bid,
                snapshot.best_ask,
                snapshot.mid_price,
                snapshot.spread_bps,
                snapshot.imbalance,
                snapshot.captured_at
            FROM market_snapshots AS snapshot
            INNER JOIN markets AS market ON market.id = snapshot.market_id
            WHERE market.symbol = $1 AND snapshot.data_source = $2
            ORDER BY snapshot.captured_at DESC
            LIMIT $3
            "#,
        )
        .bind(symbol)
        .bind(data_source)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}

/// Connects to PostgreSQL within the bounded startup retry window.
///
/// # Errors
///
/// Returns [`AppError`] after approximately 30 seconds without a successful pool connection. The
/// URL is used by SQLx but never included in service logs.
pub async fn connect_with_retry(database_url: &str) -> Result<PgPool, AppError> {
    // PostgreSQL is the only bounded startup dependency: without it the schema cannot be migrated.
    // Once startup succeeds, SQL errors are retried indefinitely by the persistence worker.
    let started_at = Instant::now();
    let mut attempt = 0_u32;
    loop {
        attempt += 1;
        match PgPoolOptions::new()
            .max_connections(10)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                tracing::info!(attempt, "connected to PostgreSQL");
                return Ok(pool);
            }
            Err(error) if started_at.elapsed() < STARTUP_DATABASE_RETRY_WINDOW => {
                tracing::warn!(attempt, error = %error, "PostgreSQL is not ready; retrying");
                tokio::time::sleep(DATABASE_RETRY_INTERVAL).await;
            }
            Err(error) => {
                return Err(AppError::Task(format!(
                    "PostgreSQL remained unavailable for approximately 30 seconds: {error}"
                )));
            }
        }
    }
}
