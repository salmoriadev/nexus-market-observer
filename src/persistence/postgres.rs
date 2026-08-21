use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::error::AppError;

use super::{HistoryPoint, HistoryReader, PersistableSnapshot};

const STARTUP_DATABASE_RETRY_WINDOW: Duration = Duration::from_secs(30);
const DATABASE_RETRY_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> Result<(), AppError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub async fn ping(&self) -> Result<(), AppError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

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

    pub async fn persist_snapshot(&self, snapshot: &PersistableSnapshot) -> Result<(), AppError> {
        self.upsert_market(&snapshot.market_id, &snapshot.symbol)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO market_snapshots (
                market_id, best_bid, best_ask, mid_price, spread, spread_bps,
                bid_depth, ask_depth, imbalance, depth_levels, source_nonce,
                source_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(&snapshot.market_id)
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
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl HistoryReader for PostgresRepository {
    async fn history(&self, symbol: &str, limit: i64) -> Result<Vec<HistoryPoint>, AppError> {
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
            WHERE market.symbol = $1
            ORDER BY snapshot.captured_at DESC
            LIMIT $2
            "#,
        )
        .bind(symbol)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}

pub async fn connect_with_retry(database_url: &str) -> Result<PgPool, AppError> {
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
