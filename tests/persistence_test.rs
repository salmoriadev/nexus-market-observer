use std::time::Duration;

use chrono::{TimeDelta, Utc};
use nexus_market_observer::persistence::{HistoryReader, PersistableSnapshot, PostgresRepository};
use rust_decimal_macros::dec;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn postgres_migrations_persistence_history_limit_null_and_precision() {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set; PostgreSQL integration test is skipped locally");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url)
        .await
        .expect("PostgreSQL test database should be available");
    let repository = PostgresRepository::new(pool.clone());
    repository.migrate().await.expect("migrations should run");

    let suffix = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let market = format!("TEST-{suffix}-USDX-PERP");
    repository
        .upsert_market(&market, &market)
        .await
        .expect("market upsert");
    let now = Utc::now();
    let first = PersistableSnapshot {
        data_source: "replay".to_string(),
        market_id: market.clone(),
        symbol: market.clone(),
        best_bid: dec!(117231.20000001),
        best_ask: dec!(117235.60000001),
        mid_price: dec!(117233.40000001),
        spread: dec!(4.40000000),
        spread_bps: dec!(0.37530001),
        bid_depth: dec!(52.35000001),
        ask_depth: dec!(36.44000001),
        imbalance: None,
        depth_levels: 10,
        source_nonce: Some(1),
        source_updated_at: now - TimeDelta::seconds(1),
    };
    repository
        .persist_snapshot(&first)
        .await
        .expect("first snapshot");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let second = PersistableSnapshot {
        source_nonce: Some(2),
        source_updated_at: now,
        imbalance: Some(dec!(-0.12345678)),
        best_bid: dec!(117232.20000001),
        ..first.clone()
    };
    repository
        .persist_snapshot(&second)
        .await
        .expect("second snapshot");

    let latest = repository
        .history(&market, "replay", 1)
        .await
        .expect("limited history");
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].best_bid, dec!(117232.20000001));
    assert_eq!(latest[0].imbalance, Some(dec!(-0.12345678)));

    let history = repository
        .history(&market, "replay", 10)
        .await
        .expect("full history");
    assert_eq!(history.len(), 2);
    assert!(history[0].captured_at >= history[1].captured_at);
    assert_eq!(history[1].best_bid, dec!(117231.20000001));
    assert_eq!(history[1].imbalance, None);

    let live = PersistableSnapshot {
        data_source: "nexus_live".to_string(),
        source_nonce: Some(3),
        source_updated_at: now + TimeDelta::seconds(1),
        ..first.clone()
    };
    repository
        .persist_snapshot(&live)
        .await
        .expect("live snapshot");
    let replay_only = repository
        .history(&market, "replay", 10)
        .await
        .expect("source-filtered history");
    let live_only = repository
        .history(&market, "nexus_live", 10)
        .await
        .expect("source-filtered history");
    assert_eq!(replay_only.len(), 2);
    assert_eq!(live_only.len(), 1);

    sqlx::query("DELETE FROM market_snapshots WHERE market_id = $1")
        .bind(&market)
        .execute(&pool)
        .await
        .expect("snapshot cleanup");
    sqlx::query("DELETE FROM markets WHERE id = $1")
        .bind(&market)
        .execute(&pool)
        .await
        .expect("market cleanup");
    pool.close().await;
}
