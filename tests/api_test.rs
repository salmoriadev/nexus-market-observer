use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::Body;
use chrono::{TimeDelta, Utc};
use http_body_util::BodyExt;
use nexus_market_observer::api::{router, AppContext};
use nexus_market_observer::config::{AppEnvironment, Config, MarketDataSourceMode, NexusNetwork};
use nexus_market_observer::error::AppError;
use nexus_market_observer::persistence::{HistoryPoint, HistoryReader};
use nexus_market_observer::state::{CatalogMarket, MarketState, ServiceStatus, SourceConnection};
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use tower::ServiceExt;

#[derive(Debug)]
struct FakeHistoryReader {
    rows: Vec<HistoryPoint>,
    observed_limit: Mutex<Option<i64>>,
}

#[async_trait]
impl HistoryReader for FakeHistoryReader {
    async fn history(
        &self,
        _symbol: &str,
        _data_source: &str,
        limit: i64,
    ) -> Result<Vec<HistoryPoint>, AppError> {
        if let Ok(mut observed) = self.observed_limit.lock() {
            *observed = Some(limit);
        }
        Ok(self.rows.iter().take(limit as usize).cloned().collect())
    }
}

fn test_config_with_source(market_data_source: MarketDataSourceMode) -> Arc<Config> {
    Arc::new(Config {
        app_env: AppEnvironment::Test,
        http_host: IpAddr::from([127, 0, 0, 1]),
        http_port: 0,
        database_url: "postgres://unused".to_string(),
        market_data_source,
        nexus_network: NexusNetwork::Testnet,
        nexus_market: "BTC-USDX-PERP".to_string(),
        snapshot_interval: Duration::from_secs(5),
        stale_after: Duration::from_secs(15),
        rest_timeout: Duration::from_secs(10),
        rust_log: "warn".to_string(),
    })
}

fn test_config() -> Arc<Config> {
    test_config_with_source(MarketDataSourceMode::Replay)
}

fn populated_state(status: ServiceStatus) -> MarketState {
    let now = Utc::now();
    let mut state = MarketState::new("BTC-USDX-PERP".to_string());
    state.service_status = status;
    state.source_connection = if status == ServiceStatus::Healthy {
        SourceConnection::Connected
    } else {
        SourceConnection::Reconnecting
    };
    state.best_bid = Some(dec!(117231.20));
    state.best_ask = Some(dec!(117235.60));
    state.mid_price = Some(dec!(117233.40));
    state.spread = Some(dec!(4.40));
    state.spread_bps = Some(dec!(0.3753));
    state.bid_depth = Some(dec!(52.3500));
    state.ask_depth = Some(dec!(36.4400));
    state.imbalance = Some(dec!(0.1792));
    state.depth_levels = 10;
    state.source_nonce_or_sequence = Some(5_820_193);
    state.last_update_at = Some(now);
    state.last_resync_at = Some(now);
    state.catalog = vec![CatalogMarket {
        market_id: "BTC-USDX-PERP".to_string(),
        symbol: "BTC-USDX-PERP".to_string(),
        base_asset: "BTC".to_string(),
        quote_asset: "USDX".to_string(),
        tracked: true,
    }];
    state.catalog_updated_at = Some(now);
    state
}

fn app(status: ServiceStatus, database_connected: bool) -> axum::Router {
    app_with_source(status, database_connected, MarketDataSourceMode::Replay)
}

fn app_with_source(
    status: ServiceStatus,
    database_connected: bool,
    source: MarketDataSourceMode,
) -> axum::Router {
    let history = FakeHistoryReader {
        rows: vec![HistoryPoint {
            best_bid: dec!(117231.20),
            best_ask: dec!(117235.60),
            mid_price: dec!(117233.40),
            spread_bps: dec!(0.3753),
            imbalance: Some(dec!(0.1792)),
            captured_at: Utc::now() - TimeDelta::seconds(1),
        }],
        observed_limit: Mutex::new(None),
    };
    router(AppContext {
        config: test_config_with_source(source),
        market_state: Arc::new(tokio::sync::RwLock::new(populated_state(status))),
        history_reader: Arc::new(history),
        database_connected: Arc::new(AtomicBool::new(database_connected)),
        started_at: Instant::now(),
        prometheus: nexus_market_observer::telemetry::isolated_metrics_handle(),
    })
}

fn empty_app() -> axum::Router {
    let history = FakeHistoryReader {
        rows: Vec::new(),
        observed_limit: Mutex::new(None),
    };
    router(AppContext {
        config: test_config(),
        market_state: Arc::new(tokio::sync::RwLock::new(MarketState::new(
            "BTC-USDX-PERP".to_string(),
        ))),
        history_reader: Arc::new(history),
        database_connected: Arc::new(AtomicBool::new(true)),
        started_at: Instant::now(),
        prometheus: nexus_market_observer::telemetry::isolated_metrics_handle(),
    })
}

async fn response_json(app: axum::Router, uri: &str) -> (u16, Value) {
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("valid test request"),
        )
        .await
        .expect("router response");
    let status = response.status().as_u16();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    let value = serde_json::from_slice(&bytes).expect("JSON response");
    (status, value)
}

#[tokio::test]
async fn health_is_healthy_with_a_current_snapshot() {
    let (status, body) = response_json(app(ServiceStatus::Healthy, true), "/health").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["data_source"]["mode"], "replay");
    assert_eq!(body["data_source"]["network"], "demo");
    assert_eq!(body["market_data"]["connection"], "connected");
    assert_eq!(body["database"]["status"], "connected");
}

#[tokio::test]
async fn health_is_unavailable_before_the_first_snapshot() {
    let (status, body) = response_json(empty_app(), "/health").await;
    assert_eq!(status, 503);
    assert_eq!(body["status"], "starting");
    assert_eq!(body["data_source"]["mode"], "replay");
}

#[tokio::test]
async fn live_health_is_explicitly_labeled_as_testnet() {
    let application = app_with_source(
        ServiceStatus::Degraded,
        true,
        MarketDataSourceMode::NexusLive,
    );
    let (status, body) = response_json(application, "/health").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "degraded");
    assert_eq!(body["data_source"]["mode"], "nexus_live");
    assert_eq!(body["data_source"]["network"], "testnet");
}

#[tokio::test]
async fn health_is_degraded_when_the_database_is_unavailable() {
    let application = app(ServiceStatus::Healthy, false);
    let (status, body) = response_json(application, "/health").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "degraded");
    assert_eq!(body["database"]["status"], "disconnected");
}

#[tokio::test]
async fn current_market_serializes_decimals_as_strings() {
    let (status, body) =
        response_json(app(ServiceStatus::Healthy, true), "/markets/BTC-USDX-PERP").await;
    assert_eq!(status, 200);
    assert_eq!(body["best_bid"], json!("117231.20"));
    assert_eq!(body["data_source"], "replay");
    assert_eq!(body["mid_price"], json!("117233.40"));
    assert_eq!(body["imbalance"], json!("0.1792"));
}

#[tokio::test]
async fn unknown_market_uses_the_consistent_error_envelope() {
    let (status, body) =
        response_json(app(ServiceStatus::Healthy, true), "/markets/BTC-INVALID").await;
    assert_eq!(status, 404);
    assert_eq!(body["error"]["code"], "MARKET_NOT_FOUND");
    assert_eq!(body["error"]["message"], "Market BTC-INVALID was not found");
}

#[tokio::test]
async fn unknown_routes_use_the_consistent_error_envelope() {
    let (status, body) = response_json(app(ServiceStatus::Healthy, true), "/unknown").await;
    assert_eq!(status, 404);
    assert_eq!(body["error"]["code"], "ROUTE_NOT_FOUND");
}

#[tokio::test]
async fn unsupported_methods_use_the_consistent_error_envelope() {
    let response = app(ServiceStatus::Healthy, true)
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/markets")
                .body(Body::empty())
                .expect("valid test request"),
        )
        .await
        .expect("router response");
    assert_eq!(
        response.status(),
        axum::http::StatusCode::METHOD_NOT_ALLOWED
    );
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(body["error"]["code"], "METHOD_NOT_ALLOWED");
}

#[tokio::test]
async fn history_returns_decimal_strings() {
    let (status, body) = response_json(
        app(ServiceStatus::Healthy, true),
        "/markets/BTC-USDX-PERP/history?limit=1",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["count"], 1);
    assert_eq!(body["data_source"], "replay");
    assert_eq!(body["data"][0]["best_bid"], "117231.20");
    assert_eq!(body["data"][0]["imbalance"], "0.1792");
}

#[tokio::test]
async fn invalid_and_oversized_history_limits_are_rejected() {
    for uri in [
        "/markets/BTC-USDX-PERP/history?limit=invalid",
        "/markets/BTC-USDX-PERP/history?limit=501",
        "/markets/BTC-USDX-PERP/history?limit=0",
    ] {
        let (status, body) = response_json(app(ServiceStatus::Healthy, true), uri).await;
        assert_eq!(status, 400);
        assert_eq!(body["error"]["code"], "INVALID_LIMIT");
    }
}

#[tokio::test]
async fn markets_lists_the_tracked_catalog_entry() {
    let (status, body) = response_json(app(ServiceStatus::Healthy, true), "/markets").await;
    assert_eq!(status, 200);
    assert_eq!(body["count"], 1);
    assert_eq!(body["data_source"], "replay");
    assert_eq!(body["data"][0]["tracked"], true);
}

#[test]
fn database_flag_is_shared_atomically() {
    let flag = AtomicBool::new(false);
    flag.store(true, Ordering::Relaxed);
    assert!(flag.load(Ordering::Relaxed));
}
