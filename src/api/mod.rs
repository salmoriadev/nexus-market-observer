//! Read-only Axum API and request instrumentation.
//!
//! The router exposes current in-memory state, PostgreSQL history, liveness/readiness detail, and
//! Prometheus metrics. It contains no write endpoints or authenticated trading functionality.

pub mod handlers;
pub mod models;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::MatchedPath;
use axum::http::Request;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;
use tower_http::trace::TraceLayer;

use crate::config::Config;
use crate::persistence::HistoryReader;
use crate::state::SharedMarketState;

/// Dependencies shared by all HTTP handlers.
///
/// The history reader is a narrow trait boundary so router tests can exercise API behavior without
/// PostgreSQL. Market state and database health are shared with their background workers.
#[derive(Clone)]
pub struct AppContext {
    /// Validated immutable service configuration.
    pub config: Arc<Config>,
    /// Latest validated market snapshot and lifecycle state.
    pub market_state: SharedMarketState,
    /// Read-only historical snapshot provider.
    pub history_reader: Arc<dyn HistoryReader>,
    /// Last observed PostgreSQL connectivity state.
    pub database_connected: Arc<AtomicBool>,
    /// Monotonic process start used for uptime reporting.
    pub started_at: Instant,
    /// Render handle for the installed Prometheus recorder.
    pub prometheus: PrometheusHandle,
}

/// Builds the complete public router for an application context.
///
/// Request metrics are applied only to matched API routes, with centralized fallbacks preserving a
/// consistent JSON error contract.
pub fn router(context: AppContext) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/markets", get(handlers::markets))
        .route("/markets/{symbol}", get(handlers::market))
        .route("/markets/{symbol}/history", get(handlers::market_history))
        .route("/metrics", get(handlers::metrics))
        .fallback(handlers::route_not_found)
        .method_not_allowed_fallback(handlers::method_not_allowed)
        .route_layer(middleware::from_fn(track_http_metrics))
        .layer(TraceLayer::new_for_http())
        .with_state(context)
}

async fn track_http_metrics(request: Request<axum::body::Body>, next: Next) -> Response {
    let started_at = Instant::now();
    let method = match request.method().as_str() {
        "GET" => "GET",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        _ => "OTHER",
    };
    // Use Axum's route template instead of the raw URI so arbitrary market symbols cannot create
    // unbounded Prometheus label cardinality.
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("unmatched")
        .to_string();
    let response = next.run(request).await;
    let status = response.status().as_u16().to_string();
    metrics::counter!(
        "nexus_http_requests_total",
        "method" => method,
        "route" => route.clone(),
        "status" => status
    )
    .increment(1);
    metrics::histogram!(
        "nexus_http_request_duration_seconds",
        "method" => method,
        "route" => route
    )
    .record(started_at.elapsed().as_secs_f64());
    response
}
