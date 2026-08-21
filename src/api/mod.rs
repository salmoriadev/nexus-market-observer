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

#[derive(Clone)]
pub struct AppContext {
    pub config: Arc<Config>,
    pub market_state: SharedMarketState,
    pub history_reader: Arc<dyn HistoryReader>,
    pub database_connected: Arc<AtomicBool>,
    pub started_at: Instant,
    pub prometheus: PrometheusHandle,
}

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
