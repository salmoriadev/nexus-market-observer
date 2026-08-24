//! HTTP handlers and query validation.
//!
//! Handlers translate internal state into stable response models without exposing SDK payloads or
//! database details. Shared-state locks are scoped to in-memory reads and are released before any
//! PostgreSQL query is awaited.

use std::sync::atomic::Ordering;

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::Utc;
use serde::Deserialize;

use crate::error::AppError;
use crate::state::ServiceStatus;

use super::models::{
    DataSourceResponse, DatabaseHealth, HealthResponse, HistoryItem, HistoryResponse,
    MarketCatalogItem, MarketDataHealth, MarketResponse, MarketsResponse,
};
use super::AppContext;

/// Reports process availability together with market-data and persistence health.
///
/// A usable but stale or degraded snapshot returns HTTP 200 because the process can still serve
/// explicitly marked historical values. HTTP 503 is reserved for startup or another state in which
/// no complete validated snapshot has ever been obtained.
pub async fn health(State(context): State<AppContext>) -> Response {
    let database_connected = context.database_connected.load(Ordering::Relaxed);
    let now = Utc::now();
    let state = context.market_state.read().await;
    let age = state
        .last_update_at
        .and_then(|updated_at| now.signed_duration_since(updated_at).to_std().ok());
    let stale = state.service_status == ServiceStatus::Stale
        || age.is_some_and(|duration| duration > context.config.stale_after);
    let usable = state.has_usable_snapshot();
    let overall_status = if !database_connected {
        "degraded"
    } else {
        state.service_status.as_str()
    };
    let last_error = if database_connected {
        state.last_error.clone()
    } else {
        Some("PostgreSQL is unavailable; snapshot persistence will retry".to_string())
    };
    let body = HealthResponse {
        status: overall_status.to_string(),
        service: "nexus-market-observer",
        version: env!("CARGO_PKG_VERSION"),
        uptime_seconds: context.started_at.elapsed().as_secs(),
        data_source: DataSourceResponse {
            mode: context.config.market_data_source.as_str(),
            network: context.config.market_data_source.network_label(),
        },
        market_data: MarketDataHealth {
            connection: state.source_connection.as_str(),
            market: context.config.nexus_market.clone(),
            last_update_at: state.last_update_at.map(|value| value.to_rfc3339()),
            last_update_age_ms: age
                .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
            last_resync_at: state.last_resync_at.map(|value| value.to_rfc3339()),
            stale,
        },
        database: DatabaseHealth {
            status: if database_connected {
                "connected"
            } else {
                "disconnected"
            },
        },
        last_error,
    };
    let status = if usable {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body)).into_response()
}

/// Returns the latest known public market catalog.
pub async fn markets(
    State(context): State<AppContext>,
) -> Json<MarketsResponse<MarketCatalogItem>> {
    let state = context.market_state.read().await;
    let data: Vec<_> = state
        .catalog
        .iter()
        .map(|market| MarketCatalogItem {
            market_id: market.market_id.clone(),
            symbol: market.symbol.clone(),
            base_asset: market.base_asset.clone(),
            quote_asset: market.quote_asset.clone(),
            tracked: market.tracked,
        })
        .collect();
    let count = data.len();
    Json(MarketsResponse {
        data_source: context.config.market_data_source.as_str(),
        data,
        count,
        catalog_updated_at: state.catalog_updated_at.map(|value| value.to_rfc3339()),
    })
}

/// Returns current analytics for the configured market.
///
/// Unknown symbols return 404; the configured symbol returns 503 until a usable snapshot exists.
pub async fn market(
    State(context): State<AppContext>,
    Path(symbol): Path<String>,
) -> Result<Json<MarketResponse>, AppError> {
    if symbol != context.config.nexus_market {
        return Err(AppError::MarketNotFound(symbol));
    }
    let state = context.market_state.read().await;
    let response = MarketResponse {
        data_source: context.config.market_data_source.as_str(),
        symbol: state.symbol.clone(),
        status: state.service_status.to_string(),
        best_bid: state
            .best_bid
            .ok_or(AppError::StateUnavailable)?
            .to_string(),
        best_ask: state
            .best_ask
            .ok_or(AppError::StateUnavailable)?
            .to_string(),
        mid_price: state
            .mid_price
            .ok_or(AppError::StateUnavailable)?
            .to_string(),
        spread: state.spread.ok_or(AppError::StateUnavailable)?.to_string(),
        spread_bps: state
            .spread_bps
            .ok_or(AppError::StateUnavailable)?
            .to_string(),
        bid_depth_top_n: state
            .bid_depth
            .ok_or(AppError::StateUnavailable)?
            .to_string(),
        ask_depth_top_n: state
            .ask_depth
            .ok_or(AppError::StateUnavailable)?
            .to_string(),
        imbalance: state.imbalance.map(|value| value.to_string()),
        depth_levels: state.depth_levels,
        source_nonce: state.source_nonce_or_sequence,
        updated_at: state
            .last_update_at
            .ok_or(AppError::StateUnavailable)?
            .to_rfc3339(),
    };
    Ok(Json(response))
}

/// Raw history query parameters.
///
/// `limit` remains a string until explicit validation so malformed values use the service's stable
/// error envelope instead of Axum's default extractor rejection body.
#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    limit: Option<String>,
}

/// Returns source-isolated, newest-first snapshots from PostgreSQL.
pub async fn market_history(
    State(context): State<AppContext>,
    Path(symbol): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HistoryResponse>, AppError> {
    // Restrict the state lock to the catalog lookup; the database query below must never run while
    // a Tokio RwLock guard is held.
    let known = {
        let state = context.market_state.read().await;
        symbol == context.config.nexus_market
            || state.catalog.iter().any(|market| market.symbol == symbol)
    };
    if !known {
        return Err(AppError::MarketNotFound(symbol));
    }

    let limit = parse_history_limit(query.limit.as_deref())?;
    let data_source = context.config.market_data_source.as_str();
    let rows = context
        .history_reader
        .history(&symbol, data_source, limit)
        .await?;
    let data: Vec<_> = rows
        .into_iter()
        .map(|row| HistoryItem {
            best_bid: row.best_bid.to_string(),
            best_ask: row.best_ask.to_string(),
            mid_price: row.mid_price.to_string(),
            spread_bps: row.spread_bps.to_string(),
            imbalance: row.imbalance.map(|value| value.to_string()),
            captured_at: row.captured_at.to_rfc3339(),
        })
        .collect();
    let count = data.len();
    Ok(Json(HistoryResponse {
        data_source,
        symbol,
        data,
        count,
    }))
}

fn parse_history_limit(raw: Option<&str>) -> Result<i64, AppError> {
    let limit = raw
        .unwrap_or("100")
        .parse::<i64>()
        .map_err(|_| AppError::InvalidLimit("must be an integer from 1 to 500".to_string()))?;
    if !(1..=500).contains(&limit) {
        return Err(AppError::InvalidLimit(
            "must be between 1 and 500".to_string(),
        ));
    }
    Ok(limit)
}

/// Renders metrics using the Prometheus text exposition format.
pub async fn metrics(State(context): State<AppContext>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        context.prometheus.render(),
    )
}

/// Produces the centralized JSON response for an unmatched route.
pub async fn route_not_found() -> AppError {
    AppError::RouteNotFound
}

/// Produces the centralized JSON response for an unsupported HTTP method.
pub async fn method_not_allowed() -> AppError {
    AppError::MethodNotAllowed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_limit_is_bounded() {
        assert_eq!(parse_history_limit(None).expect("default"), 100);
        assert_eq!(parse_history_limit(Some("1")).expect("minimum"), 1);
        assert_eq!(parse_history_limit(Some("500")).expect("maximum"), 500);
        assert!(parse_history_limit(Some("0")).is_err());
        assert!(parse_history_limit(Some("501")).is_err());
        assert!(parse_history_limit(Some("invalid")).is_err());
    }
}
