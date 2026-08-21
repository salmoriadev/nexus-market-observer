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
    DatabaseHealth, HealthResponse, HistoryItem, HistoryResponse, MarketCatalogItem,
    MarketResponse, MarketsResponse, NexusHealth,
};
use super::AppContext;

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
        service: "nexus-market-intelligence",
        version: env!("CARGO_PKG_VERSION"),
        uptime_seconds: context.started_at.elapsed().as_secs(),
        nexus: NexusHealth {
            network: context.config.nexus_network.as_str(),
            connection: state.nexus_connection.as_str(),
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
        data,
        count,
        catalog_updated_at: state.catalog_updated_at.map(|value| value.to_rfc3339()),
    })
}

pub async fn market(
    State(context): State<AppContext>,
    Path(symbol): Path<String>,
) -> Result<Json<MarketResponse>, AppError> {
    if symbol != context.config.nexus_market {
        return Err(AppError::MarketNotFound(symbol));
    }
    let state = context.market_state.read().await;
    let response = MarketResponse {
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

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    limit: Option<String>,
}

pub async fn market_history(
    State(context): State<AppContext>,
    Path(symbol): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HistoryResponse>, AppError> {
    let known = {
        let state = context.market_state.read().await;
        symbol == context.config.nexus_market
            || state.catalog.iter().any(|market| market.symbol == symbol)
    };
    if !known {
        return Err(AppError::MarketNotFound(symbol));
    }

    let limit = parse_history_limit(query.limit.as_deref())?;
    let rows = context.history_reader.history(&symbol, limit).await?;
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

pub async fn metrics(State(context): State<AppContext>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        context.prometheus.render(),
    )
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
