use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use thiserror::Error;

use crate::config::ConfigError;

#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("database migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("Nexus operation failed: {0}")]
    Nexus(String),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("I/O operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("task failed: {0}")]
    Task(String),
    #[error("market {0} was not found")]
    MarketNotFound(String),
    #[error("invalid history limit: {0}")]
    InvalidLimit(String),
    #[error("market state is not available yet")]
    StateUnavailable,
    #[error("API route was not found")]
    RouteNotFound,
    #[error("HTTP method is not allowed for this route")]
    MethodNotAllowed,
    #[error("internal service error")]
    Internal,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    #[error("order book bid side is empty")]
    EmptyBids,
    #[error("order book ask side is empty")]
    EmptyAsks,
    #[error("crossed or locked order book: best bid {best_bid} >= best ask {best_ask}")]
    CrossedBook {
        best_bid: rust_decimal::Decimal,
        best_ask: rust_decimal::Decimal,
    },
    #[error("invalid order book level: {0}")]
    InvalidLevel(String),
    #[error("depth level count must be greater than zero")]
    InvalidDepth,
    #[error("decimal calculation overflowed")]
    DecimalOverflow,
    #[error("invalid service status transition from {from} to {to}")]
    InvalidStatusTransition { from: String, to: String },
    #[error("invalid Nexus stream payload: {0}")]
    InvalidStreamPayload(String),
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            Self::MarketNotFound(symbol) => (
                StatusCode::NOT_FOUND,
                "MARKET_NOT_FOUND",
                format!("Market {symbol} was not found"),
            ),
            Self::InvalidLimit(reason) => (
                StatusCode::BAD_REQUEST,
                "INVALID_LIMIT",
                format!("Invalid history limit: {reason}"),
            ),
            Self::StateUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "MARKET_STATE_UNAVAILABLE",
                "Market state is not available yet".to_string(),
            ),
            Self::RouteNotFound => (
                StatusCode::NOT_FOUND,
                "ROUTE_NOT_FOUND",
                "The requested API route was not found".to_string(),
            ),
            Self::MethodNotAllowed => (
                StatusCode::METHOD_NOT_ALLOWED,
                "METHOD_NOT_ALLOWED",
                "The HTTP method is not allowed for this route".to_string(),
            ),
            _ => {
                tracing::error!(error = %self, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "The service could not complete the request".to_string(),
                )
            }
        };

        (
            status,
            Json(ErrorEnvelope {
                error: ErrorBody { code, message },
            }),
        )
            .into_response()
    }
}
