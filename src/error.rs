//! Error taxonomy and HTTP error boundary.
//!
//! [`DomainError`] represents invalid market-data or state transitions. [`AppError`] adds
//! infrastructure and API concerns. Only expected client errors are exposed verbatim; internal,
//! database, SDK, and task failures are logged and converted to a stable generic response.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use thiserror::Error;

use crate::config::ConfigError;

/// Top-level failure type used across startup, workers, persistence, and HTTP handlers.
#[derive(Debug, Error)]
pub enum AppError {
    /// Invalid startup configuration.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// PostgreSQL query or connection failure.
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    /// Embedded SQL migration failure.
    #[error("database migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    /// Nexus SDK or upstream protocol failure with operational context.
    #[error("Nexus operation failed: {0}")]
    Nexus(String),
    /// Invalid market data or state-machine operation.
    #[error(transparent)]
    Domain(#[from] DomainError),
    /// Local socket or operating-system I/O failure.
    #[error("I/O operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// Background task or application lifecycle failure.
    #[error("task failed: {0}")]
    Task(String),
    /// Requested market is not known to the service.
    #[error("market {0} was not found")]
    MarketNotFound(String),
    /// History pagination is outside the supported range.
    #[error("invalid history limit: {0}")]
    InvalidLimit(String),
    /// No complete, validated snapshot exists yet.
    #[error("market state is not available yet")]
    StateUnavailable,
    /// No API route matched the request.
    #[error("API route was not found")]
    RouteNotFound,
    /// The route exists but does not accept the request method.
    #[error("HTTP method is not allowed for this route")]
    MethodNotAllowed,
    /// Internal error without a safe public detail.
    #[error("internal service error")]
    Internal,
}

/// Domain validation failures that prevent untrusted data from entering shared state.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    /// The order book has no bid levels.
    #[error("order book bid side is empty")]
    EmptyBids,
    /// The order book has no ask levels.
    #[error("order book ask side is empty")]
    EmptyAsks,
    /// The best bid is greater than or equal to the best ask.
    #[error("crossed or locked order book: best bid {best_bid} >= best ask {best_ask}")]
    CrossedBook {
        best_bid: rust_decimal::Decimal,
        best_ask: rust_decimal::Decimal,
    },
    /// A price or amount violated level constraints.
    #[error("invalid order book level: {0}")]
    InvalidLevel(String),
    /// Analytics were requested with no depth levels.
    #[error("depth level count must be greater than zero")]
    InvalidDepth,
    /// Checked decimal arithmetic could not represent a result.
    #[error("decimal calculation overflowed")]
    DecimalOverflow,
    /// A state change violated the explicit service lifecycle.
    #[error("invalid service status transition from {from} to {to}")]
    InvalidStatusTransition { from: String, to: String },
    /// A WebSocket message did not match the supported Nexus envelope.
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
                // Preserve actionable detail in structured logs while keeping database and
                // upstream internals out of the public JSON contract.
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
