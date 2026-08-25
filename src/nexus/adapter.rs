//! Thin adapter around the pinned official `nexus-exchange` SDK.
//!
//! REST responses and transport errors are converted here so analytics and shared state depend on
//! decimal-safe domain models. All external REST operations are bounded by the configured timeout,
//! and construction is hard-coded to public testnet endpoints.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use nexus_exchange::ws::Subscription;
use nexus_exchange::{Client, Config as SdkConfig, Network};
use serde_json::json;

use crate::analytics::BookLevel;
use crate::error::AppError;
use crate::state::CatalogMarket;

/// Authoritative REST order-book snapshot converted into service domain types.
#[derive(Debug, Clone)]
pub struct NexusSnapshot {
    /// Market symbol returned by Nexus, verified against the request before use.
    pub symbol: String,
    /// Complete bid side as decimal values.
    pub bids: Vec<BookLevel>,
    /// Complete ask side as decimal values.
    pub asks: Vec<BookLevel>,
    /// Source nonce used for snapshot identity and persistence deduplication.
    pub nonce: i64,
    /// Exchange-provided update time, used for stale detection instead of local receipt time.
    pub source_updated_at: DateTime<Utc>,
}

/// Public testnet client with service-level timeout and user-agent policy.
///
/// The client is shared because the official SDK internally owns its HTTP and subscription state;
/// no credentials or authenticated endpoints are configured.
#[derive(Clone)]
pub struct NexusAdapter {
    client: Arc<Client>,
    operation_timeout: Duration,
}

impl std::fmt::Debug for NexusAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NexusAdapter")
            .field("network", &"testnet")
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl NexusAdapter {
    /// Creates an adapter restricted to the official Nexus testnet network.
    pub fn testnet(operation_timeout: Duration) -> Self {
        let sdk_config = SdkConfig::new(Network::Testnet)
            .with_timeout(operation_timeout)
            .with_user_agent(format!(
                "nexus-market-observer/{}",
                env!("CARGO_PKG_VERSION")
            ));
        Self {
            client: Arc::new(Client::new(sdk_config)),
            operation_timeout,
        }
    }

    /// Fetches and reduces the public market catalog.
    ///
    /// The configured market is only marked as tracked when its exact ID exists; callers must treat
    /// absence as degraded rather than silently selecting another market.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Nexus`] on timeout or an SDK request failure.
    pub async fn fetch_catalog(
        &self,
        tracked_market: &str,
    ) -> Result<Vec<CatalogMarket>, AppError> {
        let markets = tokio::time::timeout(self.operation_timeout, self.client.fetch_markets())
            .await
            .map_err(|_| AppError::Nexus("market catalog request timed out".to_string()))?
            .map_err(|error| AppError::Nexus(format!("market catalog request failed: {error}")))?;

        Ok(markets
            .into_iter()
            .map(|market| CatalogMarket {
                symbol: market.market_id.clone(),
                tracked: market.market_id == tracked_market,
                market_id: market.market_id,
                base_asset: market.base_asset,
                quote_asset: market.quote_asset,
            })
            .collect())
    }

    /// Fetches one complete public order-book snapshot through the official SDK.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] on timeout, transport failure, or invalid source timestamp.
    pub async fn fetch_snapshot(&self, market: &str) -> Result<NexusSnapshot, AppError> {
        let book =
            tokio::time::timeout(self.operation_timeout, self.client.fetch_order_book(market))
                .await
                .map_err(|_| AppError::Nexus(format!("order book request for {market} timed out")))?
                .map_err(|error| {
                    AppError::Nexus(format!("order book request for {market} failed: {error}"))
                })?;

        snapshot_from_sdk(book)
    }

    /// Opens the official SDK subscription for the current `book` channel contract.
    ///
    /// The subscription is used for connectivity, sequencing, and change notification. Its opaque
    /// payload is not guessed into local deltas; the stream coordinator refreshes from REST.
    pub fn subscribe_book(&self, market: &str) -> Subscription {
        self.client.connect(vec![json!({
            "op": "subscribe",
            "channel": "book",
            "market": market,
        })])
    }
}

fn snapshot_from_sdk(book: nexus_exchange::types::OrderBook) -> Result<NexusSnapshot, AppError> {
    let source_updated_at = DateTime::parse_from_rfc3339(&book.datetime)
        .map_err(|error| {
            AppError::Nexus(format!(
                "order book for {} contained invalid datetime {}: {error}",
                book.symbol, book.datetime
            ))
        })?
        .with_timezone(&Utc);
    let bids = book
        .bids
        .iter()
        .map(|level| BookLevel {
            price: level.price(),
            amount: level.amount(),
        })
        .collect();
    let asks = book
        .asks
        .iter()
        .map(|level| BookLevel {
            price: level.price(),
            amount: level.amount(),
        })
        .collect();

    Ok(NexusSnapshot {
        symbol: book.symbol,
        bids,
        asks,
        nonce: book.nonce,
        source_updated_at,
    })
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_official_rest_snapshot_shape() {
        // Sanitized from GET /api/exchange/api/v1/markets/BTC-USDX-PERP/orderbook
        // on Nexus Exchange Testnet, verified on 2026-08-24 (Brazil/East).
        let book = serde_json::from_value::<nexus_exchange::types::OrderBook>(json!({
            "symbol": "BTC-USDX-PERP",
            "bids": [[80648.0, 0.065], [80642.5, 0.055]],
            "asks": [[80760.0, 0.001], [80761.0, 0.015]],
            "timestamp": 1787624581670_i64,
            "datetime": "2026-08-25T02:23:01.670Z",
            "nonce": 406377
        }))
        .expect("official fixture should deserialize through SDK types");
        let snapshot = snapshot_from_sdk(book).expect("valid snapshot");
        assert_eq!(snapshot.symbol, "BTC-USDX-PERP");
        assert_eq!(snapshot.nonce, 406377);
        assert_eq!(snapshot.bids[0].price, dec!(80648.0));
        assert_eq!(snapshot.asks[0].amount, dec!(0.001));
    }

    #[test]
    fn rejects_snapshot_with_invalid_datetime() {
        let book = serde_json::from_value::<nexus_exchange::types::OrderBook>(json!({
            "symbol": "BTC-USDX-PERP",
            "bids": [[10, 1]],
            "asks": [[11, 1]],
            "timestamp": 1,
            "datetime": "not-a-timestamp",
            "nonce": 1
        }))
        .expect("SDK payload shape is valid");
        assert!(snapshot_from_sdk(book).is_err());
    }
}
