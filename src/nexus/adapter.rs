use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use nexus_exchange::ws::Subscription;
use nexus_exchange::{Client, Config as SdkConfig, Network};
use serde_json::json;

use crate::analytics::BookLevel;
use crate::error::AppError;
use crate::state::CatalogMarket;

#[derive(Debug, Clone)]
pub struct NexusSnapshot {
    pub symbol: String,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub nonce: i64,
    pub source_updated_at: DateTime<Utc>,
}

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
    pub fn testnet(operation_timeout: Duration) -> Self {
        let sdk_config = SdkConfig::new(Network::Testnet)
            .with_timeout(operation_timeout)
            .with_user_agent(format!(
                "nexus-market-intelligence/{}",
                env!("CARGO_PKG_VERSION")
            ));
        Self {
            client: Arc::new(Client::new(sdk_config)),
            operation_timeout,
        }
    }

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
        // on Nexus Exchange Testnet, verified on 2026-08-20.
        let book = serde_json::from_value::<nexus_exchange::types::OrderBook>(json!({
            "symbol": "BTC-USDX-PERP",
            "bids": [[73805.5, 0.001], [73790.5, 0.065]],
            "asks": [[73864.0, 0.015], [73867.5, 0.012]],
            "timestamp": 1787272091491_i64,
            "datetime": "2026-08-21T00:28:11.491Z",
            "nonce": 61925
        }))
        .expect("official fixture should deserialize through SDK types");
        let snapshot = snapshot_from_sdk(book).expect("valid snapshot");
        assert_eq!(snapshot.symbol, "BTC-USDX-PERP");
        assert_eq!(snapshot.nonce, 61925);
        assert_eq!(snapshot.bids[0].price, dec!(73805.5));
        assert_eq!(snapshot.asks[0].amount, dec!(0.015));
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
