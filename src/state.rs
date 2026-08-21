use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::analytics::{BookAnalytics, BookLevel};
use crate::error::DomainError;

pub type SharedMarketState = Arc<RwLock<MarketState>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceStatus {
    Starting,
    Connecting,
    Resyncing,
    Healthy,
    Stale,
    Reconnecting,
    Degraded,
    ShuttingDown,
}

impl ServiceStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Connecting => "connecting",
            Self::Resyncing => "resyncing",
            Self::Healthy => "healthy",
            Self::Stale => "stale",
            Self::Reconnecting => "reconnecting",
            Self::Degraded => "degraded",
            Self::ShuttingDown => "shutting_down",
        }
    }

    pub const fn can_transition_to(self, next: Self) -> bool {
        if self as u8 == next as u8 {
            return true;
        }
        match self {
            Self::Starting => {
                matches!(next, Self::Connecting | Self::Degraded | Self::ShuttingDown)
            }
            Self::Connecting => matches!(
                next,
                Self::Resyncing | Self::Reconnecting | Self::Degraded | Self::ShuttingDown
            ),
            Self::Resyncing => matches!(
                next,
                Self::Healthy
                    | Self::Stale
                    | Self::Reconnecting
                    | Self::Degraded
                    | Self::ShuttingDown
            ),
            Self::Healthy => matches!(
                next,
                Self::Resyncing
                    | Self::Stale
                    | Self::Reconnecting
                    | Self::Degraded
                    | Self::ShuttingDown
            ),
            Self::Stale => matches!(
                next,
                Self::Resyncing | Self::Reconnecting | Self::Degraded | Self::ShuttingDown
            ),
            Self::Reconnecting => {
                matches!(next, Self::Resyncing | Self::Degraded | Self::ShuttingDown)
            }
            Self::Degraded => matches!(
                next,
                Self::Connecting
                    | Self::Resyncing
                    | Self::Reconnecting
                    | Self::Healthy
                    | Self::Stale
                    | Self::ShuttingDown
            ),
            Self::ShuttingDown => false,
        }
    }
}

impl std::fmt::Display for ServiceStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceConnection {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

impl SourceConnection {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Reconnecting => "reconnecting",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogMarket {
    pub market_id: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tracked: bool,
}

#[derive(Debug, Clone)]
pub struct MarketState {
    pub service_status: ServiceStatus,
    pub source_connection: SourceConnection,
    pub market_id: String,
    pub symbol: String,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub mid_price: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub spread_bps: Option<Decimal>,
    pub bid_depth: Option<Decimal>,
    pub ask_depth: Option<Decimal>,
    pub imbalance: Option<Decimal>,
    pub depth_levels: usize,
    pub source_nonce_or_sequence: Option<i64>,
    pub last_stream_sequence: Option<u64>,
    pub last_update_at: Option<DateTime<Utc>>,
    pub last_resync_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub catalog: Vec<CatalogMarket>,
    pub catalog_updated_at: Option<DateTime<Utc>>,
}

impl MarketState {
    pub fn new(symbol: String) -> Self {
        Self {
            service_status: ServiceStatus::Starting,
            source_connection: SourceConnection::Disconnected,
            market_id: symbol.clone(),
            symbol,
            bids: Vec::new(),
            asks: Vec::new(),
            best_bid: None,
            best_ask: None,
            mid_price: None,
            spread: None,
            spread_bps: None,
            bid_depth: None,
            ask_depth: None,
            imbalance: None,
            depth_levels: 10,
            source_nonce_or_sequence: None,
            last_stream_sequence: None,
            last_update_at: None,
            last_resync_at: None,
            last_error: None,
            catalog: Vec::new(),
            catalog_updated_at: None,
        }
    }

    pub fn transition_to(&mut self, next: ServiceStatus) -> Result<(), DomainError> {
        if !self.service_status.can_transition_to(next) {
            return Err(DomainError::InvalidStatusTransition {
                from: self.service_status.to_string(),
                to: next.to_string(),
            });
        }
        self.service_status = next;
        Ok(())
    }

    pub fn apply_snapshot(
        &mut self,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        analytics: BookAnalytics,
        nonce: i64,
        updated_at: DateTime<Utc>,
        connected: bool,
    ) {
        self.apply_update(bids, asks, analytics, nonce, updated_at, connected);
        if self.service_status != ServiceStatus::ShuttingDown {
            self.last_resync_at = Some(Utc::now());
        }
    }

    pub fn apply_update(
        &mut self,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        analytics: BookAnalytics,
        nonce: i64,
        updated_at: DateTime<Utc>,
        connected: bool,
    ) {
        if self.service_status == ServiceStatus::ShuttingDown {
            return;
        }
        self.bids = bids;
        self.asks = asks;
        self.best_bid = Some(analytics.best_bid);
        self.best_ask = Some(analytics.best_ask);
        self.mid_price = Some(analytics.mid_price);
        self.spread = Some(analytics.spread);
        self.spread_bps = Some(analytics.spread_bps);
        self.bid_depth = Some(analytics.bid_depth);
        self.ask_depth = Some(analytics.ask_depth);
        self.imbalance = analytics.imbalance;
        self.depth_levels = analytics.depth_levels;
        self.source_nonce_or_sequence = Some(nonce);
        self.last_update_at = Some(updated_at);
        self.service_status = if connected {
            ServiceStatus::Healthy
        } else {
            ServiceStatus::Degraded
        };
    }

    pub fn has_usable_snapshot(&self) -> bool {
        self.best_bid.is_some()
            && self.best_ask.is_some()
            && self.mid_price.is_some()
            && self.spread.is_some()
            && self.spread_bps.is_some()
            && self.bid_depth.is_some()
            && self.ask_depth.is_some()
            && self.last_update_at.is_some()
    }

    pub fn mark_stale_if_due(&mut self, now: DateTime<Utc>, stale_after: Duration) -> bool {
        if matches!(
            self.service_status,
            ServiceStatus::Reconnecting | ServiceStatus::Resyncing | ServiceStatus::ShuttingDown
        ) {
            return false;
        }
        let Some(last_update_at) = self.last_update_at else {
            return false;
        };
        let age = now.signed_duration_since(last_update_at);
        let stale = age
            .to_std()
            .map(|duration| duration > stale_after)
            .unwrap_or(false);
        if stale && self.service_status != ServiceStatus::Stale {
            self.service_status = ServiceStatus::Stale;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    #[test]
    fn supports_the_documented_state_machine_transitions() {
        let transitions = [
            (ServiceStatus::Starting, ServiceStatus::Connecting),
            (ServiceStatus::Connecting, ServiceStatus::Resyncing),
            (ServiceStatus::Resyncing, ServiceStatus::Healthy),
            (ServiceStatus::Healthy, ServiceStatus::Reconnecting),
            (ServiceStatus::Healthy, ServiceStatus::Stale),
            (ServiceStatus::Stale, ServiceStatus::Resyncing),
            (ServiceStatus::Reconnecting, ServiceStatus::Resyncing),
            (ServiceStatus::Healthy, ServiceStatus::ShuttingDown),
        ];
        for (from, to) in transitions {
            assert!(from.can_transition_to(to), "{from} -> {to} should be valid");
        }
    }

    #[test]
    fn resyncing_can_become_healthy_more_than_once() {
        let mut state = MarketState::new("BTC-USDX-PERP".to_string());
        state
            .transition_to(ServiceStatus::Connecting)
            .expect("valid");
        state
            .transition_to(ServiceStatus::Resyncing)
            .expect("valid");
        state.transition_to(ServiceStatus::Healthy).expect("valid");
        state.transition_to(ServiceStatus::Stale).expect("valid");
        state
            .transition_to(ServiceStatus::Resyncing)
            .expect("valid");
        state.transition_to(ServiceStatus::Healthy).expect("valid");
    }

    #[test]
    fn stale_detection_uses_the_supplied_clock() {
        let now = Utc::now();
        let mut state = MarketState::new("BTC-USDX-PERP".to_string());
        state.service_status = ServiceStatus::Healthy;
        state.last_update_at = Some(now - TimeDelta::seconds(16));
        assert!(state.mark_stale_if_due(now, Duration::from_secs(15)));
        assert_eq!(state.service_status, ServiceStatus::Stale);
    }

    #[test]
    fn fresh_data_is_not_stale() {
        let now = Utc::now();
        let mut state = MarketState::new("BTC-USDX-PERP".to_string());
        state.service_status = ServiceStatus::Healthy;
        state.last_update_at = Some(now - TimeDelta::seconds(5));
        assert!(!state.mark_stale_if_due(now, Duration::from_secs(15)));
        assert_eq!(state.service_status, ServiceStatus::Healthy);
    }
}
