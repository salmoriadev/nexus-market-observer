//! Concurrent market state and service lifecycle.
//!
//! Collectors validate and calculate data before acquiring the write lock, then replace the
//! relevant state in one short critical section. HTTP and persistence readers take snapshots from
//! the same structure. No network or database operation is performed while this lock is held.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::analytics::{BookAnalytics, BookLevel};
use crate::error::DomainError;

/// Shared handle to the latest validated market state.
pub type SharedMarketState = Arc<RwLock<MarketState>>;

/// High-level lifecycle of market-data confidence.
///
/// Status describes whether the current values can be trusted, not merely whether the process is
/// alive. The HTTP health endpoint may therefore remain available while this status is stale or
/// degraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceStatus {
    /// State exists, but background components have not started.
    Starting,
    /// The collector is establishing its upstream source.
    Connecting,
    /// A fresh authoritative snapshot is being obtained.
    Resyncing,
    /// A complete and current snapshot is available.
    Healthy,
    /// The last valid snapshot exceeded its freshness threshold or stream continuity was lost.
    Stale,
    /// Stream connectivity was lost and recovery is in progress.
    Reconnecting,
    /// The process is serving, but the configured market-data source is impaired.
    Degraded,
    /// Coordinated shutdown has begun; collectors must no longer mutate market values.
    ShuttingDown,
}

impl ServiceStatus {
    /// Stable snake-case representation used by logs and API responses.
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

    /// Returns whether the documented lifecycle permits a transition.
    ///
    /// Self-transitions are accepted to make repeated idempotent recovery signals harmless.
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

/// Transport-level connection state, kept separate from data confidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceConnection {
    /// No active source transport exists.
    Disconnected,
    /// Initial source connection is in progress.
    Connecting,
    /// Source transport is active.
    Connected,
    /// A previously active transport is recovering.
    Reconnecting,
}

impl SourceConnection {
    /// Stable snake-case representation used by health responses.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Reconnecting => "reconnecting",
        }
    }
}

/// Minimal public market metadata retained from the upstream catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogMarket {
    /// Stable upstream identifier used by persistence foreign keys.
    pub market_id: String,
    /// Public symbol accepted by API routes.
    pub symbol: String,
    /// Base asset code.
    pub base_asset: String,
    /// Quote asset code.
    pub quote_asset: String,
    /// Whether this is the single market configured for collection.
    pub tracked: bool,
}

/// Latest validated order book, derived metrics, provenance, and recovery metadata.
///
/// `source_nonce_or_sequence` identifies the authoritative snapshot used by persistence, whereas
/// `last_stream_sequence` tracks transport continuity and is never presented as a REST nonce.
#[derive(Debug, Clone)]
pub struct MarketState {
    /// Current data-confidence lifecycle.
    pub service_status: ServiceStatus,
    /// Current transport-level connectivity.
    pub source_connection: SourceConnection,
    /// Stable catalog identifier used for persistence.
    pub market_id: String,
    /// Configured public market symbol.
    pub symbol: String,
    /// Latest complete validated bid side.
    pub bids: Vec<BookLevel>,
    /// Latest complete validated ask side.
    pub asks: Vec<BookLevel>,
    /// Highest bid from the latest analytics pass.
    pub best_bid: Option<Decimal>,
    /// Lowest ask from the latest analytics pass.
    pub best_ask: Option<Decimal>,
    /// Arithmetic mean of best bid and ask.
    pub mid_price: Option<Decimal>,
    /// Absolute best-price spread.
    pub spread: Option<Decimal>,
    /// Best-price spread in basis points.
    pub spread_bps: Option<Decimal>,
    /// Bid quantity across the effective top-N depth.
    pub bid_depth: Option<Decimal>,
    /// Ask quantity across the effective top-N depth.
    pub ask_depth: Option<Decimal>,
    /// Normalized top-N depth difference.
    pub imbalance: Option<Decimal>,
    /// Equal number of bid and ask levels included in depth metrics.
    pub depth_levels: usize,
    /// REST nonce or replay sequence identifying the published snapshot.
    pub source_nonce_or_sequence: Option<i64>,
    /// Last WebSocket sequence used solely for continuity checks.
    pub last_stream_sequence: Option<u64>,
    /// Source-provided time of the latest valid market update.
    pub last_update_at: Option<DateTime<Utc>>,
    /// Local time at which an authoritative replacement completed.
    pub last_resync_at: Option<DateTime<Utc>>,
    /// User-facing failure context exposed by the health response.
    pub last_error: Option<String>,
    /// Latest reduced market catalog.
    pub catalog: Vec<CatalogMarket>,
    /// Local time at which the catalog was last replaced.
    pub catalog_updated_at: Option<DateTime<Utc>>,
}

impl MarketState {
    /// Creates an empty state for a configured market in [`ServiceStatus::Starting`].
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

    /// Applies a lifecycle transition after checking the state-machine contract.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidStatusTransition`] when `next` is not reachable from the
    /// current status.
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

    /// Atomically replaces the current book after an authoritative resynchronization.
    ///
    /// The caller must calculate and validate `analytics` before acquiring the shared write lock.
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

    /// Atomically replaces the current book with an already validated update.
    ///
    /// Mutations are ignored once coordinated shutdown begins. A valid update becomes healthy only
    /// when the source is connected; REST fallback data remains available but degraded.
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

    /// Returns whether all required two-sided market metrics are available to API consumers.
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

    /// Marks a usable snapshot stale when its source timestamp exceeds `stale_after`.
    ///
    /// Recovery and shutdown statuses take precedence so the detector cannot overwrite a more
    /// specific lifecycle state. Returns `true` only on the transition into stale.
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
