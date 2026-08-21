use std::str::FromStr;
use std::time::Duration;

use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::analytics::{calculate, BookLevel};
use crate::error::AppError;
use crate::state::{CatalogMarket, ServiceStatus, SharedMarketState, SourceConnection};
use crate::telemetry;

const SOURCE: &str = "replay";
const ANALYTICS_DEPTH_LEVELS: usize = 10;
const UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const FAILURE_HOLD: Duration = Duration::from_secs(2);
const GAP_STEP: u64 = 8;
const DISCONNECT_STEP: u64 = 16;

#[derive(Debug, Clone)]
pub struct ReplaySource {
    market: CatalogMarket,
    frames: Vec<ReplayFrame>,
}

#[derive(Debug, Clone)]
struct ReplayFrame {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

#[derive(Debug, Deserialize)]
struct ReplayFixture {
    market: ReplayMarket,
    frames: Vec<RawReplayFrame>,
}

#[derive(Debug, Deserialize)]
struct ReplayMarket {
    market_id: String,
    symbol: String,
    base_asset: String,
    quote_asset: String,
}

#[derive(Debug, Deserialize)]
struct RawReplayFrame {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

impl ReplaySource {
    pub fn bundled() -> Result<Self, AppError> {
        Self::from_json(include_str!("../fixtures/replay/btc-usdx-perp.json"))
    }

    fn from_json(json: &str) -> Result<Self, AppError> {
        let fixture: ReplayFixture = serde_json::from_str(json)
            .map_err(|error| AppError::Task(format!("invalid bundled replay fixture: {error}")))?;
        if fixture.frames.is_empty() {
            return Err(AppError::Task(
                "bundled replay fixture contains no frames".to_string(),
            ));
        }

        let frames = fixture
            .frames
            .into_iter()
            .map(parse_frame)
            .collect::<Result<Vec<_>, _>>()?;
        let market = CatalogMarket {
            tracked: true,
            market_id: fixture.market.market_id,
            symbol: fixture.market.symbol,
            base_asset: fixture.market.base_asset,
            quote_asset: fixture.market.quote_asset,
        };
        if market.market_id != "BTC-USDX-PERP" || market.symbol != market.market_id {
            return Err(AppError::Task(
                "bundled replay fixture identifies an unexpected market".to_string(),
            ));
        }
        for frame in &frames {
            calculate(&frame.bids, &frame.asks, ANALYTICS_DEPTH_LEVELS)?;
        }
        Ok(Self { market, frames })
    }
}

fn parse_frame(frame: RawReplayFrame) -> Result<ReplayFrame, AppError> {
    Ok(ReplayFrame {
        bids: parse_levels(frame.bids)?,
        asks: parse_levels(frame.asks)?,
    })
}

fn parse_levels(raw: Vec<[String; 2]>) -> Result<Vec<BookLevel>, AppError> {
    raw.into_iter()
        .map(|[price, amount]| {
            let price = Decimal::from_str(&price).map_err(|error| {
                AppError::Task(format!("invalid replay price {price}: {error}"))
            })?;
            let amount = Decimal::from_str(&amount).map_err(|error| {
                AppError::Task(format!("invalid replay amount {amount}: {error}"))
            })?;
            Ok(BookLevel { price, amount })
        })
        .collect()
}

pub async fn run_replay_collector(
    source: ReplaySource,
    state: SharedMarketState,
    market: String,
    cancellation: CancellationToken,
) {
    if source.market.market_id != market {
        mark_degraded(
            &state,
            format!("replay fixture does not provide configured market {market}"),
        )
        .await;
        return;
    }

    initialize_replay(&source, &state).await;
    let mut step = 0_u64;
    let mut sequence = 1_000_i64;
    let mut frame_index = 1_usize;
    let mut ticker = tokio::time::interval(UPDATE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            _ = ticker.tick() => {
                step = step.saturating_add(1);
                sequence = sequence.saturating_add(1);

                if step == GAP_STEP {
                    sequence = sequence.saturating_add(1);
                    mark_stale(&state, sequence).await;
                    metrics::counter!("nexus_ws_lagged_frames_total", "source" => SOURCE).increment(1);
                    tracing::warn!(market, sequence, "replay injected a deterministic sequence gap");
                    if wait_or_cancel(&cancellation, FAILURE_HOLD).await {
                        break;
                    }
                    resync(&source, &state, sequence, frame_index).await;
                } else if step == DISCONNECT_STEP {
                    mark_reconnecting(&state).await;
                    metrics::gauge!("nexus_ws_connected", "source" => SOURCE).set(0.0);
                    tracing::warn!(market, "replay injected a deterministic disconnection");
                    if wait_or_cancel(&cancellation, FAILURE_HOLD).await {
                        break;
                    }
                    metrics::counter!("nexus_ws_reconnects_total", "source" => SOURCE).increment(1);
                    resync(&source, &state, sequence, frame_index).await;
                    step = 0;
                } else {
                    apply_update(&source, &state, sequence, frame_index).await;
                }

                frame_index = (frame_index + 1) % source.frames.len();
            }
        }
    }

    metrics::gauge!("nexus_ws_connected", "source" => SOURCE).set(0.0);
    tracing::info!(market, "replay collector stopped");
}

async fn initialize_replay(source: &ReplaySource, state: &SharedMarketState) {
    {
        let mut current = state.write().await;
        let _ = current.transition_to(ServiceStatus::Connecting);
        current.source_connection = SourceConnection::Connecting;
        current.catalog = vec![source.market.clone()];
        current.catalog_updated_at = Some(Utc::now());
        let _ = current.transition_to(ServiceStatus::Resyncing);
    }
    resync(source, state, 1_000, 0).await;
    tracing::info!(
        market = source.market.market_id,
        "deterministic replay source started"
    );
}

async fn apply_update(
    source: &ReplaySource,
    state: &SharedMarketState,
    sequence: i64,
    frame_index: usize,
) {
    let frame = &source.frames[frame_index];
    let Ok(analytics) = calculate(&frame.bids, &frame.asks, ANALYTICS_DEPTH_LEVELS) else {
        mark_degraded(
            state,
            "replay frame failed analytics validation".to_string(),
        )
        .await;
        return;
    };
    let metrics_snapshot = analytics.clone();
    {
        let mut current = state.write().await;
        current.source_connection = SourceConnection::Connected;
        current.last_stream_sequence = u64::try_from(sequence).ok();
        current.last_error = None;
        current.apply_update(
            frame.bids.clone(),
            frame.asks.clone(),
            analytics,
            sequence,
            Utc::now(),
            true,
        );
    }
    metrics::gauge!("nexus_ws_connected", "source" => SOURCE).set(1.0);
    telemetry::record_market_update(
        &source.market.market_id,
        SOURCE,
        metrics_snapshot.mid_price,
        metrics_snapshot.spread_bps,
        metrics_snapshot.imbalance,
    );
    telemetry::record_market_age(&source.market.market_id, SOURCE, Duration::ZERO);
}

async fn resync(
    source: &ReplaySource,
    state: &SharedMarketState,
    sequence: i64,
    frame_index: usize,
) {
    {
        let mut current = state.write().await;
        if current.service_status == ServiceStatus::ShuttingDown {
            return;
        }
        let _ = current.transition_to(ServiceStatus::Resyncing);
    }
    apply_update(source, state, sequence, frame_index).await;
    state.write().await.last_resync_at = Some(Utc::now());
    metrics::counter!("nexus_resyncs_total", "market" => source.market.market_id.clone(), "source" => SOURCE).increment(1);
}

async fn mark_stale(state: &SharedMarketState, sequence: i64) {
    let mut current = state.write().await;
    if current.service_status != ServiceStatus::ShuttingDown {
        current.service_status = ServiceStatus::Stale;
        current.last_stream_sequence = u64::try_from(sequence).ok();
        current.last_error = Some("replay sequence gap injected for demonstration".to_string());
    }
}

async fn mark_reconnecting(state: &SharedMarketState) {
    let mut current = state.write().await;
    if current.service_status != ServiceStatus::ShuttingDown {
        current.service_status = ServiceStatus::Reconnecting;
        current.source_connection = SourceConnection::Reconnecting;
        current.last_error = Some("replay disconnection injected for demonstration".to_string());
    }
}

async fn mark_degraded(state: &SharedMarketState, error: String) {
    let mut current = state.write().await;
    if current.service_status != ServiceStatus::ShuttingDown {
        current.service_status = ServiceStatus::Degraded;
        current.source_connection = SourceConnection::Disconnected;
        current.last_error = Some(error);
    }
}

async fn wait_or_cancel(cancellation: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = cancellation.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_fixture_is_valid_and_decimal_safe() {
        let source = ReplaySource::bundled().expect("bundled replay fixture");
        assert_eq!(source.market.market_id, "BTC-USDX-PERP");
        assert_eq!(source.frames.len(), 4);
        assert_eq!(source.frames[0].bids[0].price.to_string(), "73805.50");
        assert_eq!(source.frames[0].bids.len(), 10);
        assert_eq!(source.frames[0].asks.len(), 10);
    }

    #[test]
    fn invalid_replay_payload_is_rejected() {
        let invalid = r#"{
            "market":{"market_id":"BTC-USDX-PERP","symbol":"BTC-USDX-PERP","base_asset":"BTC","quote_asset":"USDX"},
            "frames":[{"bids":[["invalid","1"]],"asks":[["11","1"]]}]
        }"#;
        assert!(ReplaySource::from_json(invalid).is_err());
    }

    #[tokio::test]
    async fn replay_initializes_a_healthy_state() {
        let source = ReplaySource::bundled().expect("fixture");
        let state = std::sync::Arc::new(tokio::sync::RwLock::new(crate::state::MarketState::new(
            "BTC-USDX-PERP".to_string(),
        )));
        initialize_replay(&source, &state).await;
        let current = state.read().await;
        assert_eq!(current.service_status, ServiceStatus::Healthy);
        assert_eq!(current.source_connection, SourceConnection::Connected);
        assert!(current.has_usable_snapshot());
        assert_eq!(current.catalog.len(), 1);
    }

    #[tokio::test]
    async fn replay_models_gap_disconnect_and_recovery() {
        let source = ReplaySource::bundled().expect("fixture");
        let state = std::sync::Arc::new(tokio::sync::RwLock::new(crate::state::MarketState::new(
            "BTC-USDX-PERP".to_string(),
        )));
        initialize_replay(&source, &state).await;

        mark_stale(&state, 1_010).await;
        assert_eq!(state.read().await.service_status, ServiceStatus::Stale);
        resync(&source, &state, 1_010, 1).await;
        assert_eq!(state.read().await.service_status, ServiceStatus::Healthy);

        mark_reconnecting(&state).await;
        assert_eq!(
            state.read().await.service_status,
            ServiceStatus::Reconnecting
        );
        resync(&source, &state, 1_011, 2).await;
        let current = state.read().await;
        assert_eq!(current.service_status, ServiceStatus::Healthy);
        assert_eq!(current.source_connection, SourceConnection::Connected);
    }
}
