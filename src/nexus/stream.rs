//! Live Nexus collector, stream classification, resynchronization, and stale detection.
//!
//! The pinned SDK exposes sequence-bearing `book` events, but the current public payload contract
//! does not provide enough typed information to prove a complete local book. WebSocket events are
//! therefore treated as change notifications: their sequence detects loss of continuity, while a
//! debounced REST snapshot remains authoritative. Disconnect, lag, `out_of_sync`, or a sequence gap
//! discards confidence and requires a successful REST resync before the service can recover.

use std::future::pending;
use std::time::{Duration, Instant};

use chrono::Utc;
use nexus_exchange::ws::Event;
use serde::Deserialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::analytics::calculate;
use crate::error::{AppError, DomainError};
use crate::state::{ServiceStatus, SharedMarketState, SourceConnection};
use crate::telemetry;

use super::adapter::NexusAdapter;

const ANALYTICS_DEPTH_LEVELS: usize = 10;
const SOURCE: &str = "nexus_live";
const WS_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const WS_REFRESH_DEBOUNCE: Duration = Duration::from_millis(500);
/// Explanation surfaced while the official SDK has no usable public testnet WebSocket origin.
///
/// This is an upstream limitation tracked as Nexus ENG-3398, not a simulated success path. REST
/// snapshots may remain usable, but live mode stays explicitly degraded while disconnected.
pub const REST_REFRESH_FALLBACK_REASON: &str =
    "official Nexus SDK 0.9.1 has no usable testnet WebSocket origin (ENG-3398); REST refresh fallback is active";

/// Small internal event vocabulary derived from official SDK events and typed envelopes.
#[derive(Debug, Clone, PartialEq)]
pub enum CollectorEvent {
    /// The SDK established a WebSocket transport.
    Connected,
    /// The SDK transport disconnected with its reported reason.
    Disconnected(String),
    /// The SDK consumer reported dropped frames.
    Lagged { dropped: u64 },
    /// The server accepted the book subscription at a known sequence.
    Subscribed { sequence: u64 },
    /// A book change notification arrived at a known sequence.
    BookUpdate { sequence: u64 },
    /// The server reported that retained stream history cannot satisfy continuity.
    OutOfSync,
    /// The server returned a protocol-level error.
    ServerError(String),
    /// A valid but unsupported or unrelated frame, ignored for forward compatibility.
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum StreamEnvelope {
    Subscribed {
        channel: String,
        market: Option<String>,
        seq_at_join: u64,
    },
    Event {
        channel: String,
        market: Option<String>,
        seq: u64,
        payload: Value,
    },
    OutOfSync {
        channel: String,
        market: Option<String>,
        #[serde(default)]
        oldest_seq: Option<u64>,
    },
    Error {
        message: String,
    },
    Unsubscribed {
        channel: String,
        market: Option<String>,
    },
}

/// Converts an official SDK event into the collector's bounded event vocabulary.
///
/// Messages for other markets or channels become [`CollectorEvent::Unknown`]. Recognized book
/// messages are deserialized into a typed internal envelope rather than inspected through ad-hoc
/// JSON indexing.
///
/// # Errors
///
/// Returns [`DomainError::InvalidStreamPayload`] when a recognized envelope is malformed.
pub fn classify_sdk_event(
    event: Event,
    tracked_market: &str,
) -> Result<CollectorEvent, DomainError> {
    match event {
        Event::Connected => Ok(CollectorEvent::Connected),
        Event::Disconnected(reason) => Ok(CollectorEvent::Disconnected(reason)),
        Event::Lagged { dropped } => Ok(CollectorEvent::Lagged { dropped }),
        Event::Message(value) => classify_message(value, tracked_market),
        _ => Ok(CollectorEvent::Unknown),
    }
}

fn classify_message(value: Value, tracked_market: &str) -> Result<CollectorEvent, DomainError> {
    let Some(operation) = value.get("op").and_then(Value::as_str) else {
        return Err(DomainError::InvalidStreamPayload(
            "missing string field op".to_string(),
        ));
    };
    if !matches!(
        operation,
        "subscribed" | "event" | "out_of_sync" | "error" | "unsubscribed"
    ) {
        return Ok(CollectorEvent::Unknown);
    }

    let envelope: StreamEnvelope = serde_json::from_value(value)
        .map_err(|error| DomainError::InvalidStreamPayload(error.to_string()))?;
    match envelope {
        StreamEnvelope::Subscribed {
            channel,
            market,
            seq_at_join,
        } if channel == "book" && market.as_deref() == Some(tracked_market) => {
            Ok(CollectorEvent::Subscribed {
                sequence: seq_at_join,
            })
        }
        StreamEnvelope::Event {
            channel,
            market,
            seq,
            payload,
        } if channel == "book" && market.as_deref() == Some(tracked_market) => {
            if payload.is_null() {
                return Err(DomainError::InvalidStreamPayload(
                    "book event payload must not be null".to_string(),
                ));
            }
            Ok(CollectorEvent::BookUpdate { sequence: seq })
        }
        StreamEnvelope::OutOfSync {
            channel,
            market,
            oldest_seq,
        } if channel == "book" && market.as_deref() == Some(tracked_market) => {
            let _ = oldest_seq;
            Ok(CollectorEvent::OutOfSync)
        }
        StreamEnvelope::Error { message } => Ok(CollectorEvent::ServerError(message)),
        StreamEnvelope::Unsubscribed { channel, market } => {
            let _ = (channel, market);
            Ok(CollectorEvent::Unknown)
        }
        _ => Ok(CollectorEvent::Unknown),
    }
}

/// Runs the resilient live collector until cancellation.
///
/// The task combines periodic authoritative REST refreshes, WebSocket event handling, and a bounded
/// subscription retry cadence in one `tokio::select!` loop. Individual Nexus failures update shared
/// health and are retried; they do not terminate the process or HTTP server.
pub async fn run_nexus_collector(
    adapter: NexusAdapter,
    state: SharedMarketState,
    market: String,
    rest_refresh_interval: Duration,
    cancellation: CancellationToken,
) {
    {
        let mut current = state.write().await;
        let _ = current.transition_to(ServiceStatus::Connecting);
        current.source_connection = SourceConnection::Connecting;
    }

    if let Err(error) = perform_resync(&adapter, &state, &market, false).await {
        mark_degraded(&state, error.to_string()).await;
    }

    // Subscription construction is retained even when the current testnet origin is unavailable;
    // this lets the same code recover automatically if the official endpoint becomes usable.
    let mut subscription = Some(adapter.subscribe_book(&market));
    let mut ws_connected = false;
    let mut has_connected_once = false;
    let mut rest_ticker = tokio::time::interval(rest_refresh_interval);
    // Missed ticks are skipped so network pauses never cause a burst of obsolete REST requests.
    rest_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    rest_ticker.tick().await;
    let mut ws_retry_ticker = tokio::time::interval(WS_RETRY_INTERVAL);
    ws_retry_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ws_retry_ticker.tick().await;
    let mut last_ws_refresh = Instant::now()
        .checked_sub(WS_REFRESH_DEBOUNCE)
        .unwrap_or_else(Instant::now);

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            _ = rest_ticker.tick() => {
                if let Err(error) = perform_resync(&adapter, &state, &market, ws_connected).await {
                    mark_degraded(&state, error.to_string()).await;
                }
            }
            _ = ws_retry_ticker.tick(), if subscription.is_none() => {
                tracing::info!(market, "retrying official Nexus WebSocket subscription");
                subscription = Some(adapter.subscribe_book(&market));
            }
            event = next_subscription_event(&mut subscription) => {
                let Some(event) = event else {
                    subscription = None;
                    ws_connected = false;
                    metrics::gauge!("nexus_ws_connected", "source" => SOURCE).set(0.0);
                    mark_degraded(&state, REST_REFRESH_FALLBACK_REASON.to_string()).await;
                    continue;
                };
                match classify_sdk_event(event, &market) {
                    Ok(CollectorEvent::Connected) => {
                        if has_connected_once {
                            metrics::counter!("nexus_ws_reconnects_total", "source" => SOURCE).increment(1);
                        }
                        has_connected_once = true;
                        ws_connected = true;
                        metrics::gauge!("nexus_ws_connected", "source" => SOURCE).set(1.0);
                        {
                            let mut current = state.write().await;
                            current.source_connection = SourceConnection::Connected;
                            let _ = current.transition_to(ServiceStatus::Resyncing);
                        }
                        tracing::info!(market, "connected to Nexus WebSocket; resynchronizing");
                        if let Err(error) = perform_resync(&adapter, &state, &market, true).await {
                            mark_degraded(&state, error.to_string()).await;
                        }
                    }
                    Ok(CollectorEvent::Disconnected(reason)) => {
                        ws_connected = false;
                        metrics::gauge!("nexus_ws_connected", "source" => SOURCE).set(0.0);
                        {
                            let mut current = state.write().await;
                            if current.service_status != ServiceStatus::ShuttingDown {
                                current.service_status = ServiceStatus::Reconnecting;
                                current.source_connection = SourceConnection::Reconnecting;
                                current.last_error = Some("Nexus WebSocket disconnected".to_string());
                            }
                        }
                        tracing::warn!(market, reason, "Nexus WebSocket disconnected; SDK will reconnect when an endpoint is available");
                    }
                    Ok(CollectorEvent::Lagged { dropped }) => {
                        metrics::counter!("nexus_ws_lagged_frames_total", "source" => SOURCE).increment(dropped);
                        mark_stale(&state, "Nexus WebSocket consumer dropped frames").await;
                        if let Err(error) = perform_resync(&adapter, &state, &market, ws_connected).await {
                            mark_degraded(&state, error.to_string()).await;
                        }
                    }
                    Ok(CollectorEvent::Subscribed { sequence }) => {
                        set_stream_sequence(&state, sequence).await;
                        if let Err(error) = perform_resync(&adapter, &state, &market, ws_connected).await {
                            mark_degraded(&state, error.to_string()).await;
                        }
                    }
                    Ok(CollectorEvent::BookUpdate { sequence }) => {
                        let gap = update_stream_sequence_and_detect_gap(&state, sequence).await;
                        if gap {
                            metrics::counter!("nexus_ws_lagged_frames_total", "source" => SOURCE).increment(1);
                            mark_stale(&state, "Nexus WebSocket sequence gap detected").await;
                        }
                        // The payload is intentionally not applied as a delta. A debounced REST
                        // snapshot is the smallest correct fallback supported by the public contract.
                        if gap || last_ws_refresh.elapsed() >= WS_REFRESH_DEBOUNCE {
                            last_ws_refresh = Instant::now();
                            if let Err(error) = perform_resync(&adapter, &state, &market, ws_connected).await {
                                mark_degraded(&state, error.to_string()).await;
                            }
                        }
                    }
                    Ok(CollectorEvent::OutOfSync) => {
                        metrics::counter!("nexus_ws_lagged_frames_total", "source" => SOURCE).increment(1);
                        mark_stale(&state, "Nexus WebSocket reported out_of_sync").await;
                        if let Err(error) = perform_resync(&adapter, &state, &market, ws_connected).await {
                            mark_degraded(&state, error.to_string()).await;
                        }
                    }
                    Ok(CollectorEvent::ServerError(message)) => {
                        tracing::warn!(market, message, "Nexus WebSocket returned a protocol error");
                    }
                    Ok(CollectorEvent::Unknown) => {
                        tracing::debug!(market, "ignored an unknown Nexus WebSocket frame");
                    }
                    Err(error) => {
                        tracing::warn!(market, error = %error, "rejected an invalid Nexus WebSocket payload");
                    }
                }
            }
        }
    }

    if let Some(subscription) = subscription {
        subscription.close().await;
    }
    metrics::gauge!("nexus_ws_connected", "source" => SOURCE).set(0.0);
    tracing::info!(market, "Nexus collector stopped");
}

async fn next_subscription_event(
    subscription: &mut Option<nexus_exchange::ws::Subscription>,
) -> Option<Event> {
    match subscription.as_mut() {
        Some(subscription) => subscription.next().await,
        None => pending().await,
    }
}

async fn perform_resync(
    adapter: &NexusAdapter,
    state: &SharedMarketState,
    market: &str,
    connected: bool,
) -> Result<(), AppError> {
    {
        let mut current = state.write().await;
        if current.service_status == ServiceStatus::ShuttingDown {
            return Ok(());
        }
        if current
            .service_status
            .can_transition_to(ServiceStatus::Resyncing)
        {
            current.service_status = ServiceStatus::Resyncing;
        }
    }

    // Every state guard ends before external I/O. This prevents slow Nexus calls from blocking API,
    // persistence, stale detection, or shutdown state changes.
    let catalog = adapter.fetch_catalog(market).await?;
    {
        let mut current = state.write().await;
        if current.service_status == ServiceStatus::ShuttingDown {
            return Ok(());
        }
        current.catalog = catalog.clone();
        current.catalog_updated_at = Some(Utc::now());
    }
    if !catalog.iter().any(|entry| entry.market_id == market) {
        return Err(AppError::Nexus(format!(
            "configured market {market} does not exist in the Nexus testnet catalog"
        )));
    }

    let snapshot = adapter.fetch_snapshot(market).await?;
    if snapshot.symbol != market {
        return Err(AppError::Nexus(format!(
            "Nexus returned snapshot for {} while {market} was requested",
            snapshot.symbol
        )));
    }
    // Validate the entire candidate snapshot before replacing any previously usable values.
    let analytics = calculate(&snapshot.bids, &snapshot.asks, ANALYTICS_DEPTH_LEVELS)?;
    let metrics_snapshot = analytics.clone();
    {
        let mut current = state.write().await;
        if current.service_status == ServiceStatus::ShuttingDown {
            return Ok(());
        }
        current.source_connection = if connected {
            SourceConnection::Connected
        } else {
            SourceConnection::Disconnected
        };
        current.last_error = if connected {
            None
        } else {
            Some(REST_REFRESH_FALLBACK_REASON.to_string())
        };
        current.apply_snapshot(
            snapshot.bids,
            snapshot.asks,
            analytics,
            snapshot.nonce,
            snapshot.source_updated_at,
            connected,
        );
    }

    metrics::counter!("nexus_resyncs_total", "market" => market.to_string(), "source" => SOURCE)
        .increment(1);
    telemetry::record_market_update(
        market,
        SOURCE,
        metrics_snapshot.mid_price,
        metrics_snapshot.spread_bps,
        metrics_snapshot.imbalance,
    );
    tracing::debug!(
        market,
        nonce = snapshot.nonce,
        connected,
        "Nexus order book resynchronized from REST"
    );
    Ok(())
}

async fn mark_degraded(state: &SharedMarketState, error: String) {
    let mut current = state.write().await;
    if current.service_status != ServiceStatus::ShuttingDown {
        current.service_status = ServiceStatus::Degraded;
        current.last_error = Some(error);
    }
}

async fn mark_stale(state: &SharedMarketState, error: &str) {
    let mut current = state.write().await;
    if current.service_status != ServiceStatus::ShuttingDown {
        current.service_status = ServiceStatus::Stale;
        current.last_error = Some(error.to_string());
    }
}

async fn set_stream_sequence(state: &SharedMarketState, sequence: u64) {
    state.write().await.last_stream_sequence = Some(sequence);
}

async fn update_stream_sequence_and_detect_gap(state: &SharedMarketState, sequence: u64) -> bool {
    let mut current = state.write().await;
    let gap = current
        .last_stream_sequence
        .is_some_and(|previous| sequence != previous.saturating_add(1));
    current.last_stream_sequence = Some(sequence);
    gap
}

/// Periodically derives data age from the source timestamp and marks overdue values stale.
///
/// The worker records the age even when another recovery status takes precedence. It never clears
/// old prices or substitutes zeroes, allowing clients to see both the last valid values and their
/// loss of freshness.
pub async fn run_stale_detector(
    state: SharedMarketState,
    stale_after: Duration,
    source: &'static str,
    cancellation: CancellationToken,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                tracing::info!("stale detection worker stopped");
                break;
            }
            _ = ticker.tick() => {
                let now = Utc::now();
                let (market, age, became_stale) = {
                    let mut current = state.write().await;
                    let age = current
                        .last_update_at
                        .and_then(|last| elapsed_since(now, last));
                    let became_stale = current.mark_stale_if_due(now, stale_after);
                    (current.symbol.clone(), age, became_stale)
                };
                if let Some(age) = age {
                    telemetry::record_market_age(&market, source, age);
                }
                if became_stale {
                    tracing::warn!(market, "market data exceeded the stale threshold");
                }
            }
        }
    }
}

fn elapsed_since(
    now: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
) -> Option<Duration> {
    now.signed_duration_since(updated_at).to_std().ok()
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_current_documented_book_event_envelope() {
        let event = Event::Message(json!({
            "op": "event",
            "channel": "book",
            "market": "BTC-USDX-PERP",
            "seq": 43,
            "payload": {"kind": "delta"}
        }));
        assert_eq!(
            classify_sdk_event(event, "BTC-USDX-PERP").expect("valid fixture"),
            CollectorEvent::BookUpdate { sequence: 43 }
        );
    }

    #[test]
    fn parses_connected_disconnected_and_lagged_events() {
        assert_eq!(
            classify_sdk_event(Event::Connected, "BTC-USDX-PERP").expect("valid"),
            CollectorEvent::Connected
        );
        assert_eq!(
            classify_sdk_event(
                Event::Disconnected("temporary failure".to_string()),
                "BTC-USDX-PERP"
            )
            .expect("valid"),
            CollectorEvent::Disconnected("temporary failure".to_string())
        );
        assert_eq!(
            classify_sdk_event(Event::Lagged { dropped: 7 }, "BTC-USDX-PERP").expect("valid"),
            CollectorEvent::Lagged { dropped: 7 }
        );
    }

    #[test]
    fn out_of_sync_requests_resynchronization() {
        let event = Event::Message(json!({
            "op": "out_of_sync",
            "channel": "book",
            "market": "BTC-USDX-PERP",
            "oldest_seq": 100
        }));
        assert_eq!(
            classify_sdk_event(event, "BTC-USDX-PERP").expect("valid"),
            CollectorEvent::OutOfSync
        );
    }

    #[test]
    fn unknown_payload_does_not_crash_the_collector() {
        let event = Event::Message(json!({"op": "future_operation", "data": {}}));
        assert_eq!(
            classify_sdk_event(event, "BTC-USDX-PERP").expect("forward compatible"),
            CollectorEvent::Unknown
        );
    }

    #[test]
    fn invalid_payload_is_rejected() {
        let event = Event::Message(json!({"channel": "book"}));
        assert!(classify_sdk_event(event, "BTC-USDX-PERP").is_err());
    }

    #[tokio::test]
    async fn sequence_gap_is_detected() {
        let state = std::sync::Arc::new(tokio::sync::RwLock::new(crate::state::MarketState::new(
            "BTC-USDX-PERP".to_string(),
        )));
        set_stream_sequence(&state, 10).await;
        assert!(!update_stream_sequence_and_detect_gap(&state, 11).await);
        assert!(update_stream_sequence_and_detect_gap(&state, 13).await);
    }

    #[test]
    fn source_age_uses_the_exchange_timestamp_instead_of_refresh_time() {
        let now = Utc::now();
        let source_updated_at = now - TimeDelta::seconds(37);
        assert_eq!(
            elapsed_since(now, source_updated_at),
            Some(Duration::from_secs(37))
        );
    }
}
