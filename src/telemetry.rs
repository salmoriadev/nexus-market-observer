//! Structured logging and Prometheus instrumentation.
//!
//! Metric labels are restricted to configured markets, fixed source names, normalized HTTP routes,
//! and bounded operation names. Financial values remain [`rust_decimal::Decimal`] everywhere else;
//! lossy `f64` conversion occurs only at the Prometheus boundary, where the metrics protocol
//! requires floating-point samples.

use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::EnvFilter;

use crate::config::{AppEnvironment, Config};

/// Installs the global tracing subscriber selected by the application environment.
///
/// Production uses JSON for log aggregation; development and tests use compact human-readable
/// output. This function must be called at most once per process.
pub fn init_tracing(config: &Config) -> Result<(), String> {
    let filter = EnvFilter::try_new(&config.rust_log)
        .map_err(|error| format!("invalid RUST_LOG filter: {error}"))?;

    match config.app_env {
        AppEnvironment::Production => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init()
            .map_err(|error| format!("failed to initialize JSON tracing: {error}")),
        AppEnvironment::Development | AppEnvironment::Test => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .compact()
            .try_init()
            .map_err(|error| format!("failed to initialize tracing: {error}")),
    }
}

/// Installs the process-wide metrics recorder and pre-creates bounded time series.
///
/// Pre-initialization makes zero-valued counters visible to Prometheus and Grafana before the first
/// source event. This function must be called at most once per process.
pub fn init_metrics(market: &str) -> Result<PrometheusHandle, String> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|error| format!("failed to install Prometheus recorder: {error}"))?;
    initialize_metric_series(market);
    Ok(handle)
}

/// Creates a recorder handle for in-memory router tests without installing a global recorder.
pub fn isolated_metrics_handle() -> PrometheusHandle {
    PrometheusBuilder::new().build_recorder().handle()
}

fn initialize_metric_series(market: &str) {
    for source in ["replay", "nexus_live"] {
        metrics::gauge!("nexus_ws_connected", "source" => source).set(0.0);
        metrics::counter!("nexus_ws_reconnects_total", "source" => source).absolute(0);
        metrics::counter!("nexus_ws_lagged_frames_total", "source" => source).absolute(0);
        metrics::counter!("nexus_resyncs_total", "market" => market.to_string(), "source" => source).absolute(0);
        metrics::counter!("nexus_market_updates_total", "market" => market.to_string(), "source" => source).absolute(0);
        metrics::counter!("nexus_snapshots_persisted_total", "market" => market.to_string(), "source" => source).absolute(0);
    }
    for operation in ["persist_snapshot", "ping"] {
        metrics::counter!("nexus_database_errors_total", "operation" => operation).absolute(0);
    }
}

/// Records a validated market update and its derived gauges.
///
/// Values that cannot be represented by the telemetry backend are omitted rather than replaced
/// with a plausible zero.
pub fn record_market_update(
    market: &str,
    source: &'static str,
    mid_price: rust_decimal::Decimal,
    spread_bps: rust_decimal::Decimal,
    imbalance: Option<rust_decimal::Decimal>,
) {
    metrics::counter!("nexus_market_updates_total", "market" => market.to_string(), "source" => source).increment(1);
    if let Some(value) = decimal_to_f64(mid_price) {
        metrics::gauge!("nexus_market_mid_price", "market" => market.to_string(), "source" => source).set(value);
    }
    if let Some(value) = decimal_to_f64(spread_bps) {
        metrics::gauge!("nexus_market_spread_bps", "market" => market.to_string(), "source" => source).set(value);
    }
    if let Some(value) = imbalance.and_then(decimal_to_f64) {
        metrics::gauge!("nexus_market_imbalance", "market" => market.to_string(), "source" => source).set(value);
    }
}

/// Updates the age gauge from an exchange or replay source timestamp.
pub fn record_market_age(market: &str, source: &'static str, age: Duration) {
    metrics::gauge!("nexus_market_data_age_seconds", "market" => market.to_string(), "source" => source)
        .set(age.as_secs_f64());
}

fn decimal_to_f64(value: rust_decimal::Decimal) -> Option<f64> {
    use rust_decimal::prelude::ToPrimitive;
    value.to_f64()
}
