use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::EnvFilter;

use crate::config::{AppEnvironment, Config};

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

pub fn init_metrics(market: &str) -> Result<PrometheusHandle, String> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|error| format!("failed to install Prometheus recorder: {error}"))?;
    initialize_metric_series(market);
    Ok(handle)
}

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

pub fn record_market_age(market: &str, source: &'static str, age: Duration) {
    metrics::gauge!("nexus_market_data_age_seconds", "market" => market.to_string(), "source" => source)
        .set(age.as_secs_f64());
}

fn decimal_to_f64(value: rust_decimal::Decimal) -> Option<f64> {
    use rust_decimal::prelude::ToPrimitive;
    value.to_f64()
}
