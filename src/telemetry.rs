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

pub fn init_metrics() -> Result<PrometheusHandle, String> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|error| format!("failed to install Prometheus recorder: {error}"))?;
    initialize_metric_series();
    Ok(handle)
}

pub fn isolated_metrics_handle() -> PrometheusHandle {
    PrometheusBuilder::new().build_recorder().handle()
}

fn initialize_metric_series() {
    metrics::gauge!("nexus_ws_connected").set(0.0);
    metrics::counter!("nexus_ws_reconnects_total").absolute(0);
    metrics::counter!("nexus_ws_lagged_frames_total").absolute(0);
    metrics::counter!("nexus_resyncs_total").absolute(0);
    metrics::counter!("nexus_snapshots_persisted_total").absolute(0);
    metrics::counter!("nexus_database_errors_total").absolute(0);
    metrics::counter!("nexus_http_requests_total", "method" => "none", "route" => "none", "status" => "0").absolute(0);
    metrics::histogram!("nexus_http_request_duration_seconds", "method" => "none", "route" => "none").record(0.0);
}

pub fn record_market_update(
    market: &str,
    mid_price: rust_decimal::Decimal,
    spread_bps: rust_decimal::Decimal,
    imbalance: Option<rust_decimal::Decimal>,
) {
    metrics::counter!("nexus_market_updates_total", "market" => market.to_string()).increment(1);
    if let Some(value) = decimal_to_f64(mid_price) {
        metrics::gauge!("nexus_market_mid_price", "market" => market.to_string()).set(value);
    }
    if let Some(value) = decimal_to_f64(spread_bps) {
        metrics::gauge!("nexus_market_spread_bps", "market" => market.to_string()).set(value);
    }
    if let Some(value) = imbalance.and_then(decimal_to_f64) {
        metrics::gauge!("nexus_market_imbalance", "market" => market.to_string()).set(value);
    }
}

pub fn record_market_age(market: &str, age: Duration) {
    metrics::gauge!("nexus_market_data_age_seconds", "market" => market.to_string())
        .set(age.as_secs_f64());
}

fn decimal_to_f64(value: rust_decimal::Decimal) -> Option<f64> {
    use rust_decimal::prelude::ToPrimitive;
    value.to_f64()
}
