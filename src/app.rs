//! Application composition and coordinated process lifecycle.
//!
//! PostgreSQL is a startup dependency because migrations must succeed before the API can serve.
//! Nexus is deliberately not a startup dependency: collectors can report degraded state and keep
//! retrying while the HTTP process remains available. Shutdown uses cancellation tokens followed
//! by bounded joins so no background task is abandoned silently.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::{self, AppContext};
use crate::config::{Config, MarketDataSourceMode};
use crate::error::AppError;
use crate::nexus::{run_nexus_collector, run_stale_detector, NexusAdapter};
use crate::persistence::{connect_with_retry, run_persistence_worker, PostgresRepository};
use crate::replay::{run_replay_collector, ReplaySource};
use crate::state::{MarketState, ServiceStatus, SharedMarketState};

const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts all application components and runs until a shutdown signal or HTTP server failure.
///
/// Startup order is intentional: metrics and database schema are ready before collectors can
/// publish data, and the HTTP listener is bound before worker tasks are spawned. On shutdown,
/// background work is cancelled, HTTP connections receive a bounded drain window, workers are
/// joined, and the database pool is closed last.
///
/// # Errors
///
/// Returns [`AppError`] when required startup infrastructure fails or the HTTP server terminates
/// unexpectedly. Temporary Nexus and post-startup PostgreSQL failures are handled by their workers.
pub async fn run(config: Config) -> Result<(), AppError> {
    let prometheus =
        crate::telemetry::init_metrics(&config.nexus_market).map_err(AppError::Task)?;
    tracing::info!(
        service = "nexus-market-observer",
        version = env!("CARGO_PKG_VERSION"),
        data_source = config.market_data_source.as_str(),
        network = config.market_data_source.network_label(),
        market = config.nexus_market,
        "starting service"
    );

    let pool = connect_with_retry(&config.database_url).await?;
    let repository = PostgresRepository::new(pool.clone());
    repository.migrate().await?;
    tracing::info!("database migrations completed");

    let config = Arc::new(config);
    let market_state: SharedMarketState = Arc::new(tokio::sync::RwLock::new(MarketState::new(
        config.nexus_market.clone(),
    )));
    let database_connected = Arc::new(AtomicBool::new(true));
    let cancellation = CancellationToken::new();
    let listener = tokio::net::TcpListener::bind(config.socket_addr()).await?;
    tracing::info!(address = %config.socket_addr(), "HTTP server listening");

    // Both sources own the same state contract; only their ingestion and recovery mechanics differ.
    let collector = match config.market_data_source {
        MarketDataSourceMode::Replay => {
            let source = ReplaySource::bundled()?;
            tokio::spawn(run_replay_collector(
                source,
                Arc::clone(&market_state),
                config.nexus_market.clone(),
                cancellation.clone(),
            ))
        }
        MarketDataSourceMode::NexusLive => {
            let adapter = NexusAdapter::testnet(config.rest_timeout);
            tokio::spawn(run_nexus_collector(
                adapter,
                Arc::clone(&market_state),
                config.nexus_market.clone(),
                config.snapshot_interval,
                cancellation.clone(),
            ))
        }
    };
    let persistence = tokio::spawn(run_persistence_worker(
        repository.clone(),
        Arc::clone(&market_state),
        Arc::clone(&database_connected),
        config.snapshot_interval,
        config.market_data_source.as_str(),
        cancellation.clone(),
    ));
    let stale_detector = tokio::spawn(run_stale_detector(
        Arc::clone(&market_state),
        config.stale_after,
        config.market_data_source.as_str(),
        cancellation.clone(),
    ));

    let context = AppContext {
        config: Arc::clone(&config),
        market_state: Arc::clone(&market_state),
        history_reader: Arc::new(repository.clone()),
        database_connected,
        started_at: Instant::now(),
        prometheus,
    };
    let server_shutdown = CancellationToken::new();
    let server_shutdown_signal = server_shutdown.clone();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, api::router(context))
            .with_graceful_shutdown(server_shutdown_signal.cancelled_owned())
            .await
    });

    // An unexpected server exit is treated like a shutdown trigger, but retained as the final
    // application error after the remaining resources are drained.
    let server_finished = tokio::select! {
        _ = wait_for_shutdown_signal() => {
            tracing::info!("shutdown signal received; stopping workers and HTTP server");
            None
        }
        result = &mut server => Some(result),
    };

    // Cancellation begins immediately. HTTP gets a separate bounded drain
    // window while collectors and workers stop accepting more work.
    cancellation.cancel();
    mark_shutting_down(&market_state).await;
    server_shutdown.cancel();
    let server_error = match server_finished {
        Some(result) => server_result_error(result, true),
        None => await_http_server(&mut server, HTTP_SHUTDOWN_TIMEOUT).await,
    };
    await_worker("market data collector", collector).await;
    await_worker("snapshot persistence", persistence).await;
    await_worker("stale detector", stale_detector).await;
    pool.close().await;
    tracing::info!("service shutdown completed");
    match server_error {
        Some(error) => Err(AppError::Task(error)),
        None => Ok(()),
    }
}

fn server_result_error(
    result: Result<Result<(), std::io::Error>, tokio::task::JoinError>,
    unexpected: bool,
) -> Option<String> {
    match result {
        Ok(Ok(())) if unexpected => Some("HTTP server stopped unexpectedly".to_string()),
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("HTTP server failed: {error}")),
        Err(error) => Some(format!("HTTP server task failed: {error}")),
    }
}

async fn await_http_server(
    handle: &mut JoinHandle<Result<(), std::io::Error>>,
    timeout: Duration,
) -> Option<String> {
    match tokio::time::timeout(timeout, &mut *handle).await {
        Ok(result) => server_result_error(result, false),
        Err(_) => {
            tracing::error!(
                timeout_seconds = timeout.as_secs(),
                "HTTP connections did not drain before the shutdown timeout; aborting the server task"
            );
            handle.abort();
            let _ = handle.await;
            None
        }
    }
}

async fn mark_shutting_down(state: &SharedMarketState) {
    let mut state = state.write().await;
    if state
        .service_status
        .can_transition_to(ServiceStatus::ShuttingDown)
    {
        state.service_status = ServiceStatus::ShuttingDown;
    }
}

async fn await_worker(name: &'static str, handle: JoinHandle<()>) {
    let mut handle = handle;
    match tokio::time::timeout(WORKER_SHUTDOWN_TIMEOUT, &mut handle).await {
        Ok(Ok(())) => tracing::debug!(worker = name, "worker stopped cleanly"),
        Ok(Err(error)) => tracing::error!(worker = name, error = %error, "worker task failed"),
        Err(_) => {
            tracing::error!(
                worker = name,
                "worker did not stop before the shutdown timeout; aborting it"
            );
            handle.abort();
            let _ = handle.await;
        }
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match terminate {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::error!(error = %error, "failed to listen for SIGINT");
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::error!(error = %error, "failed to install SIGTERM handler; waiting for SIGINT");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %error, "failed to listen for shutdown signal");
        }
    }
}

/// Loads configuration, initializes tracing, and maps application completion to a process code.
///
/// Configuration errors are written directly to stderr because tracing is intentionally configured
/// only after `RUST_LOG` and the environment mode have been validated.
pub async fn main_entry() -> ExitCode {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration error: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = crate::telemetry::init_tracing(&config) {
        eprintln!("telemetry initialization error: {error}");
        return ExitCode::FAILURE;
    }
    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "service terminated with an error");
            ExitCode::FAILURE
        }
    }
}

/// Performs the lightweight liveness check used by the Docker image.
///
/// This probes `/metrics`, not `/health`: temporary loss of Nexus data affects readiness and API
/// status, but should not cause Docker to restart an otherwise functioning recovery loop.
pub fn container_healthcheck() -> ExitCode {
    let port = match Config::from_env() {
        Ok(config) => config.http_port,
        Err(error) => {
            eprintln!("healthcheck configuration error: {error}");
            return ExitCode::FAILURE;
        }
    };
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let mut stream = match TcpStream::connect_timeout(&address, Duration::from_secs(2)) {
        Ok(stream) => stream,
        Err(_) => return ExitCode::FAILURE,
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
    if stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return ExitCode::FAILURE;
    }
    let mut response = [0_u8; 64];
    let count = match stream.read(&mut response) {
        Ok(count) => count,
        Err(_) => return ExitCode::FAILURE,
    };
    let status = String::from_utf8_lossy(&response[..count]);
    // Container health is a liveness check. Readiness remains represented by
    // /health without blocking monitoring during a Nexus outage.
    if status.starts_with("HTTP/1.1 200") || status.starts_with("HTTP/1.0 200") {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    #[tokio::test]
    async fn http_shutdown_drain_is_bounded() {
        let mut server = tokio::spawn(pending::<Result<(), std::io::Error>>());
        let error = await_http_server(&mut server, Duration::from_millis(10)).await;
        assert!(error.is_none());
        assert!(server.is_finished());
    }
}
