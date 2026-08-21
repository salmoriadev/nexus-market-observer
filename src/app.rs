use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::{self, AppContext};
use crate::config::Config;
use crate::error::AppError;
use crate::nexus::{run_collector, run_stale_detector, NexusAdapter};
use crate::persistence::{connect_with_retry, run_persistence_worker, PostgresRepository};
use crate::state::{MarketState, ServiceStatus, SharedMarketState};

const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(config: Config) -> Result<(), AppError> {
    let prometheus = crate::telemetry::init_metrics().map_err(AppError::Task)?;
    tracing::info!(
        service = "nexus-market-intelligence",
        version = env!("CARGO_PKG_VERSION"),
        network = config.nexus_network.as_str(),
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
    let adapter = NexusAdapter::testnet(config.rest_timeout);

    let collector = tokio::spawn(run_collector(
        adapter,
        Arc::clone(&market_state),
        config.nexus_market.clone(),
        config.snapshot_interval,
        cancellation.clone(),
    ));
    let persistence = tokio::spawn(run_persistence_worker(
        repository.clone(),
        Arc::clone(&market_state),
        Arc::clone(&database_connected),
        config.snapshot_interval,
        cancellation.clone(),
    ));
    let stale_detector = tokio::spawn(run_stale_detector(
        Arc::clone(&market_state),
        config.stale_after,
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
    let listener = tokio::net::TcpListener::bind(config.socket_addr()).await?;
    tracing::info!(address = %config.socket_addr(), "HTTP server listening");

    let shutdown_state = Arc::clone(&market_state);
    let shutdown_token = cancellation.clone();
    let server = axum::serve(listener, api::router(context)).with_graceful_shutdown(async move {
        wait_for_shutdown_signal().await;
        {
            let mut state = shutdown_state.write().await;
            if state
                .service_status
                .can_transition_to(ServiceStatus::ShuttingDown)
            {
                state.service_status = ServiceStatus::ShuttingDown;
            }
        }
        tracing::info!("shutdown signal received; stopping workers and HTTP server");
        shutdown_token.cancel();
    });

    if let Err(error) = server.await {
        cancellation.cancel();
        tracing::error!(error = %error, "HTTP server failed");
    }

    cancellation.cancel();
    mark_shutting_down(&market_state).await;
    await_worker("Nexus collector", collector).await;
    await_worker("snapshot persistence", persistence).await;
    await_worker("stale detector", stale_detector).await;
    pool.close().await;
    tracing::info!("service shutdown completed");
    Ok(())
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

pub fn container_healthcheck() -> ExitCode {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
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
    if status.starts_with("HTTP/1.1 ") || status.starts_with("HTTP/1.0 ") {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
