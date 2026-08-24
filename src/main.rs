#![forbid(unsafe_code)]

//! Process entry point.
//!
//! Runtime composition lives in `nexus_market_observer::app`; keeping this binary thin lets the
//! application lifecycle be exercised independently from command-line dispatch.

#[tokio::main]
async fn main() -> std::process::ExitCode {
    // The Docker healthcheck uses the production binary itself, avoiding a second HTTP client in
    // the runtime image. It checks process liveness rather than upstream market-data readiness.
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return nexus_market_observer::app::container_healthcheck();
    }
    nexus_market_observer::app::main_entry().await
}
