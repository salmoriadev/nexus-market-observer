#![forbid(unsafe_code)]

#[tokio::main]
async fn main() -> std::process::ExitCode {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return nexus_market_observer::app::container_healthcheck();
    }
    nexus_market_observer::app::main_entry().await
}
