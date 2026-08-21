use std::env;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppEnvironment {
    Development,
    Production,
    Test,
}

impl FromStr for AppEnvironment {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "development" => Ok(Self::Development),
            "production" => Ok(Self::Production),
            "test" => Ok(Self::Test),
            _ => Err(ConfigError::InvalidValue {
                name: "APP_ENV",
                reason: "must be development, production, or test".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NexusNetwork {
    Testnet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataSourceMode {
    Replay,
    NexusLive,
}

impl MarketDataSourceMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::NexusLive => "nexus_live",
        }
    }

    pub const fn network_label(self) -> &'static str {
        match self {
            Self::Replay => "demo",
            Self::NexusLive => "testnet",
        }
    }
}

impl FromStr for MarketDataSourceMode {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "replay" => Ok(Self::Replay),
            "nexus_live" => Ok(Self::NexusLive),
            _ => Err(ConfigError::InvalidValue {
                name: "MARKET_DATA_SOURCE",
                reason: "must be replay or nexus_live".to_string(),
            }),
        }
    }
}

impl NexusNetwork {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Testnet => "testnet",
        }
    }
}

impl FromStr for NexusNetwork {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case("testnet") {
            Ok(Self::Testnet)
        } else {
            Err(ConfigError::InvalidValue {
                name: "NEXUS_NETWORK",
                reason: "must be testnet; mainnet is intentionally refused by this service"
                    .to_string(),
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub app_env: AppEnvironment,
    pub http_host: IpAddr,
    pub http_port: u16,
    pub database_url: String,
    pub market_data_source: MarketDataSourceMode,
    pub nexus_network: NexusNetwork,
    pub nexus_market: String,
    pub snapshot_interval: Duration,
    pub stale_after: Duration,
    pub rest_timeout: Duration,
    pub rust_log: String,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let _ = dotenvy::dotenv();

        let snapshot_interval =
            Duration::from_secs(parse::<u64>("SNAPSHOT_INTERVAL_SECONDS", "5")?);
        let stale_after = Duration::from_secs(parse::<u64>("STALE_AFTER_SECONDS", "15")?);
        let rest_timeout = Duration::from_secs(parse::<u64>("REST_TIMEOUT_SECONDS", "10")?);

        let config = Self {
            app_env: value_or("APP_ENV", "development").parse()?,
            http_host: parse("HTTP_HOST", "0.0.0.0")?,
            http_port: parse("HTTP_PORT", "8080")?,
            database_url: required("DATABASE_URL")?,
            market_data_source: value_or("MARKET_DATA_SOURCE", "replay").parse()?,
            nexus_network: value_or("NEXUS_NETWORK", "testnet").parse()?,
            nexus_market: value_or("NEXUS_MARKET", "BTC-USDX-PERP"),
            snapshot_interval,
            stale_after,
            rest_timeout,
            rust_log: value_or("RUST_LOG", "info,nexus_market_observer=debug"),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.database_url.trim().is_empty() {
            return Err(ConfigError::Missing("DATABASE_URL"));
        }
        if self.nexus_market.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                name: "NEXUS_MARKET",
                reason: "must not be blank".to_string(),
            });
        }
        if self.market_data_source == MarketDataSourceMode::Replay
            && self.nexus_market != "BTC-USDX-PERP"
        {
            return Err(ConfigError::InvalidValue {
                name: "NEXUS_MARKET",
                reason: "replay mode only provides BTC-USDX-PERP".to_string(),
            });
        }
        if self.snapshot_interval.is_zero() {
            return Err(ConfigError::InvalidValue {
                name: "SNAPSHOT_INTERVAL_SECONDS",
                reason: "must be greater than zero".to_string(),
            });
        }
        if self.stale_after <= self.snapshot_interval {
            return Err(ConfigError::InvalidValue {
                name: "STALE_AFTER_SECONDS",
                reason: "must be greater than SNAPSHOT_INTERVAL_SECONDS".to_string(),
            });
        }
        if self.rest_timeout.is_zero() {
            return Err(ConfigError::InvalidValue {
                name: "REST_TIMEOUT_SECONDS",
                reason: "must be greater than zero".to_string(),
            });
        }
        Ok(())
    }

    pub fn socket_addr(&self) -> std::net::SocketAddr {
        std::net::SocketAddr::new(self.http_host, self.http_port)
    }

    #[cfg(test)]
    pub fn test(database_url: String) -> Self {
        Self {
            app_env: AppEnvironment::Test,
            http_host: IpAddr::from([127, 0, 0, 1]),
            http_port: 0,
            database_url,
            market_data_source: MarketDataSourceMode::Replay,
            nexus_network: NexusNetwork::Testnet,
            nexus_market: "BTC-USDX-PERP".to_string(),
            snapshot_interval: Duration::from_secs(5),
            stale_after: Duration::from_secs(15),
            rest_timeout: Duration::from_secs(10),
            rust_log: "warn".to_string(),
        }
    }
}

fn value_or(name: &'static str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn required(name: &'static str) -> Result<String, ConfigError> {
    env::var(name).map_err(|_| ConfigError::Missing(name))
}

fn parse<T>(name: &'static str, default: &str) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value_or(name, default)
        .parse::<T>()
        .map_err(|error| ConfigError::InvalidValue {
            name,
            reason: error.to_string(),
        })
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable {0}")]
    Missing(&'static str),
    #[error("invalid {name}: {reason}")]
    InvalidValue { name: &'static str, reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn testnet_is_the_only_accepted_network() {
        assert!(matches!(
            "testnet".parse::<NexusNetwork>(),
            Ok(NexusNetwork::Testnet)
        ));
        assert!("mainnet".parse::<NexusNetwork>().is_err());
    }

    #[test]
    fn stale_threshold_must_exceed_snapshot_interval() {
        let mut config = Config::test("postgres://localhost/test".to_string());
        config.stale_after = config.snapshot_interval;
        assert!(config.validate().is_err());
    }

    #[test]
    fn accepts_only_explicit_market_data_sources() {
        assert_eq!(
            "replay".parse::<MarketDataSourceMode>().expect("replay"),
            MarketDataSourceMode::Replay
        );
        assert_eq!(
            "nexus_live".parse::<MarketDataSourceMode>().expect("live"),
            MarketDataSourceMode::NexusLive
        );
        assert!("demo".parse::<MarketDataSourceMode>().is_err());
    }

    #[test]
    fn replay_rejects_an_unknown_fixture_market() {
        let mut config = Config::test("postgres://localhost/test".to_string());
        config.nexus_market = "ETH-USDX-PERP".to_string();
        assert!(config.validate().is_err());
    }
}
