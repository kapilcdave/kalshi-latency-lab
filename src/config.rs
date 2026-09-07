use std::{fs, path::Path};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub environment: Environment,
    pub market_tickers: Vec<String>,
    pub output: String,
    pub tennis_events: Option<String>,
    #[serde(default)]
    pub replay_delay_ms: u64,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    Production,
    #[default]
    Demo,
}

impl Environment {
    pub const fn websocket_url(self) -> &'static str {
        match self {
            Self::Production => "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
            Self::Demo => "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
        }
    }

    pub const fn rest_url(self) -> &'static str {
        match self {
            Self::Production => "https://external-api.kalshi.com/trade-api/v2",
            Self::Demo => "https://external-api.demo.kalshi.co/trade-api/v2",
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read configuration at {}", path.display()))?;
        let config: Self = serde_json::from_slice(&bytes).context("invalid configuration JSON")?;
        if config.market_tickers.is_empty() {
            bail!("market_tickers must not be empty");
        }
        if config.channel_capacity == 0 {
            bail!("channel_capacity must be greater than zero");
        }
        Ok(config)
    }
}

const fn default_channel_capacity() -> usize {
    8192
}
