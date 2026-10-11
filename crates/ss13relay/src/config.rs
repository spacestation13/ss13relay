use std::fmt;
use std::net::IpAddr;

use anyhow::{Context, bail};
use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::Deserialize;
use ss13relay_proto::steam::DEFAULT_SEND_RATE;

/// SS13 Steam app id, can be overridden for playtest app id
pub const DEFAULT_STEAM_APP_ID: u32 = 5287320;

const DEFAULT_CONFIG_PATH: &str = "/etc/ss13relay/config.toml";

#[derive(Debug, Clone)]
pub struct Target {
    pub address: String,
    /// The Steam virtual port players connect to, which must equal the server's
    /// `SS13LIB_SERVER_PORT` (the port the hub lists). Defaults to the target's port.
    pub hub_port: u16,
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.address.ends_with(&format!(":{}", self.hub_port)) {
            write!(f, "{}", self.address)
        } else {
            write!(f, "{} (hub port {})", self.address, self.hub_port)
        }
    }
}

#[derive(Debug)]
pub struct Config {
    pub targets: Vec<Target>,
    /// Shared with the game server's `SS13LIB_RELAY_SECRET`.
    pub secret: String,
    pub source_address: Option<IpAddr>,
    pub send_rate: i32,
    pub ice: bool,
    pub steam_app_id: u32,
    pub gslt: Option<String>,
    pub game_port: u16,
    pub query_port: u16,
    pub steam_verbose: bool,
}

#[derive(Deserialize)]
struct RawConfig {
    targets: Option<RawTargets>,
    secret: Option<String>,
    source_address: Option<IpAddr>,
    send_rate: Option<i32>,
    ice: Option<bool>,
    steam_app_id: Option<u32>,
    gslt: Option<String>,
    game_port: Option<u16>,
    query_port: Option<u16>,
    steam_verbose: Option<bool>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawTargets {
    Csv(String),
    List(Vec<RawTarget>),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawTarget {
    Address(String),
    Full {
        address: String,
        hub_port: Option<u16>,
    },
}

impl Config {
    pub fn load() -> anyhow::Result<Config> {
        let path = std::env::var("SS13RELAY_CONFIG").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.into());
        let raw: RawConfig = Figment::new()
            .merge(Toml::file(&path))
            .merge(Env::prefixed("SS13RELAY_").ignore(&["config"]))
            .extract()
            .context("invalid configuration")?;
        Config::from_raw(raw)
    }

    fn from_raw(raw: RawConfig) -> anyhow::Result<Config> {
        let targets = match raw.targets {
            None => Vec::new(),
            Some(RawTargets::Csv(csv)) => csv
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|address| parse_target(address, None))
                .collect::<anyhow::Result<_>>()?,
            Some(RawTargets::List(list)) => list
                .into_iter()
                .map(|t| match t {
                    RawTarget::Address(address) => parse_target(&address, None),
                    RawTarget::Full { address, hub_port } => parse_target(&address, hub_port),
                })
                .collect::<anyhow::Result<_>>()?,
        };
        if targets.is_empty() {
            bail!(
                "no targets configured: set SS13RELAY_TARGETS (e.g. 127.0.0.1:1337) or `targets` in the config file"
            );
        }

        if let Some(source) = raw.source_address
            && source == IpAddr::from([127, 0, 0, 1])
        {
            bail!(
                "source_address must not be 127.0.0.1: DreamDaemon treats that as the server host. \
                 Leave it unset (127.0.0.2 is used for loopback targets) or pick another address"
            );
        }

        let Some(secret) = raw.secret.filter(|s| !s.is_empty()) else {
            bail!(
                "no secret configured: set SS13RELAY_SECRET (or `secret` in the config file) to the same \
                 value as the game server's SS13LIB_RELAY_SECRET"
            );
        };

        let send_rate = raw.send_rate.unwrap_or(DEFAULT_SEND_RATE);
        if send_rate <= 0 {
            bail!("send_rate must be positive");
        }

        Ok(Config {
            targets,
            secret,
            source_address: raw.source_address,
            send_rate,
            ice: raw.ice.unwrap_or(true),
            steam_app_id: raw.steam_app_id.unwrap_or(DEFAULT_STEAM_APP_ID),
            gslt: raw.gslt.filter(|s| !s.is_empty()),
            game_port: raw.game_port.unwrap_or(27015),
            query_port: raw.query_port.unwrap_or(27016),
            steam_verbose: raw.steam_verbose.unwrap_or(false),
        })
    }
}

fn parse_target(address: &str, hub_port: Option<u16>) -> anyhow::Result<Target> {
    let (_, port) = address
        .rsplit_once(':')
        .with_context(|| format!("target {address:?} must be host:port"))?;
    let port: u16 = port
        .parse()
        .with_context(|| format!("target {address:?} has an invalid port"))?;
    Ok(Target {
        address: address.to_owned(),
        hub_port: hub_port.unwrap_or(port),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(targets: Option<RawTargets>) -> RawConfig {
        RawConfig {
            targets,
            secret: Some("test-secret".into()),
            source_address: None,
            send_rate: None,
            ice: None,
            steam_app_id: None,
            gslt: None,
            game_port: None,
            query_port: None,
            steam_verbose: None,
        }
    }

    #[test]
    fn parses_csv_targets() {
        let config = Config::from_raw(raw(Some(RawTargets::Csv(
            "127.0.0.1:1337, game:4000".into(),
        ))))
        .unwrap();
        assert_eq!(config.targets.len(), 2);
        assert_eq!(config.targets[0].hub_port, 1337);
        assert_eq!(config.targets[1].address, "game:4000");
        assert_eq!(config.steam_app_id, DEFAULT_STEAM_APP_ID);
        assert_eq!(config.send_rate, DEFAULT_SEND_RATE);
        assert!(config.ice);
    }

    #[test]
    fn hub_port_override() {
        let targets = RawTargets::List(vec![RawTarget::Full {
            address: "game:4000".into(),
            hub_port: Some(1337),
        }]);
        let config = Config::from_raw(raw(Some(targets))).unwrap();
        assert_eq!(config.targets[0].hub_port, 1337);
    }

    #[test]
    fn rejects_bad_config() {
        assert!(Config::from_raw(raw(None)).is_err());
        assert!(Config::from_raw(raw(Some(RawTargets::Csv("nope".into())))).is_err());
        let mut loopback = raw(Some(RawTargets::Csv("127.0.0.1:1337".into())));
        loopback.source_address = Some(IpAddr::from([127, 0, 0, 1]));
        assert!(Config::from_raw(loopback).is_err());
    }

    #[test]
    fn requires_secret() {
        for secret in [None, Some(String::new())] {
            let mut config = raw(Some(RawTargets::Csv("127.0.0.1:1337".into())));
            config.secret = secret;
            assert!(Config::from_raw(config).is_err());
        }
    }
}
