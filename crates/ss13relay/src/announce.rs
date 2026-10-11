//! Relay to SS13Lib before connections and every 60s, for source IP, steam ID, etc

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::watch;

use crate::config::Target;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TOPIC_TIMEOUT: Duration = Duration::from_secs(20);
pub const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(60);

const LOOPBACK_SOURCE: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SteamInfo {
    pub steam_id: u64,
    pub ping_location: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Accepted,
    /// ss13lib answered but refused: relay support disabled, wrong secret, or an older ss13lib.
    Rejected(String),
    /// couldn't get an answer (DreamDaemon down or rebooting).
    Unreachable(String),
}

pub struct Announcer {
    secret: String,
    source_address: Option<IpAddr>,
    steam: watch::Receiver<SteamInfo>,
}

impl Announcer {
    pub fn new(
        secret: String,
        source_address: Option<IpAddr>,
        steam: watch::Receiver<SteamInfo>,
    ) -> Arc<Announcer> {
        Arc::new(Announcer {
            secret,
            source_address,
            steam,
        })
    }

    pub fn steam_changes(&self) -> watch::Receiver<SteamInfo> {
        self.steam.clone()
    }

    pub async fn announce(&self, target: &Target) -> Outcome {
        let info = self.steam.borrow().clone();
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("ss13lib_relay", "1")
            .append_pair("steam_id", &info.steam_id.to_string())
            .append_pair("port", &target.hub_port.to_string())
            .append_pair("ping_location", &info.ping_location)
            .append_pair("secret", &self.secret)
            .finish();

        let response = match tokio::time::timeout(TOPIC_TIMEOUT, self.topic(target, &query)).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Outcome::Unreachable(format!("{e:#}")),
            Err(_) => {
                return Outcome::Unreachable(format!("no Topic response within {TOPIC_TIMEOUT:?}"));
            }
        };

        match serde_json::from_str::<serde_json::Value>(&response) {
            Ok(json) if json["status"] == "ok" => Outcome::Accepted,
            Ok(json) if json["status"] == "rejected" => {
                Outcome::Rejected("ss13lib rejected the announce (check SS13LIB_RELAY_ENABLED and that SS13LIB_RELAY_SECRET matches)".into())
            }
            _ => Outcome::Rejected(format!(
                "unrecognised Topic response {response:?} (does the server's ss13lib support relays?)"
            )),
        }
    }

    /// Open a TCP connection to the target from the relay's source address.
    pub async fn connect(&self, target: &Target) -> anyhow::Result<TcpStream> {
        let addr = tokio::net::lookup_host(&target.address)
            .await
            .with_context(|| format!("failed to resolve {}", target.address))?
            .find(SocketAddr::is_ipv4)
            .with_context(|| {
                format!(
                    "{} has no IPv4 address (DreamDaemon is IPv4 only)",
                    target.address
                )
            })?;

        let socket = TcpSocket::new_v4()?;
        let source = self
            .source_address
            .or_else(|| addr.ip().is_loopback().then_some(LOOPBACK_SOURCE));
        if let Some(source) = source {
            socket
                .bind(SocketAddr::new(source, 0))
                .with_context(|| format!("failed to bind source address {source}"))?;
        }
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, socket.connect(addr))
            .await
            .map_err(|_| anyhow::anyhow!("connecting to {addr} timed out"))?
            .with_context(|| format!("failed to connect to {addr}"))?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    async fn topic(&self, target: &Target, query: &str) -> anyhow::Result<String> {
        let mut stream = self.connect(target).await?;
        stream.write_all(&topic_packet(query)?).await?;

        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .await
            .context("reading Topic response header")?;
        if header[..2] != [0x00, 0x83] {
            bail!("not a BYOND Topic response");
        }
        let mut body = vec![0u8; u16::from_be_bytes([header[2], header[3]]) as usize];
        stream
            .read_exact(&mut body)
            .await
            .context("reading Topic response body")?;
        parse_topic_response(&body)
    }
}

/// Re-announce to a target periodically and whenever the relay's SteamID or ping location
/// changes, logging only when the outcome changes.
pub async fn announce_loop(announcer: Arc<Announcer>, target: Arc<Target>) {
    let mut changes = announcer.steam_changes();
    let mut last: Option<Outcome> = None;
    loop {
        let outcome = announcer.announce(&target).await;
        if last.as_ref() != Some(&outcome) {
            match &outcome {
                Outcome::Accepted => tracing::info!(%target, "announced to ss13lib"),
                Outcome::Rejected(why) => tracing::error!(%target, "announce rejected: {why}"),
                Outcome::Unreachable(why) => {
                    tracing::warn!(%target, "can't reach the game server: {why}")
                }
            }
            last = Some(outcome);
        }
        tokio::select! {
            _ = tokio::time::sleep(ANNOUNCE_INTERVAL) => {}
            changed = changes.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

/// BYOND Topic request: `00 83 <len u16 BE> 00*5 '?' query 00`.
fn topic_packet(query: &str) -> anyhow::Result<Vec<u8>> {
    let mut payload = vec![0u8; 5];
    payload.push(b'?');
    payload.extend_from_slice(query.as_bytes());
    payload.push(0);
    let len = u16::try_from(payload.len()).context("Topic query too long")?;
    let mut packet = vec![0x00, 0x83];
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(&payload);
    Ok(packet)
}

/// BYOND Topic response body: `06 <NUL-terminated text>` or `2a <f32 LE>`.
fn parse_topic_response(body: &[u8]) -> anyhow::Result<String> {
    match body.split_first() {
        Some((0x06, text)) => {
            let text = text.strip_suffix(&[0]).unwrap_or(text);
            Ok(String::from_utf8_lossy(text).into_owned())
        }
        Some((0x2a, number)) if number.len() >= 4 => {
            Ok(f32::from_le_bytes([number[0], number[1], number[2], number[3]]).to_string())
        }
        _ => bail!("empty or unknown Topic response"),
    }
}
