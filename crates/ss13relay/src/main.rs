//! ss13relay: accepts Steam Datagram Relay connections from the launcher and relays them to DreamDaemon.

mod announce;
mod config;
mod session;

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use ss13relay_proto::driver::{Driver, DriverEvent, Mode};
use ss13relay_proto::steam::{self, LinkConfig};
use steamworks::{
    Server, ServerMode, SteamServerConnectFailure, SteamServersConnected, SteamServersDisconnected,
};
use tokio::sync::{mpsc, watch};

use crate::announce::{Announcer, SteamInfo};
use crate::config::Config;

const LOGON_TIMEOUT: Duration = Duration::from_secs(60);
const RELAY_NETWORK_TIMEOUT: Duration = Duration::from_secs(60);
const STEAM_INFO_REFRESH: Duration = Duration::from_secs(30);

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SS13RELAY_LOG")
                .unwrap_or_else(|_| "info,steamnet=warn".into()),
        )
        .init();

    let config = Config::load()?;
    let secret = config.secret.clone();
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        steam_app_id = config.steam_app_id,
        targets = ?config.targets.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
        "starting ss13relay"
    );

    let (server, client) = start_steam(&config)?;
    let server = Arc::new(server);
    let link = LinkConfig {
        send_rate: config.send_rate,
        ice: config.ice,
    };
    link.apply_globally();

    let utils = client.networking_utils();
    utils.init_relay_network_access();
    let waited = steam::wait_for_relay_network(
        || server.run_callbacks(),
        || utils.relay_network_status(),
        RELAY_NETWORK_TIMEOUT,
    )
    .map_err(anyhow::Error::msg)?;
    tracing::info!(?waited, "Steam relay network ready");

    let ping_location = wait_for_ping_location(&server)?;
    let info = SteamInfo {
        steam_id: server.steam_id().raw(),
        ping_location,
    };
    tracing::info!(steam_id = info.steam_id, "Steam game server logged on");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(server, config, secret, link, info))
}

fn start_steam(config: &Config) -> anyhow::Result<(Server, steamworks::Client)> {
    // SAFETY: called before any other threads exist. The Steam API reads the app ID from these.
    unsafe {
        std::env::set_var("SteamAppId", config.steam_app_id.to_string());
        std::env::set_var("SteamGameId", config.steam_app_id.to_string());
    }

    let (server, client) = Server::init(
        Ipv4Addr::UNSPECIFIED,
        config.game_port,
        config.query_port,
        ServerMode::NoAuthentication,
        env!("CARGO_PKG_VERSION"),
    )
    .map_err(|e| anyhow::anyhow!(
        "SteamGameServer_Init failed: {e:?}. steamclient.so (and libsteamwebrtc.so for direct routes) must be in \
         $HOME/.steam/sdk64; the Docker image fetches them with SteamCMD on first start"
    ))?;
    steam::install_debug_output(config.steam_verbose);

    let logged_on = Arc::new(AtomicBool::new(false));
    let flag = logged_on.clone();

	// Kept registered for the life of the process.
    std::mem::forget(
        server
            .register_callback(move |_: SteamServersConnected| flag.store(true, Ordering::SeqCst)),
    );
    std::mem::forget(server.register_callback(|f: SteamServerConnectFailure| {
        tracing::warn!(reason = ?f.reason, still_retrying = f.still_retrying, "Steam logon failed");
    }));
    std::mem::forget(server.register_callback(|d: SteamServersDisconnected| {
        tracing::warn!(reason = ?d.reason, "disconnected from Steam; it will reconnect");
    }));

    server.set_product("ss13relay");
    server.set_game_description("SS13 relay");
    server.set_mod_dir("ss13relay");
    server.set_dedicated_server(true);
    match &config.gslt {
        Some(token) => server.log_on(token),
        None => server.log_on_anonymous(),
    }

    let start = Instant::now();
    while !logged_on.load(Ordering::SeqCst) {
        server.run_callbacks();
        if start.elapsed() > LOGON_TIMEOUT {
            bail!("Steam game server did not log on within {LOGON_TIMEOUT:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok((server, client))
}

fn wait_for_ping_location(server: &Server) -> anyhow::Result<String> {
    let start = Instant::now();
    loop {
        server.run_callbacks();
        if let Some(location) = steam::local_ping_location() {
            return Ok(location);
        }
        if start.elapsed() > RELAY_NETWORK_TIMEOUT {
            return Err(anyhow::anyhow!(
                "no Steam ping location after {RELAY_NETWORK_TIMEOUT:?}"
            ))
            .context("relay pings never completed");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

async fn run(
    server: Arc<Server>,
    config: Config,
    secret: String,
    link: LinkConfig,
    info: SteamInfo,
) -> anyhow::Result<()> {
    let (steam_tx, steam_rx) = watch::channel(info);
    let announcer = Announcer::new(secret, config.source_address, steam_rx);
    let targets: Vec<Arc<config::Target>> = config.targets.iter().cloned().map(Arc::new).collect();

    tokio::spawn({
        let server = server.clone();
        async move {
            loop {
                tokio::time::sleep(STEAM_INFO_REFRESH).await;
                let steam_id = server.steam_id().raw();
                let ping_location = steam::local_ping_location();
                steam_tx.send_if_modified(|current| {
                    let mut changed = false;
                    if steam_id != 0 && steam_id != current.steam_id {
                        tracing::warn!(old = current.steam_id, new = steam_id, "SteamID changed");
                        current.steam_id = steam_id;
                        changed = true;
                    }
                    if let Some(location) = ping_location
                        && location != current.ping_location
                    {
                        current.ping_location = location;
                        changed = true;
                    }
                    changed
                });
            }
        }
    });

    for target in &targets {
        tokio::spawn(announce::announce_loop(announcer.clone(), target.clone()));
    }

    let (events_tx, mut events) = mpsc::unbounded_channel();
    let virtual_ports = targets.iter().map(|t| i32::from(t.hub_port)).collect();
    let driver = Driver::spawn(
        server.networking_sockets(),
        {
            let server = server.clone();
            move || server.run_callbacks()
        },
        Mode::Listen { virtual_ports },
        link,
        events_tx,
    )?;

    loop {
        tokio::select! {
            event = events.recv() => match event {
                Some(DriverEvent::Connected(connection)) => {
                    let port = connection.virtual_port;
                    match targets.iter().find(|t| i32::from(t.hub_port) == port) {
                        Some(target) => {
                            tokio::spawn(session::run(connection, target.clone(), announcer.clone()));
                        }
                        None => tracing::error!(port, "connection on a virtual port with no target"),
                    }
                }
                Some(DriverEvent::Closed { .. } | DriverEvent::ConnectFailed { .. }) => {}
                None => bail!("Steam driver stopped"),
            },
            _ = shutdown_signal() => {
                tracing::info!("shutting down");
                break;
            }
        }
    }

    driver.shutdown();
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
