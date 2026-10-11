//! Stand-in for the launcher's proxy for testing ss13relay without the launcher.
//!
//! Usage: ss13relay-devclient --relay <steamid> --port <virtual port> [--app <app id>] [--listen <addr>]
//! Requires Steam running and logged in to an account that owns the app.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, bail};
use ss13relay_proto::driver::{Connection, Driver, DriverEvent, Mode};
use ss13relay_proto::pump::{self, StreamInput};
use ss13relay_proto::steam::{self, LinkConfig};
use ss13relay_proto::{CloseReason, Frame, PROTOCOL_VERSION, StreamId};
use steamworks::{Client, SteamId};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

const DEFAULT_APP_ID: u32 = 5287320;

struct Args {
    app: u32,
    relay: u64,
    port: i32,
    listen: String,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = Args {
        app: DEFAULT_APP_ID,
        relay: 0,
        port: 0,
        listen: "127.0.0.1:0".into(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it.next().with_context(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--app" => args.app = value.parse()?,
            "--relay" => args.relay = value.parse()?,
            "--port" => args.port = value.parse()?,
            "--listen" => args.listen = value,
            other => bail!("unknown flag {other}"),
        }
    }
    if args.relay == 0 || args.port == 0 {
        bail!(
            "usage: ss13relay-devclient --relay <steamid> --port <virtual port> [--app <id>] [--listen <addr>]"
        );
    }
    Ok(args)
}

struct StreamRoute {
    opened: Option<oneshot::Sender<Result<(), CloseReason>>>,
    input: mpsc::Sender<StreamInput>,
}

type Routes = Arc<Mutex<HashMap<StreamId, StreamRoute>>>;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,steamnet=warn".into()),
        )
        .init();
    let args = parse_args()?;

    let client = Client::init_app(args.app).map_err(|e| {
        anyhow::anyhow!("Steam init failed (is Steam running and the app owned?): {e:?}")
    })?;
    steam::install_debug_output(false);
    let link = LinkConfig::default();
    link.apply_globally();

    let utils = client.networking_utils();
    utils.init_relay_network_access();
    steam::wait_for_relay_network(
        || client.run_callbacks(),
        || utils.relay_network_status(),
        Duration::from_secs(60),
    )
    .map_err(anyhow::Error::msg)?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(client, args, link))
}

async fn run(client: Client, args: Args, link: LinkConfig) -> anyhow::Result<()> {
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let _driver = Driver::spawn(
        client.networking_sockets(),
        {
            let client = client.clone();
            move || client.run_callbacks()
        },
        Mode::Connect {
            remote: SteamId::from_raw(args.relay),
            virtual_port: args.port,
        },
        link,
        events_tx,
    )?;

    let Connection {
        mut inbound,
        outbound,
        ..
    } = match events.recv().await {
        Some(DriverEvent::Connected(connection)) => connection,
        Some(DriverEvent::ConnectFailed { reason }) => {
            bail!("couldn't connect to the relay: {reason}")
        }
        _ => bail!("Steam driver stopped"),
    };
    outbound
        .send(
            Frame::Hello {
                version: PROTOCOL_VERSION,
            }
            .encode(),
        )
        .await?;
    match tokio::time::timeout(Duration::from_secs(10), inbound.recv()).await {
        Ok(Some(bytes))
            if Frame::decode(&bytes)
                == Ok(Frame::Hello {
                    version: PROTOCOL_VERSION,
                }) => {}
        other => bail!("relay didn't answer HELLO: {other:?}"),
    }

    let listener = TcpListener::bind(&args.listen).await?;
    let local = listener.local_addr()?;
    tracing::info!("connected to relay; point DreamSeeker at byond://{local}");
    println!("byond://{local}");

    let routes: Routes = Arc::default();
    let mux = tokio::spawn(route_inbound(inbound, routes.clone()));

    let mut next_stream: StreamId = 1;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (tcp, peer) = accepted?;
                let stream = next_stream;
                next_stream += 1;
                tracing::info!(stream, %peer, "DreamSeeker connected, opening stream");
                tokio::spawn(run_stream(stream, tcp, routes.clone(), outbound.clone()));
            }
            _ = events.recv() => {}
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
        if mux.is_finished() {
            bail!("Steam connection to the relay closed");
        }
    }
}

async fn route_inbound(mut inbound: mpsc::Receiver<Vec<u8>>, routes: Routes) {
    while let Some(bytes) = inbound.recv().await {
        let frame = match Frame::decode(&bytes) {
            Ok(frame) => frame,
            Err(e) => {
                tracing::warn!(%e, "malformed frame from relay");
                continue;
            }
        };
        match frame {
            Frame::Opened { stream } => {
                if let Some(opened) = routes
                    .lock()
                    .unwrap()
                    .get_mut(&stream)
                    .and_then(|r| r.opened.take())
                {
                    let _ = opened.send(Ok(()));
                }
            }
            Frame::Data { stream, data } => {
                let input = routes.lock().unwrap().get(&stream).map(|r| r.input.clone());
                if let Some(input) = input {
                    let _ = input.send(StreamInput::Data(data)).await;
                }
            }
            Frame::Close { stream, reason } => {
                let route = routes.lock().unwrap().remove(&stream);
                if let Some(mut route) = route {
                    match route.opened.take() {
                        Some(opened) => {
                            let _ = opened.send(Err(reason));
                        }
                        None => {
                            let _ = route.input.send(StreamInput::Close(reason)).await;
                        }
                    }
                }
            }
            other => tracing::warn!(?other, "unexpected frame from relay"),
        }
    }
}

async fn run_stream(
    stream: StreamId,
    tcp: tokio::net::TcpStream,
    routes: Routes,
    outbound: mpsc::Sender<Vec<u8>>,
) {
    let (opened_tx, opened_rx) = oneshot::channel();
    let (input_tx, input_rx) = mpsc::channel(64);
    routes.lock().unwrap().insert(
        stream,
        StreamRoute {
            opened: Some(opened_tx),
            input: input_tx,
        },
    );

    if outbound
        .send(Frame::Open { stream }.encode())
        .await
        .is_err()
    {
        return;
    }
    match opened_rx.await {
        Ok(Ok(())) => tracing::info!(stream, "stream opened"),
        Ok(Err(reason)) => {
            tracing::warn!(stream, ?reason, "relay refused the stream");
            return;
        }
        Err(_) => return,
    }

    let stats = pump::pump(stream, tcp, input_rx, outbound).await;
    routes.lock().unwrap().remove(&stream);
    tracing::info!(stream, end = ?stats.end, sent = stats.to_steam, received = stats.from_steam, "stream ended");
}
