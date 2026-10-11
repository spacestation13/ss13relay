//! Launcher to steam session

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ss13relay_proto::driver::Connection;
use ss13relay_proto::pump::{self, PumpEnd, StreamInput};
use ss13relay_proto::{CloseReason, Frame, PROTOCOL_VERSION, StreamId};
use steamworks::SteamId;
use tokio::sync::mpsc;

use crate::announce::{Announcer, Outcome};
use crate::config::Target;

const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// how long a new stream keeps retrying while DreamDaemon is unreachable
const OPEN_RETRY_FOR: Duration = Duration::from_secs(30);
const OPEN_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const STREAM_INPUT_CAPACITY: usize = 64;

pub async fn run(connection: Connection, target: Arc<Target>, announcer: Arc<Announcer>) {
    let Connection {
        id,
        remote,
        mut inbound,
        outbound,
        ..
    } = connection;

    match tokio::time::timeout(HELLO_TIMEOUT, inbound.recv()).await {
        Ok(Some(bytes)) => match Frame::decode(&bytes) {
            Ok(Frame::Hello { version }) if version == PROTOCOL_VERSION => {}
            Ok(Frame::Hello { version }) => {
                tracing::warn!(
                    id,
                    ?remote,
                    version,
                    expected = PROTOCOL_VERSION,
                    "protocol version mismatch, closing"
                );
                return;
            }
            other => {
                tracing::warn!(id, ?remote, ?other, "expected HELLO, closing");
                return;
            }
        },
        Ok(None) => return,
        Err(_) => {
            tracing::warn!(id, ?remote, "no HELLO within {HELLO_TIMEOUT:?}, closing");
            return;
        }
    }
    if outbound
        .send(
            Frame::Hello {
                version: PROTOCOL_VERSION,
            }
            .encode(),
        )
        .await
        .is_err()
    {
        return;
    }
    tracing::info!(id, ?remote, %target, "launcher connected");

    let mut streams: HashMap<StreamId, mpsc::Sender<StreamInput>> = HashMap::new();
    while let Some(bytes) = inbound.recv().await {
        match Frame::decode(&bytes) {
            Ok(Frame::Open { stream }) => {
                let (input_tx, input_rx) = mpsc::channel(STREAM_INPUT_CAPACITY);
                if streams.insert(stream, input_tx).is_some() {
                    tracing::warn!(id, stream, "stream reopened while still open");
                }
                tokio::spawn(open_stream(
                    stream,
                    input_rx,
                    outbound.clone(),
                    target.clone(),
                    announcer.clone(),
                    remote,
                ));
            }
            Ok(Frame::Data { stream, data }) => {
                if let Some(input) = streams.get(&stream)
                    && input.send(StreamInput::Data(data)).await.is_err()
                {
                    streams.remove(&stream);
                }
            }
            Ok(Frame::Close { stream, reason }) => {
                if let Some(input) = streams.remove(&stream) {
                    let _ = input.send(StreamInput::Close(reason)).await;
                }
            }
            Ok(other) => tracing::warn!(id, ?other, "unexpected frame from launcher, ignoring"),
            Err(e) => {
                tracing::warn!(id, ?remote, %e, "malformed frame, closing connection");
                return;
            }
        }
    }
    tracing::info!(id, ?remote, "launcher disconnected");
}

async fn open_stream(
    stream: StreamId,
    input: mpsc::Receiver<StreamInput>,
    output: mpsc::Sender<Vec<u8>>,
    target: Arc<Target>,
    announcer: Arc<Announcer>,
    remote: Option<SteamId>,
) {
    let started = Instant::now();
    let deadline = started + OPEN_RETRY_FOR;
    let mut attempts = 0u32;

    let tcp = loop {
        attempts += 1;
        let failure = match announcer.announce(&target).await {
            Outcome::Accepted => match announcer.connect(&target).await {
                Ok(tcp) => break tcp,
                Err(e) => format!("{e:#}"),
            },
            Outcome::Rejected(why) => {
                tracing::error!(stream, ?remote, %target, "not relaying: {why}");
                let _ = output
                    .send(
                        Frame::Close {
                            stream,
                            reason: CloseReason::AnnounceRejected,
                        }
                        .encode(),
                    )
                    .await;
                return;
            }
            Outcome::Unreachable(why) => why,
        };
        if input.is_closed() {
            return;
        }
        if Instant::now() >= deadline {
            tracing::warn!(stream, ?remote, %target, attempts, "giving up on stream: {failure}");
            let _ = output
                .send(
                    Frame::Close {
                        stream,
                        reason: CloseReason::TargetUnreachable,
                    }
                    .encode(),
                )
                .await;
            return;
        }
        tokio::time::sleep(OPEN_RETRY_INTERVAL).await;
    };

    if output
        .send(Frame::Opened { stream }.encode())
        .await
        .is_err()
    {
        return;
    }
    tracing::info!(stream, ?remote, %target, attempts, waited = ?started.elapsed(), "stream opened");

    let stats = pump::pump(stream, tcp, input, output).await;
    let end = match stats.end {
        PumpEnd::TcpClosed => "game server closed it".to_owned(),
        PumpEnd::PeerClosed(reason) => format!("launcher closed it ({reason:?})"),
        PumpEnd::SteamGone => "Steam connection lost".to_owned(),
    };
    tracing::info!(
        stream,
        ?remote,
        duration = ?started.elapsed(),
        bytes_to_player = stats.to_steam,
        bytes_from_player = stats.from_steam,
        "stream ended: {end}"
    );
}
