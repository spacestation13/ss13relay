//! Steam side of the link, uses its own thread
//!
//! Steam networking has no async notifications, so one thread:
//! - pumps callbacks,
//! - accepts or makes connections,
//! - receives every connection's messages through a poll group,
//! - sends each connection's queued frames.
//!
//! Backpressure: a connection's outbound channel is only drained while Steam's queue of unsent
//! reliable bytes is below [`MAX_PENDING_RELIABLE`], so when the link is saturated the channel
//! fills and senders wait. Inbound messages queue here while a connection's inbound channel is full.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use steamworks::SteamId;
use steamworks::networking_sockets::{NetConnection, NetPollGroup, NetworkingSockets};
use steamworks::networking_types::{
    ListenSocketEvent, NetworkingConnectionState, NetworkingIdentity, SendFlags,
};
use tokio::sync::mpsc::{self, error::TryRecvError, error::TrySendError};

use crate::steam::LinkConfig;

const INBOUND_CAPACITY: usize = 256;
const OUTBOUND_CAPACITY: usize = 64;

/// Stop draining a connection's outbound channel above this many unsent reliable bytes
const MAX_PENDING_RELIABLE: i32 = 512 * 1024;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Link health is sampled this often and summarised in a log line every `REPORT_INTERVAL`.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const REPORT_INTERVAL: Duration = Duration::from_secs(10);
/// Warn when a message sent now would wait this long in Steam's send queue. Steam's ping only
/// covers the network path, so queueing behind bulk data (resource downloads) never shows in it.
const QUEUE_DELAY_WARN: Duration = Duration::from_millis(150);
/// Warn when ping exceeds the recent baseline by this factor and by at least `PING_SPIKE_MIN_MS`.
const PING_SPIKE_FACTOR: f32 = 2.0;
const PING_SPIKE_MIN_MS: f32 = 20.0;
const STATE_CHECK_INTERVAL: Duration = Duration::from_millis(100);

pub enum Mode {
    /// Relay: accept connections on these virtual ports.
    Listen { virtual_ports: Vec<i32> },
    /// Launcher: connect to a relay.
    Connect { remote: SteamId, virtual_port: i32 },
}

/// One Steam connection, as seen by async code. Dropping `outbound` closes the connection.
pub struct Connection {
    pub id: u64,
    pub remote: Option<SteamId>,
    pub virtual_port: i32,
    pub inbound: mpsc::Receiver<Vec<u8>>,
    pub outbound: mpsc::Sender<Vec<u8>>,
}

pub enum DriverEvent {
    Connected(Connection),
    /// connection ended (peer closed, timed out, or we dropped its outbound sender).
    Closed {
        id: u64,
        reason: String,
    },
    /// connect mode only: the connection was never established.
    ConnectFailed {
        reason: String,
    },
}

pub struct Driver {
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Driver {
    pub fn spawn(
        sockets: NetworkingSockets,
        run_callbacks: impl FnMut() + Send + 'static,
        mode: Mode,
        link: LinkConfig,
        events: mpsc::UnboundedSender<DriverEvent>,
    ) -> std::io::Result<Driver> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let thread = std::thread::Builder::new()
            .name("steam-driver".into())
            .spawn(move || {
                let mut state = State::new(sockets, mode, link, events);
                state.run(run_callbacks, &thread_shutdown);
            })?;
        Ok(Driver {
            shutdown,
            thread: Some(thread),
        })
    }

    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Conn {
    conn: NetConnection,
    remote: Option<SteamId>,
    inbound: mpsc::Sender<Vec<u8>>,
    backlog: VecDeque<Vec<u8>>,
    outbound: mpsc::Receiver<Vec<u8>>,
    independent: bool,
    bytes_sent: u64,
    bytes_received: u64,
    health: LinkHealth,
}

struct LinkHealth {
    last_sample: Instant,
    window_start: Instant,
    window_bytes_sent: u64,
    window_bytes_received: u64,
    samples: u32,
    ping_min: i32,
    ping_max: i32,
    ping_sum: i64,
    queue_max: Duration,
    quality_min: f32,
    baseline_ping: Option<f32>,
    last_warning: Option<Instant>,
}

impl LinkHealth {
    fn new() -> LinkHealth {
        let now = Instant::now();
        LinkHealth {
            last_sample: now,
            window_start: now,
            window_bytes_sent: 0,
            window_bytes_received: 0,
            samples: 0,
            ping_min: i32::MAX,
            ping_max: 0,
            ping_sum: 0,
            queue_max: Duration::ZERO,
            quality_min: 1.0,
            baseline_ping: None,
            last_warning: None,
        }
    }

    fn reset_window(&mut self, bytes_sent: u64, bytes_received: u64) {
        self.window_start = Instant::now();
        self.window_bytes_sent = bytes_sent;
        self.window_bytes_received = bytes_received;
        self.samples = 0;
        self.ping_min = i32::MAX;
        self.ping_max = 0;
        self.ping_sum = 0;
        self.queue_max = Duration::ZERO;
        self.quality_min = 1.0;
    }
}

struct State {
    sockets: NetworkingSockets,
    poll: NetPollGroup,
    link: LinkConfig,
    events: mpsc::UnboundedSender<DriverEvent>,
    listeners: Vec<(i32, steamworks::networking_sockets::ListenSocket)>,
    pending_connect: Option<(NetConnection, SteamId, i32, Instant)>,
    conns: HashMap<u64, Conn>,
    next_id: u64,
    last_state_check: Instant,
}

impl State {
    fn new(
        sockets: NetworkingSockets,
        mode: Mode,
        link: LinkConfig,
        events: mpsc::UnboundedSender<DriverEvent>,
    ) -> State {
        let poll = sockets.create_poll_group();
        let mut state = State {
            sockets,
            poll,
            link,
            events,
            listeners: Vec::new(),
            pending_connect: None,
            conns: HashMap::new(),
            next_id: 1,
            last_state_check: Instant::now(),
        };

        match mode {
            Mode::Listen { virtual_ports } => {
                for port in virtual_ports {
                    match state
                        .sockets
                        .create_listen_socket_p2p(port, link.connection_options())
                    {
                        Ok(socket) => {
                            tracing::info!(
                                virtual_port = port,
                                "listening for Steam P2P connections"
                            );
                            state.listeners.push((port, socket));
                        }
                        Err(_) => tracing::error!(
                            virtual_port = port,
                            "failed to create P2P listen socket"
                        ),
                    }
                }
            }
            Mode::Connect {
                remote,
                virtual_port,
            } => {
                let identity = NetworkingIdentity::new_steam_id(remote);
                match state
                    .sockets
                    .connect_p2p(identity, virtual_port, link.connection_options())
                {
                    Ok(conn) => {
                        state.pending_connect = Some((conn, remote, virtual_port, Instant::now()))
                    }
                    Err(_) => {
                        let _ = state.events.send(DriverEvent::ConnectFailed {
                            reason: "ConnectP2P returned an invalid handle".into(),
                        });
                    }
                }
            }
        }
        state
    }

    fn run(&mut self, mut run_callbacks: impl FnMut(), shutdown: &AtomicBool) {
        while !shutdown.load(Ordering::SeqCst) {
            run_callbacks();
            let mut busy = self.handle_listen_events();
            self.handle_connect_progress();
            busy |= self.receive();
            busy |= self.flush();
            self.monitor_links();
            if !busy {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        for (id, _) in self.conns.drain() {
            let _ = self.events.send(DriverEvent::Closed {
                id,
                reason: "driver shut down".into(),
            });
        }
    }

    fn handle_listen_events(&mut self) -> bool {
        let mut busy = false;
        let mut connected = Vec::new();
        let mut closed = Vec::new();
        for (port, socket) in &self.listeners {
            while let Some(event) = socket.try_receive_event() {
                busy = true;
                match event {
                    ListenSocketEvent::Connecting(request) => {
                        let remote = request.remote().steam_id();
                        if let Err(e) = request.accept() {
                            tracing::warn!(?remote, ?e, "failed to accept Steam connection");
                        }
                    }
                    ListenSocketEvent::Connected(event) => {
                        let remote = event.remote().steam_id();
                        connected.push((event.take_connection(), remote, *port));
                    }
                    ListenSocketEvent::Disconnected(event) => {
                        closed.push((
                            event.user_data() as u64,
                            format!("{:?}", event.end_reason()),
                        ));
                    }
                }
            }
        }
        for (conn, remote, port) in connected {
            self.register(conn, remote, port, false);
        }
        for (id, reason) in closed {
            self.close(id, reason);
        }
        busy
    }

    fn handle_connect_progress(&mut self) {
        let Some((conn, _, _, started)) = &self.pending_connect else {
            return;
        };
        let state = self
            .sockets
            .get_connection_info(conn)
            .ok()
            .and_then(|i| i.state().ok());
        match state {
            Some(NetworkingConnectionState::Connected) => {
                let (conn, remote, port, started) =
                    self.pending_connect.take().expect("checked above");
                tracing::info!(?remote, elapsed = ?started.elapsed(), "connected to relay");
                self.register(conn, Some(remote), port, true);
            }
            Some(
                NetworkingConnectionState::Connecting | NetworkingConnectionState::FindingRoute,
            ) if started.elapsed() < CONNECT_TIMEOUT => {}
            _ => {
                let reason = self
                    .sockets
                    .get_connection_info(conn)
                    .ok()
                    .and_then(|i| i.end_reason())
                    .map(|r| format!("{r:?}"))
                    .unwrap_or_else(|| format!("not connected after {CONNECT_TIMEOUT:?}"));
                self.pending_connect = None;
                let _ = self.events.send(DriverEvent::ConnectFailed { reason });
            }
        }
    }

    fn register(
        &mut self,
        conn: NetConnection,
        remote: Option<SteamId>,
        virtual_port: i32,
        independent: bool,
    ) {
        let id = self.next_id;
        self.next_id += 1;
        if conn.set_connection_user_data(id as i64).is_err() {
            tracing::warn!(?remote, "connection closed before it could be registered");
            return;
        }
        conn.set_poll_group(&self.poll);

        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_CAPACITY);
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CAPACITY);
        self.conns.insert(
            id,
            Conn {
                conn,
                remote,
                inbound: inbound_tx,
                backlog: VecDeque::new(),
                outbound: outbound_rx,
                independent,
                bytes_sent: 0,
                bytes_received: 0,
                health: LinkHealth::new(),
            },
        );
        let _ = self.events.send(DriverEvent::Connected(Connection {
            id,
            remote,
            virtual_port,
            inbound: inbound_rx,
            outbound: outbound_tx,
        }));
    }

    fn close(&mut self, id: u64, reason: String) {
        // dropping the NetConnection closes it.
        if let Some(conn) = self.conns.remove(&id) {
            tracing::info!(
                id,
                remote = ?conn.remote,
                bytes_sent = conn.bytes_sent,
                bytes_received = conn.bytes_received,
                %reason,
                "Steam connection closed"
            );
            let _ = self.events.send(DriverEvent::Closed { id, reason });
        }
    }

    fn receive(&mut self) -> bool {
        let messages = self.poll.receive_messages(256);
        let busy = !messages.is_empty();
        for message in messages {
            let id = message.connection_user_data() as u64;
            if let Some(conn) = self.conns.get_mut(&id) {
                conn.bytes_received += message.data().len() as u64;
                conn.backlog.push_back(message.data().to_vec());
            }
        }

        if self.last_state_check.elapsed() >= STATE_CHECK_INTERVAL {
            self.last_state_check = Instant::now();
            let ended: Vec<u64> = self
                .conns
                .iter()
                .filter(|(_, c)| c.independent)
                .filter(|(_, c)| {
                    let state = self
                        .sockets
                        .get_connection_info(&c.conn)
                        .ok()
                        .and_then(|i| i.state().ok());
                    state != Some(NetworkingConnectionState::Connected)
                })
                .map(|(id, _)| *id)
                .collect();
            for id in ended {
                self.close(id, "peer closed or connection lost".into());
            }
        }
        busy
    }

    fn flush(&mut self) -> bool {
        let mut busy = false;
        let mut dropped = Vec::new();
        for (id, conn) in &mut self.conns {
            // Steam to async
            while let Some(message) = conn.backlog.pop_front() {
                match conn.inbound.try_send(message) {
                    Ok(()) => busy = true,
                    Err(TrySendError::Full(message)) => {
                        conn.backlog.push_front(message);
                        break;
                    }
                    Err(TrySendError::Closed(_)) => {
                        dropped.push(*id);
                        break;
                    }
                }
            }

            // async to Steam
            loop {
                let pending = self
                    .sockets
                    .get_realtime_connection_status(&conn.conn, 0)
                    .map(|(info, _)| info.pending_reliable())
                    .unwrap_or(0);
                if pending > MAX_PENDING_RELIABLE {
                    break;
                }
                match conn.outbound.try_recv() {
                    Ok(frame) => {
                        busy = true;
                        conn.bytes_sent += frame.len() as u64;
                        if let Err(e) = conn.conn.send_message(&frame, SendFlags::RELIABLE_NO_NAGLE)
                        {
                            tracing::warn!(id, ?e, "failed to queue Steam message");
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        dropped.push(*id);
                        break;
                    }
                }
            }
        }
        for id in dropped {
            self.close(id, "closed locally".into());
        }
        busy
    }

    fn monitor_links(&mut self) {
        for (id, conn) in &mut self.conns {
            let health = &mut conn.health;
            if health.last_sample.elapsed() < SAMPLE_INTERVAL {
                continue;
            }
            health.last_sample = Instant::now();
            let Ok((info, _)) = self.sockets.get_realtime_connection_status(&conn.conn, 0) else {
                continue;
            };

            let ping = info.ping();
            // this returns m_usecQueueTime, how long a message sent now would wait before going on the wire
            let queue = Duration::from_micros(info.queued_send_bytes().max(0) as u64);
            if ping >= 0 {
                health.samples += 1;
                health.ping_min = health.ping_min.min(ping);
                health.ping_max = health.ping_max.max(ping);
                health.ping_sum += i64::from(ping);
            }
            health.queue_max = health.queue_max.max(queue);
            // negative means Steam has no measurement yet
            let quality = info.connection_quality_local();
            if quality >= 0.0 {
                health.quality_min = health.quality_min.min(quality);
            }

            let ping_spike = ping >= 0
                && health.baseline_ping.is_some_and(|baseline| {
                    let ping = ping as f32;
                    ping > baseline * PING_SPIKE_FACTOR && ping > baseline + PING_SPIKE_MIN_MS
                });
            if ping_spike || queue >= QUEUE_DELAY_WARN {
                if health
                    .last_warning
                    .is_none_or(|t| t.elapsed() >= REPORT_INTERVAL)
                {
                    health.last_warning = Some(Instant::now());
                    tracing::warn!(
                        id,
                        remote = ?conn.remote,
                        ping_ms = ping,
                        baseline_ping_ms = health.baseline_ping.map(|b| b.round() as i32),
                        queue_delay_ms = queue.as_millis() as u64,
                        pending_reliable = info.pending_reliable(),
                        out_bytes_per_sec = info.out_bytes_per_sec() as u64,
                        in_bytes_per_sec = info.in_bytes_per_sec() as u64,
                        quality_local = info.connection_quality_local(),
                        quality_remote = info.connection_quality_remote(),
                        "Steam link latency spike"
                    );
                }
            } else if ping >= 0 {
                let ping = ping as f32;
                health.baseline_ping =
                    Some(health.baseline_ping.map_or(ping, |b| b * 0.9 + ping * 0.1));
            }

            let window = health.window_start.elapsed();
            if window < REPORT_INTERVAL {
                continue;
            }
            let secs = window.as_secs_f64();
            let ping_avg =
                (health.samples > 0).then(|| health.ping_sum as f64 / f64::from(health.samples));
            tracing::info!(
                id,
                remote = ?conn.remote,
                ping_ms_min = (health.samples > 0).then_some(health.ping_min),
                ping_ms_avg = ping_avg.map(|p| p.round() as i64),
                ping_ms_max = (health.samples > 0).then_some(health.ping_max),
                queue_delay_ms_max = health.queue_max.as_millis() as u64,
                quality_local_min = health.quality_min,
                quality_remote = info.connection_quality_remote(),
                out_bytes_per_sec = ((conn.bytes_sent - health.window_bytes_sent) as f64 / secs) as u64,
                in_bytes_per_sec = ((conn.bytes_received - health.window_bytes_received) as f64 / secs) as u64,
                pending_reliable = info.pending_reliable(),
                send_rate_limit = self.link.send_rate,
                "Steam link health"
            );
            health.reset_window(conn.bytes_sent, conn.bytes_received);
        }
    }
}
