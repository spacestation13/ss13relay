//! Copies one stream between a local TCP connection and its Steam connection.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::frame::{CloseReason, Frame, MAX_DATA_LEN, StreamId};

#[derive(Debug)]
pub enum StreamInput {
    Data(Vec<u8>),
    Close(CloseReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpEnd {
    TcpClosed,
    PeerClosed(CloseReason),
    SteamGone,
}

#[derive(Debug, Clone, Copy)]
pub struct PumpStats {
    pub to_steam: u64,
    pub from_steam: u64,
    pub end: PumpEnd,
}

/// Run until either side closes. Sending to `output` waits when the Steam link is saturated,
/// which pauses TCP reads
pub async fn pump(
    stream: StreamId,
    tcp: TcpStream,
    mut input: mpsc::Receiver<StreamInput>,
    output: mpsc::Sender<Vec<u8>>,
) -> PumpStats {
    let _ = tcp.set_nodelay(true);
    let (mut reader, mut writer) = tcp.into_split();
    let mut buf = vec![0u8; MAX_DATA_LEN];
    let mut stats = PumpStats {
        to_steam: 0,
        from_steam: 0,
        end: PumpEnd::TcpClosed,
    };

    let close = |reason| Frame::Close { stream, reason }.encode();

    loop {
        tokio::select! {
            read = reader.read(&mut buf) => match read {
                Ok(0) | Err(_) => {
                    let _ = output.send(close(CloseReason::Normal)).await;
                    stats.end = PumpEnd::TcpClosed;
                    return stats;
                }
                Ok(n) => {
                    let frame = Frame::Data { stream, data: buf[..n].to_vec() }.encode();
                    if output.send(frame).await.is_err() {
                        stats.end = PumpEnd::SteamGone;
                        return stats;
                    }
                    stats.to_steam += n as u64;
                }
            },
            message = input.recv() => match message {
                Some(StreamInput::Data(data)) => {
                    if writer.write_all(&data).await.is_err() {
                        let _ = output.send(close(CloseReason::Normal)).await;
                        stats.end = PumpEnd::TcpClosed;
                        return stats;
                    }
                    stats.from_steam += data.len() as u64;
                }
                Some(StreamInput::Close(reason)) => {
                    let _ = writer.shutdown().await;
                    stats.end = PumpEnd::PeerClosed(reason);
                    return stats;
                }
                None => {
                    let _ = writer.shutdown().await;
                    stats.end = PumpEnd::SteamGone;
                    return stats;
                }
            },
        }
    }
}
