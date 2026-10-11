//! Frames carried as Steam reliable messages: `[kind: u8][stream: u32 LE][payload]`,
//! except `Hello`, which is `[kind][version: u16 LE]`.

pub const PROTOCOL_VERSION: u16 = 1;

pub type StreamId = u32;

/// capped well Steam's 512 KiB message limit
pub const MAX_DATA_LEN: usize = 16 * 1024;

const HELLO: u8 = 0;
const OPEN: u8 = 1;
const OPENED: u8 = 2;
const DATA: u8 = 3;
const CLOSE: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// first message each way, relay drops incompatible versions
    Hello {
        version: u16,
    },
    /// launcher to relay: DreamSeeker opened a TCP connection, connect it to DreamDaemon.
    Open {
        stream: StreamId,
    },
    /// relay to launcher: DreamDaemon accepted the connection, data may flow.
    Opened {
        stream: StreamId,
    },
    Data {
        stream: StreamId,
        data: Vec<u8>,
    },
    /// either side, the stream ended. the steam connection stays open for further streams.
    Close {
        stream: StreamId,
        reason: CloseReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CloseReason {
    Normal = 0,
    /// relay couldn't reach DreamDaemon (eg still rebooting after ~30s of retries)
    TargetUnreachable = 1,
    /// server's ss13lib rejected the relay's announce (eg relay support off or wrong secret)
    AnnounceRejected = 2,
    ProtocolError = 3,
    ShuttingDown = 4,
    Unknown = 255,
}

impl From<u8> for CloseReason {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Normal,
            1 => Self::TargetUnreachable,
            2 => Self::AnnounceRejected,
            3 => Self::ProtocolError,
            4 => Self::ShuttingDown,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("empty frame")]
    Empty,
    #[error("truncated {0} frame")]
    Truncated(&'static str),
    #[error("unknown frame kind {0}")]
    UnknownKind(u8),
    #[error("DATA frame of {0} bytes exceeds the {MAX_DATA_LEN} byte limit")]
    TooLarge(usize),
}

impl Frame {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Frame::Hello { version } => {
                let mut out = vec![HELLO];
                out.extend_from_slice(&version.to_le_bytes());
                out
            }
            Frame::Open { stream } => header(OPEN, *stream, 0),
            Frame::Opened { stream } => header(OPENED, *stream, 0),
            Frame::Data { stream, data } => {
                let mut out = header(DATA, *stream, data.len());
                out.extend_from_slice(data);
                out
            }
            Frame::Close { stream, reason } => {
                let mut out = header(CLOSE, *stream, 1);
                out.push(*reason as u8);
                out
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Frame, FrameError> {
        let (&kind, rest) = bytes.split_first().ok_or(FrameError::Empty)?;
        if kind == HELLO {
            let version = rest
                .get(..2)
                .ok_or(FrameError::Truncated("HELLO"))?
                .try_into()
                .map(u16::from_le_bytes)
                .map_err(|_| FrameError::Truncated("HELLO"))?;
            return Ok(Frame::Hello { version });
        }

        let name = match kind {
            OPEN => "OPEN",
            OPENED => "OPENED",
            DATA => "DATA",
            CLOSE => "CLOSE",
            other => return Err(FrameError::UnknownKind(other)),
        };
        let stream_bytes: [u8; 4] = rest
            .get(..4)
            .ok_or(FrameError::Truncated(name))?
            .try_into()
            .map_err(|_| FrameError::Truncated(name))?;
        let stream = u32::from_le_bytes(stream_bytes);
        let payload = &rest[4..];

        Ok(match kind {
            OPEN => Frame::Open { stream },
            OPENED => Frame::Opened { stream },
            DATA => {
                if payload.len() > MAX_DATA_LEN {
                    return Err(FrameError::TooLarge(payload.len()));
                }
                Frame::Data {
                    stream,
                    data: payload.to_vec(),
                }
            }
            _ => {
                let reason = *payload.first().ok_or(FrameError::Truncated(name))?;
                Frame::Close {
                    stream,
                    reason: reason.into(),
                }
            }
        })
    }
}

fn header(kind: u8, stream: StreamId, payload_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload_len);
    out.push(kind);
    out.extend_from_slice(&stream.to_le_bytes());
    out
}
