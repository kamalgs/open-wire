//! Leaf node connection I/O wrappers for upstream hub communication.
//!
//! Contains `LeafConn` (handshake phase), `LeafReader` and `LeafWriter`
//! (split I/O halves), `HubStream` (plain TCP or TLS), and
//! `UpstreamConnectCreds` (authentication).

use std::io::{self, BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use crate::buf::AdaptiveBuf;
use crate::nats_proto::{self, LeafOp, MsgBuilder};
use crate::types::HeaderMap;
use crate::util::LockExt;

/// Resolved credentials to include in a leaf CONNECT message to the hub.
#[derive(Debug, Default)]
pub(crate) struct UpstreamConnectCreds {
    pub user: Option<String>,
    pub pass: Option<String>,
    pub token: Option<String>,
    pub jwt: Option<String>,
    pub nkey: Option<String>,
    pub sig: Option<String>,
}

/// Stream wrapper for upstream hub connections — plain TCP or TLS.
///
/// For TLS, both halves share the same `ClientConnection` behind an `Arc<Mutex>`.
/// Each half also holds a cloned `TcpStream` so reads and writes go to the same socket.
pub(crate) enum HubStream {
    Plain(TcpStream),
    Tls {
        tls: Arc<Mutex<rustls::ClientConnection>>,
        tcp: TcpStream,
    },
}

impl Read for HubStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            HubStream::Plain(s) => s.read(buf),
            HubStream::Tls { tls, tcp } => {
                let mut conn = tls.lock_or_poison();
                match conn.read_tls(tcp) {
                    Ok(0) => return Ok(0),
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
                conn.process_new_packets()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                match conn.reader().read(buf) {
                    Ok(n) => Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(e),
                    Err(e) => Err(e),
                }
            }
        }
    }
}

impl Write for HubStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            HubStream::Plain(s) => s.write(buf),
            HubStream::Tls { tls, tcp } => {
                let mut conn = tls.lock_or_poison();
                let n = conn.writer().write(buf)?;
                conn.write_tls(tcp)?;
                Ok(n)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            HubStream::Plain(s) => s.flush(),
            HubStream::Tls { tls, tcp } => {
                let mut conn = tls.lock_or_poison();
                conn.writer().flush()?;
                conn.write_tls(tcp)?;
                tcp.flush()
            }
        }
    }
}

/// Outgoing leaf node connection to a hub.
/// Owns the raw stream and its own read buffer for parsing hub operations.
/// Used during the handshake phase; call `split()` to get independent
/// reader/writer halves for the I/O loop.
pub(crate) struct LeafConn {
    stream: HubStream,
    read_buf: AdaptiveBuf,
    buf_config: crate::buf::BufConfig,
}

impl LeafConn {
    pub(crate) fn new(stream: TcpStream, buf_config: crate::buf::BufConfig) -> Self {
        Self {
            stream: HubStream::Plain(stream),
            read_buf: AdaptiveBuf::new(buf_config.max_read_buf),
            buf_config,
        }
    }

    /// Create a new leaf connection over TLS.
    pub(crate) fn new_tls(
        tcp: TcpStream,
        tls_conn: rustls::ClientConnection,
        buf_config: crate::buf::BufConfig,
    ) -> Self {
        Self {
            stream: HubStream::Tls {
                tls: Arc::new(Mutex::new(tls_conn)),
                tcp,
            },
            read_buf: AdaptiveBuf::new(buf_config.max_read_buf),
            buf_config,
        }
    }

    /// Split into independent reader and writer halves.
    /// The writer is wrapped in a BufWriter for batched I/O.
    pub(crate) fn split(self) -> io::Result<(LeafReader, LeafWriter)> {
        match self.stream {
            HubStream::Plain(tcp) => {
                let writer_tcp = tcp.try_clone()?;
                Ok((
                    LeafReader {
                        reader: HubStream::Plain(tcp),
                        read_buf: self.read_buf,
                    },
                    LeafWriter {
                        writer: BufWriter::with_capacity(
                            self.buf_config.write_buf,
                            HubStream::Plain(writer_tcp),
                        ),
                        msg_builder: MsgBuilder::new(),
                    },
                ))
            }
            HubStream::Tls { tls, tcp } => {
                let writer_tcp = tcp.try_clone()?;
                Ok((
                    LeafReader {
                        reader: HubStream::Tls {
                            tls: Arc::clone(&tls),
                            tcp,
                        },
                        read_buf: self.read_buf,
                    },
                    LeafWriter {
                        writer: BufWriter::with_capacity(
                            self.buf_config.write_buf,
                            HubStream::Tls {
                                tls,
                                tcp: writer_tcp,
                            },
                        ),
                        msg_builder: MsgBuilder::new(),
                    },
                ))
            }
        }
    }

    /// Read the next leaf operation from the hub.
    pub(crate) fn read_leaf_op(&mut self) -> io::Result<Option<LeafOp>> {
        loop {
            if let Some(op) = nats_proto::try_parse_leaf_op(&mut self.read_buf)? {
                self.read_buf.try_shrink();
                return Ok(Some(op));
            }
            let n = self.read_buf.read_from(&mut self.stream)?;
            if n == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                }
                return Err(io::ErrorKind::ConnectionReset.into());
            }
            self.read_buf.after_read(n);
        }
    }

    /// Send a leaf node CONNECT to the hub, optionally including credentials.
    pub(crate) fn send_leaf_connect(
        &mut self,
        name: &str,
        headers: bool,
        creds: Option<&UpstreamConnectCreds>,
    ) -> io::Result<()> {
        let mut map = serde_json::Map::new();
        map.insert("verbose".into(), false.into());
        map.insert("pedantic".into(), false.into());
        map.insert("headers".into(), headers.into());
        map.insert("no_responders".into(), true.into());
        map.insert("name".into(), name.into());
        map.insert("version".into(), "0.5.0".into());
        map.insert("protocol".into(), 1.into());

        if let Some(c) = creds {
            if let Some(ref u) = c.user {
                map.insert("user".into(), u.clone().into());
            }
            if let Some(ref p) = c.pass {
                map.insert("pass".into(), p.clone().into());
            }
            if let Some(ref t) = c.token {
                map.insert("auth_token".into(), t.clone().into());
            }
            if let Some(ref j) = c.jwt {
                map.insert("jwt".into(), j.clone().into());
            }
            if let Some(ref n) = c.nkey {
                map.insert("nkey".into(), n.clone().into());
            }
            if let Some(ref s) = c.sig {
                map.insert("sig".into(), s.clone().into());
            }
        }

        let json = serde_json::Value::Object(map);
        let line = format!("CONNECT {json}\r\n");
        self.stream.write_all(line.as_bytes())
    }

    pub(crate) fn send_ping(&mut self) -> io::Result<()> {
        self.stream.write_all(b"PING\r\n")
    }

    pub(crate) fn send_pong(&mut self) -> io::Result<()> {
        self.stream.write_all(b"PONG\r\n")
    }

    /// Send LS+ subscription interest to the hub.
    pub(crate) fn send_leaf_sub(&mut self, subject: &str) -> io::Result<()> {
        let line = format!("LS+ {subject}\r\n");
        self.stream.write_all(line.as_bytes())
    }

    /// Send LS+ with queue group to the hub.
    pub(crate) fn send_leaf_sub_queue(&mut self, subject: &str, queue: &str) -> io::Result<()> {
        let line = format!("LS+ {subject} {queue}\r\n");
        self.stream.write_all(line.as_bytes())
    }

    /// Flush buffered writes to the wire.
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

/// Read half of a leaf connection.
pub(crate) struct LeafReader {
    reader: HubStream,
    read_buf: AdaptiveBuf,
}

impl LeafReader {
    /// Read the next leaf operation from the hub.
    /// Performs I/O if the buffer doesn't contain a complete op.
    pub(crate) fn read_leaf_op(&mut self) -> io::Result<Option<LeafOp>> {
        loop {
            if let Some(op) = self.try_parse_leaf_op()? {
                return Ok(Some(op));
            }
            let n = self.read_buf.read_from(&mut self.reader)?;
            if n == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                }
                return Err(io::ErrorKind::ConnectionReset.into());
            }
            self.read_buf.after_read(n);
        }
    }

    /// Try to parse the next leaf op from the buffer without I/O.
    pub(crate) fn try_parse_leaf_op(&mut self) -> io::Result<Option<LeafOp>> {
        let result = nats_proto::try_parse_leaf_op(&mut self.read_buf);
        self.read_buf.try_shrink();
        result
    }
}

/// Write half of a leaf connection, wrapped in BufWriter.
pub(crate) struct LeafWriter {
    writer: BufWriter<HubStream>,
    msg_builder: MsgBuilder,
}

impl LeafWriter {
    /// Send LS+ subscription interest to the hub.
    pub(crate) fn send_leaf_sub(&mut self, subject: &[u8]) -> io::Result<()> {
        let data = self.msg_builder.build_leaf_sub(subject);
        self.writer.write_all(data)
    }

    /// Send LS- unsubscribe to the hub.
    pub(crate) fn send_leaf_unsub(&mut self, subject: &[u8]) -> io::Result<()> {
        let data = self.msg_builder.build_leaf_unsub(subject);
        self.writer.write_all(data)
    }

    /// Send LS+ with queue group to the hub.
    pub(crate) fn send_leaf_sub_queue(&mut self, subject: &[u8], queue: &[u8]) -> io::Result<()> {
        let data = self.msg_builder.build_leaf_sub_queue(subject, queue);
        self.writer.write_all(data)
    }

    /// Send LS- with queue group to the hub.
    pub(crate) fn send_leaf_unsub_queue(&mut self, subject: &[u8], queue: &[u8]) -> io::Result<()> {
        let data = self.msg_builder.build_leaf_unsub_queue(subject, queue);
        self.writer.write_all(data)
    }

    /// Send PONG to the hub.
    pub(crate) fn send_pong(&mut self) -> io::Result<()> {
        self.writer.write_all(b"PONG\r\n")
    }

    /// Send LMSG to the hub.
    /// Writes header and payload separately to avoid copying the payload
    /// into the MsgBuilder scratch buffer. The BufWriter coalesces them.
    pub(crate) fn send_leaf_msg(
        &mut self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        let hdr = self
            .msg_builder
            .build_lmsg_header(subject, reply, headers, payload.len());
        self.writer.write_all(hdr)?;
        self.writer.write_all(payload)?;
        self.writer.write_all(b"\r\n")
    }

    /// Flush buffered writes to the wire.
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
