//! Adaptive read buffer and connection I/O primitives.
//!
//! `AdaptiveBuf` implements Go-style dynamic buffer sizing (512B → 64KB).
//! `BufConfig` controls buffer sizes and slow-consumer limits.
//! `ServerConn` is a test-only wrapper for unit testing protocol parsing.

use std::io::{self, Read};
#[cfg(test)]
use std::io::{BufWriter, Write};
#[cfg(test)]
use std::net::TcpStream;
use std::ops::{Deref, DerefMut};
use std::os::fd::RawFd;
use std::time::Duration;

use bytes::{BufMut, BytesMut};

#[cfg(test)]
use crate::infra::nats_proto::{self, MsgBuilder};
#[cfg(test)]
use crate::infra::types::HeaderMap;
#[cfg(test)]
use crate::infra::types::ServerInfo;

// Re-export parsed op types so the rest of the crate uses nats_proto's types.
pub(crate) use crate::infra::nats_proto::ClientOp;
#[cfg(any(feature = "leaf", feature = "hub"))]
pub(crate) use crate::infra::nats_proto::LeafOp;
#[cfg(feature = "cluster")]
pub(crate) use crate::infra::nats_proto::RouteOp;

// --- Adaptive read buffer (Go-style dynamic sizing) ---

const DEFAULT_START_BUF: usize = 512;
const DEFAULT_MIN_BUF: usize = 64;
const DEFAULT_MAX_BUF: usize = 65536;
const SHORTS_TO_SHRINK: u8 = 2;

/// Configuration for adaptive read buffer sizing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BufConfig {
    pub max_read_buf: usize,
    pub write_buf: usize,
    /// Maximum pending write bytes per connection before disconnecting as slow consumer.
    /// 0 means unlimited.
    pub max_pending: usize,
}

impl Default for BufConfig {
    fn default() -> Self {
        Self {
            max_read_buf: DEFAULT_MAX_BUF,
            write_buf: DEFAULT_MAX_BUF,
            max_pending: 64 * 1024 * 1024,
        }
    }
}

/// A read buffer that starts small and grows/shrinks based on utilization,
/// matching Go's nats-server strategy: start at 512B, double on full reads,
/// halve after 2 consecutive short reads, floor at 64B, ceiling at max.
pub(crate) struct AdaptiveBuf {
    buf: BytesMut,
    target_cap: usize,
    max_cap: usize,
    shorts: u8,
}

impl AdaptiveBuf {
    pub(crate) fn new(max_cap: usize) -> Self {
        let start = DEFAULT_START_BUF.min(max_cap);
        Self {
            buf: BytesMut::with_capacity(start),
            target_cap: start,
            max_cap,
            shorts: 0,
        }
    }

    /// Called after each successful socket read with the number of bytes read.
    /// Adjusts the target capacity and reallocates if appropriate.
    pub(crate) fn after_read(&mut self, n: usize) {
        if n >= self.target_cap && self.target_cap < self.max_cap {
            // Buffer was fully utilized — grow
            self.target_cap = (self.target_cap * 2).min(self.max_cap);
            // Ensure we have enough capacity for the next read
            let additional = self
                .target_cap
                .saturating_sub(self.buf.capacity() - self.buf.len());
            if additional > 0 {
                self.buf.reserve(additional);
            }
            self.shorts = 0;
        } else if n < self.target_cap / 2 {
            // Short read
            self.shorts = self.shorts.saturating_add(1);
            if self.shorts > SHORTS_TO_SHRINK && self.target_cap > DEFAULT_MIN_BUF {
                self.target_cap = (self.target_cap / 2).max(DEFAULT_MIN_BUF);
                // Only reallocate when buffer is empty (all data consumed)
                if self.buf.is_empty() {
                    self.buf = BytesMut::with_capacity(self.target_cap);
                }
            }
        } else {
            self.shorts = 0;
        }
    }

    /// Try to shrink the buffer if it is empty and oversized.
    /// Call this after parsing has consumed all data.
    pub(crate) fn try_shrink(&mut self) {
        if self.buf.is_empty() && self.buf.capacity() > self.target_cap * 2 {
            self.buf = BytesMut::with_capacity(self.target_cap);
        }
    }

    /// Read from a raw fd into the buffer's spare capacity (non-blocking).
    /// Uses libc::read directly for non-blocking socket I/O.
    pub(crate) fn read_from_fd(&mut self, fd: RawFd) -> io::Result<usize> {
        if self.buf.remaining_mut() == 0 {
            self.buf.reserve(self.target_cap.max(DEFAULT_START_BUF));
        }
        let chunk = self.buf.chunk_mut();
        let n = unsafe { libc::read(fd, chunk.as_mut_ptr() as *mut libc::c_void, chunk.len()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let n = n as usize;
        unsafe { self.buf.advance_mut(n) };
        Ok(n)
    }

    /// Read from a reader into the buffer's spare capacity.
    /// Ensures spare capacity exists before reading.
    pub(crate) fn read_from(&mut self, reader: &mut impl Read) -> io::Result<usize> {
        if self.buf.remaining_mut() == 0 {
            self.buf.reserve(self.target_cap.max(DEFAULT_START_BUF));
        }
        let chunk = self.buf.chunk_mut();
        let n = unsafe {
            let raw = std::slice::from_raw_parts_mut(chunk.as_mut_ptr(), chunk.len());
            reader.read(raw)?
        };
        unsafe { self.buf.advance_mut(n) };
        Ok(n)
    }
}

impl Deref for AdaptiveBuf {
    type Target = BytesMut;
    fn deref(&self) -> &BytesMut {
        &self.buf
    }
}

impl DerefMut for AdaptiveBuf {
    fn deref_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }
}

/// Server-side connection wrapper for unit tests.
#[cfg(test)]
pub(crate) struct ServerConn {
    reader: TcpStream,
    writer: BufWriter<TcpStream>,
    read_buf: AdaptiveBuf,
    msg_builder: MsgBuilder,
}

#[cfg(test)]
#[allow(dead_code)]
impl ServerConn {
    pub(crate) fn from_tcp(stream: TcpStream, buf_config: BufConfig) -> io::Result<Self> {
        let writer_stream = stream.try_clone()?;
        Ok(Self {
            reader: stream,
            writer: BufWriter::with_capacity(buf_config.write_buf, writer_stream),
            read_buf: AdaptiveBuf::new(buf_config.max_read_buf),
            msg_builder: MsgBuilder::new(),
        })
    }

    /// Get the raw fd of the reader socket for use with poll().
    #[cfg(unix)]
    pub(crate) fn reader_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.reader.as_raw_fd()
    }

    /// Send INFO to connected client.
    pub(crate) fn send_info(&mut self, info: &ServerInfo) -> io::Result<()> {
        let json = serde_json::to_string(info)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let line = format!("INFO {json}\r\n");
        self.write_flush(line.as_bytes())
    }

    /// Send MSG to connected client (write + flush).
    pub(crate) fn send_msg(
        &mut self,
        subject: &str,
        sid: u64,
        reply: Option<&str>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        self.write_msg(
            subject.as_bytes(),
            sid,
            reply.map(|r| r.as_bytes()),
            headers,
            payload,
        )?;
        self.flush()
    }

    /// Write a MSG to the client without flushing.
    /// Uses direct byte assembly — no `write!()` formatting.
    pub(crate) fn write_msg(
        &mut self,
        subject: &[u8],
        sid: u64,
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        let sid_bytes = nats_proto::sid_to_bytes(sid);
        let data = self
            .msg_builder
            .build_msg(subject, &sid_bytes, reply, headers, payload);
        self.writer.write_all(data)
    }

    /// Write pre-formatted raw bytes to the client (no flush).
    pub(crate) fn write_raw(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)
    }

    /// Flush buffered writes to the wire.
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    pub(crate) fn send_ping(&mut self) -> io::Result<()> {
        self.write_flush(b"PING\r\n")
    }

    pub(crate) fn send_pong(&mut self) -> io::Result<()> {
        self.write_flush(b"PONG\r\n")
    }

    pub(crate) fn send_ok(&mut self) -> io::Result<()> {
        self.write_flush(b"+OK\r\n")
    }

    pub(crate) fn send_err(&mut self, msg: &str) -> io::Result<()> {
        let line = format!("-ERR '{msg}'\r\n");
        self.write_flush(line.as_bytes())
    }

    /// Read the next client operation from the wire.
    pub(crate) fn read_client_op(&mut self) -> io::Result<Option<ClientOp>> {
        self.read_client_op_inner(false)
    }

    pub(crate) fn read_client_op_inner(&mut self, skip_pub: bool) -> io::Result<Option<ClientOp>> {
        loop {
            let parsed = if skip_pub {
                self.try_skip_or_parse_client_op()?
            } else {
                self.try_parse_client_op()?
            };
            if let Some(op) = parsed {
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

    /// Read the next non-PUB/HPUB client operation, skipping all publishes
    /// in a tight loop. Used when there are no subscribers and no upstream,
    /// avoiding poll/Notify overhead entirely.
    pub(crate) fn read_next_non_pub(&mut self) -> io::Result<Option<ClientOp>> {
        loop {
            // Skip all buffered PUBs, return on first non-PUB op
            loop {
                match nats_proto::try_skip_or_parse_client_op(&mut self.read_buf)? {
                    Some(ClientOp::Pong) => continue, // skipped PUB/HPUB
                    Some(op) => return Ok(Some(op)),
                    None => break, // need more data
                }
            }
            self.read_buf.try_shrink();
            // Read more data from socket
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

    pub(crate) fn try_parse_client_op(&mut self) -> io::Result<Option<ClientOp>> {
        let result = nats_proto::try_parse_client_op(&mut self.read_buf);
        self.read_buf.try_shrink();
        result
    }

    /// Parse the next op, but skip PUB/HPUB without creating Bytes objects.
    /// Used when there are no subscribers and no upstream to save CPU.
    pub(crate) fn try_skip_or_parse_client_op(&mut self) -> io::Result<Option<ClientOp>> {
        let result = nats_proto::try_skip_or_parse_client_op(&mut self.read_buf);
        self.read_buf.try_shrink();
        result
    }

    fn write_flush(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)?;
        self.writer.flush()?;
        Ok(())
    }
}

/// Exponential backoff with jitter for reconnection attempts.
pub(crate) struct Backoff {
    current: Duration,
    initial: Duration,
    max: Duration,
}

impl Backoff {
    /// Create a new backoff starting at `initial`, capping at `max`.
    pub(crate) fn new(initial: Duration, max: Duration) -> Self {
        Self {
            current: initial,
            initial,
            max,
        }
    }

    /// Return the next backoff duration (with ±25% jitter) and advance.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let base = self.current;
        // Double for next time, capped
        self.current = (self.current * 2).min(self.max);
        // Apply ±25% jitter
        let jitter_range = base.as_millis() as f64 * 0.25;
        let jitter = (rand::random::<f64>() - 0.5) * 2.0 * jitter_range;
        let ms = (base.as_millis() as f64 + jitter).max(1.0) as u64;
        Duration::from_millis(ms)
    }

    /// Reset backoff to the initial value (called on successful connect).
    pub(crate) fn reset(&mut self) {
        self.current = self.initial;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    /// Create a TCP loopback pair for testing.
    fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        (server, client)
    }

    fn make_pair() -> (ServerConn, TcpStream) {
        let (server, client) = tcp_pair();
        let conn = ServerConn::from_tcp(server, BufConfig::default()).unwrap();
        (conn, client)
    }

    #[test]
    fn test_send_info() {
        let (mut conn, mut client) = make_pair();
        let info = ServerInfo {
            server_id: "test".to_string(),
            max_payload: 1024 * 1024,
            proto: 1,
            headers: true,
            ..Default::default()
        };
        conn.send_info(&info).unwrap();

        let mut buf = vec![0u8; 4096];
        let n = client.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(s.starts_with("INFO "));
        assert!(s.ends_with("\r\n"));
        assert!(s.contains("\"server_id\":\"test\""));
    }

    // Client protocol parsing tests live in nats_proto.rs.

    #[test]
    fn test_send_msg() {
        let (mut conn, mut client) = make_pair();
        conn.send_msg("test.sub", 1, None, None, b"hello").unwrap();

        let mut buf = vec![0u8; 4096];
        let n = client.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "MSG test.sub 1 5\r\nhello\r\n");
    }

    #[test]
    fn test_send_msg_with_reply() {
        let (mut conn, mut client) = make_pair();
        conn.send_msg("test.sub", 1, Some("reply.to"), None, b"hi")
            .unwrap();

        let mut buf = vec![0u8; 4096];
        let n = client.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "MSG test.sub 1 reply.to 2\r\nhi\r\n");
    }

    // Leaf protocol parsing tests live in nats_proto.rs.

    #[cfg(feature = "leaf")]
    fn make_leaf_pair() -> (crate::leaf::LeafConn, TcpStream) {
        let (server, client) = tcp_pair();
        let conn = crate::leaf::LeafConn::new(server, BufConfig::default());
        (conn, client)
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_send_leaf_sub_unsub() {
        let (conn, mut hub) = make_leaf_pair();
        let (_reader, mut writer) = conn.split().unwrap();
        writer.send_leaf_sub(b"foo.>").unwrap();
        writer.send_leaf_unsub(b"foo.>").unwrap();
        writer.flush().unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "LS+ foo.>\r\nLS- foo.>\r\n");
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_send_lmsg_no_headers() {
        let (conn, mut hub) = make_leaf_pair();
        let (_reader, mut writer) = conn.split().unwrap();
        writer
            .send_leaf_msg(b"test.sub", None, None, b"hello")
            .unwrap();
        writer.flush().unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "LMSG test.sub 5\r\nhello\r\n");
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_send_lmsg_with_reply() {
        let (conn, mut hub) = make_leaf_pair();
        let (_reader, mut writer) = conn.split().unwrap();
        writer
            .send_leaf_msg(b"test.sub", Some(b"reply.to"), None, b"hi")
            .unwrap();
        writer.flush().unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "LMSG test.sub reply.to 2\r\nhi\r\n");
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_connect_no_creds() {
        let (mut conn, mut hub) = make_leaf_pair();
        conn.send_leaf_connect("test-leaf", true, None).unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(s.starts_with("CONNECT "));
        assert!(s.ends_with("\r\n"));
        // Should not contain auth fields
        assert!(!s.contains("\"user\""));
        assert!(!s.contains("\"auth_token\""));
        assert!(s.contains("\"name\":\"test-leaf\""));
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_connect_with_creds() {
        use crate::leaf::UpstreamConnectCreds;
        let (mut conn, mut hub) = make_leaf_pair();
        let creds = UpstreamConnectCreds {
            user: Some("admin".into()),
            pass: Some("secret".into()),
            token: Some("tok".into()),
            ..Default::default()
        };
        conn.send_leaf_connect("test-leaf", true, Some(&creds))
            .unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(s.contains("\"user\":\"admin\""));
        assert!(s.contains("\"pass\":\"secret\""));
        assert!(s.contains("\"auth_token\":\"tok\""));
    }

    // --- LeafConn split test ---

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_split_produces_independent_halves() {
        let (conn, mut hub) = make_leaf_pair();
        let (mut reader, mut writer) = conn.split().unwrap();

        // Writer can send without blocking the reader
        writer.send_leaf_sub(b"foo.>").unwrap();
        writer.flush().unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"LS+ foo.>\r\n");

        // Hub sends data that reader can receive independently
        hub.write_all(b"PING\r\n").unwrap();
        hub.flush().unwrap();

        let op = reader.read_leaf_op().unwrap().unwrap();
        assert!(matches!(op, crate::infra::nats_proto::LeafOp::Ping));
    }

    // --- AdaptiveBuf tests ---

    #[test]
    fn test_adaptive_buf_grows_on_full_read() {
        let mut ab = AdaptiveBuf::new(DEFAULT_MAX_BUF);
        assert_eq!(ab.target_cap, DEFAULT_START_BUF); // 512

        // Simulate a full read (n >= target_cap)
        ab.after_read(DEFAULT_START_BUF);
        assert_eq!(ab.target_cap, DEFAULT_START_BUF * 2); // 1024
    }

    #[test]
    fn test_adaptive_buf_shrinks_after_short_reads() {
        let mut ab = AdaptiveBuf::new(DEFAULT_MAX_BUF);
        // Grow first so we have room to shrink
        ab.after_read(DEFAULT_START_BUF); // 512 → 1024
        ab.after_read(1024); // 1024 → 2048
        assert_eq!(ab.target_cap, 2048);

        // Short reads (< target_cap / 2 = 1024): need SHORTS_TO_SHRINK+1 consecutive
        ab.after_read(100); // shorts = 1
        ab.after_read(100); // shorts = 2
        assert_eq!(ab.target_cap, 2048); // not yet
        ab.after_read(100); // shorts = 3, triggers shrink
        assert_eq!(ab.target_cap, 1024);
    }

    #[test]
    fn test_adaptive_buf_floor_at_min() {
        let mut ab = AdaptiveBuf::new(DEFAULT_MAX_BUF);
        // Force target_cap to minimum by repeated short reads
        ab.target_cap = DEFAULT_MIN_BUF * 2; // 128
        for _ in 0..10 {
            ab.after_read(0);
        }
        assert_eq!(ab.target_cap, DEFAULT_MIN_BUF); // 64, never below
    }

    #[test]
    fn test_adaptive_buf_ceiling_at_max() {
        let max = 1024;
        let mut ab = AdaptiveBuf::new(max);
        // Repeatedly simulate full reads
        for _ in 0..20 {
            let cap = ab.target_cap;
            ab.after_read(cap);
        }
        assert_eq!(ab.target_cap, max);
    }

    #[test]
    fn test_adaptive_buf_try_shrink_when_empty() {
        let mut ab = AdaptiveBuf::new(DEFAULT_MAX_BUF);
        // Manually inflate the buffer capacity way beyond target
        ab.buf.reserve(65536);
        assert!(ab.buf.capacity() > ab.target_cap * 2);

        // try_shrink should reallocate when buffer is empty
        ab.try_shrink();
        assert!(ab.buf.capacity() <= ab.target_cap * 2);
    }
}
