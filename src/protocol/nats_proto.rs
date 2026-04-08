//! High-performance NATS protocol parser and message builder.
//!
//! # Parser
//!
//! Inspired by the Go nats-server parser, this module uses:
//! - **First-byte verb dispatch** instead of scanning for `\r\n` then trying
//!   multiple `starts_with` checks.
//! - **Raw byte scanning** for argument splitting — no UTF-8 validation, no
//!   `str::split_whitespace`, no iterator allocation.
//! - **Hand-rolled `parse_size`** on `&[u8]` — avoids `from_utf8` + `str::parse`.
//! - **Zero-copy borrows** where possible — parsed subjects and reply-to are
//!   `&[u8]` slices into the read buffer.
//!
//! # Message Builder
//!
//! Builds outgoing `MSG`/`HMSG`/`LMSG` protocol lines using direct
//! `extend_from_slice` appends instead of `write!()` formatting, eliminating
//! the `std::fmt` machinery from the hot path.

use bytes::{Buf, Bytes, BytesMut};
use std::io;

use crate::types::ServerInfo;
use crate::types::{ConnectInfo, HeaderMap};

use crate::buf::AdaptiveBuf;

macro_rules! int_to_buf {
    ($name:ident, $ty:ty) => {
        #[inline]
        fn $name(n: $ty, buf: &mut [u8; 20]) -> &[u8] {
            if n == 0 {
                buf[0] = b'0';
                return &buf[..1];
            }
            let mut i = 20;
            let mut v = n;
            while v > 0 {
                i -= 1;
                buf[i] = b'0' + (v % 10) as u8;
                v /= 10;
            }
            &buf[i..]
        }
    };
}

int_to_buf!(usize_to_buf, usize);
int_to_buf!(u64_to_buf, u64);

macro_rules! parse_int {
    ($name:ident, $ty:ty, $max_digits:expr, $label:expr) => {
        #[inline]
        fn $name(d: &[u8]) -> io::Result<$ty> {
            if d.is_empty() || d.len() > $max_digits {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, $label));
            }
            let mut n: $ty = 0;
            for &b in d {
                if !b.is_ascii_digit() {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, $label));
                }
                n = n * 10 + (b - b'0') as $ty;
            }
            Ok(n)
        }
    };
}

parse_int!(parse_size, usize, 9, "bad size");
parse_int!(parse_u64, u64, 19, "bad u64");

/// Find the next `\n` in `buf`. Returns the index of `\n`.
#[inline]
fn find_newline(buf: &[u8]) -> Option<usize> {
    memchr::memchr(b'\n', buf)
}

/// Split arguments in a byte slice by spaces/tabs.
/// Returns up to `N` argument slices (sub-slices of `line`).
/// Panics if there are more than N arguments — callers should check.
#[inline]
fn split_args<const N: usize>(line: &[u8]) -> ([&[u8]; N], usize) {
    let mut args = [&[] as &[u8]; N];
    let mut count = 0;
    let mut start = None;
    for (i, &b) in line.iter().enumerate() {
        match b {
            b' ' | b'\t' => {
                if let Some(s) = start.take() {
                    if count < N {
                        args[count] = &line[s..i];
                        count += 1;
                    } else {
                        return (args, count + 1); // overflow signal
                    }
                }
            }
            _ => {
                if start.is_none() {
                    start = Some(i);
                }
            }
        }
    }
    if let Some(s) = start {
        if count < N {
            args[count] = &line[s..];
            count += 1;
        } else {
            return (args, count + 1);
        }
    }
    (args, count)
}

/// A parsed client protocol operation. Subjects and reply-to are raw `Bytes`
/// slices to avoid allocation on the hot path.
#[derive(Debug)]
pub enum ClientOp {
    Ping,
    Pong,
    Connect(ConnectInfo),
    Publish {
        subject: Bytes,
        respond: Option<Bytes>,
        headers: Option<HeaderMap>,
        payload: Bytes,
        /// The payload/total size as original ASCII bytes from the protocol
        /// line, so outgoing MSG can reuse them without re-formatting.
        #[allow(dead_code)]
        size_bytes: Bytes,
    },
    Subscribe {
        sid: u64,
        subject: Bytes,
        queue_group: Option<Bytes>,
    },
    Unsubscribe {
        sid: u64,
        max: Option<u64>,
    },
}

/// A parsed hub→leaf operation.
#[derive(Debug)]
pub enum LeafOp {
    Info(Box<ServerInfo>),
    Ping,
    Pong,
    Ok,
    Err(String),
    LeafSub {
        subject: Bytes,
        queue: Option<Bytes>,
    },
    LeafUnsub {
        subject: Bytes,
        queue: Option<Bytes>,
    },
    LeafMsg {
        subject: Bytes,
        reply: Option<Bytes>,
        headers: Option<HeaderMap>,
        payload: Bytes,
    },
}

/// A parsed route protocol operation (RS+, RS-, RMSG, INFO, CONNECT, PING, PONG).
#[derive(Debug)]
pub enum RouteOp {
    Info(Box<ServerInfo>),
    Connect(Box<ConnectInfo>),
    Ping,
    Pong,
    /// RS+ account subject [queue [weight]]
    RouteSub {
        #[cfg(feature = "accounts")]
        account: Bytes,
        subject: Bytes,
        queue: Option<Bytes>,
    },
    /// RS- account subject
    RouteUnsub {
        #[cfg(feature = "accounts")]
        account: Bytes,
        subject: Bytes,
    },
    /// RMSG account subject [reply] [hdr_size] total_size\r\n<payload>\r\n
    RouteMsg {
        #[cfg(feature = "accounts")]
        account: Bytes,
        subject: Bytes,
        reply: Option<Bytes>,
        headers: Option<HeaderMap>,
        payload: Bytes,
    },
}

/// Gateway operations reuse the route wire format (RS+/RS-/RMSG).
pub type GatewayOp = RouteOp;

/// Scratch buffer for building outgoing protocol lines. Reuse across calls
/// to avoid allocation.
pub struct MsgBuilder {
    buf: Vec<u8>,
}

impl Default for MsgBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Try to parse the next client operation from `buf`.
///
/// Returns `Ok(Some(op))` if a complete operation was parsed (bytes consumed),
/// `Ok(None)` if more data is needed, or `Err` on protocol error.
///
/// Compatibility shim for tests and trait impls. Production code calls
/// [`try_parse_client_op_cursor`] directly for incremental scanning.
pub fn try_parse_client_op(buf: &mut BytesMut) -> io::Result<Option<ClientOp>> {
    let owned = std::mem::take(buf);
    let mut abuf = AdaptiveBuf::from_bytes_mut(owned);
    let result = try_parse_client_op_cursor(&mut abuf);
    *buf = abuf.into_inner();
    result
}

/// Try to parse the next client operation, but skip PUB/HPUB entirely
/// (just advance past the bytes without creating any Bytes objects).
/// Used when there are no subscribers and no upstream — saves ~8% CPU
/// by avoiding all Bytes refcount bumps + split_to + freeze.
///
/// Returns `Ok(Some(op))` for non-publish ops, `Ok(Some(ClientOp::Pong))`
/// as a sentinel for skipped publishes (caller checks), `Ok(None)` if
/// more data is needed, or `Err` on protocol error.
///
/// Compatibility shim. Production code calls
/// [`try_skip_or_parse_client_op_cursor`] directly.
pub fn try_skip_or_parse_client_op(buf: &mut BytesMut) -> io::Result<Option<ClientOp>> {
    let owned = std::mem::take(buf);
    let mut abuf = AdaptiveBuf::from_bytes_mut(owned);
    let result = try_skip_or_parse_client_op_cursor(&mut abuf);
    *buf = abuf.into_inner();
    result
}

/// Cursor-aware variant of [`try_parse_client_op`].
///
/// Uses `AdaptiveBuf`'s incremental newline scanner so that partial-message
/// retries never rescan bytes that have already been examined.
pub(crate) fn try_parse_client_op_cursor(buf: &mut AdaptiveBuf) -> io::Result<Option<ClientOp>> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'P' | b'p' => {
            if buf.len() < 4 {
                return Ok(None);
            }
            match buf[1] {
                b'U' | b'u' => parse_pub(buf),
                b'I' | b'i' => parse_ping_pong(buf, true),
                b'O' | b'o' => parse_ping_pong(buf, false),
                _ => proto_err(buf, "unknown op starting with P"),
            }
        }
        b'H' | b'h' => parse_hpub(buf),
        b'S' | b's' => parse_sub(buf),
        b'U' | b'u' => parse_unsub(buf),
        b'C' | b'c' => parse_connect(buf),
        _ => proto_err(buf, "unknown client operation"),
    }
}

/// Cursor-aware variant of [`try_skip_or_parse_client_op`].
pub(crate) fn try_skip_or_parse_client_op_cursor(
    buf: &mut AdaptiveBuf,
) -> io::Result<Option<ClientOp>> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'P' | b'p' => {
            if buf.len() < 4 {
                return Ok(None);
            }
            match buf[1] {
                b'U' | b'u' => skip_pub(buf),
                b'I' | b'i' => parse_ping_pong(buf, true),
                b'O' | b'o' => parse_ping_pong(buf, false),
                _ => proto_err(buf, "unknown op starting with P"),
            }
        }
        b'H' | b'h' => skip_hpub(buf),
        b'S' | b's' => parse_sub(buf),
        b'U' | b'u' => parse_unsub(buf),
        b'C' | b'c' => parse_connect(buf),
        _ => proto_err(buf, "unknown client operation"),
    }
}

/// Skip a PUB message without creating any Bytes objects.
fn skip_pub(buf: &mut AdaptiveBuf) -> io::Result<Option<ClientOp>> {
    let nl = match buf.find_newline() {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_end = trim_cr(buf, nl);
    if line_end < 4 {
        return proto_err(buf, "PUB too short");
    }
    let args_bytes = &buf[4..line_end];
    let size_arg = match memchr::memrchr2(b' ', b'\t', args_bytes) {
        Some(i) => &args_bytes[i + 1..],
        None => return proto_err(buf, "invalid PUB arguments"),
    };
    let payload_len = parse_size(size_arg)?;
    let total_needed = nl + 1 + payload_len + 2;
    if buf.len() < total_needed {
        return Ok(None);
    }
    buf.consume(total_needed);
    Ok(Some(ClientOp::Pong))
}

/// Skip an HPUB message without creating any Bytes objects.
fn skip_hpub(buf: &mut AdaptiveBuf) -> io::Result<Option<ClientOp>> {
    let nl = match buf.find_newline() {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_end = trim_cr(buf, nl);
    if line_end < 5 {
        return proto_err(buf, "HPUB too short");
    }
    let args_bytes = &buf[5..line_end];
    let total_size_arg = match memchr::memrchr2(b' ', b'\t', args_bytes) {
        Some(i) => &args_bytes[i + 1..],
        None => return proto_err(buf, "invalid HPUB arguments"),
    };
    let total_len = parse_size(total_size_arg)?;
    let total_needed = nl + 1 + total_len + 2;
    if buf.len() < total_needed {
        return Ok(None);
    }
    buf.consume(total_needed);
    Ok(Some(ClientOp::Pong))
}

#[inline]
fn parse_ping_pong(buf: &mut AdaptiveBuf, is_ping: bool) -> io::Result<Option<ClientOp>> {
    let nl = match buf.find_newline() {
        Some(i) => i,
        None => return Ok(None),
    };
    buf.consume(nl + 1);
    Ok(Some(if is_ping {
        ClientOp::Ping
    } else {
        ClientOp::Pong
    }))
}

fn parse_connect(buf: &mut AdaptiveBuf) -> io::Result<Option<ClientOp>> {
    let nl = match buf.find_newline() {
        Some(i) => i,
        None => return Ok(None),
    };
    let line = &buf[..nl];
    let space = match memchr::memchr(b' ', line) {
        Some(i) => i,
        None => return proto_err(buf, "CONNECT missing args"),
    };
    let json = &line[space + 1..trim_cr(line, nl)];
    let info: ConnectInfo =
        serde_json::from_slice(json).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    buf.consume(nl + 1);
    Ok(Some(ClientOp::Connect(info)))
}

fn parse_pub(buf: &mut AdaptiveBuf) -> io::Result<Option<ClientOp>> {
    let nl = match buf.find_newline() {
        Some(i) => i,
        None => return Ok(None),
    };

    let line_end = trim_cr(buf, nl);
    if line_end < 4 {
        return proto_err(buf, "PUB too short");
    }
    let args_bytes = &buf[4..line_end];
    let (args, argc) = split_args::<3>(args_bytes);

    let (subject, respond, size_arg) = match argc {
        2 => (args[0], None, args[1]),
        3 => (args[0], Some(args[1]), args[2]),
        _ => return proto_err(buf, "invalid PUB arguments"),
    };

    let payload_len = parse_size(size_arg)?;

    let total_needed = nl + 1 + payload_len + 2;
    if buf.len() < total_needed {
        return Ok(None);
    }

    let buf_ptr = buf.as_ptr() as usize;
    let subj_off = subject.as_ptr() as usize - buf_ptr;
    let subj_len = subject.len();
    let size_off = size_arg.as_ptr() as usize - buf_ptr;
    let size_len = size_arg.len();
    let respond_range = respond.map(|r| {
        let off = r.as_ptr() as usize - buf_ptr;
        off..off + r.len()
    });

    let header_line = buf.split_and_consume(nl + 1).freeze();
    let subject = header_line.slice(subj_off..subj_off + subj_len);
    let size_bytes = header_line.slice(size_off..size_off + size_len);
    let respond = respond_range.map(|r| header_line.slice(r));

    let payload = buf.split_and_consume(payload_len).freeze();
    buf.consume(2); // trailing \r\n

    Ok(Some(ClientOp::Publish {
        subject,
        respond,
        headers: None,
        payload,
        size_bytes,
    }))
}

fn parse_hpub(buf: &mut AdaptiveBuf) -> io::Result<Option<ClientOp>> {
    let nl = match buf.find_newline() {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_end = trim_cr(buf, nl);
    if line_end < 5 {
        return proto_err(buf, "HPUB too short");
    }
    let args_bytes = &buf[5..line_end];
    let (args, argc) = split_args::<4>(args_bytes);

    let (subject, respond, hdr_len, total_len, total_size_arg) = match argc {
        3 => {
            let h = parse_size(args[1])?;
            let t = parse_size(args[2])?;
            (args[0], None, h, t, args[2])
        }
        4 => {
            let h = parse_size(args[2])?;
            let t = parse_size(args[3])?;
            (args[0], Some(args[1]), h, t, args[3])
        }
        _ => return proto_err(buf, "invalid HPUB arguments"),
    };

    let total_needed = nl + 1 + total_len + 2;
    if buf.len() < total_needed {
        return Ok(None);
    }

    let buf_ptr = buf.as_ptr() as usize;
    let subj_off = subject.as_ptr() as usize - buf_ptr;
    let subj_len = subject.len();
    let size_off = total_size_arg.as_ptr() as usize - buf_ptr;
    let size_len = total_size_arg.len();
    let respond_range = respond.map(|r| {
        let off = r.as_ptr() as usize - buf_ptr;
        off..off + r.len()
    });

    let header_line = buf.split_and_consume(nl + 1).freeze();
    let subject = header_line.slice(subj_off..subj_off + subj_len);
    let size_bytes = header_line.slice(size_off..size_off + size_len);
    let respond = respond_range.map(|r| header_line.slice(r));

    let hdr_data = buf.split_and_consume(hdr_len);
    let payload = buf.split_and_consume(total_len - hdr_len).freeze();
    buf.consume(2);

    let headers = parse_headers(&hdr_data)?;

    Ok(Some(ClientOp::Publish {
        subject,
        respond,
        headers: Some(headers),
        payload,
        size_bytes,
    }))
}

fn parse_sub(buf: &mut AdaptiveBuf) -> io::Result<Option<ClientOp>> {
    let nl = match buf.find_newline() {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_end = trim_cr(buf, nl);
    if line_end < 4 {
        return proto_err(buf, "SUB too short");
    }
    let args_bytes = &buf[4..line_end];
    let (args, argc) = split_args::<3>(args_bytes);

    let buf_ptr = buf.as_ptr() as usize;

    let op = match argc {
        2 => {
            let sid = parse_u64(args[1])?;
            let subj_off = args[0].as_ptr() as usize - buf_ptr;
            let subj_len = args[0].len();
            let header_line = buf.split_and_consume(nl + 1).freeze();
            ClientOp::Subscribe {
                sid,
                subject: header_line.slice(subj_off..subj_off + subj_len),
                queue_group: None,
            }
        }
        3 => {
            let sid = parse_u64(args[2])?;
            let subj_off = args[0].as_ptr() as usize - buf_ptr;
            let subj_len = args[0].len();
            let queue_off = args[1].as_ptr() as usize - buf_ptr;
            let queue_len = args[1].len();
            let header_line = buf.split_and_consume(nl + 1).freeze();
            ClientOp::Subscribe {
                sid,
                subject: header_line.slice(subj_off..subj_off + subj_len),
                queue_group: Some(header_line.slice(queue_off..queue_off + queue_len)),
            }
        }
        _ => return proto_err(buf, "invalid SUB arguments"),
    };
    Ok(Some(op))
}

fn parse_unsub(buf: &mut AdaptiveBuf) -> io::Result<Option<ClientOp>> {
    let nl = match buf.find_newline() {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_end = trim_cr(buf, nl);
    if line_end < 6 {
        return proto_err(buf, "UNSUB too short");
    }
    let args_bytes = &buf[6..line_end];
    let (args, argc) = split_args::<2>(args_bytes);

    let op = match argc {
        1 => ClientOp::Unsubscribe {
            sid: parse_u64(args[0])?,
            max: None,
        },
        2 => ClientOp::Unsubscribe {
            sid: parse_u64(args[0])?,
            max: Some(parse_u64(args[1])?),
        },
        _ => return proto_err(buf, "invalid UNSUB arguments"),
    };
    buf.consume(nl + 1);
    Ok(Some(op))
}

/// Strip trailing `\r` from line if present. Returns the effective end index.
#[inline]
fn trim_cr(_buf: &[u8], nl_pos: usize) -> usize {
    if nl_pos > 0 && _buf[nl_pos - 1] == b'\r' {
        nl_pos - 1
    } else {
        nl_pos
    }
}

fn proto_err<T>(buf: &mut AdaptiveBuf, msg: &str) -> io::Result<T> {
    if let Some(nl) = buf.find_newline() {
        buf.consume(nl + 1);
    } else {
        buf.clear();
    }
    Err(io::Error::new(io::ErrorKind::InvalidInput, msg))
}

/// Try to parse the next hub→leaf operation from `buf`.
pub fn try_parse_leaf_op(buf: &mut BytesMut) -> io::Result<Option<LeafOp>> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'P' | b'p' => {
            if buf.len() < 4 {
                return Ok(None);
            }
            match buf[1] {
                b'I' | b'i' => {
                    let nl = match find_newline(buf) {
                        Some(i) => i,
                        None => return Ok(None),
                    };
                    buf.advance(nl + 1);
                    Ok(Some(LeafOp::Ping))
                }
                b'O' | b'o' => {
                    let nl = match find_newline(buf) {
                        Some(i) => i,
                        None => return Ok(None),
                    };
                    buf.advance(nl + 1);
                    Ok(Some(LeafOp::Pong))
                }
                _ => leaf_proto_err(buf, "unknown op starting with P"),
            }
        }
        b'+' => {
            let nl = match find_newline(buf) {
                Some(i) => i,
                None => return Ok(None),
            };
            buf.advance(nl + 1);
            Ok(Some(LeafOp::Ok))
        }
        b'-' => {
            let nl = match find_newline(buf) {
                Some(i) => i,
                None => return Ok(None),
            };
            let line_end = trim_cr(buf, nl);
            let msg = if line_end > 5 {
                let raw = &buf[5..line_end];
                let s = std::str::from_utf8(raw).unwrap_or("<invalid>");
                s.trim_matches('\'').to_string()
            } else {
                String::new()
            };
            buf.advance(nl + 1);
            Ok(Some(LeafOp::Err(msg)))
        }
        b'I' | b'i' => {
            let nl = match find_newline(buf) {
                Some(i) => i,
                None => return Ok(None),
            };
            let line_end = trim_cr(buf, nl);
            let space = match memchr::memchr(b' ', &buf[..line_end]) {
                Some(i) => i,
                None => return leaf_proto_err(buf, "INFO missing args"),
            };
            let json = &buf[space + 1..line_end];
            let info: ServerInfo = serde_json::from_slice(json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            buf.advance(nl + 1);
            Ok(Some(LeafOp::Info(Box::new(info))))
        }
        b'L' | b'l' => {
            if buf.len() < 3 {
                return Ok(None);
            }
            match buf[1] {
                b'S' | b's' => parse_leaf_sub_unsub(buf),
                b'M' | b'm' => parse_lmsg(buf),
                _ => leaf_proto_err(buf, "unknown leaf op"),
            }
        }
        _ => leaf_proto_err(buf, "unknown leaf operation"),
    }
}

fn parse_leaf_sub_unsub(buf: &mut BytesMut) -> io::Result<Option<LeafOp>> {
    let nl = match find_newline(buf) {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_end = trim_cr(buf, nl);
    if line_end < 4 {
        return leaf_proto_err(buf, "LS+/LS- too short");
    }
    // buf[0..2] = "LS", buf[2] = '+' or '-'
    let is_sub = buf[2] == b'+';
    // Skip "LS+ " or "LS- " (4 bytes)
    let args_bytes = &buf[4..line_end];
    let (args, argc) = split_args::<2>(args_bytes);

    let op = match argc {
        1 => {
            let subject = Bytes::copy_from_slice(args[0]);
            if is_sub {
                LeafOp::LeafSub {
                    subject,
                    queue: None,
                }
            } else {
                LeafOp::LeafUnsub {
                    subject,
                    queue: None,
                }
            }
        }
        2 => {
            let subject = Bytes::copy_from_slice(args[0]);
            let queue = Bytes::copy_from_slice(args[1]);
            if is_sub {
                LeafOp::LeafSub {
                    subject,
                    queue: Some(queue),
                }
            } else {
                LeafOp::LeafUnsub {
                    subject,
                    queue: Some(queue),
                }
            }
        }
        _ => return leaf_proto_err(buf, "invalid LS+/LS- arguments"),
    };
    buf.advance(nl + 1);
    Ok(Some(op))
}

fn parse_lmsg(buf: &mut BytesMut) -> io::Result<Option<LeafOp>> {
    // LMSG subject [reply] [hdr_size] total_size\r\n[payload]\r\n
    let nl = match find_newline(buf) {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_end = trim_cr(buf, nl);
    if line_end < 5 {
        return leaf_proto_err(buf, "LMSG too short");
    }
    let args_bytes = &buf[5..line_end];
    let (args, argc) = split_args::<4>(args_bytes);

    // Compute offsets for zero-copy slicing (same approach as parse_pub).
    let buf_ptr = buf.as_ptr() as usize;
    let subj_off = args[0].as_ptr() as usize - buf_ptr;
    let subj_len = args[0].len();

    match argc {
        // LMSG subject size
        2 => {
            let size = parse_size(args[1])?;
            let total_needed = nl + 1 + size + 2;
            if buf.len() < total_needed {
                return Ok(None);
            }
            // Freeze header line — zero-copy sub-slicing via Arc refcount bump
            let header_line = buf.split_to(nl + 1).freeze();
            let subject = header_line.slice(subj_off..subj_off + subj_len);
            let payload = buf.split_to(size).freeze();
            buf.advance(2);
            Ok(Some(LeafOp::LeafMsg {
                subject,
                reply: None,
                headers: None,
                payload,
            }))
        }
        // LMSG subject reply size  OR  LMSG subject hdr_size total_size
        3 => {
            let a1 = parse_size(args[1]);
            let a2 = parse_size(args[2]);
            match (a1, a2) {
                (Result::Ok(hdr_size), Result::Ok(total_size)) => {
                    let total_needed = nl + 1 + total_size + 2;
                    if buf.len() < total_needed {
                        return Ok(None);
                    }
                    let header_line = buf.split_to(nl + 1).freeze();
                    let subject = header_line.slice(subj_off..subj_off + subj_len);
                    let hdr_data = buf.split_to(hdr_size);
                    let payload = buf.split_to(total_size - hdr_size).freeze();
                    buf.advance(2);
                    let headers = parse_headers(&hdr_data)?;
                    Ok(Some(LeafOp::LeafMsg {
                        subject,
                        reply: None,
                        headers: Some(headers),
                        payload,
                    }))
                }
                _ => {
                    // args[1] is reply (not a number), args[2] is size
                    let size = parse_size(args[2])?;
                    let total_needed = nl + 1 + size + 2;
                    if buf.len() < total_needed {
                        return Ok(None);
                    }
                    let reply_off = args[1].as_ptr() as usize - buf_ptr;
                    let reply_len = args[1].len();
                    let header_line = buf.split_to(nl + 1).freeze();
                    let subject = header_line.slice(subj_off..subj_off + subj_len);
                    let reply = header_line.slice(reply_off..reply_off + reply_len);
                    let payload = buf.split_to(size).freeze();
                    buf.advance(2);
                    Ok(Some(LeafOp::LeafMsg {
                        subject,
                        reply: Some(reply),
                        headers: None,
                        payload,
                    }))
                }
            }
        }
        // LMSG subject reply hdr_size total_size
        4 => {
            let hdr_size = parse_size(args[2])?;
            let total_size = parse_size(args[3])?;
            let total_needed = nl + 1 + total_size + 2;
            if buf.len() < total_needed {
                return Ok(None);
            }
            let reply_off = args[1].as_ptr() as usize - buf_ptr;
            let reply_len = args[1].len();
            let header_line = buf.split_to(nl + 1).freeze();
            let subject = header_line.slice(subj_off..subj_off + subj_len);
            let reply = header_line.slice(reply_off..reply_off + reply_len);
            let hdr_data = buf.split_to(hdr_size);
            let payload = buf.split_to(total_size - hdr_size).freeze();
            buf.advance(2);
            let headers = parse_headers(&hdr_data)?;
            Ok(Some(LeafOp::LeafMsg {
                subject,
                reply: Some(reply),
                headers: Some(headers),
                payload,
            }))
        }
        _ => leaf_proto_err(buf, "invalid LMSG arguments"),
    }
}

fn leaf_proto_err<T>(buf: &mut BytesMut, msg: &str) -> io::Result<T> {
    if let Some(nl) = find_newline(buf) {
        buf.advance(nl + 1);
    } else {
        buf.clear();
    }
    Err(io::Error::new(io::ErrorKind::InvalidInput, msg))
}

/// Try to parse the next gateway protocol operation from `buf`.
/// Gateways use the same wire format as routes.
pub fn try_parse_gateway_op(buf: &mut BytesMut) -> io::Result<Option<GatewayOp>> {
    try_parse_route_op(buf)
}

/// Try to parse the next route protocol operation from `buf`.
pub fn try_parse_route_op(buf: &mut BytesMut) -> io::Result<Option<RouteOp>> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'P' | b'p' => {
            if buf.len() < 4 {
                return Ok(None);
            }
            match buf[1] {
                b'I' | b'i' => {
                    let nl = match find_newline(buf) {
                        Some(i) => i,
                        None => return Ok(None),
                    };
                    buf.advance(nl + 1);
                    Ok(Some(RouteOp::Ping))
                }
                b'O' | b'o' => {
                    let nl = match find_newline(buf) {
                        Some(i) => i,
                        None => return Ok(None),
                    };
                    buf.advance(nl + 1);
                    Ok(Some(RouteOp::Pong))
                }
                _ => route_proto_err(buf, "unknown op starting with P"),
            }
        }
        b'I' | b'i' => {
            let nl = match find_newline(buf) {
                Some(i) => i,
                None => return Ok(None),
            };
            let line_end = trim_cr(buf, nl);
            let space = match memchr::memchr(b' ', &buf[..line_end]) {
                Some(i) => i,
                None => return route_proto_err(buf, "INFO missing args"),
            };
            let json = &buf[space + 1..line_end];
            let info: ServerInfo = serde_json::from_slice(json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            buf.advance(nl + 1);
            Ok(Some(RouteOp::Info(Box::new(info))))
        }
        b'C' | b'c' => {
            let nl = match find_newline(buf) {
                Some(i) => i,
                None => return Ok(None),
            };
            let line_end = trim_cr(buf, nl);
            let space = match memchr::memchr(b' ', &buf[..line_end]) {
                Some(i) => i,
                None => return route_proto_err(buf, "CONNECT missing args"),
            };
            let json = &buf[space + 1..line_end];
            let info: ConnectInfo = serde_json::from_slice(json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            buf.advance(nl + 1);
            Ok(Some(RouteOp::Connect(Box::new(info))))
        }
        b'+' => {
            let nl = match find_newline(buf) {
                Some(i) => i,
                None => return Ok(None),
            };
            buf.advance(nl + 1);
            Ok(None) // +OK — ignore
        }
        b'-' => {
            let nl = match find_newline(buf) {
                Some(i) => i,
                None => return Ok(None),
            };
            buf.advance(nl + 1);
            Ok(None) // -ERR — ignore
        }
        b'R' | b'r' => {
            if buf.len() < 3 {
                return Ok(None);
            }
            match buf[1] {
                b'S' | b's' => parse_route_sub_unsub(buf),
                b'M' | b'm' => parse_rmsg(buf),
                _ => route_proto_err(buf, "unknown route op"),
            }
        }
        _ => route_proto_err(buf, "unknown route operation"),
    }
}

/// Parse `RS+ account subject [queue [weight]]` or `RS- account subject`.
fn parse_route_sub_unsub(buf: &mut BytesMut) -> io::Result<Option<RouteOp>> {
    let nl = match find_newline(buf) {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_end = trim_cr(buf, nl);
    if line_end < 4 {
        return route_proto_err(buf, "RS+/RS- too short");
    }
    // buf[0..2] = "RS", buf[2] = '+' or '-'
    let is_sub = buf[2] == b'+';
    // Skip "RS+ " or "RS- " (4 bytes)
    let args_bytes = &buf[4..line_end];
    let (args, argc) = split_args::<4>(args_bytes);

    let op = if is_sub {
        match argc {
            // RS+ account subject
            2 => RouteOp::RouteSub {
                #[cfg(feature = "accounts")]
                account: Bytes::copy_from_slice(args[0]),
                subject: Bytes::copy_from_slice(args[1]),
                queue: None,
            },
            // RS+ account subject queue weight
            3 | 4 => RouteOp::RouteSub {
                #[cfg(feature = "accounts")]
                account: Bytes::copy_from_slice(args[0]),
                subject: Bytes::copy_from_slice(args[1]),
                queue: Some(Bytes::copy_from_slice(args[2])),
            },
            _ => return route_proto_err(buf, "invalid RS+ arguments"),
        }
    } else {
        match argc {
            // RS- account subject
            2 => RouteOp::RouteUnsub {
                #[cfg(feature = "accounts")]
                account: Bytes::copy_from_slice(args[0]),
                subject: Bytes::copy_from_slice(args[1]),
            },
            _ => return route_proto_err(buf, "invalid RS- arguments"),
        }
    };
    buf.advance(nl + 1);
    Ok(Some(op))
}

/// Parse `RMSG account subject [reply] [hdr_size] total_size\r\n<payload>\r\n`.
fn parse_rmsg(buf: &mut BytesMut) -> io::Result<Option<RouteOp>> {
    let nl = match find_newline(buf) {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_end = trim_cr(buf, nl);
    if line_end < 5 {
        return route_proto_err(buf, "RMSG too short");
    }
    // Skip "RMSG " (5 bytes)
    let args_bytes = &buf[5..line_end];
    let (args, argc) = split_args::<5>(args_bytes);

    // Compute offsets for zero-copy slicing (same approach as parse_lmsg).
    let buf_ptr = buf.as_ptr() as usize;
    // args[0] = account, args[1] = subject
    #[cfg(feature = "accounts")]
    let acct_off = args[0].as_ptr() as usize - buf_ptr;
    #[cfg(feature = "accounts")]
    let acct_len = args[0].len();
    let subj_off = args[1].as_ptr() as usize - buf_ptr;
    let subj_len = args[1].len();

    match argc {
        // RMSG account subject size
        3 => {
            let size = parse_size(args[2])?;
            let total = nl + 1 + size + 2; // header line + payload + \r\n
            if buf.len() < total {
                return Ok(None); // need more data
            }
            let payload_start = nl + 1;
            let frozen = buf.split_to(total).freeze();
            #[cfg(feature = "accounts")]
            let account = frozen.slice(acct_off..acct_off + acct_len);
            let subject = frozen.slice(subj_off..subj_off + subj_len);
            let payload = frozen.slice(payload_start..payload_start + size);
            Ok(Some(RouteOp::RouteMsg {
                #[cfg(feature = "accounts")]
                account,
                subject,
                reply: None,
                headers: None,
                payload,
            }))
        }
        // RMSG account subject reply size
        4 => {
            let reply_off = args[2].as_ptr() as usize - buf_ptr;
            let reply_len = args[2].len();
            let size = parse_size(args[3])?;
            let total = nl + 1 + size + 2;
            if buf.len() < total {
                return Ok(None);
            }
            let payload_start = nl + 1;
            let frozen = buf.split_to(total).freeze();
            #[cfg(feature = "accounts")]
            let account = frozen.slice(acct_off..acct_off + acct_len);
            let subject = frozen.slice(subj_off..subj_off + subj_len);
            let reply = frozen.slice(reply_off..reply_off + reply_len);
            let payload = frozen.slice(payload_start..payload_start + size);
            Ok(Some(RouteOp::RouteMsg {
                #[cfg(feature = "accounts")]
                account,
                subject,
                reply: Some(reply),
                headers: None,
                payload,
            }))
        }
        // RMSG account subject reply hdr_size total_size
        5 => {
            let reply_off = args[2].as_ptr() as usize - buf_ptr;
            let reply_len = args[2].len();
            let hdr_size = parse_size(args[3])?;
            let total_size = parse_size(args[4])?;
            let total = nl + 1 + total_size + 2;
            if buf.len() < total {
                return Ok(None);
            }
            let payload_start = nl + 1;
            let frozen = buf.split_to(total).freeze();
            #[cfg(feature = "accounts")]
            let account = frozen.slice(acct_off..acct_off + acct_len);
            let subject = frozen.slice(subj_off..subj_off + subj_len);
            let reply = frozen.slice(reply_off..reply_off + reply_len);
            let hdr_data = &frozen[payload_start..payload_start + hdr_size];
            let headers = parse_headers(hdr_data)?;
            let payload = frozen.slice(payload_start + hdr_size..payload_start + total_size);
            Ok(Some(RouteOp::RouteMsg {
                #[cfg(feature = "accounts")]
                account,
                subject,
                reply: Some(reply),
                headers: Some(headers),
                payload,
            }))
        }
        _ => route_proto_err(buf, "invalid RMSG arguments"),
    }
}

fn route_proto_err<T>(buf: &mut BytesMut, msg: &str) -> io::Result<T> {
    if let Some(nl) = find_newline(buf) {
        buf.advance(nl + 1);
    } else {
        buf.clear();
    }
    Err(io::Error::new(io::ErrorKind::InvalidInput, msg))
}

pub(crate) fn parse_headers(data: &[u8]) -> io::Result<HeaderMap> {
    let text = std::str::from_utf8(data)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "header isn't valid utf-8"))?;

    let mut lines = text.lines().peekable();

    // Parse version line — extract optional status code and description.
    let version_line = lines.next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "no header version line found")
    })?;

    let mut headers = HeaderMap::new();

    // Extract status from version line: "NATS/1.0", "NATS/1.0 503", "NATS/1.0 408 Request Timeout"
    let after = version_line
        .strip_prefix("NATS/1.0")
        .unwrap_or("")
        .trim_start();
    if !after.is_empty() {
        let (code_str, desc) = match after.split_once(' ') {
            Some((c, d)) => (c, Some(d)),
            None => (after, None),
        };
        if let Ok(code) = code_str.parse::<u16>() {
            headers.set_status(code, desc.map(String::from));
        }
    }

    while let Some(line) = lines.next() {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid header line"))?;
        let mut value = value.trim_start().to_owned();
        while let Some(v) = lines.next_if(|s| s.starts_with(char::is_whitespace)) {
            value.push(' ');
            value.push_str(v.trim_start());
        }
        value.truncate(value.trim_end().len());
        headers.append(name, value);
    }

    Ok(headers)
}

impl MsgBuilder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(1024),
        }
    }

    /// Build `MSG subject sid [reply] size\r\npayload\r\n` into the internal
    /// buffer and return the complete bytes ready for writing.
    pub fn build_msg(
        &mut self,
        subject: &[u8],
        sid_bytes: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload.len();
                let mut tmp = [0u8; 20];

                self.buf.extend_from_slice(b"HMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                self.buf.extend_from_slice(sid_bytes);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf.extend_from_slice(usize_to_buf(hdr_len, &mut tmp));
                self.buf.push(b' ');
                self.buf
                    .extend_from_slice(usize_to_buf(total_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(&hdr_bytes);
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
            _ => {
                self.buf.extend_from_slice(b"MSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                self.buf.extend_from_slice(sid_bytes);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                let mut tmp = [0u8; 20];
                self.buf
                    .extend_from_slice(usize_to_buf(payload.len(), &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
        }
        &self.buf
    }

    /// Build `LMSG subject [reply] [hdr_len] total_len\r\npayload\r\n`.
    pub fn build_lmsg(
        &mut self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        let mut tmp = [0u8; 20];
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload.len();

                self.buf.extend_from_slice(b"LMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf.extend_from_slice(usize_to_buf(hdr_len, &mut tmp));
                self.buf.push(b' ');
                self.buf
                    .extend_from_slice(usize_to_buf(total_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(&hdr_bytes);
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
            _ => {
                self.buf.extend_from_slice(b"LMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf
                    .extend_from_slice(usize_to_buf(payload.len(), &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
        }
        &self.buf
    }

    /// Build `LMSG` header only (no payload copy).
    /// Returns the protocol header line ending with `\r\n`, plus any serialized
    /// headers. Caller writes payload + `\r\n` separately.
    pub fn build_lmsg_header(
        &mut self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload_len: usize,
    ) -> &[u8] {
        self.buf.clear();
        let mut tmp = [0u8; 20];
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload_len;

                self.buf.extend_from_slice(b"LMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf.extend_from_slice(usize_to_buf(hdr_len, &mut tmp));
                self.buf.push(b' ');
                self.buf
                    .extend_from_slice(usize_to_buf(total_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(&hdr_bytes);
            }
            _ => {
                self.buf.extend_from_slice(b"LMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf
                    .extend_from_slice(usize_to_buf(payload_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
            }
        }
        &self.buf
    }

    /// Build `LS+ subject\r\n`.
    pub fn build_leaf_sub(&mut self, subject: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"LS+ ");
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `LS- subject\r\n`.
    pub fn build_leaf_unsub(&mut self, subject: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"LS- ");
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `LS+ subject queue\r\n` for queue group subscriptions.
    pub fn build_leaf_sub_queue(&mut self, subject: &[u8], queue: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"LS+ ");
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b" ");
        self.buf.extend_from_slice(queue);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `LS- subject queue\r\n` for queue group unsubscriptions.
    pub fn build_leaf_unsub_queue(&mut self, subject: &[u8], queue: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"LS- ");
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b" ");
        self.buf.extend_from_slice(queue);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `RMSG $G subject [reply] [hdr_len] total_len\r\npayload\r\n`.
    pub fn build_rmsg(
        &mut self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        let mut tmp = [0u8; 20];
        #[cfg(feature = "accounts")]
        let acct = account;
        #[cfg(not(feature = "accounts"))]
        let acct = b"$G".as_slice();
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload.len();

                self.buf.extend_from_slice(b"RMSG ");
                self.buf.extend_from_slice(acct);
                self.buf.push(b' ');
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf.extend_from_slice(usize_to_buf(hdr_len, &mut tmp));
                self.buf.push(b' ');
                self.buf
                    .extend_from_slice(usize_to_buf(total_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(&hdr_bytes);
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
            _ => {
                self.buf.extend_from_slice(b"RMSG ");
                self.buf.extend_from_slice(acct);
                self.buf.push(b' ');
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf
                    .extend_from_slice(usize_to_buf(payload.len(), &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
        }
        &self.buf
    }

    /// Build `RS+ account subject\r\n`.
    pub fn build_route_sub(
        &mut self,
        subject: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"RS+ ");
        #[cfg(feature = "accounts")]
        self.buf.extend_from_slice(account);
        #[cfg(not(feature = "accounts"))]
        self.buf.extend_from_slice(b"$G");
        self.buf.push(b' ');
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `RS- account subject\r\n`.
    pub fn build_route_unsub(
        &mut self,
        subject: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"RS- ");
        #[cfg(feature = "accounts")]
        self.buf.extend_from_slice(account);
        #[cfg(not(feature = "accounts"))]
        self.buf.extend_from_slice(b"$G");
        self.buf.push(b' ');
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `RS+ account subject queue weight\r\n` for queue group route subscriptions.
    pub fn build_route_sub_queue(
        &mut self,
        subject: &[u8],
        queue: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"RS+ ");
        #[cfg(feature = "accounts")]
        self.buf.extend_from_slice(account);
        #[cfg(not(feature = "accounts"))]
        self.buf.extend_from_slice(b"$G");
        self.buf.push(b' ');
        self.buf.extend_from_slice(subject);
        self.buf.push(b' ');
        self.buf.extend_from_slice(queue);
        self.buf.extend_from_slice(b" 1\r\n");
        &self.buf
    }

    /// Build `RS- account subject\r\n` for queue group route unsubscriptions.
    pub fn build_route_unsub_queue(
        &mut self,
        subject: &[u8],
        queue: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        // RS- doesn't use queue — it unsubscribes the subject entirely
        // (Go nats-server just uses RS- $G subject)
        let _ = queue;
        self.buf.clear();
        self.buf.extend_from_slice(b"RS- ");
        #[cfg(feature = "accounts")]
        self.buf.extend_from_slice(account);
        #[cfg(not(feature = "accounts"))]
        self.buf.extend_from_slice(b"$G");
        self.buf.push(b' ');
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }
}

/// Pre-format a `u64` sid as decimal ASCII bytes for reuse in MSG lines.
pub fn sid_to_bytes(sid: u64) -> Bytes {
    let mut tmp = [0u8; 20];
    let s = u64_to_buf(sid, &mut tmp);
    Bytes::copy_from_slice(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- parse_size / parse_u64 -------------------------------------------------

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size(b"0").unwrap(), 0);
        assert_eq!(parse_size(b"128").unwrap(), 128);
        assert_eq!(parse_size(b"999999999").unwrap(), 999999999);
        assert!(parse_size(b"").is_err());
        assert!(parse_size(b"abc").is_err());
        assert!(parse_size(b"1234567890").is_err()); // 10 digits
    }

    #[test]
    fn test_parse_u64() {
        assert_eq!(parse_u64(b"0").unwrap(), 0);
        assert_eq!(parse_u64(b"42").unwrap(), 42);
        assert!(parse_u64(b"").is_err());
        assert!(parse_u64(b"x").is_err());
    }

    // -- split_args -------------------------------------------------------------

    #[test]
    fn test_split_args() {
        let (args, n) = split_args::<4>(b"foo bar baz");
        assert_eq!(n, 3);
        assert_eq!(args[0], b"foo");
        assert_eq!(args[1], b"bar");
        assert_eq!(args[2], b"baz");
    }

    #[test]
    fn test_split_args_extra_spaces() {
        let (args, n) = split_args::<4>(b"  foo   bar  ");
        assert_eq!(n, 2);
        assert_eq!(args[0], b"foo");
        assert_eq!(args[1], b"bar");
    }

    #[test]
    fn test_split_args_overflow() {
        let (_, n) = split_args::<2>(b"a b c");
        assert_eq!(n, 3); // signals overflow
    }

    // -- itoa -------------------------------------------------------------------

    #[test]
    fn test_usize_to_buf() {
        let mut tmp = [0u8; 20];
        assert_eq!(usize_to_buf(0, &mut tmp), b"0");
        assert_eq!(usize_to_buf(128, &mut tmp), b"128");
        assert_eq!(usize_to_buf(1000000, &mut tmp), b"1000000");
    }

    #[test]
    fn test_u64_to_buf() {
        let mut tmp = [0u8; 20];
        assert_eq!(u64_to_buf(0, &mut tmp), b"0");
        assert_eq!(u64_to_buf(42, &mut tmp), b"42");
    }

    #[test]
    fn test_sid_to_bytes() {
        assert_eq!(&sid_to_bytes(0)[..], b"0");
        assert_eq!(&sid_to_bytes(123)[..], b"123");
    }

    // -- Client op parsing ------------------------------------------------------

    #[test]
    fn test_parse_ping() {
        let mut buf = BytesMut::from("PING\r\n");
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op, ClientOp::Ping));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_pong() {
        let mut buf = BytesMut::from("PONG\r\n");
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op, ClientOp::Pong));
    }

    #[test]
    fn test_parse_connect() {
        let mut buf = BytesMut::from(
            "CONNECT {\"verbose\":false,\"pedantic\":false,\"lang\":\"rust\",\"version\":\"0.1\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":true,\"tls_required\":false}\r\n"
        );
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        match op {
            ClientOp::Connect(info) => {
                assert_eq!(info.lang, "rust");
                assert!(info.headers);
            }
            _ => panic!("expected Connect"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_pub_no_reply() {
        let mut buf = BytesMut::from("PUB test.subject 5\r\nhello\r\n");
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        match op {
            ClientOp::Publish {
                subject,
                respond,
                payload,
                headers,
                size_bytes,
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert!(respond.is_none());
                assert_eq!(&payload[..], b"hello");
                assert!(headers.is_none());
                assert_eq!(&size_bytes[..], b"5");
            }
            _ => panic!("expected Publish"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_pub_with_reply() {
        let mut buf = BytesMut::from("PUB test.subject reply.to 5\r\nhello\r\n");
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        match op {
            ClientOp::Publish {
                subject,
                respond,
                payload,
                ..
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&respond.unwrap()[..], b"reply.to");
                assert_eq!(&payload[..], b"hello");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_parse_pub_incomplete_line() {
        let mut buf = BytesMut::from("PUB test.subject 5\r\n");
        // We have the line but not the payload
        let result = try_parse_client_op(&mut buf).unwrap();
        assert!(result.is_none());
        // Buffer should not be consumed
        assert_eq!(&buf[..], b"PUB test.subject 5\r\n");
    }

    #[test]
    fn test_parse_pub_incomplete_payload() {
        let mut buf = BytesMut::from("PUB test.subject 5\r\nhel");
        let result = try_parse_client_op(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_sub_no_queue() {
        let mut buf = BytesMut::from("SUB test.subject 1\r\n");
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        match op {
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                assert_eq!(sid, 1);
                assert_eq!(&subject[..], b"test.subject");
                assert!(queue_group.is_none());
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn test_parse_sub_with_queue() {
        let mut buf = BytesMut::from("SUB test.queue myqueue 2\r\n");
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        match op {
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                assert_eq!(sid, 2);
                assert_eq!(&subject[..], b"test.queue");
                assert_eq!(&queue_group.unwrap()[..], b"myqueue");
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn test_parse_unsub() {
        let mut buf = BytesMut::from("UNSUB 1\r\n");
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        match op {
            ClientOp::Unsubscribe { sid, max } => {
                assert_eq!(sid, 1);
                assert!(max.is_none());
            }
            _ => panic!("expected Unsubscribe"),
        }
    }

    #[test]
    fn test_parse_unsub_with_max() {
        let mut buf = BytesMut::from("UNSUB 2 5\r\n");
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        match op {
            ClientOp::Unsubscribe { sid, max } => {
                assert_eq!(sid, 2);
                assert_eq!(max, Some(5));
            }
            _ => panic!("expected Unsubscribe"),
        }
    }

    #[test]
    fn test_parse_hpub() {
        let hdr = b"NATS/1.0\r\nX-Key: val\r\n\r\n";
        let payload = b"data";
        let hdr_len = hdr.len();
        let total_len = hdr_len + payload.len();
        let line = format!("HPUB test.subject {hdr_len} {total_len}\r\n");
        let mut raw = Vec::new();
        raw.extend_from_slice(line.as_bytes());
        raw.extend_from_slice(hdr);
        raw.extend_from_slice(payload);
        raw.extend_from_slice(b"\r\n");
        let mut buf = BytesMut::from(&raw[..]);

        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        match op {
            ClientOp::Publish {
                subject,
                headers,
                payload,
                ..
            } => {
                assert_eq!(&subject[..], b"test.subject");
                let hdrs = headers.unwrap();
                assert_eq!(
                    hdrs.get("X-Key").map(|v| v.to_string()),
                    Some("val".to_string())
                );
                assert_eq!(&payload[..], b"data");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_parse_multiple_ops() {
        let mut buf = BytesMut::from("PING\r\nPONG\r\nSUB foo 1\r\n");
        let op1 = try_parse_client_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op1, ClientOp::Ping));
        let op2 = try_parse_client_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op2, ClientOp::Pong));
        let op3 = try_parse_client_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op3, ClientOp::Subscribe { sid: 1, .. }));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_empty_buf() {
        let mut buf = BytesMut::new();
        assert!(try_parse_client_op(&mut buf).unwrap().is_none());
    }

    // -- Leaf op parsing --------------------------------------------------------

    #[test]

    fn test_leaf_ping_pong_ok_err() {
        let mut buf = BytesMut::from("PING\r\nPONG\r\n+OK\r\n-ERR 'test error'\r\n");
        assert!(matches!(
            try_parse_leaf_op(&mut buf).unwrap().unwrap(),
            LeafOp::Ping
        ));
        assert!(matches!(
            try_parse_leaf_op(&mut buf).unwrap().unwrap(),
            LeafOp::Pong
        ));
        assert!(matches!(
            try_parse_leaf_op(&mut buf).unwrap().unwrap(),
            LeafOp::Ok
        ));
        match try_parse_leaf_op(&mut buf).unwrap().unwrap() {
            LeafOp::Err(msg) => assert_eq!(msg, "test error"),
            _ => panic!("expected Err"),
        }
    }

    #[test]

    fn test_leaf_info() {
        let mut buf = BytesMut::from("INFO {\"server_id\":\"hub1\",\"max_payload\":1048576}\r\n");
        match try_parse_leaf_op(&mut buf).unwrap().unwrap() {
            LeafOp::Info(info) => {
                assert_eq!(info.server_id, "hub1");
                assert_eq!(info.max_payload, 1048576);
            }
            _ => panic!("expected Info"),
        }
    }

    #[test]

    fn test_leaf_sub_unsub() {
        let mut buf = BytesMut::from("LS+ foo.bar\r\nLS+ baz.* myqueue\r\nLS- foo.bar\r\n");
        match try_parse_leaf_op(&mut buf).unwrap().unwrap() {
            LeafOp::LeafSub { subject, queue } => {
                assert_eq!(&subject[..], b"foo.bar");
                assert!(queue.is_none());
            }
            _ => panic!("expected LeafSub"),
        }
        match try_parse_leaf_op(&mut buf).unwrap().unwrap() {
            LeafOp::LeafSub { subject, queue } => {
                assert_eq!(&subject[..], b"baz.*");
                assert_eq!(&queue.unwrap()[..], b"myqueue");
            }
            _ => panic!("expected LeafSub"),
        }
        match try_parse_leaf_op(&mut buf).unwrap().unwrap() {
            LeafOp::LeafUnsub { subject, queue } => {
                assert_eq!(&subject[..], b"foo.bar");
                assert!(queue.is_none());
            }
            _ => panic!("expected LeafUnsub"),
        }
    }

    #[test]

    fn test_leaf_lmsg_no_reply() {
        let mut buf = BytesMut::from("LMSG test.subject 5\r\nhello\r\n");
        match try_parse_leaf_op(&mut buf).unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert!(reply.is_none());
                assert!(headers.is_none());
                assert_eq!(&payload[..], b"hello");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[test]

    fn test_leaf_lmsg_with_reply() {
        let mut buf = BytesMut::from("LMSG test.subject reply.to 5\r\nhello\r\n");
        match try_parse_leaf_op(&mut buf).unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&reply.unwrap()[..], b"reply.to");
                assert!(headers.is_none());
                assert_eq!(&payload[..], b"hello");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[test]

    fn test_leaf_lmsg_with_headers() {
        let hdr = b"NATS/1.0\r\nX-Key: val\r\n\r\n";
        let payload = b"data";
        let hdr_len = hdr.len();
        let total_len = hdr_len + payload.len();
        let line = format!("LMSG test.subject {hdr_len} {total_len}\r\n");
        let mut raw = Vec::new();
        raw.extend_from_slice(line.as_bytes());
        raw.extend_from_slice(hdr);
        raw.extend_from_slice(payload);
        raw.extend_from_slice(b"\r\n");
        let mut buf = BytesMut::from(&raw[..]);

        match try_parse_leaf_op(&mut buf).unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert!(reply.is_none());
                let hdrs = headers.unwrap();
                assert_eq!(
                    hdrs.get("X-Key").map(|v| v.to_string()),
                    Some("val".to_string())
                );
                assert_eq!(&payload[..], b"data");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[test]

    fn test_leaf_lmsg_with_reply_and_headers() {
        let hdr = b"NATS/1.0\r\nFoo: bar\r\n\r\n";
        let payload = b"body";
        let hdr_len = hdr.len();
        let total_len = hdr_len + payload.len();
        let line = format!("LMSG test.subject reply.inbox {hdr_len} {total_len}\r\n");
        let mut raw = Vec::new();
        raw.extend_from_slice(line.as_bytes());
        raw.extend_from_slice(hdr);
        raw.extend_from_slice(payload);
        raw.extend_from_slice(b"\r\n");
        let mut buf = BytesMut::from(&raw[..]);

        match try_parse_leaf_op(&mut buf).unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&reply.unwrap()[..], b"reply.inbox");
                let hdrs = headers.unwrap();
                assert_eq!(
                    hdrs.get("Foo").map(|v| v.to_string()),
                    Some("bar".to_string())
                );
                assert_eq!(&payload[..], b"body");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    // -- MsgBuilder tests -------------------------------------------------------

    #[test]
    fn test_build_msg_no_reply() {
        let mut b = MsgBuilder::new();
        let result = b.build_msg(b"test.sub", b"1", None, None, b"hello");
        assert_eq!(result, b"MSG test.sub 1 5\r\nhello\r\n");
    }

    #[test]
    fn test_build_msg_with_reply() {
        let mut b = MsgBuilder::new();
        let result = b.build_msg(b"test.sub", b"1", Some(b"reply.to"), None, b"hi");
        assert_eq!(result, b"MSG test.sub 1 reply.to 2\r\nhi\r\n");
    }

    #[test]
    fn test_build_msg_empty_payload() {
        let mut b = MsgBuilder::new();
        let result = b.build_msg(b"foo", b"42", None, None, b"");
        assert_eq!(result, b"MSG foo 42 0\r\n\r\n");
    }

    #[test]

    fn test_build_lmsg_no_reply() {
        let mut b = MsgBuilder::new();
        let result = b.build_lmsg(b"test.sub", None, None, b"hello");
        assert_eq!(result, b"LMSG test.sub 5\r\nhello\r\n");
    }

    #[test]

    fn test_build_lmsg_with_reply() {
        let mut b = MsgBuilder::new();
        let result = b.build_lmsg(b"test.sub", Some(b"reply.to"), None, b"hi");
        assert_eq!(result, b"LMSG test.sub reply.to 2\r\nhi\r\n");
    }

    #[test]

    fn test_build_leaf_sub() {
        let mut b = MsgBuilder::new();
        assert_eq!(b.build_leaf_sub(b"foo.>"), b"LS+ foo.>\r\n");
    }

    #[test]

    fn test_build_leaf_unsub() {
        let mut b = MsgBuilder::new();
        assert_eq!(b.build_leaf_unsub(b"foo.>"), b"LS- foo.>\r\n");
    }

    #[test]

    fn test_build_leaf_sub_queue() {
        let mut b = MsgBuilder::new();
        assert_eq!(
            b.build_leaf_sub_queue(b"foo.bar", b"myqueue"),
            b"LS+ foo.bar myqueue\r\n"
        );
    }

    #[test]

    fn test_build_leaf_unsub_queue() {
        let mut b = MsgBuilder::new();
        assert_eq!(
            b.build_leaf_unsub_queue(b"foo.bar", b"myqueue"),
            b"LS- foo.bar myqueue\r\n"
        );
    }

    #[test]
    fn test_roundtrip_pub_msg() {
        // Parse a PUB, then build the corresponding MSG
        let mut buf = BytesMut::from("PUB test.subject 5\r\nhello\r\n");
        let op = try_parse_client_op(&mut buf).unwrap().unwrap();
        match op {
            ClientOp::Publish {
                subject, payload, ..
            } => {
                let sid_bytes = sid_to_bytes(1);
                let mut builder = MsgBuilder::new();
                let msg = builder.build_msg(&subject, &sid_bytes, None, None, &payload);
                assert_eq!(msg, b"MSG test.subject 1 5\r\nhello\r\n");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_skip_pub() {
        let mut buf = BytesMut::from("PUB foo 5\r\nhello\r\nPING\r\n");
        // skip_pub should consume the PUB and return Pong as sentinel
        let op = try_skip_or_parse_client_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op, ClientOp::Pong));
        // Next op should be PING
        let op = try_skip_or_parse_client_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op, ClientOp::Ping));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_skip_pub_with_reply() {
        let mut buf = BytesMut::from("PUB foo reply.to 3\r\nabc\r\n");
        let op = try_skip_or_parse_client_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op, ClientOp::Pong));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_skip_hpub() {
        // HPUB subject hdr_len total_len\r\n[headers+payload]\r\n
        // Headers: "NATS/1.0\r\nKey: Val\r\n\r\n" = 24 bytes
        // Payload: "hello" = 5 bytes, total = 29
        let hdr = "NATS/1.0\r\nKey: Val\r\n\r\n";
        let payload = "hello";
        let hdr_len = hdr.len(); // 22
        let total_len = hdr_len + payload.len(); // 27
        let msg = format!(
            "HPUB foo {} {}\r\n{}{}\r\n",
            hdr_len, total_len, hdr, payload
        );
        let mut buf = BytesMut::from(msg.as_str());
        let op = try_skip_or_parse_client_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op, ClientOp::Pong));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_skip_pub_incomplete() {
        let mut buf = BytesMut::from("PUB foo 5\r\nhel");
        let op = try_skip_or_parse_client_op(&mut buf).unwrap();
        assert!(op.is_none()); // not enough data
    }

    // -- parse_headers status line -----------------------------------------------

    #[test]
    fn test_parse_headers_status_503() {
        let data = b"NATS/1.0 503\r\n\r\n";
        let h = parse_headers(data).unwrap();
        assert_eq!(h.status(), Some(503));
        assert_eq!(h.description(), None);
    }

    #[test]
    fn test_parse_headers_status_with_description() {
        let data = b"NATS/1.0 408 Request Timeout\r\n\r\n";
        let h = parse_headers(data).unwrap();
        assert_eq!(h.status(), Some(408));
        assert_eq!(h.description(), Some("Request Timeout"));
    }

    #[test]
    fn test_parse_headers_no_status() {
        let data = b"NATS/1.0\r\nX-Key: val\r\n\r\n";
        let h = parse_headers(data).unwrap();
        assert_eq!(h.status(), None);
        assert_eq!(h.get("X-Key"), Some("val"));
    }

    #[test]
    fn test_parse_headers_status_round_trip() {
        let data = b"NATS/1.0 408 Request Timeout\r\nX-Foo: bar\r\n\r\n";
        let h = parse_headers(data).unwrap();
        let bytes = h.to_bytes();
        let h2 = parse_headers(&bytes).unwrap();
        assert_eq!(h2.status(), Some(408));
        assert_eq!(h2.description(), Some("Request Timeout"));
        assert_eq!(h2.get("X-Foo"), Some("bar"));
    }

    #[test]
    fn test_parse_headers_continuation_spacing() {
        let data = b"NATS/1.0\r\nX-Multi: line1\r\n  line2\r\n\r\n";
        let h = parse_headers(data).unwrap();
        assert_eq!(h.get("X-Multi"), Some("line1 line2"));
    }

    // -- Route protocol parser tests -------------------------------------------

    #[test]

    fn test_parse_route_sub() {
        let mut buf = BytesMut::from("RS+ $G test.subject\r\n");
        let op = try_parse_route_op(&mut buf).unwrap().unwrap();
        match op {
            RouteOp::RouteSub { subject, queue, .. } => {
                assert_eq!(&subject[..], b"test.subject");
                assert!(queue.is_none());
            }
            _ => panic!("expected RouteSub"),
        }
        assert!(buf.is_empty());
    }

    #[test]

    fn test_parse_route_sub_queue() {
        let mut buf = BytesMut::from("RS+ $G test.subject myqueue 1\r\n");
        let op = try_parse_route_op(&mut buf).unwrap().unwrap();
        match op {
            RouteOp::RouteSub { subject, queue, .. } => {
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&queue.unwrap()[..], b"myqueue");
            }
            _ => panic!("expected RouteSub with queue"),
        }
    }

    #[test]

    fn test_parse_route_unsub() {
        let mut buf = BytesMut::from("RS- $G test.subject\r\n");
        let op = try_parse_route_op(&mut buf).unwrap().unwrap();
        match op {
            RouteOp::RouteUnsub { subject, .. } => {
                assert_eq!(&subject[..], b"test.subject");
            }
            _ => panic!("expected RouteUnsub"),
        }
    }

    #[test]

    fn test_parse_rmsg_no_reply() {
        let mut buf = BytesMut::from("RMSG $G test.sub 5\r\nhello\r\n");
        let op = try_parse_route_op(&mut buf).unwrap().unwrap();
        match op {
            RouteOp::RouteMsg {
                subject,
                reply,
                headers,
                payload,
                ..
            } => {
                assert_eq!(&subject[..], b"test.sub");
                assert!(reply.is_none());
                assert!(headers.is_none());
                assert_eq!(&payload[..], b"hello");
            }
            _ => panic!("expected RouteMsg"),
        }
        assert!(buf.is_empty());
    }

    #[test]

    fn test_parse_rmsg_with_reply() {
        let mut buf = BytesMut::from("RMSG $G test.sub reply.to 2\r\nhi\r\n");
        let op = try_parse_route_op(&mut buf).unwrap().unwrap();
        match op {
            RouteOp::RouteMsg {
                subject,
                reply,
                payload,
                ..
            } => {
                assert_eq!(&subject[..], b"test.sub");
                assert_eq!(&reply.unwrap()[..], b"reply.to");
                assert_eq!(&payload[..], b"hi");
            }
            _ => panic!("expected RouteMsg"),
        }
    }

    #[test]

    fn test_parse_rmsg_incomplete() {
        let mut buf = BytesMut::from("RMSG $G test.sub 10\r\nhel");
        let op = try_parse_route_op(&mut buf).unwrap();
        assert!(op.is_none()); // need more data
    }

    #[test]

    fn test_parse_route_ping_pong() {
        let mut buf = BytesMut::from("PING\r\nPONG\r\n");
        let op = try_parse_route_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op, RouteOp::Ping));
        let op = try_parse_route_op(&mut buf).unwrap().unwrap();
        assert!(matches!(op, RouteOp::Pong));
    }

    #[test]

    fn test_parse_route_info() {
        let mut buf = BytesMut::from(
            "INFO {\"server_id\":\"test\",\"server_name\":\"node1\",\"port\":4248}\r\n",
        );
        let op = try_parse_route_op(&mut buf).unwrap().unwrap();
        match op {
            RouteOp::Info(info) => {
                assert_eq!(info.server_id, "test");
                assert_eq!(info.server_name, "node1");
                assert_eq!(info.port, 4248);
            }
            _ => panic!("expected Info"),
        }
    }

    #[test]

    fn test_build_rmsg() {
        let mut b = MsgBuilder::new();
        let data = b.build_rmsg(
            b"test.sub",
            None,
            None,
            b"hello",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RMSG $G test.sub 5\r\nhello\r\n");
    }

    #[test]

    fn test_build_rmsg_with_reply() {
        let mut b = MsgBuilder::new();
        let data = b.build_rmsg(
            b"test.sub",
            Some(b"reply.to"),
            None,
            b"hi",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RMSG $G test.sub reply.to 2\r\nhi\r\n");
    }

    #[test]

    fn test_build_route_sub() {
        let mut b = MsgBuilder::new();
        let data = b.build_route_sub(
            b"test.subject",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RS+ $G test.subject\r\n");
    }

    #[test]

    fn test_build_route_unsub() {
        let mut b = MsgBuilder::new();
        let data = b.build_route_unsub(
            b"test.subject",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RS- $G test.subject\r\n");
    }

    #[test]

    fn test_build_route_sub_queue() {
        let mut b = MsgBuilder::new();
        let data = b.build_route_sub_queue(
            b"test.subject",
            b"q1",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RS+ $G test.subject q1 1\r\n");
    }

    #[test]

    fn test_rmsg_roundtrip() {
        let mut builder = MsgBuilder::new();
        let wire = builder.build_rmsg(
            b"foo.bar",
            Some(b"reply"),
            None,
            b"payload",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let mut buf = BytesMut::from(wire);
        let op = try_parse_route_op(&mut buf).unwrap().unwrap();
        match op {
            RouteOp::RouteMsg {
                subject,
                reply,
                payload,
                ..
            } => {
                assert_eq!(&subject[..], b"foo.bar");
                assert_eq!(&reply.unwrap()[..], b"reply");
                assert_eq!(&payload[..], b"payload");
            }
            _ => panic!("expected RouteMsg"),
        }
    }
}
