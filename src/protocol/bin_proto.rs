//! Binary inter-node framing for open-wire peer connections.
//!
//! Frame layout (9-byte fixed header):
//!
//! ```text
//! ┌────┬──────────┬──────────┬──────────────┐
//! │ op │ subj_len │ repl_len │   pay_len    │
//! │ u8 │  u16 LE  │  u16 LE  │   u32 LE     │
//! └────┴──────────┴──────────┴──────────────┘
//!   1B      2B        2B           4B
//! ```
//!
//! Then `subj_len` bytes of subject, `repl_len` bytes of reply/queue,
//! `pay_len` bytes of payload.
//!
//! Op semantics:
//! - `Ping`/`Pong`: all lengths zero.
//! - `Msg`: subject=subject, reply=reply subject (empty→none), payload=payload.
//! - `HMsg`: subject=subject, reply=reply subject, payload=`[4B hdr_len LE][hdr_bytes][payload]`.
//!   `hdr_bytes` is NATS wire-format (`NATS/1.0\r\nKey: Val\r\n\r\n`).
//! - `Sub`: subject=subject, reply=queue group (empty→none), payload=account (e.g. `$G`).
//! - `Unsub`: subject=subject, reply=empty, payload=account.
//!
//! Negotiated via `"open_wire":1` in route INFO/CONNECT.

use bytes::{Bytes, BytesMut};

pub(crate) const HEADER_LEN: usize = 9;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinOp {
    Ping = 0x01,
    Pong = 0x02,
    Msg = 0x03,
    HMsg = 0x04,
    Sub = 0x05,
    Unsub = 0x06,
}

impl BinOp {
    pub(crate) fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Ping),
            0x02 => Some(Self::Pong),
            0x03 => Some(Self::Msg),
            0x04 => Some(Self::HMsg),
            0x05 => Some(Self::Sub),
            0x06 => Some(Self::Unsub),
            _ => None,
        }
    }
}

/// A decoded binary frame. Fields are zero-copy slices of the original buffer bytes.
#[derive(Debug)]
pub(crate) struct BinFrame {
    pub op: BinOp,
    /// `Msg`/`HMsg`: subject. `Sub`/`Unsub`: subject.
    pub subject: Bytes,
    /// `Msg`/`HMsg`: reply subject (empty = no reply). `Sub`: queue group (empty = no queue).
    pub reply: Bytes,
    /// `Msg`: payload bytes. `HMsg`: `[4B hdr_len][hdr_bytes][payload]`. `Sub`/`Unsub`: account.
    pub payload: Bytes,
}

/// Try to decode one frame from `buf`, advancing it past the consumed bytes.
///
/// Returns `None` if `buf` contains fewer bytes than needed for a complete frame.
/// On unknown op code the frame is consumed but returns `None`.
pub(crate) fn try_decode(buf: &mut BytesMut) -> Option<BinFrame> {
    if buf.len() < HEADER_LEN {
        return None;
    }
    let op = BinOp::from_u8(buf[0])?;
    let subj_len = u16::from_le_bytes([buf[1], buf[2]]) as usize;
    let repl_len = u16::from_le_bytes([buf[3], buf[4]]) as usize;
    let pay_len = u32::from_le_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
    let total = HEADER_LEN
        .checked_add(subj_len)
        .and_then(|s| s.checked_add(repl_len))
        .and_then(|s| s.checked_add(pay_len))?;
    if buf.len() < total {
        return None;
    }
    let data = buf.split_to(total).freeze();
    let subject = data.slice(HEADER_LEN..HEADER_LEN + subj_len);
    let reply = data.slice(HEADER_LEN + subj_len..HEADER_LEN + subj_len + repl_len);
    let payload = data.slice(HEADER_LEN + subj_len + repl_len..total);
    Some(BinFrame {
        op,
        subject,
        reply,
        payload,
    })
}

// ── Encoders ─────────────────────────────────────────────────────────────────

pub(crate) fn encode_into(
    op: BinOp,
    subject: &[u8],
    reply: &[u8],
    payload: &[u8],
    out: &mut BytesMut,
) {
    out.extend_from_slice(&[op as u8]);
    out.extend_from_slice(&(subject.len() as u16).to_le_bytes());
    out.extend_from_slice(&(reply.len() as u16).to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(subject);
    out.extend_from_slice(reply);
    out.extend_from_slice(payload);
}

pub(crate) fn write_pong(out: &mut BytesMut) {
    encode_into(BinOp::Pong, b"", b"", b"", out);
}

/// Build the 9-byte binary Msg frame header for scatter-gather (zero-copy) writes.
///
/// Used by `MsgWriter::write_rmsg` in binary mode to store just the header inline
/// while keeping subject and payload as `Bytes` refs — no copy of message data.
pub(crate) fn msg_header(subject: &[u8], reply: &[u8], payload: &[u8]) -> [u8; 9] {
    let mut h = [0u8; 9];
    h[0] = BinOp::Msg as u8;
    h[1..3].copy_from_slice(&(subject.len() as u16).to_le_bytes());
    h[3..5].copy_from_slice(&(reply.len() as u16).to_le_bytes());
    h[5..9].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    h
}

/// Write an HMsg (message with NATS wire-format headers).
///
/// `hdr_bytes` is a serialized NATS header block (e.g. from `HeaderMap::to_bytes()`).
/// The frame payload is `[4B hdr_len LE][hdr_bytes][payload_bytes]` — no intermediate
/// allocation required.
pub(crate) fn write_hmsg(
    subject: &[u8],
    reply: &[u8],
    hdr_bytes: &[u8],
    payload: &[u8],
    out: &mut BytesMut,
) {
    let pay_total = 4 + hdr_bytes.len() + payload.len();
    out.extend_from_slice(&[BinOp::HMsg as u8]);
    out.extend_from_slice(&(subject.len() as u16).to_le_bytes());
    out.extend_from_slice(&(reply.len() as u16).to_le_bytes());
    out.extend_from_slice(&(pay_total as u32).to_le_bytes());
    out.extend_from_slice(subject);
    out.extend_from_slice(reply);
    out.extend_from_slice(&(hdr_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(hdr_bytes);
    out.extend_from_slice(payload);
}

pub(crate) fn write_sub(subject: &[u8], queue: &[u8], account: &[u8], out: &mut BytesMut) {
    encode_into(BinOp::Sub, subject, queue, account, out);
}

pub(crate) fn write_unsub(subject: &[u8], account: &[u8], out: &mut BytesMut) {
    encode_into(BinOp::Unsub, subject, b"", account, out);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(op: BinOp, subject: &[u8], reply: &[u8], payload: &[u8]) -> BinFrame {
        let mut buf = BytesMut::new();
        encode_into(op, subject, reply, payload, &mut buf);
        try_decode(&mut buf).expect("should decode")
    }

    #[test]
    fn ping_pong_roundtrip() {
        let mut buf = BytesMut::new();
        encode_into(BinOp::Ping, b"", b"", b"", &mut buf);
        let f = try_decode(&mut buf).unwrap();
        assert_eq!(f.op, BinOp::Ping);
        assert!(f.subject.is_empty());

        write_pong(&mut buf);
        let f = try_decode(&mut buf).unwrap();
        assert_eq!(f.op, BinOp::Pong);
    }

    #[test]
    fn msg_roundtrip() {
        let f = roundtrip(BinOp::Msg, b"foo.bar", b"_INBOX.1", b"hello world");
        assert_eq!(f.op, BinOp::Msg);
        assert_eq!(&f.subject[..], b"foo.bar");
        assert_eq!(&f.reply[..], b"_INBOX.1");
        assert_eq!(&f.payload[..], b"hello world");
    }

    #[test]
    fn sub_roundtrip() {
        let f = roundtrip(BinOp::Sub, b"foo.>", b"q1", b"$G");
        assert_eq!(f.op, BinOp::Sub);
        assert_eq!(&f.subject[..], b"foo.>"); // subject
        assert_eq!(&f.reply[..], b"q1"); // queue
        assert_eq!(&f.payload[..], b"$G"); // account
    }

    #[test]
    fn unsub_roundtrip() {
        let f = roundtrip(BinOp::Unsub, b"foo.>", b"", b"$G");
        assert_eq!(f.op, BinOp::Unsub);
        assert_eq!(&f.subject[..], b"foo.>");
        assert!(f.reply.is_empty());
        assert_eq!(&f.payload[..], b"$G");
    }

    #[test]
    fn incomplete_header_returns_none() {
        let mut buf = BytesMut::from(&b"\x03\x07\x00"[..]);
        assert!(try_decode(&mut buf).is_none());
        assert_eq!(buf.len(), 3); // not advanced
    }

    #[test]
    fn partial_body_returns_none() {
        let mut buf = BytesMut::new();
        encode_into(BinOp::Msg, b"foo", b"", b"hello", &mut buf);
        buf.truncate(buf.len() - 2);
        assert!(try_decode(&mut buf).is_none());
    }

    #[test]
    fn sequential_frames() {
        let mut buf = BytesMut::new();
        encode_into(BinOp::Msg, b"a", b"", b"1", &mut buf);
        encode_into(BinOp::Msg, b"b", b"", b"2", &mut buf);
        let f1 = try_decode(&mut buf).unwrap();
        assert_eq!(&f1.subject[..], b"a");
        let f2 = try_decode(&mut buf).unwrap();
        assert_eq!(&f2.subject[..], b"b");
        assert!(try_decode(&mut buf).is_none());
    }

    #[test]
    fn hmsg_roundtrip() {
        let mut buf = BytesMut::new();
        let hdr = b"NATS/1.0\r\nX-Test: val\r\n\r\n";
        write_hmsg(b"test", b"", hdr, b"data", &mut buf);
        let f = try_decode(&mut buf).unwrap();
        assert_eq!(f.op, BinOp::HMsg);
        assert_eq!(&f.subject[..], b"test");
        assert!(f.reply.is_empty());
        let hdr_len =
            u32::from_le_bytes([f.payload[0], f.payload[1], f.payload[2], f.payload[3]]) as usize;
        assert_eq!(&f.payload[4..4 + hdr_len], hdr);
        assert_eq!(&f.payload[4 + hdr_len..], b"data");
    }
}
