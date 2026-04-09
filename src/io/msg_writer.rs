//! MsgWriter — shared buffer + eventfd notification for cross-worker message delivery.
//!
//! Instead of sending structs through an mpsc channel, the upstream reader formats
//! MSG/HMSG wire bytes directly into this shared buffer. The worker thread is
//! notified via a shared eventfd to flush the buffer to TCP.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};

use crate::nats_proto::MsgBuilder;

use crate::protocol::bin_proto;
use crate::types::HeaderMap;

// ── Segment buffer types for zero-copy binary route delivery ──────────────────

/// A single binary message frame using zero-copy subject and payload refs.
pub(crate) struct BinMsgFrame {
    /// 9-byte framing header (op + subj_len + repl_len + pay_len).
    pub header: [u8; 9],
    pub subject: Bytes,
    /// Reply subject, or empty `Bytes` if none.
    pub reply: Bytes,
    pub payload: Bytes,
}

/// A segment in the binary direct buffer.
pub(crate) enum BinSeg {
    /// Control frame (sub/unsub, ping/pong): small bytes copied inline.
    Inline(Bytes),
    /// Message frame: zero-copy subject + payload refs.
    Msg(BinMsgFrame),
}

/// Segment accumulator for binary route connections (zero-copy).
pub(crate) struct BinSegBuf {
    pub segs: Vec<BinSeg>,
    /// Running total of logical bytes (for slow-consumer detection).
    pub total_len: usize,
}
impl BinSegBuf {
    pub fn new() -> Self {
        BinSegBuf {
            segs: Vec::new(),
            total_len: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.segs.is_empty()
    }

    /// Push a zero-copy message frame. O(1) — subject/payload are Bytes clones.
    pub fn push_msg(&mut self, header: [u8; 9], subject: Bytes, reply: Bytes, payload: Bytes) {
        self.total_len += 9 + subject.len() + reply.len() + payload.len();
        self.segs.push(BinSeg::Msg(BinMsgFrame {
            header,
            subject,
            reply,
            payload,
        }));
    }

    /// Push a control frame (already encoded as bytes).
    pub fn push_inline(&mut self, data: Bytes) {
        self.total_len += data.len();
        self.segs.push(BinSeg::Inline(data));
    }

    /// Drain all segments (O(1) swap).
    pub fn take(&mut self) -> Vec<BinSeg> {
        self.total_len = 0;
        std::mem::take(&mut self.segs)
    }

    /// Materialize all segments into a flat `BytesMut` (used for partial-write recovery).
    pub fn materialize_into(&mut self, out: &mut BytesMut) {
        for seg in self.segs.drain(..) {
            match seg {
                BinSeg::Inline(b) => out.extend_from_slice(&b),
                BinSeg::Msg(f) => {
                    out.extend_from_slice(&f.header);
                    out.extend_from_slice(&f.subject);
                    out.extend_from_slice(&f.reply);
                    out.extend_from_slice(&f.payload);
                }
            }
        }
        self.total_len = 0;
    }
}

/// Per-connection direct buffer: text (copy into BytesMut) or binary (zero-copy segments).
pub(crate) enum DirectBuf {
    /// For client / leaf / gateway connections — bytes are copied.
    Text(BytesMut),
    /// For binary route connections — segment list, zero-copy.
    Binary(BinSegBuf),
}

impl DirectBuf {
    pub fn is_empty(&self) -> bool {
        match self {
            DirectBuf::Text(b) => b.is_empty(),

            DirectBuf::Binary(s) => s.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            DirectBuf::Text(b) => b.len(),

            DirectBuf::Binary(s) => s.total_len,
        }
    }
}

/// Create a Linux eventfd for notification.
pub(crate) fn create_eventfd() -> OwnedFd {
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    assert!(
        fd >= 0,
        "eventfd creation failed: {}",
        std::io::Error::last_os_error()
    );
    unsafe { OwnedFd::from_raw_fd(fd) }
}

/// A shared write buffer + eventfd pair for zero-channel message delivery.
///
/// Instead of sending `ClientMsg` structs through an mpsc channel (which costs
/// atomic ops + linked-list push + task wake per message), the upstream reader
/// formats MSG/HMSG wire bytes directly into this shared buffer. The worker
/// thread is notified via a shared eventfd to flush the buffer to TCP.
///
/// Multiple `MsgWriter`s on the same worker share one eventfd, so fan-out
/// to N connections on one worker costs only 1 eventfd write.
#[derive(Clone)]
pub(crate) struct MsgWriter {
    buf: Arc<Mutex<DirectBuf>>,
    event_fd: Arc<OwnedFd>,
    has_pending: Arc<AtomicBool>,
    /// Pre-built MsgBuilder for formatting — kept per-writer to avoid allocation.
    msg_builder: Arc<Mutex<MsgBuilder>>,
    /// When true, encode outgoing route frames as binary (open-wire binary protocol).
    binary: bool,
    /// Cross-worker congestion signal set by the drainer (route writer thread or
    /// flush_pending), read by publishers before writing.
    /// 0 = clear, 1 = soft (25-75%), 2 = hard (>75%).
    congestion: Arc<AtomicU8>,
}

impl MsgWriter {
    /// Create a MsgWriter with an externally-owned shared `DirectBuf` (used by worker).
    pub(crate) fn new(
        buf: Arc<Mutex<DirectBuf>>,
        has_pending: Arc<AtomicBool>,
        event_fd: Arc<OwnedFd>,
    ) -> Self {
        Self {
            buf,
            has_pending,
            event_fd,
            msg_builder: Arc::new(Mutex::new(MsgBuilder::new())),

            binary: false,
            congestion: Arc::new(AtomicU8::new(0)),
        }
    }

    /// Create a standalone text-mode MsgWriter with its own eventfd (for tests/benchmarks).
    pub(crate) fn new_dummy() -> Self {
        let buf = Arc::new(Mutex::new(DirectBuf::Text(BytesMut::with_capacity(65536))));
        let has_pending = Arc::new(AtomicBool::new(false));
        let event_fd = Arc::new(create_eventfd());
        Self {
            buf,
            has_pending,
            event_fd,
            msg_builder: Arc::new(Mutex::new(MsgBuilder::new())),

            binary: false,
            congestion: Arc::new(AtomicU8::new(0)),
        }
    }

    /// Create a standalone binary-mode MsgWriter (for binary outbound route connections).
    pub(crate) fn new_binary_dummy() -> Self {
        let buf = Arc::new(Mutex::new(DirectBuf::Binary(BinSegBuf::new())));
        let has_pending = Arc::new(AtomicBool::new(false));
        let event_fd = Arc::new(create_eventfd());
        Self {
            buf,
            has_pending,
            event_fd,
            msg_builder: Arc::new(Mutex::new(MsgBuilder::new())),
            binary: true,
            congestion: Arc::new(AtomicU8::new(0)),
        }
    }

    /// Returns true if this writer encodes frames in binary (open-wire) format.
    pub(crate) fn is_binary(&self) -> bool {
        self.binary
    }

    /// Create a binary-mode MsgWriter sharing the given `DirectBuf` and notification Arcs.
    ///
    /// Used to upgrade an inbound route connection's writer to binary mode while
    /// keeping `flush_pending` in the worker pointing at the same shared data.
    pub(crate) fn new_binary_shared(
        buf: Arc<Mutex<DirectBuf>>,
        has_pending: Arc<AtomicBool>,
        event_fd: Arc<OwnedFd>,
    ) -> Self {
        Self {
            buf,
            has_pending,
            event_fd,
            msg_builder: Arc::new(Mutex::new(MsgBuilder::new())),
            binary: true,
            congestion: Arc::new(AtomicU8::new(0)),
        }
    }

    /// Proportional backpressure for client/leaf connections: if buffer is above
    /// the soft HWM, sleep briefly on the caller's thread.
    ///
    /// Route connections use a different mechanism (per-connection read budget via
    /// the `congestion` atomic) so this is only called from `write_msg`/`write_lmsg`.
    #[inline]
    fn backpressure_check(&self) {
        const HWM: usize = 32 * 1024 * 1024;
        const MAX: usize = 64 * 1024 * 1024;
        let len = self.buf.lock().expect("buf lock").len();
        if len > HWM {
            let congestion = ((len - HWM) as f64 / (MAX - HWM) as f64).min(1.0);
            let sleep_us = 1 + (congestion * 500.0) as u64;
            std::thread::sleep(std::time::Duration::from_micros(sleep_us));
        }
    }

    /// Current congestion level (0=clear, 1=soft, 2=hard).
    /// Lock-free read — safe to call from any worker thread.
    #[inline]
    pub(crate) fn congestion(&self) -> u8 {
        self.congestion.load(Ordering::Relaxed)
    }

    /// Set the congestion level (called by the drainer after each flush cycle).
    #[inline]
    pub(crate) fn set_congestion(&self, level: u8) {
        self.congestion.store(level, Ordering::Relaxed);
    }

    /// Current buffer length (acquires the lock briefly).
    pub(crate) fn buf_len(&self) -> usize {
        self.buf.lock().expect("buf lock").len()
    }

    /// Append pre-formatted text bytes to the shared buffer and mark as pending.
    #[inline]
    fn append_text(&self, data: &[u8]) {
        let mut buf = self.buf.lock().expect("buf lock");
        if let DirectBuf::Text(b) = &mut *buf {
            b.extend_from_slice(data);
        }
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Format and append a MSG/HMSG to the shared buffer. Fully synchronous.
    pub(crate) fn write_msg(
        &self,
        subject: &[u8],
        sid_bytes: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) {
        self.backpressure_check();
        let mut builder = self.msg_builder.lock().expect("msg_builder lock");
        let data = builder.build_msg(subject, sid_bytes, reply, headers, payload);
        self.append_text(data);
    }

    /// Format and append an LMSG to the shared buffer (for leaf node delivery).
    pub(crate) fn write_lmsg(
        &self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) {
        self.backpressure_check();
        let mut builder = self.msg_builder.lock().expect("msg_builder lock");
        let data = builder.build_lmsg(subject, reply, headers, payload);
        self.append_text(data);
    }

    /// Format and append an RMSG to the shared buffer (for route/gateway delivery).
    ///
    /// In binary mode, `subject` and `payload` are stored as zero-copy `Bytes`
    /// segment refs (O(1) clone) rather than being copied, eliminating two memcpy
    /// calls per routed message. Headers are rare and still serialised inline.
    /// Format and append an RMSG to the shared buffer (for route/gateway delivery).
    ///
    /// No sleep-based backpressure here — route congestion is handled by the
    /// per-connection read budget in the worker event loop (non-blocking).
    pub(crate) fn write_rmsg(
        &self,
        subject: &Bytes,
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &Bytes,
        #[cfg(feature = "accounts")] account: &[u8],
    ) {
        if self.binary {
            let reply_slice = reply.unwrap_or(b"");
            let mut buf = self.buf.lock().expect("buf lock");
            if let Some(hdrs) = headers {
                // Headers are rare and serialised inline; store as Inline segment.
                let mut tmp = BytesMut::new();
                let hdr_bytes = hdrs.to_bytes();
                bin_proto::write_hmsg(
                    subject.as_ref(),
                    reply_slice,
                    &hdr_bytes,
                    payload.as_ref(),
                    &mut tmp,
                );
                if let DirectBuf::Binary(seg) = &mut *buf {
                    seg.push_inline(tmp.freeze());
                }
            } else {
                // Zero-copy: store subject + payload as Bytes refs in segment list.
                let header = bin_proto::msg_header(subject.as_ref(), reply_slice, payload.as_ref());
                let reply_bytes = if reply_slice.is_empty() {
                    Bytes::new()
                } else {
                    Bytes::copy_from_slice(reply_slice)
                };
                if let DirectBuf::Binary(seg) = &mut *buf {
                    seg.push_msg(header, subject.clone(), reply_bytes, payload.clone());
                }
            }
            drop(buf);
            self.has_pending.store(true, Ordering::Release);
            return;
        }
        let mut builder = self.msg_builder.lock().expect("msg_builder lock");
        let data = builder.build_rmsg(
            subject.as_ref(),
            reply,
            headers,
            payload.as_ref(),
            #[cfg(feature = "accounts")]
            account,
        );
        self.append_text(data);
    }

    /// Append a binary inline segment and mark as pending.
    #[inline]
    fn append_binary_inline(&self, data: Bytes) {
        let mut buf = self.buf.lock().expect("buf lock");
        if let DirectBuf::Binary(seg) = &mut *buf {
            seg.push_inline(data);
        }
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Write a route sub (RS+ or binary Sub) to the shared buffer.
    pub(crate) fn write_route_sub(
        &self,
        subject: &[u8],
        queue: Option<&[u8]>,
        #[cfg(feature = "accounts")] account: &[u8],
    ) {
        if self.binary {
            #[cfg(not(feature = "accounts"))]
            let account: &[u8] = b"$G";
            let mut tmp = BytesMut::new();
            bin_proto::write_sub(subject, queue.unwrap_or(b""), account, &mut tmp);
            self.append_binary_inline(tmp.freeze());
            return;
        }
        let mut builder = self.msg_builder.lock().expect("msg_builder lock");
        let data = if let Some(q) = queue {
            builder.build_route_sub_queue(
                subject,
                q,
                #[cfg(feature = "accounts")]
                account,
            )
        } else {
            builder.build_route_sub(
                subject,
                #[cfg(feature = "accounts")]
                account,
            )
        };
        self.append_text(data);
    }

    /// Write a route unsub (RS- or binary Unsub) to the shared buffer.
    pub(crate) fn write_route_unsub(
        &self,
        subject: &[u8],
        queue: Option<&[u8]>,
        #[cfg(feature = "accounts")] account: &[u8],
    ) {
        if self.binary {
            #[cfg(not(feature = "accounts"))]
            let account: &[u8] = b"$G";
            let mut tmp = BytesMut::new();
            bin_proto::write_unsub(subject, account, &mut tmp);
            self.append_binary_inline(tmp.freeze());
            return;
        }
        let mut builder = self.msg_builder.lock().expect("msg_builder lock");
        let data = if let Some(q) = queue {
            builder.build_route_unsub_queue(
                subject,
                q,
                #[cfg(feature = "accounts")]
                account,
            )
        } else {
            builder.build_route_unsub(
                subject,
                #[cfg(feature = "accounts")]
                account,
            )
        };
        self.append_text(data);
    }

    /// Append raw protocol bytes to the shared buffer (e.g. LS+/LS-/RS+ lines).
    pub(crate) fn write_raw(&self, data: &[u8]) {
        self.append_text(data);
    }

    /// Notify the worker thread that there is data to flush.
    /// Writes 1 to the eventfd — wakes epoll_wait() on the worker thread.
    /// Multiple writers sharing one eventfd collapse into a single wake.
    pub(crate) fn notify(&self) {
        let val: u64 = 1;
        unsafe {
            libc::write(
                self.event_fd.as_raw_fd(),
                &val as *const u64 as *const libc::c_void,
                8,
            );
        }
    }

    /// Drain all buffered data. Returns `None` if buffer was empty.
    ///
    /// For binary-mode buffers, segments are materialised into a flat `BytesMut`
    /// so callers (e.g. tests) can inspect the raw bytes.
    pub(crate) fn drain(&self) -> Option<BytesMut> {
        let mut buf = self.buf.lock().expect("buf lock");
        if buf.is_empty() {
            return None;
        }
        match &mut *buf {
            DirectBuf::Text(b) => Some(b.split()),

            DirectBuf::Binary(seg_buf) => {
                let mut out = BytesMut::with_capacity(seg_buf.total_len);
                seg_buf.materialize_into(&mut out);
                Some(out)
            }
        }
    }

    /// Get the raw fd of the eventfd.
    pub(crate) fn event_raw_fd(&self) -> std::os::fd::RawFd {
        self.event_fd.as_raw_fd()
    }

    /// Read the eventfd to reset it after poll() returns POLLIN.
    #[cfg(test)]
    pub(crate) fn consume_notify(&self) {
        let mut val: u64 = 0;
        unsafe {
            libc::read(
                self.event_fd.as_raw_fd(),
                &mut val as *mut u64 as *mut libc::c_void,
                8,
            );
        }
    }
}

impl std::fmt::Debug for MsgWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MsgWriter").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clone_shares_buffer() {
        let writer1 = MsgWriter::new_dummy();
        let writer2 = writer1.clone();

        writer1.write_msg(b"a", b"1", None, None, b"x");
        writer2.write_msg(b"b", b"2", None, None, b"y");

        let data = writer1.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.contains("MSG a 1 1\r\nx\r\n"));
        assert!(s.contains("MSG b 2 1\r\ny\r\n"));
    }

    #[test]
    fn test_batches_multiple_writes() {
        let writer = MsgWriter::new_dummy();

        writer.write_msg(b"a", b"1", None, None, b"one");
        writer.write_msg(b"b", b"2", None, None, b"two");
        writer.write_msg(b"c", b"3", None, None, b"three");

        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(
            s,
            "MSG a 1 3\r\none\r\nMSG b 2 3\r\ntwo\r\nMSG c 3 5\r\nthree\r\n"
        );
    }

    #[test]
    fn test_drain_empty() {
        let writer = MsgWriter::new_dummy();
        assert!(writer.drain().is_none());
    }

    #[test]
    fn test_drain_resets_buffer() {
        let writer = MsgWriter::new_dummy();

        writer.write_msg(b"a", b"1", None, None, b"x");
        let _ = writer.drain().unwrap();

        // Second drain should be empty
        assert!(writer.drain().is_none());

        // Write again — should work
        writer.write_msg(b"b", b"2", None, None, b"y");
        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG b 2 1\r\ny\r\n");
    }

    #[test]
    fn test_notify_wakes() {
        let writer = MsgWriter::new_dummy();
        let writer2 = writer.clone();

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            writer2.write_msg(b"test", b"1", None, None, b"hello");
            writer2.notify();
        });

        let mut pfd = [libc::pollfd {
            fd: writer.event_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        let ret = unsafe { libc::poll(pfd.as_mut_ptr(), 1, 5000) };
        assert!(ret > 0, "poll should have returned ready");
        writer.consume_notify();

        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG test 1 5\r\nhello\r\n");
    }

    #[test]
    fn test_notify_stores_permit() {
        let writer = MsgWriter::new_dummy();

        writer.write_msg(b"test", b"1", None, None, b"early");
        writer.notify();

        std::thread::sleep(std::time::Duration::from_millis(10));

        let mut pfd = [libc::pollfd {
            fd: writer.event_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        let ret = unsafe { libc::poll(pfd.as_mut_ptr(), 1, 0) };
        assert!(ret > 0, "poll should return immediately for stored notify");
        writer.consume_notify();

        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG test 1 5\r\nearly\r\n");
    }

    #[test]
    fn test_fast_producer_slow_consumer() {
        let writer = MsgWriter::new_dummy();
        let producer_writer = writer.clone();
        let total_msgs = 10_000;

        let producer = std::thread::spawn(move || {
            for i in 0..total_msgs {
                let payload = format!("msg{i}");
                producer_writer.write_msg(b"test", b"1", None, None, payload.as_bytes());
                producer_writer.notify();
            }
        });

        let mut total_msgs_seen = 0usize;
        let mut pfd = [libc::pollfd {
            fd: writer.event_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        loop {
            while let Some(data) = writer.drain() {
                let s = std::str::from_utf8(&data).unwrap();
                total_msgs_seen += s.matches("MSG test 1").count();
            }
            if total_msgs_seen >= total_msgs {
                break;
            }
            unsafe { libc::poll(pfd.as_mut_ptr(), 1, 5000) };
            writer.consume_notify();
        }

        producer.join().unwrap();
        assert_eq!(total_msgs_seen, total_msgs);
    }

    #[test]
    fn test_producer_finishes_before_consumer() {
        let writer = MsgWriter::new_dummy();
        let total_msgs = 1_000;

        for i in 0..total_msgs {
            let payload = format!("m{i}");
            writer.write_msg(b"x", b"1", None, None, payload.as_bytes());
        }
        writer.notify();

        let mut total_msgs_seen = 0usize;
        let mut pfd = [libc::pollfd {
            fd: writer.event_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        loop {
            while let Some(data) = writer.drain() {
                let s = std::str::from_utf8(&data).unwrap();
                total_msgs_seen += s.matches("MSG x 1").count();
            }
            if total_msgs_seen >= total_msgs {
                break;
            }
            let ret = unsafe { libc::poll(pfd.as_mut_ptr(), 1, 1000) };
            if ret > 0 {
                writer.consume_notify();
            } else {
                panic!("consumer hung! only received {total_msgs_seen}/{total_msgs} messages");
            }
        }
        assert_eq!(total_msgs_seen, total_msgs);
    }

    #[test]
    fn test_write_then_drain_clears_pending() {
        let writer = MsgWriter::new_dummy();

        assert!(!writer.has_pending.load(Ordering::Acquire));

        writer.write_msg(b"test", b"1", None, None, b"data");
        assert!(writer.has_pending.load(Ordering::Acquire));

        let data = writer.drain().unwrap();
        assert_eq!(&data[..], b"MSG test 1 4\r\ndata\r\n");
        // has_pending is still true — the worker is responsible for clearing it
        assert!(writer.has_pending.load(Ordering::Acquire));
    }
}

/// Tests for binary-mode MsgWriter (open-wire inter-node framing).
#[cfg(test)]
mod binary_tests {
    use super::*;
    use crate::protocol::bin_proto::{self, BinOp};

    /// Drain the writer buffer and decode exactly one binary frame.
    fn drain_one(w: &MsgWriter) -> bin_proto::BinFrame {
        let mut data = w.drain().expect("writer buffer was empty");
        bin_proto::try_decode(&mut data).expect("failed to decode binary frame")
    }

    #[test]
    fn write_rmsg_encodes_msg_frame() {
        let w = MsgWriter::new_binary_dummy();
        w.write_rmsg(
            &Bytes::from_static(b"foo.bar"),
            None,
            None,
            &Bytes::from_static(b"hello"),
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let frame = drain_one(&w);
        assert_eq!(frame.op, BinOp::Msg);
        assert_eq!(&frame.subject[..], b"foo.bar");
        assert!(frame.reply.is_empty());
        assert_eq!(&frame.payload[..], b"hello");
    }

    #[test]
    fn write_rmsg_encodes_reply() {
        let w = MsgWriter::new_binary_dummy();
        w.write_rmsg(
            &Bytes::from_static(b"foo"),
            Some(b"_INBOX.123"),
            None,
            &Bytes::new(),
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let frame = drain_one(&w);
        assert_eq!(frame.op, BinOp::Msg);
        assert_eq!(&frame.reply[..], b"_INBOX.123");
    }

    #[test]
    fn write_rmsg_encodes_hmsg_with_headers() {
        let w = MsgWriter::new_binary_dummy();
        let mut hdrs = HeaderMap::new();
        hdrs.insert("X-Test", "v".to_string());
        w.write_rmsg(
            &Bytes::from_static(b"sub"),
            None,
            Some(&hdrs),
            &Bytes::from_static(b"body"),
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let frame = drain_one(&w);
        assert_eq!(frame.op, BinOp::HMsg);
        assert_eq!(&frame.subject[..], b"sub");
        // payload layout: [4B hdr_len LE][hdr_bytes][body]
        assert!(frame.payload.len() >= 4);
        let hdr_len = u32::from_le_bytes([
            frame.payload[0],
            frame.payload[1],
            frame.payload[2],
            frame.payload[3],
        ]) as usize;
        assert!(hdr_len > 0, "header length must be non-zero");
        assert!(
            frame.payload.len() >= 4 + hdr_len,
            "payload too short for claimed header length"
        );
        assert_eq!(&frame.payload[4 + hdr_len..], b"body");
    }

    #[test]
    fn write_rmsg_empty_payload() {
        let w = MsgWriter::new_binary_dummy();
        w.write_rmsg(
            &Bytes::from_static(b"a.b.c"),
            None,
            None,
            &Bytes::new(),
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let frame = drain_one(&w);
        assert_eq!(frame.op, BinOp::Msg);
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn write_route_sub_binary_no_queue() {
        let w = MsgWriter::new_binary_dummy();
        w.write_route_sub(
            b"foo.>",
            None,
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let frame = drain_one(&w);
        assert_eq!(frame.op, BinOp::Sub);
        assert_eq!(&frame.subject[..], b"foo.>");
        assert!(frame.reply.is_empty(), "no queue → reply field empty");
        assert_eq!(&frame.payload[..], b"$G", "account defaults to $G");
    }

    #[test]
    fn write_route_sub_binary_with_queue() {
        let w = MsgWriter::new_binary_dummy();
        w.write_route_sub(
            b"events.>",
            Some(b"workers"),
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let frame = drain_one(&w);
        assert_eq!(frame.op, BinOp::Sub);
        assert_eq!(&frame.subject[..], b"events.>");
        assert_eq!(&frame.reply[..], b"workers", "queue group in reply field");
    }

    #[test]
    fn write_route_unsub_binary() {
        let w = MsgWriter::new_binary_dummy();
        w.write_route_unsub(
            b"foo.>",
            None,
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let frame = drain_one(&w);
        assert_eq!(frame.op, BinOp::Unsub);
        assert_eq!(&frame.subject[..], b"foo.>");
        assert!(frame.reply.is_empty());
        assert_eq!(&frame.payload[..], b"$G");
    }

    #[test]
    fn text_mode_write_route_sub_produces_text_not_binary() {
        let w = MsgWriter::new_dummy(); // text mode
        w.write_route_sub(
            b"foo",
            None,
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let data = w.drain().expect("text writer should have data");
        // Text RS+ must begin with "RS+"
        assert!(
            data.starts_with(b"RS+"),
            "text mode must produce RS+ line, got: {:?}",
            &data[..]
        );
        // Text output should NOT be parseable as a binary frame with valid op
        let mut clone = data.clone();
        let frame = bin_proto::try_decode(&mut clone);
        if let Some(f) = frame {
            // If it accidentally decodes, the op byte ('R' = 0x52) is not a valid BinOp
            assert_ne!(f.op, BinOp::Sub, "text RS+ must not decode as binary Sub");
        }
    }

    #[test]
    fn multiple_frames_in_sequence() {
        let w = MsgWriter::new_binary_dummy();
        w.write_route_sub(
            b"a",
            None,
            #[cfg(feature = "accounts")]
            b"$G",
        );
        w.write_route_sub(
            b"b",
            Some(b"q"),
            #[cfg(feature = "accounts")]
            b"$G",
        );
        w.write_rmsg(
            &Bytes::from_static(b"c"),
            None,
            None,
            &Bytes::from_static(b"data"),
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let mut data = w.drain().unwrap();
        let f1 = bin_proto::try_decode(&mut data).unwrap();
        let f2 = bin_proto::try_decode(&mut data).unwrap();
        let f3 = bin_proto::try_decode(&mut data).unwrap();
        assert!(
            bin_proto::try_decode(&mut data).is_none(),
            "should be no more frames"
        );
        assert_eq!(f1.op, BinOp::Sub);
        assert_eq!(&f1.subject[..], b"a");
        assert_eq!(f2.op, BinOp::Sub);
        assert_eq!(&f2.reply[..], b"q");
        assert_eq!(f3.op, BinOp::Msg);
        assert_eq!(&f3.subject[..], b"c");
    }

    #[test]
    fn binary_frame_size_is_compact() {
        let w = MsgWriter::new_binary_dummy();
        w.write_rmsg(
            &Bytes::from_static(b"x"),
            None,
            None,
            &Bytes::from_static(b"y"),
            #[cfg(feature = "accounts")]
            b"$G",
        );
        let data = w.drain().unwrap();
        // 9-byte header + 1 subject + 0 reply + 1 payload = 11 bytes
        assert_eq!(
            data.len(),
            11,
            "compact frame: header(9) + subj(1) + repl(0) + pay(1)"
        );
    }

    #[test]
    fn has_pending_set_after_binary_write() {
        let w = MsgWriter::new_binary_dummy();
        assert!(!w.has_pending.load(std::sync::atomic::Ordering::Acquire));
        w.write_rmsg(
            &Bytes::from_static(b"t"),
            None,
            None,
            &Bytes::new(),
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert!(w.has_pending.load(std::sync::atomic::Ordering::Acquire));
    }
}
