//! MsgWriter — shared buffer + eventfd notification for cross-worker message delivery.
//!
//! Instead of sending structs through an mpsc channel, the upstream reader formats
//! MSG/HMSG wire bytes directly into this shared buffer. The worker thread is
//! notified via a shared eventfd to flush the buffer to TCP.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;

use crate::infra::nats_proto::MsgBuilder;
use crate::infra::types::HeaderMap;

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
    buf: Arc<Mutex<BytesMut>>,
    event_fd: Arc<OwnedFd>,
    has_pending: Arc<AtomicBool>,
    /// Pre-built MsgBuilder for formatting — kept per-writer to avoid allocation.
    msg_builder: Arc<Mutex<MsgBuilder>>,
}

impl MsgWriter {
    /// Create a MsgWriter with an externally-owned eventfd (shared by worker).
    pub(crate) fn new(
        buf: Arc<Mutex<BytesMut>>,
        has_pending: Arc<AtomicBool>,
        event_fd: Arc<OwnedFd>,
    ) -> Self {
        Self {
            buf,
            has_pending,
            event_fd,
            msg_builder: Arc::new(Mutex::new(MsgBuilder::new())),
        }
    }

    /// Create a standalone MsgWriter with its own eventfd (for tests/benchmarks).
    pub(crate) fn new_dummy() -> Self {
        let buf = Arc::new(Mutex::new(BytesMut::with_capacity(65536)));
        let has_pending = Arc::new(AtomicBool::new(false));
        let event_fd = Arc::new(create_eventfd());
        Self {
            buf,
            has_pending,
            event_fd,
            msg_builder: Arc::new(Mutex::new(MsgBuilder::new())),
        }
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
        let mut builder = self.msg_builder.lock().unwrap();
        let data = builder.build_msg(subject, sid_bytes, reply, headers, payload);
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Format and append an LMSG to the shared buffer (for leaf node delivery).
    #[cfg(any(feature = "leaf", feature = "hub"))]
    pub(crate) fn write_lmsg(
        &self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) {
        let mut builder = self.msg_builder.lock().unwrap();
        let data = builder.build_lmsg(subject, reply, headers, payload);
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Format and append an RMSG to the shared buffer (for route/gateway delivery).
    #[cfg(any(feature = "cluster", feature = "gateway"))]
    pub(crate) fn write_rmsg(
        &self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) {
        let mut builder = self.msg_builder.lock().unwrap();
        let data = builder.build_rmsg(
            subject,
            reply,
            headers,
            payload,
            #[cfg(feature = "accounts")]
            account,
        );
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Append raw protocol bytes to the shared buffer (e.g. LS+/LS-/RS+ lines).
    #[cfg(any(feature = "hub", feature = "cluster", feature = "gateway"))]
    pub(crate) fn write_raw(&self, data: &[u8]) {
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
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
    #[cfg(any(test, feature = "cluster", feature = "gateway"))]
    pub(crate) fn drain(&self) -> Option<BytesMut> {
        let mut buf = self.buf.lock().unwrap();
        if buf.is_empty() {
            None
        } else {
            Some(buf.split())
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
