//! Shared infrastructure for outbound connector managers (mesh routes, gateways).
//!
//! Both route and gateway connections use the same supervisor-with-backoff and
//! writer-thread-with-eventfd patterns. This module provides generic implementations
//! so each connector only needs to supply the protocol-specific connect function.

use std::io::{self, BufWriter, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, error};

use crate::buf::Backoff;
use crate::sub_list::MsgWriter;

/// Supervisor loop: repeatedly calls `connect_fn` with exponential backoff on failure.
///
/// Runs until `shutdown` is set. On successful return from `connect_fn` (connection
/// lost), resets backoff. On error, applies backoff delay before retrying.
pub(crate) fn run_supervisor<F>(label: &str, shutdown: &Arc<AtomicBool>, mut connect_fn: F)
where
    F: FnMut() -> Result<(), Box<dyn std::error::Error>>,
{
    let mut backoff = Backoff::new(Duration::from_millis(250), Duration::from_secs(30));

    loop {
        if shutdown.load(Ordering::Acquire) {
            debug!(peer = %label, "supervisor shutting down");
            return;
        }

        match connect_fn() {
            Ok(()) => {
                backoff.reset();
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                tracing::warn!(peer = %label, "connection lost, will reconnect");
            }
            Err(e) => {
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                tracing::warn!(peer = %label, error = %e, "connection failed");
            }
        }

        if shutdown.load(Ordering::Acquire) {
            return;
        }

        let delay = backoff.next_delay();
        debug!(peer = %label, delay_ms = delay.as_millis(), "reconnecting");

        let end = std::time::Instant::now() + delay;
        while std::time::Instant::now() < end {
            if shutdown.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// Writer loop: polls the MsgWriter's eventfd and flushes buffered data to TCP.
///
/// This is the basic writer without congestion signaling (used by gateways).
/// For routes with backpressure, use the extended version in `mesh::conn`.
pub(crate) fn run_writer_loop(tcp: TcpStream, dw: MsgWriter, shutdown: Arc<AtomicBool>) {
    let efd = dw.event_raw_fd();
    let mut pfds = [libc::pollfd {
        fd: efd,
        events: libc::POLLIN,
        revents: 0,
    }];
    let mut tcp_out = BufWriter::new(tcp);

    loop {
        if shutdown.load(Ordering::Acquire) {
            if let Some(data) = dw.drain() {
                let _ = tcp_out.write_all(&data);
                let _ = tcp_out.flush();
            }
            return;
        }

        pfds[0].revents = 0;
        let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 1, 500) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            error!(error = %err, "writer poll error");
            return;
        }

        if pfds[0].revents & libc::POLLIN != 0 {
            let mut val: u64 = 0;
            unsafe {
                libc::read(efd, &mut val as *mut u64 as *mut libc::c_void, 8);
            }
        }

        if let Some(data) = dw.drain() {
            if let Err(e) = tcp_out.write_all(&data) {
                debug!(error = %e, "writer TCP error");
                return;
            }
            if let Err(e) = tcp_out.flush() {
                debug!(error = %e, "writer flush error");
                return;
            }
        }
    }
}
