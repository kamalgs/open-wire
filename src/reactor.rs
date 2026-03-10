// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! I/O reactor abstraction over epoll and io_uring.
//!
//! Both backends expose readiness notifications using a common event format.
//! The worker event loop is generic over `Reactor`, so protocol logic is shared.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// Readiness flag: the fd is readable.
pub(crate) const READABLE: u32 = libc::POLLIN as u32;

/// Readiness flag: the fd is writable.
pub(crate) const WRITABLE: u32 = libc::POLLOUT as u32;

/// Readiness flag: the fd has an error condition.
pub(crate) const ERROR: u32 = libc::POLLERR as u32;

/// I/O event multiplexer used by the worker event loop.
///
/// `wait()` fills a caller-provided buffer with `(token, revents)` pairs where
/// `revents` uses the `READABLE` / `WRITABLE` / `ERROR` flags defined above.
pub(crate) trait Reactor {
    /// Start monitoring `fd` for read readiness, identified by `token`.
    fn register(&mut self, fd: RawFd, token: u64) -> io::Result<()>;

    /// Stop monitoring the fd previously registered with `token`.
    fn deregister(&mut self, fd: RawFd, token: u64);

    /// Update interest for `fd`: always monitors read, optionally write.
    fn modify(&mut self, fd: RawFd, token: u64, enable_out: bool);

    /// Block until at least one event is ready (or timeout expires).
    ///
    /// Returns the number of events written into `events`.
    /// `timeout_ms == -1` means wait indefinitely.
    fn wait(&mut self, events: &mut [(u64, u32)], timeout_ms: i32) -> io::Result<usize>;
}

// ---------------------------------------------------------------------------
// EpollReactor — always compiled
// ---------------------------------------------------------------------------

/// Reactor backed by Linux `epoll`.
pub(crate) struct EpollReactor {
    fd: OwnedFd,
}

impl EpollReactor {
    /// Create a new epoll instance.
    pub(crate) fn new() -> io::Result<Self> {
        let fd = unsafe { libc::epoll_create1(0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }
}

impl Reactor for EpollReactor {
    fn register(&mut self, fd: RawFd, token: u64) -> io::Result<()> {
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: token,
        };
        let ret = unsafe { libc::epoll_ctl(self.fd.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn deregister(&mut self, fd: RawFd, _token: u64) {
        unsafe {
            libc::epoll_ctl(
                self.fd.as_raw_fd(),
                libc::EPOLL_CTL_DEL,
                fd,
                std::ptr::null_mut(),
            );
        }
    }

    fn modify(&mut self, fd: RawFd, token: u64, enable_out: bool) {
        let events = if enable_out {
            libc::EPOLLIN as u32 | libc::EPOLLOUT as u32
        } else {
            libc::EPOLLIN as u32
        };
        let mut ev = libc::epoll_event { events, u64: token };
        unsafe {
            libc::epoll_ctl(self.fd.as_raw_fd(), libc::EPOLL_CTL_MOD, fd, &mut ev);
        }
    }

    fn wait(&mut self, events: &mut [(u64, u32)], timeout_ms: i32) -> io::Result<usize> {
        // Use a stack buffer of epoll_events, then translate.
        // 256 matches the worker's event batch size.
        let max = events.len().min(256);
        let mut raw = [libc::epoll_event { events: 0, u64: 0 }; 256];

        let n = unsafe {
            libc::epoll_wait(
                self.fd.as_raw_fd(),
                raw.as_mut_ptr(),
                max as i32,
                timeout_ms,
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let n = n as usize;
        for i in 0..n {
            // EPOLLIN/EPOLLOUT/EPOLLERR have the same values as POLLIN/POLLOUT/POLLERR
            events[i] = (raw[i].u64, raw[i].events);
        }
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// IoUringReactor — feature-gated
// ---------------------------------------------------------------------------

#[cfg(feature = "io-uring")]
mod uring {
    use super::*;
    use std::collections::HashMap;

    /// Sentinel user_data for timeout CQEs.
    const TIMEOUT_TOKEN: u64 = u64::MAX;

    /// Sentinel user_data for poll-remove / cancel CQEs.
    const CANCEL_TOKEN: u64 = u64::MAX - 1;

    /// Reactor backed by `io_uring` using `POLL_ADD` for readiness notification.
    ///
    /// This is a hybrid approach: io_uring notifies readiness, but actual I/O
    /// still uses regular `read`/`write`/`writev` syscalls. This gives an
    /// apples-to-apples comparison with the epoll backend.
    pub(crate) struct IoUringReactor {
        ring: io_uring::IoUring,
        /// token → (fd, current poll mask)
        polls: HashMap<u64, (RawFd, u32)>,
    }

    impl IoUringReactor {
        /// Create a new io_uring instance with 256 SQ entries.
        pub(crate) fn new() -> io::Result<Self> {
            let ring = io_uring::IoUring::new(256)?;
            Ok(Self {
                ring,
                polls: HashMap::new(),
            })
        }

        /// Submit a POLL_ADD SQE for the given fd/token/mask.
        fn submit_poll_add(&mut self, fd: RawFd, token: u64, mask: u32) {
            let entry = io_uring::opcode::PollAdd::new(io_uring::types::Fd(fd), mask)
                .build()
                .user_data(token);
            // SAFETY: the SQE is valid for the lifetime of the ring submission.
            unsafe {
                // If the SQ is full, flush pending submissions first.
                if self.ring.submission().push(&entry).is_err() {
                    let _ = self.ring.submit();
                    let _ = self.ring.submission().push(&entry);
                }
            }
        }

        /// Submit a POLL_REMOVE SQE to cancel a pending poll by token.
        fn submit_poll_remove(&mut self, token: u64) {
            let entry = io_uring::opcode::PollRemove::new(token)
                .build()
                .user_data(CANCEL_TOKEN);
            unsafe {
                if self.ring.submission().push(&entry).is_err() {
                    let _ = self.ring.submit();
                    let _ = self.ring.submission().push(&entry);
                }
            }
        }
    }

    impl Reactor for IoUringReactor {
        fn register(&mut self, fd: RawFd, token: u64) -> io::Result<()> {
            let mask = libc::POLLIN as u32;
            self.polls.insert(token, (fd, mask));
            self.submit_poll_add(fd, token, mask);
            Ok(())
        }

        fn deregister(&mut self, _fd: RawFd, token: u64) {
            if self.polls.remove(&token).is_some() {
                self.submit_poll_remove(token);
            }
        }

        fn modify(&mut self, fd: RawFd, token: u64, enable_out: bool) {
            let mask = if enable_out {
                libc::POLLIN as u32 | libc::POLLOUT as u32
            } else {
                libc::POLLIN as u32
            };
            self.polls.insert(token, (fd, mask));
            // Cancel the old poll and re-arm with the new mask.
            self.submit_poll_remove(token);
            self.submit_poll_add(fd, token, mask);
        }

        fn wait(&mut self, events: &mut [(u64, u32)], timeout_ms: i32) -> io::Result<usize> {
            // Submit a timeout SQE if the caller wants a bounded wait.
            if timeout_ms > 0 {
                let secs = (timeout_ms / 1000) as u64;
                let nsecs = ((timeout_ms % 1000) as u32) * 1_000_000;
                let ts = io_uring::types::Timespec::new().sec(secs).nsec(nsecs);
                let entry = io_uring::opcode::Timeout::new(&ts)
                    .build()
                    .user_data(TIMEOUT_TOKEN);
                unsafe {
                    if self.ring.submission().push(&entry).is_err() {
                        let _ = self.ring.submit();
                        let _ = self.ring.submission().push(&entry);
                    }
                }
            }

            // Submit pending SQEs and wait for at least 1 CQE.
            if timeout_ms == 0 {
                // Non-blocking: just submit and drain whatever is ready.
                self.ring.submit()?;
            } else {
                self.ring.submit_and_wait(1)?;
            }

            // Drain CQEs into a temp buffer first (to release the borrow on
            // self.ring before we re-arm polls).
            let mut count = 0;
            let max = events.len();
            // Stack buffer for tokens that need re-arming.
            let mut rearm: [(u64, RawFd, u32); 256] = [(0, 0, 0); 256];
            let mut rearm_count = 0;

            for cqe in self.ring.completion() {
                let ud = cqe.user_data();

                // Skip sentinel tokens (timeout, cancel).
                if ud == TIMEOUT_TOKEN || ud == CANCEL_TOKEN {
                    continue;
                }

                // Skip stale CQEs for deregistered tokens.
                let (fd, mask) = match self.polls.get(&ud) {
                    Some(entry) => *entry,
                    None => continue,
                };

                let result = cqe.result();
                let revents = if result < 0 {
                    // -ECANCELED from modify's remove → re-arm is already queued.
                    if result == -libc::ECANCELED {
                        continue;
                    }
                    libc::POLLERR as u32
                } else {
                    result as u32
                };

                if count < max {
                    events[count] = (ud, revents);
                    count += 1;
                }

                // Record for re-arm after the CQE drain loop.
                if rearm_count < rearm.len() {
                    rearm[rearm_count] = (ud, fd, mask);
                    rearm_count += 1;
                }
            }

            // Re-arm: oneshot POLL_ADD needs to be resubmitted after each event.
            for &(token, fd, mask) in &rearm[..rearm_count] {
                self.submit_poll_add(fd, token, mask);
            }

            Ok(count)
        }
    }
}

#[cfg(feature = "io-uring")]
pub(crate) use uring::IoUringReactor;
