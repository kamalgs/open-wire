// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::{HashMap, HashSet};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};

use crate::types::HeaderMap;

use crate::nats_proto::MsgBuilder;

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
/// Multiple DirectWriters on the same worker share one eventfd, so fan-out
/// to N connections on one worker costs only 1 eventfd write.
#[derive(Clone)]
pub(crate) struct DirectWriter {
    buf: Arc<Mutex<BytesMut>>,
    event_fd: Arc<OwnedFd>,
    has_pending: Arc<AtomicBool>,
    /// Pre-built MsgBuilder for formatting — kept per-writer to avoid allocation.
    msg_builder: Arc<Mutex<MsgBuilder>>,
}

impl DirectWriter {
    /// Create a DirectWriter with an externally-owned eventfd (shared by worker).
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

    /// Create a standalone DirectWriter with its own eventfd (for tests/benchmarks).
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

    /// Format and append an RMSG to the shared buffer (for route delivery).
    #[cfg(feature = "cluster")]
    pub(crate) fn write_rmsg(
        &self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) {
        let mut builder = self.msg_builder.lock().unwrap();
        let data = builder.build_rmsg(subject, reply, headers, payload);
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Append raw protocol bytes to the shared buffer (e.g. LS+/LS- lines).
    #[cfg(any(feature = "hub", feature = "cluster"))]
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
    #[cfg(any(test, feature = "cluster"))]
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

impl std::fmt::Debug for DirectWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectWriter").finish()
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Subscription {
    pub conn_id: u64,
    pub sid: u64,
    /// Pre-computed ASCII bytes of the SID (e.g. `b"42"`), cloned (atomic inc)
    /// at delivery time instead of heap-allocating via `sid_to_bytes()` per message.
    pub sid_bytes: Bytes,
    pub subject: String,
    pub queue: Option<String>,
    /// Direct writer to the client's shared write buffer.
    /// Formats MSG bytes synchronously, bypassing mpsc channel overhead.
    pub(crate) writer: DirectWriter,
    /// Maximum messages to deliver before auto-unsubscribe. 0 = no limit.
    /// Atomic because `for_each_match` holds a read lock while multiple workers deliver.
    pub(crate) max_msgs: AtomicU64,
    /// Number of messages delivered so far (only incremented when max_msgs > 0).
    pub(crate) delivered: AtomicU64,
    /// True for inbound leaf node subscriptions (deliver via LMSG, not MSG).
    pub is_leaf: bool,
    /// True for route peer subscriptions (deliver via RMSG, not MSG).
    #[cfg(feature = "cluster")]
    pub is_route: bool,
}

impl Clone for Subscription {
    fn clone(&self) -> Self {
        Self {
            conn_id: self.conn_id,
            sid: self.sid,
            sid_bytes: self.sid_bytes.clone(),
            subject: self.subject.clone(),
            queue: self.queue.clone(),
            writer: self.writer.clone(),
            max_msgs: AtomicU64::new(self.max_msgs.load(Ordering::Relaxed)),
            delivered: AtomicU64::new(self.delivered.load(Ordering::Relaxed)),
            is_leaf: self.is_leaf,
            #[cfg(feature = "cluster")]
            is_route: self.is_route,
        }
    }
}

impl Subscription {
    /// Create a subscription without a real DirectWriter (for benchmarks/tests).
    /// Messages written to this subscription's writer are discarded.
    pub fn new_dummy(conn_id: u64, sid: u64, subject: String, queue: Option<String>) -> Self {
        let writer = DirectWriter::new_dummy();
        Self {
            conn_id,
            sid,
            sid_bytes: Bytes::from(sid.to_string().into_bytes()),
            subject,
            queue,
            writer,
            max_msgs: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            is_leaf: false,
            #[cfg(feature = "cluster")]
            is_route: false,
        }
    }
}

/// Returns true if the subject pattern contains wildcard characters (`*` or `>`).
#[inline]
fn is_wildcard(subject: &str) -> bool {
    memchr::memchr2(b'*', b'>', subject.as_bytes()).is_some()
}

/// Subscription list supporting NATS wildcard matching.
///
/// Splits subscriptions into exact (HashMap) and wildcard (Vec) for fast
/// lookups: exact subjects get O(1) HashMap lookup, only wildcard patterns
/// require linear scanning.
#[derive(Debug, Default)]
pub struct SubList {
    /// Exact (non-wildcard) subscriptions indexed by subject.
    exact: HashMap<String, Vec<Subscription>>,
    /// Wildcard subscriptions (patterns with `*` or `>`).
    wild: Vec<Subscription>,
    /// Round-robin counter for queue group delivery.
    queue_counter: AtomicUsize,
}

impl SubList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, sub: Subscription) {
        if is_wildcard(&sub.subject) {
            self.wild.push(sub);
        } else {
            self.exact.entry(sub.subject.clone()).or_default().push(sub);
        }
    }

    pub fn remove(&mut self, conn_id: u64, sid: u64) -> Option<Subscription> {
        // Try wildcard list first
        if let Some(pos) = self
            .wild
            .iter()
            .position(|s| s.conn_id == conn_id && s.sid == sid)
        {
            return Some(self.wild.swap_remove(pos));
        }
        // Search exact map
        for subs in self.exact.values_mut() {
            if let Some(pos) = subs
                .iter()
                .position(|s| s.conn_id == conn_id && s.sid == sid)
            {
                let removed = subs.swap_remove(pos);
                return Some(removed);
            }
        }
        None
    }

    pub fn remove_conn(&mut self, conn_id: u64) -> Vec<Subscription> {
        let mut removed = Vec::new();

        // Remove from wildcard list
        let mut i = 0;
        while i < self.wild.len() {
            if self.wild[i].conn_id == conn_id {
                removed.push(self.wild.swap_remove(i));
            } else {
                i += 1;
            }
        }

        // Remove from exact map
        self.exact.retain(|_, subs| {
            let mut i = 0;
            while i < subs.len() {
                if subs[i].conn_id == conn_id {
                    removed.push(subs.swap_remove(i));
                } else {
                    i += 1;
                }
            }
            !subs.is_empty()
        });

        removed
    }

    pub fn match_subject(&self, subject: &str) -> Vec<&Subscription> {
        let mut result = Vec::new();
        // O(1) exact lookup
        if let Some(subs) = self.exact.get(subject) {
            result.extend(subs.iter());
        }
        // Linear scan of wildcard patterns only
        for sub in &self.wild {
            if subject_matches(&sub.subject, subject) {
                result.push(sub);
            }
        }
        result
    }

    /// Set the auto-unsubscribe max on an existing subscription.
    /// Works under a read lock (uses atomics). Returns `true` if the sub was found.
    /// If the sub has already delivered >= max messages, returns `true` but caller
    /// should remove it under a write lock.
    pub fn set_unsub_max(&self, conn_id: u64, sid: u64, max: u64) -> bool {
        // Check exact subs
        for subs in self.exact.values() {
            for sub in subs {
                if sub.conn_id == conn_id && sub.sid == sid {
                    sub.max_msgs.store(max, Ordering::Relaxed);
                    return true;
                }
            }
        }
        // Check wildcard subs
        for sub in &self.wild {
            if sub.conn_id == conn_id && sub.sid == sid {
                sub.max_msgs.store(max, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Check if a subscription has already reached its delivery limit.
    pub fn is_expired(&self, conn_id: u64, sid: u64) -> bool {
        let find = |sub: &Subscription| -> bool {
            let max = sub.max_msgs.load(Ordering::Relaxed);
            max > 0 && sub.delivered.load(Ordering::Relaxed) >= max
        };
        for subs in self.exact.values() {
            for sub in subs {
                if sub.conn_id == conn_id && sub.sid == sid {
                    return find(sub);
                }
            }
        }
        for sub in &self.wild {
            if sub.conn_id == conn_id && sub.sid == sid {
                return find(sub);
            }
        }
        false
    }

    /// Iterate over matching subscriptions without allocating a Vec.
    /// Returns `(match_count, expired_subs)` where expired_subs is a list of
    /// `(conn_id, sid)` pairs for subscriptions that reached their max delivery limit.
    ///
    /// Queue group semantics: non-queue subs receive every message,
    /// while each queue group delivers to exactly one member (round-robin).
    pub fn for_each_match(
        &self,
        subject: &str,
        mut f: impl FnMut(&Subscription),
    ) -> (usize, Vec<(u64, u64)>) {
        let mut count = 0;
        let mut expired: Vec<(u64, u64)> = Vec::new();
        // Collect queue group subs lazily — only allocate when needed.
        // Uses indices to avoid lifetime issues with the closure.
        // (source, index): source 0 = exact, source 1 = wild
        let mut queue_groups: Vec<(&str, Vec<(u8, usize)>)> = Vec::new();

        // Check max_msgs limit, deliver if allowed, track expired.
        macro_rules! deliver_sub {
            ($sub:expr) => {{
                let max = $sub.max_msgs.load(Ordering::Relaxed);
                if max > 0 {
                    let prev = $sub.delivered.fetch_add(1, Ordering::Relaxed);
                    if prev >= max {
                        // Already over limit — undo increment, skip delivery
                        $sub.delivered.fetch_sub(1, Ordering::Relaxed);
                    } else {
                        if prev + 1 >= max {
                            expired.push(($sub.conn_id, $sub.sid));
                        }
                        f($sub);
                        count += 1;
                    }
                } else {
                    f($sub);
                    count += 1;
                }
            }};
        }

        // Route a single matching sub: deliver non-queue immediately,
        // collect queue subs by group name.
        macro_rules! route_sub {
            ($sub:expr, $source:expr, $idx:expr) => {
                if let Some(ref q) = $sub.queue {
                    if let Some(group) = queue_groups
                        .iter_mut()
                        .find(|(name, _)| *name == q.as_str())
                    {
                        group.1.push(($source, $idx));
                    } else {
                        queue_groups.push((q.as_str(), vec![($source, $idx)]));
                    }
                } else {
                    deliver_sub!($sub);
                }
            };
        }

        // O(1) exact lookup
        if let Some(subs) = self.exact.get(subject) {
            for (i, sub) in subs.iter().enumerate() {
                route_sub!(sub, 0u8, i);
            }
        }
        // Linear scan of wildcard patterns only
        for (i, sub) in self.wild.iter().enumerate() {
            if subject_matches(&sub.subject, subject) {
                route_sub!(sub, 1u8, i);
            }
        }

        // Deliver to exactly one member per queue group (round-robin)
        if !queue_groups.is_empty() {
            let rr = self.queue_counter.fetch_add(1, Ordering::Relaxed);
            let exact_subs = self.exact.get(subject);
            for (_name, members) in &queue_groups {
                let idx = rr % members.len();
                let (source, sub_idx) = members[idx];
                let sub = if source == 0 {
                    &exact_subs.unwrap()[sub_idx]
                } else {
                    &self.wild[sub_idx]
                };
                deliver_sub!(sub);
            }
        }

        (count, expired)
    }

    /// Returns true if the sublist has no subscriptions at all.
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.wild.is_empty()
    }

    #[allow(dead_code)]
    pub fn unique_subjects(&self) -> HashSet<&str> {
        let mut subjects: HashSet<&str> = self.exact.keys().map(|s| s.as_str()).collect();
        for sub in &self.wild {
            subjects.insert(&sub.subject);
        }
        subjects
    }

    /// Returns unique non-leaf, non-route (subject, queue) pairs for leaf interest propagation.
    /// Only includes subscriptions from client connections (not leaf or route connections).
    #[cfg(feature = "hub")]
    pub fn client_interests(&self) -> Vec<(&str, Option<&str>)> {
        let mut set: HashSet<(&str, Option<&str>)> = HashSet::new();
        for (subj, subs) in &self.exact {
            for sub in subs {
                if !sub.is_leaf {
                    #[cfg(feature = "cluster")]
                    if sub.is_route {
                        continue;
                    }
                    set.insert((subj.as_str(), sub.queue.as_deref()));
                }
            }
        }
        for sub in &self.wild {
            if !sub.is_leaf {
                #[cfg(feature = "cluster")]
                if sub.is_route {
                    continue;
                }
                set.insert((sub.subject.as_str(), sub.queue.as_deref()));
            }
        }
        set.into_iter().collect()
    }

    /// Returns unique local (client + leaf) (subject, queue) pairs for route interest propagation.
    /// Excludes route subscriptions (avoids loops).
    #[cfg(feature = "cluster")]
    pub fn local_interests(&self) -> Vec<(&str, Option<&str>)> {
        let mut set: HashSet<(&str, Option<&str>)> = HashSet::new();
        for (subj, subs) in &self.exact {
            for sub in subs {
                if !sub.is_route {
                    set.insert((subj.as_str(), sub.queue.as_deref()));
                }
            }
        }
        for sub in &self.wild {
            if !sub.is_route {
                set.insert((sub.subject.as_str(), sub.queue.as_deref()));
            }
        }
        set.into_iter().collect()
    }

    /// Returns unique (subject, queue) pairs for upstream interest sync.
    /// Non-queue subs have `queue = None`. Queue subs are deduplicated
    /// by (subject, queue_name) so each group is announced once.
    pub fn unique_interests(&self) -> Vec<(&str, Option<&str>)> {
        let mut set: HashSet<(&str, Option<&str>)> = HashSet::new();
        for (subj, subs) in &self.exact {
            for sub in subs {
                set.insert((subj.as_str(), sub.queue.as_deref()));
            }
        }
        for sub in &self.wild {
            set.insert((sub.subject.as_str(), sub.queue.as_deref()));
        }
        set.into_iter().collect()
    }
}

/// NATS wildcard matching.
/// `*` matches a single token, `>` matches one or more tokens (tail match).
///
/// Uses byte-level dot scanning instead of `str::split('.')` to avoid
/// the `SplitInternal` iterator machinery (which was ~6% of CPU in profiles).
pub fn subject_matches(pattern: &str, subject: &str) -> bool {
    subject_matches_bytes(pattern.as_bytes(), subject.as_bytes())
}

#[inline]
fn subject_matches_bytes(pattern: &[u8], subject: &[u8]) -> bool {
    let mut pp = 0; // pattern position
    let mut sp = 0; // subject position

    loop {
        if pp >= pattern.len() {
            return sp >= subject.len();
        }
        if sp >= subject.len() {
            return false;
        }

        // Find end of current pattern token
        let pe = match memchr::memchr(b'.', &pattern[pp..]) {
            Some(i) => pp + i,
            None => pattern.len(),
        };

        // Check for ">"
        if pe - pp == 1 && pattern[pp] == b'>' {
            return true;
        }

        // Find end of current subject token
        let se = match memchr::memchr(b'.', &subject[sp..]) {
            Some(i) => sp + i,
            None => subject.len(),
        };

        // Check match: "*" matches any single token, otherwise exact
        let is_star = pe - pp == 1 && pattern[pp] == b'*';
        if !is_star && pattern[pp..pe] != subject[sp..se] {
            return false;
        }

        // Advance past token + dot
        pp = if pe < pattern.len() { pe + 1 } else { pe };
        sp = if se < subject.len() { se + 1 } else { se };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test subscription with a dummy DirectWriter.
    fn test_sub(conn_id: u64, sid: u64, subject: &str) -> Subscription {
        Subscription::new_dummy(conn_id, sid, subject.to_string(), None)
    }

    #[test]
    fn test_exact_match() {
        assert!(subject_matches("foo.bar.baz", "foo.bar.baz"));
        assert!(!subject_matches("foo.bar.baz", "foo.bar.qux"));
        assert!(!subject_matches("foo.bar", "foo.bar.baz"));
        assert!(!subject_matches("foo.bar.baz", "foo.bar"));
    }

    #[test]
    fn test_star_wildcard() {
        assert!(subject_matches("foo.*", "foo.bar"));
        assert!(subject_matches("foo.*.baz", "foo.bar.baz"));
        assert!(!subject_matches("foo.*", "foo.bar.baz"));
        assert!(subject_matches("*.*.*", "a.b.c"));
        assert!(!subject_matches("*.*", "a.b.c"));
    }

    #[test]
    fn test_gt_wildcard() {
        assert!(subject_matches("foo.>", "foo.bar"));
        assert!(subject_matches("foo.>", "foo.bar.baz"));
        assert!(subject_matches("foo.>", "foo.bar.baz.qux"));
        assert!(!subject_matches("foo.>", "bar.baz"));
        assert!(subject_matches(">", "anything.at.all"));
        assert!(subject_matches(">", "single"));
    }

    #[test]
    fn test_combined_wildcards() {
        assert!(subject_matches("foo.*.>", "foo.bar.baz"));
        assert!(subject_matches("foo.*.>", "foo.bar.baz.qux"));
        assert!(!subject_matches("foo.*.>", "foo.bar"));
        // `>` requires at least one token
    }

    #[test]
    fn test_sublist_insert_match() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.bar"));
        sl.insert(test_sub(1, 2, "foo.*"));
        sl.insert(test_sub(2, 1, "baz.>"));

        let matches = sl.match_subject("foo.bar");
        assert_eq!(matches.len(), 2);

        let matches = sl.match_subject("foo.qux");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].subject, "foo.*");

        let matches = sl.match_subject("baz.one.two");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].subject, "baz.>");
    }

    #[test]
    fn test_sublist_remove() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_sub(1, 2, "bar"));

        let removed = sl.remove(1, 1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().subject, "foo");

        let matches = sl.match_subject("foo");
        assert!(matches.is_empty());
    }

    #[test]
    fn test_sublist_remove_conn() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_sub(1, 2, "bar"));
        sl.insert(test_sub(2, 1, "foo"));

        let removed = sl.remove_conn(1);
        assert_eq!(removed.len(), 2);

        let matches = sl.match_subject("foo");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].conn_id, 2);
    }

    #[test]
    fn test_unique_subjects() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_sub(2, 1, "foo"));
        sl.insert(test_sub(1, 2, "bar"));

        let subjects = sl.unique_subjects();
        assert_eq!(subjects.len(), 2);
        assert!(subjects.contains("foo"));
        assert!(subjects.contains("bar"));
    }

    #[test]
    fn test_exact_vs_wildcard_split() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.bar"));
        sl.insert(test_sub(1, 2, "foo.*"));
        sl.insert(test_sub(1, 3, "baz.>"));
        sl.insert(test_sub(2, 1, "foo.bar"));

        // "foo.bar" should match 2 exact + 1 wildcard
        let matches = sl.match_subject("foo.bar");
        assert_eq!(matches.len(), 3);

        // "foo.qux" should match only the wildcard "foo.*"
        let matches = sl.match_subject("foo.qux");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].subject, "foo.*");

        // is_empty
        assert!(!sl.is_empty());
        let empty = SubList::new();
        assert!(empty.is_empty());
    }

    // --- DirectWriter tests ---

    #[test]
    #[cfg(any(feature = "leaf", feature = "hub"))]
    fn test_direct_writer_formats_lmsg() {
        let writer = DirectWriter::new_dummy();

        writer.write_lmsg(b"test.sub", None, None, b"hello");

        let data = writer.drain().expect("should have data");
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "LMSG test.sub 5\r\nhello\r\n");
    }

    #[test]
    #[cfg(any(feature = "leaf", feature = "hub"))]
    fn test_direct_writer_formats_lmsg_with_reply() {
        let writer = DirectWriter::new_dummy();

        writer.write_lmsg(b"test.sub", Some(b"reply.to"), None, b"hi");

        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "LMSG test.sub reply.to 2\r\nhi\r\n");
    }

    #[test]
    fn test_direct_writer_formats_msg() {
        let writer = DirectWriter::new_dummy();

        writer.write_msg(b"test.sub", b"1", None, None, b"hello");

        let data = writer.drain().expect("should have data");
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG test.sub 1 5\r\nhello\r\n");
    }

    #[test]
    fn test_direct_writer_formats_msg_with_reply() {
        let writer = DirectWriter::new_dummy();

        writer.write_msg(b"test.sub", b"42", Some(b"reply.to"), None, b"hi");

        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG test.sub 42 reply.to 2\r\nhi\r\n");
    }

    #[test]
    fn test_direct_writer_formats_hmsg_with_headers() {
        let writer = DirectWriter::new_dummy();

        let mut headers = HeaderMap::new();
        headers.insert("X-Key", "val".into());

        writer.write_msg(b"test.sub", b"1", None, Some(&headers), b"data");

        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.starts_with("HMSG test.sub 1 "));
        assert!(s.contains("X-Key: val"));
        assert!(s.ends_with("data\r\n"));
    }

    #[test]
    fn test_direct_writer_batches_multiple_writes() {
        let writer = DirectWriter::new_dummy();

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
    fn test_direct_writer_drain_empty() {
        let writer = DirectWriter::new_dummy();
        assert!(writer.drain().is_none());
    }

    #[test]
    fn test_direct_writer_drain_resets_buffer() {
        let writer = DirectWriter::new_dummy();

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
    fn test_direct_writer_clone_shares_buffer() {
        let writer1 = DirectWriter::new_dummy();
        let writer2 = writer1.clone();

        writer1.write_msg(b"a", b"1", None, None, b"x");
        writer2.write_msg(b"b", b"2", None, None, b"y");

        let data = writer1.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        // Both messages should be in the same buffer
        assert!(s.contains("MSG a 1 1\r\nx\r\n"));
        assert!(s.contains("MSG b 2 1\r\ny\r\n"));
    }

    #[test]
    fn test_direct_writer_notify_wakes() {
        let writer = DirectWriter::new_dummy();
        let writer2 = writer.clone();

        // Spawn a thread that writes and notifies after a short delay
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            writer2.write_msg(b"test", b"1", None, None, b"hello");
            writer2.notify();
        });

        // Wait for notification using poll
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
    fn test_direct_writer_notify_stores_permit() {
        let writer = DirectWriter::new_dummy();

        // Notify BEFORE waiting — the eventfd counter should be stored
        writer.write_msg(b"test", b"1", None, None, b"early");
        writer.notify();

        // Small delay to ensure eventfd is written
        std::thread::sleep(std::time::Duration::from_millis(10));

        // poll should return immediately because eventfd is ready
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

    /// Simulate the producer/consumer pattern used in the server:
    /// a fast producer writes many messages, consumer must receive all of them.
    /// Uses the correct drain-before-wait pattern that avoids the lost-notify race.
    #[test]
    fn test_direct_writer_fast_producer_slow_consumer() {
        let writer = DirectWriter::new_dummy();
        let producer_writer = writer.clone();
        let total_msgs = 10_000;

        // Producer: write all messages rapidly (all at once, simulating burst)
        let producer = std::thread::spawn(move || {
            for i in 0..total_msgs {
                let payload = format!("msg{i}");
                producer_writer.write_msg(b"test", b"1", None, None, payload.as_bytes());
                producer_writer.notify();
            }
        });

        // Consumer: drain-before-wait pattern (matches worker event loop)
        let mut total_msgs_seen = 0usize;
        let mut pfd = [libc::pollfd {
            fd: writer.event_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        loop {
            // Always check buffer first before blocking
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

    /// Test the race where producer finishes before consumer starts draining.
    /// This catches the lost-notify bug: all notify() calls collapse into one
    /// eventfd counter, consumer must drain without relying on future notifications.
    #[test]
    fn test_direct_writer_producer_finishes_before_consumer() {
        let writer = DirectWriter::new_dummy();
        let total_msgs = 1_000;

        // Producer writes everything synchronously (no yield points)
        for i in 0..total_msgs {
            let payload = format!("m{i}");
            writer.write_msg(b"x", b"1", None, None, payload.as_bytes());
        }
        writer.notify(); // single notify at end

        // Consumer: must get all messages even though notify was called once
        // The drain-before-wait pattern handles this.
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
            // Use timeout to detect hang (would fail without drain-before-wait)
            let ret = unsafe { libc::poll(pfd.as_mut_ptr(), 1, 1000) };
            if ret > 0 {
                writer.consume_notify();
            } else {
                panic!("consumer hung! only received {total_msgs_seen}/{total_msgs} messages");
            }
        }
        assert_eq!(total_msgs_seen, total_msgs);
    }

    // --- Queue group tests ---

    fn test_queue_sub(conn_id: u64, sid: u64, subject: &str, queue: &str) -> Subscription {
        Subscription::new_dummy(conn_id, sid, subject.to_string(), Some(queue.to_string()))
    }

    #[test]
    fn test_queue_group_delivers_to_one() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));
        sl.insert(test_queue_sub(3, 1, "foo", "q1"));

        let mut delivered = Vec::new();
        let (count, _expired) = sl.for_each_match("foo", |sub| {
            delivered.push(sub.conn_id);
        });
        assert_eq!(count, 1);
        assert_eq!(delivered.len(), 1);
    }

    #[test]
    fn test_queue_group_round_robin() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));

        let mut counts = [0u32; 3]; // conn_id 1 and 2
        for _ in 0..100 {
            let _ = sl.for_each_match("foo", |sub| {
                counts[sub.conn_id as usize] += 1;
            });
        }
        // Both should get roughly half
        assert!(counts[1] > 0);
        assert!(counts[2] > 0);
        assert_eq!(counts[1] + counts[2], 100);
    }

    #[test]
    fn test_queue_and_non_queue_mix() {
        let mut sl = SubList::new();
        // 2 queue subs in group "q1"
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));
        // 1 non-queue sub
        sl.insert(test_sub(3, 1, "foo"));

        let mut delivered = Vec::new();
        let (count, _) = sl.for_each_match("foo", |sub| {
            delivered.push(sub.conn_id);
        });
        // Non-queue sub always delivered + 1 from queue group
        assert_eq!(count, 2);
        assert!(delivered.contains(&3)); // non-queue always present
    }

    #[test]
    fn test_multiple_queue_groups() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));
        sl.insert(test_queue_sub(3, 1, "foo", "q2"));
        sl.insert(test_queue_sub(4, 1, "foo", "q2"));

        let mut delivered = Vec::new();
        let (count, _) = sl.for_each_match("foo", |sub| {
            delivered.push(sub.conn_id);
        });
        // 1 from q1 + 1 from q2
        assert_eq!(count, 2);
        assert_eq!(delivered.len(), 2);
    }

    #[test]
    fn test_queue_group_with_wildcard() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo.*", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo.*", "q1"));

        let mut delivered = Vec::new();
        let (count, _) = sl.for_each_match("foo.bar", |sub| {
            delivered.push(sub.conn_id);
        });
        assert_eq!(count, 1);
    }

    #[test]
    fn test_unique_interests() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_sub(2, 2, "foo"));
        sl.insert(test_queue_sub(3, 1, "bar", "q1"));
        sl.insert(test_queue_sub(4, 2, "bar", "q1"));
        sl.insert(test_queue_sub(5, 3, "bar", "q2"));

        let interests = sl.unique_interests();
        // foo (no queue) + bar/q1 + bar/q2 = 3
        assert_eq!(interests.len(), 3);
        assert!(interests.contains(&("foo", None)));
        assert!(interests.contains(&("bar", Some("q1"))));
        assert!(interests.contains(&("bar", Some("q2"))));
    }

    // --- UNSUB max tests ---

    #[test]
    fn test_unsub_max_delivery_limit() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.set_unsub_max(1, 1, 3);

        // Deliver 3 messages — all should succeed
        for _ in 0..3 {
            let (count, _) = sl.for_each_match("foo", |_sub| {});
            assert_eq!(count, 1);
        }

        // 4th message should be skipped and sub expired
        let (count, expired) = sl.for_each_match("foo", |_sub| {});
        assert_eq!(count, 0);
        assert!(expired.is_empty()); // already expired on 3rd delivery
    }

    #[test]
    fn test_unsub_max_single_shot() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.set_unsub_max(1, 1, 1);

        let (count, expired) = sl.for_each_match("foo", |_sub| {});
        assert_eq!(count, 1);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], (1, 1));

        // Next delivery should skip
        let (count, _) = sl.for_each_match("foo", |_sub| {});
        assert_eq!(count, 0);
    }

    #[test]
    fn test_unsub_max_with_queue_group() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));
        sl.set_unsub_max(1, 1, 2);

        // Deliver enough messages that conn_id=1 gets 2
        let mut conn1_count = 0u32;
        for _ in 0..100 {
            let (_, expired) = sl.for_each_match("foo", |sub| {
                if sub.conn_id == 1 {
                    conn1_count += 1;
                }
            });
            // Remove expired
            for (cid, sid) in expired {
                sl.remove(cid, sid);
            }
        }
        // conn_id=1 should have received exactly 2
        assert_eq!(conn1_count, 2);
    }

    #[test]
    fn test_unsub_max_already_expired() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));

        // Set max=3, then deliver 3 messages to reach the limit
        sl.set_unsub_max(1, 1, 3);
        for _ in 0..3 {
            let _ = sl.for_each_match("foo", |_sub| {});
        }

        // Should now be expired
        assert!(sl.is_expired(1, 1));

        // Increasing the max should un-expire it
        sl.set_unsub_max(1, 1, 5);
        assert!(!sl.is_expired(1, 1));
    }

    #[test]
    #[cfg(feature = "hub")]
    fn test_client_interests_excludes_leaf_subs() {
        let mut sl = SubList::new();
        // Client subs
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_queue_sub(2, 1, "bar", "q1"));

        // Leaf sub — should be excluded from client_interests
        let mut leaf_sub = Subscription::new_dummy(10, 1, "foo".to_string(), None);
        leaf_sub.is_leaf = true;
        sl.insert(leaf_sub);

        let mut leaf_queue_sub =
            Subscription::new_dummy(11, 1, "bar".to_string(), Some("q1".to_string()));
        leaf_queue_sub.is_leaf = true;
        sl.insert(leaf_queue_sub);

        let interests = sl.client_interests();
        // Should only have client interests: ("foo", None) and ("bar", Some("q1"))
        assert_eq!(interests.len(), 2);
        assert!(interests.contains(&("foo", None)));
        assert!(interests.contains(&("bar", Some("q1"))));

        // Verify leaf subs are still in the list for matching
        let matches = sl.match_subject("foo");
        assert_eq!(matches.len(), 2); // client + leaf
    }

    #[test]
    #[cfg(feature = "hub")]
    fn test_direct_writer_write_raw() {
        let writer = DirectWriter::new_dummy();
        // No data yet
        assert!(writer.drain().is_none());

        writer.write_raw(b"LS+ foo\r\n");
        let data = writer.drain().unwrap();
        assert_eq!(&data[..], b"LS+ foo\r\n");

        // Multiple raw writes accumulate
        writer.write_raw(b"LS+ bar\r\n");
        writer.write_raw(b"LS- baz\r\n");
        let data = writer.drain().unwrap();
        assert_eq!(&data[..], b"LS+ bar\r\nLS- baz\r\n");
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn test_direct_writer_formats_rmsg() {
        let writer = DirectWriter::new_dummy();
        writer.write_rmsg(b"test.sub", None, None, b"hello");
        let data = writer.drain().expect("should have data");
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "RMSG $G test.sub 5\r\nhello\r\n");
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn test_direct_writer_formats_rmsg_with_reply() {
        let writer = DirectWriter::new_dummy();
        writer.write_rmsg(b"test.sub", Some(b"reply.to"), None, b"hi");
        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "RMSG $G test.sub reply.to 2\r\nhi\r\n");
    }

    #[test]
    fn test_set_unsub_max_wildcard() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.*"));
        assert!(sl.set_unsub_max(1, 1, 2));

        let (count, _) = sl.for_each_match("foo.bar", |_sub| {});
        assert_eq!(count, 1);
        let (count, expired) = sl.for_each_match("foo.baz", |_sub| {});
        assert_eq!(count, 1);
        assert_eq!(expired.len(), 1);
    }
}
