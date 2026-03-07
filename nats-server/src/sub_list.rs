// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tokio::sync::Notify;

use async_nats::header::HeaderMap;

use crate::nats_proto::MsgBuilder;

/// A shared write buffer + notify pair for zero-channel message delivery.
///
/// Instead of sending `ClientMsg` structs through an mpsc channel (which costs
/// atomic ops + linked-list push + task wake per message), the upstream reader
/// formats MSG/HMSG wire bytes directly into this shared buffer. The client
/// writer task is notified once per batch to flush the buffer to TCP.
#[derive(Clone)]
pub(crate) struct DirectWriter {
    buf: Arc<Mutex<BytesMut>>,
    notify: Arc<Notify>,
    /// Pre-built MsgBuilder for formatting — kept per-writer to avoid allocation.
    msg_builder: Arc<Mutex<MsgBuilder>>,
}

impl DirectWriter {
    pub(crate) fn new() -> (Self, WriterHandle) {
        let buf = Arc::new(Mutex::new(BytesMut::with_capacity(65536)));
        let notify = Arc::new(Notify::new());
        let writer = Self {
            buf: Arc::clone(&buf),
            notify: Arc::clone(&notify),
            msg_builder: Arc::new(Mutex::new(MsgBuilder::new())),
        };
        let handle = WriterHandle { buf, notify };
        (writer, handle)
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
    }

    /// Notify the client writer task that there is data to flush.
    pub(crate) fn notify(&self) {
        self.notify.notify_one();
    }
}

impl std::fmt::Debug for DirectWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectWriter").finish()
    }
}

/// Handle held by the client writer task to drain the shared buffer.
pub(crate) struct WriterHandle {
    buf: Arc<Mutex<BytesMut>>,
    notify: Arc<Notify>,
}

impl WriterHandle {
    /// Wait until notified that data is available.
    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }

    /// Drain all buffered data. Returns `None` if buffer was empty.
    pub(crate) fn drain(&self) -> Option<BytesMut> {
        let mut buf = self.buf.lock().unwrap();
        if buf.is_empty() {
            None
        } else {
            Some(buf.split())
        }
    }
}

#[derive(Debug, Clone)]
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
}

impl SubList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, sub: Subscription) {
        if is_wildcard(&sub.subject) {
            self.wild.push(sub);
        } else {
            self.exact
                .entry(sub.subject.clone())
                .or_default()
                .push(sub);
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

    /// Iterate over matching subscriptions without allocating a Vec.
    /// Returns the total number of matches.
    pub fn for_each_match(&self, subject: &str, mut f: impl FnMut(&Subscription)) -> usize {
        let mut count = 0;
        // O(1) exact lookup
        if let Some(subs) = self.exact.get(subject) {
            for sub in subs {
                f(sub);
                count += 1;
            }
        }
        // Linear scan of wildcard patterns only
        for sub in &self.wild {
            if subject_matches(&sub.subject, subject) {
                f(sub);
                count += 1;
            }
        }
        count
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
        let (writer, _handle) = DirectWriter::new();
        Subscription {
            conn_id,
            sid,
            sid_bytes: Bytes::from(sid.to_string().into_bytes()),
            subject: subject.to_string(),
            queue: None,
            writer,
        }
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
    fn test_direct_writer_formats_msg() {
        let (writer, handle) = DirectWriter::new();

        writer.write_msg(b"test.sub", b"1", None, None, b"hello");

        let data = handle.drain().expect("should have data");
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG test.sub 1 5\r\nhello\r\n");
    }

    #[test]
    fn test_direct_writer_formats_msg_with_reply() {
        let (writer, handle) = DirectWriter::new();

        writer.write_msg(b"test.sub", b"42", Some(b"reply.to"), None, b"hi");

        let data = handle.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG test.sub 42 reply.to 2\r\nhi\r\n");
    }

    #[test]
    fn test_direct_writer_formats_hmsg_with_headers() {
        use async_nats::header::{HeaderName, IntoHeaderValue};
        let (writer, handle) = DirectWriter::new();

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("X-Key"),
            "val".into_header_value(),
        );

        writer.write_msg(b"test.sub", b"1", None, Some(&headers), b"data");

        let data = handle.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.starts_with("HMSG test.sub 1 "));
        assert!(s.contains("X-Key: val"));
        assert!(s.ends_with("data\r\n"));
    }

    #[test]
    fn test_direct_writer_batches_multiple_writes() {
        let (writer, handle) = DirectWriter::new();

        writer.write_msg(b"a", b"1", None, None, b"one");
        writer.write_msg(b"b", b"2", None, None, b"two");
        writer.write_msg(b"c", b"3", None, None, b"three");

        let data = handle.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(
            s,
            "MSG a 1 3\r\none\r\nMSG b 2 3\r\ntwo\r\nMSG c 3 5\r\nthree\r\n"
        );
    }

    #[test]
    fn test_direct_writer_drain_empty() {
        let (_writer, handle) = DirectWriter::new();
        assert!(handle.drain().is_none());
    }

    #[test]
    fn test_direct_writer_drain_resets_buffer() {
        let (writer, handle) = DirectWriter::new();

        writer.write_msg(b"a", b"1", None, None, b"x");
        let _ = handle.drain().unwrap();

        // Second drain should be empty
        assert!(handle.drain().is_none());

        // Write again — should work
        writer.write_msg(b"b", b"2", None, None, b"y");
        let data = handle.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG b 2 1\r\ny\r\n");
    }

    #[test]
    fn test_direct_writer_clone_shares_buffer() {
        let (writer1, handle) = DirectWriter::new();
        let writer2 = writer1.clone();

        writer1.write_msg(b"a", b"1", None, None, b"x");
        writer2.write_msg(b"b", b"2", None, None, b"y");

        let data = handle.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        // Both messages should be in the same buffer
        assert!(s.contains("MSG a 1 1\r\nx\r\n"));
        assert!(s.contains("MSG b 2 1\r\ny\r\n"));
    }

    #[tokio::test]
    async fn test_direct_writer_notify_wakes_handle() {
        let (writer, handle) = DirectWriter::new();

        // Spawn a task that writes and notifies after a short delay
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            writer.write_msg(b"test", b"1", None, None, b"hello");
            writer.notify();
        });

        // This should wake up when notified
        handle.notified().await;
        let data = handle.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG test 1 5\r\nhello\r\n");
    }

    #[tokio::test]
    async fn test_direct_writer_notify_stores_permit() {
        let (writer, handle) = DirectWriter::new();

        // Notify BEFORE waiting — the permit should be stored
        writer.write_msg(b"test", b"1", None, None, b"early");
        writer.notify();

        // Small delay to ensure we're calling notified() after notify()
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Should return immediately because permit was stored
        handle.notified().await;
        let data = handle.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert_eq!(s, "MSG test 1 5\r\nearly\r\n");
    }

    /// Simulate the producer/consumer pattern used in the server:
    /// a fast producer writes many messages, consumer must receive all of them.
    /// Uses the correct drain-before-wait pattern that avoids the lost-notify race.
    #[tokio::test]
    async fn test_direct_writer_fast_producer_slow_consumer() {
        let (writer, handle) = DirectWriter::new();
        let total_msgs = 10_000;

        // Producer: write all messages rapidly (all at once, simulating burst)
        let producer = tokio::spawn(async move {
            for i in 0..total_msgs {
                let payload = format!("msg{i}");
                writer.write_msg(b"test", b"1", None, None, payload.as_bytes());
                writer.notify();
            }
        });

        // Consumer: drain-before-wait pattern (matches client_conn message_loop)
        let mut total_msgs_seen = 0usize;
        loop {
            // Always check buffer first before blocking
            while let Some(data) = handle.drain() {
                let s = std::str::from_utf8(&data).unwrap();
                total_msgs_seen += s.matches("MSG test 1").count();
            }
            if total_msgs_seen >= total_msgs {
                break;
            }
            handle.notified().await;
        }

        producer.await.unwrap();
        assert_eq!(total_msgs_seen, total_msgs);
    }

    /// Test the race where producer finishes before consumer starts draining.
    /// This catches the lost-notify bug: all notify() calls collapse into one
    /// permit, consumer must drain without relying on future notifications.
    #[tokio::test]
    async fn test_direct_writer_producer_finishes_before_consumer() {
        let (writer, handle) = DirectWriter::new();
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
        loop {
            while let Some(data) = handle.drain() {
                let s = std::str::from_utf8(&data).unwrap();
                total_msgs_seen += s.matches("MSG x 1").count();
            }
            if total_msgs_seen >= total_msgs {
                break;
            }
            // Use timeout to detect hang (would fail without drain-before-wait)
            tokio::select! {
                _ = handle.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    panic!(
                        "consumer hung! only received {total_msgs_seen}/{total_msgs} messages"
                    );
                }
            }
        }
        assert_eq!(total_msgs_seen, total_msgs);
    }
}
