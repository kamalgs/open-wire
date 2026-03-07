// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use tokio::sync::mpsc;

use async_nats::header::HeaderMap;

/// A message to be delivered to a local client connection.
/// Uses pre-formatted `sid_bytes` to avoid integer formatting on every write.
#[derive(Debug)]
pub(crate) struct ClientMsg {
    pub subject: Bytes,
    pub sid_bytes: Bytes,
    pub reply: Option<Bytes>,
    pub headers: Option<HeaderMap>,
    pub payload: Bytes,
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
    /// Direct sender to the client's message channel.
    /// Avoids conns HashMap lookup + lock on every publish.
    pub(crate) msg_tx: mpsc::UnboundedSender<ClientMsg>,
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

    /// Create a test subscription with a dummy msg_tx sender.
    fn test_sub(conn_id: u64, sid: u64, subject: &str) -> Subscription {
        let (tx, _rx) = mpsc::unbounded_channel();
        Subscription {
            conn_id,
            sid,
            sid_bytes: Bytes::from(sid.to_string().into_bytes()),
            subject: subject.to_string(),
            queue: None,
            msg_tx: tx,
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
}
