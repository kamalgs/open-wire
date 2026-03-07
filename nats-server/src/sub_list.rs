// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;

use bytes::Bytes;

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
}

/// Subscription list supporting NATS wildcard matching.
#[derive(Debug, Default)]
pub struct SubList {
    subs: Vec<Subscription>,
}

impl SubList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, sub: Subscription) {
        self.subs.push(sub);
    }

    pub fn remove(&mut self, conn_id: u64, sid: u64) -> Option<Subscription> {
        if let Some(pos) = self
            .subs
            .iter()
            .position(|s| s.conn_id == conn_id && s.sid == sid)
        {
            Some(self.subs.swap_remove(pos))
        } else {
            None
        }
    }

    pub fn remove_conn(&mut self, conn_id: u64) -> Vec<Subscription> {
        let mut removed = Vec::new();
        let mut i = 0;
        while i < self.subs.len() {
            if self.subs[i].conn_id == conn_id {
                removed.push(self.subs.swap_remove(i));
            } else {
                i += 1;
            }
        }
        removed
    }

    pub fn match_subject(&self, subject: &str) -> Vec<&Subscription> {
        self.subs
            .iter()
            .filter(|s| subject_matches(&s.subject, subject))
            .collect()
    }

    /// Iterate over matching subscriptions without allocating a Vec.
    /// Returns the total number of matches.
    pub fn for_each_match(&self, subject: &str, mut f: impl FnMut(&Subscription)) -> usize {
        let mut count = 0;
        for sub in &self.subs {
            if subject_matches(&sub.subject, subject) {
                f(sub);
                count += 1;
            }
        }
        count
    }

    #[allow(dead_code)]
    pub fn unique_subjects(&self) -> HashSet<&str> {
        self.subs.iter().map(|s| s.subject.as_str()).collect()
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
        sl.insert(Subscription {
            conn_id: 1,
            sid: 1,
            sid_bytes: Bytes::from_static(b"1"),
            subject: "foo.bar".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 1,
            sid: 2,
            sid_bytes: Bytes::from_static(b"2"),
            subject: "foo.*".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 2,
            sid: 1,
            sid_bytes: Bytes::from_static(b"1"),
            subject: "baz.>".to_string(),
            queue: None,
        });

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
        sl.insert(Subscription {
            conn_id: 1,
            sid: 1,
            sid_bytes: Bytes::from_static(b"1"),
            subject: "foo".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 1,
            sid: 2,
            sid_bytes: Bytes::from_static(b"2"),
            subject: "bar".to_string(),
            queue: None,
        });

        let removed = sl.remove(1, 1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().subject, "foo");

        let matches = sl.match_subject("foo");
        assert!(matches.is_empty());
    }

    #[test]
    fn test_sublist_remove_conn() {
        let mut sl = SubList::new();
        sl.insert(Subscription {
            conn_id: 1,
            sid: 1,
            sid_bytes: Bytes::from_static(b"1"),
            subject: "foo".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 1,
            sid: 2,
            sid_bytes: Bytes::from_static(b"2"),
            subject: "bar".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 2,
            sid: 1,
            sid_bytes: Bytes::from_static(b"1"),
            subject: "foo".to_string(),
            queue: None,
        });

        let removed = sl.remove_conn(1);
        assert_eq!(removed.len(), 2);

        let matches = sl.match_subject("foo");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].conn_id, 2);
    }

    #[test]
    fn test_unique_subjects() {
        let mut sl = SubList::new();
        sl.insert(Subscription {
            conn_id: 1,
            sid: 1,
            sid_bytes: Bytes::from_static(b"1"),
            subject: "foo".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 2,
            sid: 1,
            sid_bytes: Bytes::from_static(b"1"),
            subject: "foo".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 1,
            sid: 2,
            sid_bytes: Bytes::from_static(b"2"),
            subject: "bar".to_string(),
            queue: None,
        });

        let subjects = sl.unique_subjects();
        assert_eq!(subjects.len(), 2);
        assert!(subjects.contains("foo"));
        assert!(subjects.contains("bar"));
    }
}
