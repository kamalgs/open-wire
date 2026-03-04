// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct Subscription {
    pub conn_id: u64,
    pub sid: u64,
    pub subject: String,
    pub queue: Option<String>,
}

/// Subscription list supporting NATS wildcard matching.
#[derive(Debug, Default)]
pub(crate) struct SubList {
    subs: Vec<Subscription>,
}

impl SubList {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, sub: Subscription) {
        self.subs.push(sub);
    }

    pub(crate) fn remove(&mut self, conn_id: u64, sid: u64) -> Option<Subscription> {
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

    pub(crate) fn remove_conn(&mut self, conn_id: u64) -> Vec<Subscription> {
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

    pub(crate) fn match_subject(&self, subject: &str) -> Vec<&Subscription> {
        self.subs
            .iter()
            .filter(|s| subject_matches(&s.subject, subject))
            .collect()
    }

    #[allow(dead_code)]
    pub(crate) fn unique_subjects(&self) -> HashSet<&str> {
        self.subs.iter().map(|s| s.subject.as_str()).collect()
    }
}

/// NATS wildcard matching.
/// `*` matches a single token, `>` matches one or more tokens (tail match).
pub(crate) fn subject_matches(pattern: &str, subject: &str) -> bool {
    let pattern_tokens: Vec<&str> = pattern.split('.').collect();
    let subject_tokens: Vec<&str> = subject.split('.').collect();

    let mut pi = 0;
    let mut si = 0;

    while pi < pattern_tokens.len() && si < subject_tokens.len() {
        let pt = pattern_tokens[pi];
        if pt == ">" {
            // `>` matches one or more remaining tokens
            return true;
        }
        if pt != "*" && pt != subject_tokens[si] {
            return false;
        }
        pi += 1;
        si += 1;
    }

    pi == pattern_tokens.len() && si == subject_tokens.len()
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
            subject: "foo.bar".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 1,
            sid: 2,
            subject: "foo.*".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 2,
            sid: 1,
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
            subject: "foo".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 1,
            sid: 2,
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
            subject: "foo".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 1,
            sid: 2,
            subject: "bar".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 2,
            sid: 1,
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
            subject: "foo".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 2,
            sid: 1,
            subject: "foo".to_string(),
            queue: None,
        });
        sl.insert(Subscription {
            conn_id: 1,
            sid: 2,
            subject: "bar".to_string(),
            queue: None,
        });

        let subjects = sl.unique_subjects();
        assert_eq!(subjects.len(), 2);
        assert!(subjects.contains("foo"));
        assert!(subjects.contains("bar"));
    }
}
