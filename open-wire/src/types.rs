// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Minimal local types for NATS protocol, replacing async-nats dependency.
//!
//! Only the subset actually used by nats-server is implemented.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

// ────────────────────────────────────────────────────────────────────────────
// ServerInfo — serialized as JSON in INFO protocol line
// ────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    #[serde(default)]
    pub server_id: String,
    #[serde(default)]
    pub server_name: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub proto: i8,
    #[serde(default)]
    pub max_payload: usize,
    #[serde(default)]
    pub headers: bool,
    #[serde(default)]
    pub auth_required: bool,
    #[serde(default)]
    pub tls_required: bool,
    #[serde(default)]
    pub client_id: u64,
    #[serde(default)]
    pub go: String,
    #[serde(default)]
    pub nonce: String,
    #[serde(default)]
    pub connect_urls: Vec<String>,
    #[serde(default)]
    pub client_ip: String,
    #[serde(default, rename = "ldm")]
    pub lame_duck_mode: bool,
    #[serde(default)]
    pub jetstream: bool,
}

// ────────────────────────────────────────────────────────────────────────────
// ConnectInfo — deserialized from client CONNECT JSON
// ────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
pub struct ConnectInfo {
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub pedantic: bool,
    #[serde(default)]
    pub lang: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub protocol: u8,
    #[serde(default)]
    pub echo: bool,
    #[serde(default)]
    pub headers: bool,
    #[serde(default)]
    pub no_responders: bool,
    #[serde(default)]
    pub tls_required: bool,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub pass: Option<String>,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default, rename = "jwt")]
    pub user_jwt: Option<String>,
    #[serde(default)]
    pub nkey: Option<String>,
    #[serde(default, rename = "sig")]
    pub signature: Option<String>,
}

// ────────────────────────────────────────────────────────────────────────────
// HeaderMap — NATS message headers (HashMap<String, Vec<String>>)
// ────────────────────────────────────────────────────────────────────────────

/// A simple NATS header map. Keys are case-preserved strings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeaderMap {
    inner: HashMap<String, Vec<String>>,
}

impl HeaderMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Append a value to the given header key (allows multiple values per key).
    pub fn append(&mut self, name: &str, value: String) {
        self.inner
            .entry(name.to_owned())
            .or_default()
            .push(value);
    }

    /// Set a header key to a single value, replacing any existing values.
    pub fn insert(&mut self, name: &str, value: String) {
        self.inner.insert(name.to_owned(), vec![value]);
    }

    /// Get the first value for a header key (case-sensitive).
    pub fn get(&self, name: &str) -> Option<&str> {
        self.inner.get(name).and_then(|v| v.first()).map(|s| s.as_str())
    }

    /// Serialize headers to NATS wire format:
    /// ```text
    /// NATS/1.0\r\n
    /// Key: Value\r\n
    /// \r\n
    /// ```
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"NATS/1.0\r\n");
        for (k, vs) in &self.inner {
            for v in vs {
                buf.extend_from_slice(k.as_bytes());
                buf.extend_from_slice(b": ");
                buf.extend_from_slice(v.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

impl fmt::Display for HeaderMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (k, vs) in &self.inner {
            for v in vs {
                writeln!(f, "{k}: {v}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_info_roundtrip() {
        let info = ServerInfo {
            server_id: "TEST".into(),
            proto: 1,
            max_payload: 1024,
            headers: true,
            ..Default::default()
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: ServerInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, parsed);
    }

    #[test]
    fn connect_info_parse() {
        let json = br#"{"lang":"rust","version":"1.0","headers":true,"protocol":1}"#;
        let info: ConnectInfo = serde_json::from_slice(json).unwrap();
        assert_eq!(info.lang, "rust");
        assert!(info.headers);
        assert_eq!(info.protocol, 1);
    }

    #[test]
    fn header_map_basics() {
        let mut h = HeaderMap::new();
        assert!(h.is_empty());
        h.append("X-Key", "val".into());
        assert!(!h.is_empty());
        assert_eq!(h.get("X-Key"), Some("val"));
        assert_eq!(h.get("Missing"), None);
    }

    #[test]
    fn header_map_to_bytes() {
        let mut h = HeaderMap::new();
        h.insert("Foo", "Bar".into());
        let bytes = h.to_bytes();
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(s.starts_with("NATS/1.0\r\n"));
        assert!(s.contains("Foo: Bar\r\n"));
        assert!(s.ends_with("\r\n\r\n"));
    }

    #[test]
    fn header_map_multiple_values() {
        let mut h = HeaderMap::new();
        h.append("X", "a".into());
        h.append("X", "b".into());
        // get() returns first value
        assert_eq!(h.get("X"), Some("a"));
    }

    #[test]
    fn connect_info_unknown_fields_ignored() {
        let json = br#"{"lang":"go","unknown_field":42}"#;
        let info: ConnectInfo = serde_json::from_slice(json).unwrap();
        assert_eq!(info.lang, "go");
    }
}
