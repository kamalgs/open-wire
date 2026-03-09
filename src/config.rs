// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go nats-server `.conf` file parser.
//!
//! Parses the native NATS configuration format (not TOML/JSON) so that
//! open-wire can be a drop-in replacement for Go nats-server leaf nodes
//! using existing config files.
//!
//! Supported syntax:
//! - Comments: `#` and `//` to end of line
//! - Key-value separators: `=`, `:`, or whitespace
//! - Strings: unquoted, `"double-quoted"`, `'single-quoted'`
//! - Booleans: `true`/`false`/`yes`/`no`/`on`/`off`
//! - Numbers: integers with optional size suffixes (`kb`, `mb`, `gb`)
//! - Duration: integers with time suffixes (`s`, `m`, `h`)
//! - Blocks: `name { ... }`
//! - Arrays: `[ item, item, ... ]`

use std::fmt;
use std::path::Path;

use crate::server::{ClientAuth, HubCredentials, LeafServerConfig};

/// Errors that can occur during config parsing.
#[derive(Debug)]
pub enum ConfigError {
    /// I/O error reading the config file.
    Io(std::io::Error),
    /// Syntax error in the config file.
    Parse {
        line: usize,
        col: usize,
        msg: String,
    },
    /// Semantic error (e.g., invalid value for a known field).
    Value(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "config I/O error: {e}"),
            ConfigError::Parse { line, col, msg } => {
                write!(f, "config parse error at line {line}, col {col}: {msg}")
            }
            ConfigError::Value(msg) => write!(f, "config value error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        ConfigError::Io(e)
    }
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Equals,
    Colon,
    Semicolon,
    Newline,
    Str(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Eof,
}

struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
    line: usize,
    col: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            pos: 0,
            line: 1,
            col: 1,
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let b = self.input.get(self.pos).copied()?;
        self.pos += 1;
        if b == b'\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(b)
    }

    fn err(&self, msg: impl Into<String>) -> ConfigError {
        ConfigError::Parse {
            line: self.line,
            col: self.col,
            msg: msg.into(),
        }
    }

    /// Skip whitespace (except newlines) and comments.
    fn skip_ws_and_comments(&mut self) {
        loop {
            match self.peek_byte() {
                Some(b' ' | b'\t' | b'\r') => {
                    self.advance();
                }
                Some(b'#') => {
                    // Line comment
                    while let Some(b) = self.peek_byte() {
                        if b == b'\n' {
                            break;
                        }
                        self.advance();
                    }
                }
                Some(b'/') if self.input.get(self.pos + 1) == Some(&b'/') => {
                    // // line comment
                    while let Some(b) = self.peek_byte() {
                        if b == b'\n' {
                            break;
                        }
                        self.advance();
                    }
                }
                _ => break,
            }
        }
    }

    fn next_token(&mut self) -> Result<Token, ConfigError> {
        self.skip_ws_and_comments();

        let b = match self.peek_byte() {
            None => return Ok(Token::Eof),
            Some(b) => b,
        };

        match b {
            b'\n' => {
                self.advance();
                Ok(Token::Newline)
            }
            b'{' => {
                self.advance();
                Ok(Token::LBrace)
            }
            b'}' => {
                self.advance();
                Ok(Token::RBrace)
            }
            b'[' => {
                self.advance();
                Ok(Token::LBracket)
            }
            b']' => {
                self.advance();
                Ok(Token::RBracket)
            }
            b',' => {
                self.advance();
                Ok(Token::Comma)
            }
            b'=' => {
                self.advance();
                Ok(Token::Equals)
            }
            b':' => {
                self.advance();
                Ok(Token::Colon)
            }
            b';' => {
                self.advance();
                Ok(Token::Semicolon)
            }
            b'"' => self.lex_double_quoted(),
            b'\'' => self.lex_single_quoted(),
            _ => self.lex_bare(),
        }
    }

    fn lex_double_quoted(&mut self) -> Result<Token, ConfigError> {
        self.advance(); // skip opening "
        let mut s = String::new();
        loop {
            match self.advance() {
                None => return Err(self.err("unterminated double-quoted string")),
                Some(b'"') => return Ok(Token::Str(s)),
                Some(b'\\') => match self.advance() {
                    Some(b'n') => s.push('\n'),
                    Some(b't') => s.push('\t'),
                    Some(b'r') => s.push('\r'),
                    Some(b'\\') => s.push('\\'),
                    Some(b'"') => s.push('"'),
                    Some(c) => {
                        s.push('\\');
                        s.push(c as char);
                    }
                    None => return Err(self.err("unterminated escape in string")),
                },
                Some(c) => s.push(c as char),
            }
        }
    }

    fn lex_single_quoted(&mut self) -> Result<Token, ConfigError> {
        self.advance(); // skip opening '
        let mut s = String::new();
        loop {
            match self.advance() {
                None => return Err(self.err("unterminated single-quoted string")),
                Some(b'\'') => return Ok(Token::Str(s)),
                Some(c) => s.push(c as char),
            }
        }
    }

    /// Lex a bare (unquoted) token. Could be: number, bool, or unquoted string.
    fn lex_bare(&mut self) -> Result<Token, ConfigError> {
        let start = self.pos;
        while let Some(b) = self.peek_byte() {
            match b {
                b' ' | b'\t' | b'\r' | b'\n' | b'{' | b'}' | b'[' | b']' | b',' | b'=' | b':'
                | b';' | b'#' => break,
                b'/' if self.input.get(self.pos + 1) == Some(&b'/') => break,
                _ => {
                    self.advance();
                }
            }
        }
        let word = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|_| self.err("invalid UTF-8"))?;

        // Try bool
        match word.to_ascii_lowercase().as_str() {
            "true" | "yes" | "on" => return Ok(Token::Bool(true)),
            "false" | "no" | "off" => return Ok(Token::Bool(false)),
            _ => {}
        }

        // Try integer (no suffix — suffixes handled at semantic level from Str)
        if let Ok(n) = word.parse::<i64>() {
            return Ok(Token::Integer(n));
        }

        // Try float
        if let Ok(f) = word.parse::<f64>() {
            return Ok(Token::Float(f));
        }

        // Otherwise it's an unquoted string
        Ok(Token::Str(word.to_string()))
    }
}

// ---------------------------------------------------------------------------
// AST / Value
// ---------------------------------------------------------------------------

/// A parsed configuration value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A string value (quoted or unquoted).
    Str(String),
    /// An integer value.
    Int(i64),
    /// A floating-point value.
    Float(f64),
    /// A boolean value.
    Bool(bool),
    /// An ordered map of key-value pairs (from `{ }` blocks or top-level).
    Map(Vec<(String, Value)>),
    /// An array of values (from `[ ]`).
    Array(Vec<Value>),
}

impl Value {
    /// Look up a key in a Map value (case-insensitive).
    #[cfg(test)]
    fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(entries) => entries
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(key))
                .map(|(_, v)| v),
            _ => None,
        }
    }

    /// Get as string.
    #[cfg(test)]
    fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Get as i64.
    #[cfg(test)]
    fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// Get as bool.
    #[cfg(test)]
    fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Get as array.
    fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Get as map.
    fn as_map(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Map(entries) => Some(entries),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser<'a> {
    lexer: Lexer<'a>,
    /// Lookahead token.
    peeked: Option<Token>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            lexer: Lexer::new(input),
            peeked: None,
        }
    }

    fn peek(&mut self) -> Result<&Token, ConfigError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.lexer.next_token()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }

    fn next(&mut self) -> Result<Token, ConfigError> {
        if let Some(t) = self.peeked.take() {
            Ok(t)
        } else {
            self.lexer.next_token()
        }
    }

    fn skip_separators(&mut self) -> Result<(), ConfigError> {
        while let Token::Newline | Token::Semicolon | Token::Comma = self.peek()? {
            self.next()?;
        }
        Ok(())
    }

    fn err(&self, msg: impl Into<String>) -> ConfigError {
        ConfigError::Parse {
            line: self.lexer.line,
            col: self.lexer.col,
            msg: msg.into(),
        }
    }

    /// Parse the entire input as a top-level map (no surrounding braces).
    fn parse_top_level(&mut self) -> Result<Value, ConfigError> {
        self.parse_map_body(true)
    }

    /// Parse key-value pairs until `}` (or EOF if `is_top_level`).
    fn parse_map_body(&mut self, is_top_level: bool) -> Result<Value, ConfigError> {
        let mut entries = Vec::new();
        loop {
            self.skip_separators()?;
            match self.peek()? {
                Token::Eof if is_top_level => break,
                Token::RBrace if !is_top_level => {
                    self.next()?;
                    break;
                }
                Token::Eof => return Err(self.err("unexpected EOF, expected '}'")),
                _ => {}
            }

            // Key
            let key = match self.next()? {
                Token::Str(s) => s.to_ascii_lowercase(),
                Token::Bool(b) => b.to_string(),
                Token::Integer(n) => n.to_string(),
                t => return Err(self.err(format!("expected key, got {t:?}"))),
            };

            // Optional separator: `=`, `:`, or just whitespace
            self.skip_separators()?;
            match self.peek()? {
                Token::Equals | Token::Colon => {
                    self.next()?;
                    self.skip_separators()?;
                }
                _ => {
                    // whitespace-separated — value follows directly
                }
            }

            // Value
            let value = self.parse_value()?;
            entries.push((key, value));
        }
        Ok(Value::Map(entries))
    }

    fn parse_value(&mut self) -> Result<Value, ConfigError> {
        match self.peek()? {
            Token::LBrace => {
                self.next()?;
                self.parse_map_body(false)
            }
            Token::LBracket => self.parse_array(),
            _ => {
                let val = match self.next()? {
                    Token::Str(s) => Value::Str(s),
                    Token::Integer(n) => Value::Int(n),
                    Token::Float(f) => Value::Float(f),
                    Token::Bool(b) => Value::Bool(b),
                    t => return Err(self.err(format!("expected value, got {t:?}"))),
                };
                // Handle colon-separated values like `127.0.0.1:4225` or
                // `leaf://host:port` that the lexer splits at `:`.
                self.try_concat_colon(val)
            }
        }
    }

    /// If the next token is a colon followed by more value tokens, concatenate
    /// them into a single string (e.g., `127.0.0.1` `:` `4225` → `"127.0.0.1:4225"`).
    fn try_concat_colon(&mut self, val: Value) -> Result<Value, ConfigError> {
        if !matches!(self.peek()?, Token::Colon) {
            return Ok(val);
        }
        // Build up a concatenated string: val:next[:next...]
        let mut s = match &val {
            Value::Str(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            _ => return Ok(val),
        };
        while matches!(self.peek()?, Token::Colon) {
            self.next()?; // consume ':'
            s.push(':');
            match self.peek()? {
                Token::Str(_) | Token::Integer(_) | Token::Float(_) | Token::Bool(_) => {
                    match self.next()? {
                        Token::Str(v) => s.push_str(&v),
                        Token::Integer(n) => s.push_str(&n.to_string()),
                        Token::Float(f) => s.push_str(&f.to_string()),
                        Token::Bool(b) => s.push_str(&b.to_string()),
                        _ => unreachable!(),
                    }
                }
                _ => break, // colon at end of value (shouldn't happen, but don't crash)
            }
        }
        Ok(Value::Str(s))
    }

    fn parse_array(&mut self) -> Result<Value, ConfigError> {
        self.next()?; // consume '['
        let mut items = Vec::new();
        loop {
            self.skip_separators()?;
            if self.peek()? == &Token::RBracket {
                self.next()?;
                break;
            }
            let value = self.parse_value()?;
            items.push(value);
            self.skip_separators()?;
            match self.peek()? {
                Token::Comma => {
                    self.next()?;
                }
                Token::RBracket => {} // will be consumed next iteration
                _ => {}               // allow newline-separated items
            }
        }
        Ok(Value::Array(items))
    }
}

/// Parse a config string into a [`Value`] tree.
pub fn parse(input: &str) -> Result<Value, ConfigError> {
    let mut parser = Parser::new(input.as_bytes());
    parser.parse_top_level()
}

// ---------------------------------------------------------------------------
// Size / Duration helpers
// ---------------------------------------------------------------------------

/// Parse a string with optional size suffix into bytes.
/// Supports: `k`/`kb`/`kib` (×1024), `m`/`mb`/`mib` (×1024²), `g`/`gb`/`gib` (×1024³).
fn parse_size(v: &Value) -> Result<usize, ConfigError> {
    match v {
        Value::Int(n) => {
            usize::try_from(*n).map_err(|_| ConfigError::Value(format!("size out of range: {n}")))
        }
        Value::Str(s) => parse_size_str(s),
        _ => Err(ConfigError::Value(format!("expected size, got {v:?}"))),
    }
}

fn parse_size_str(s: &str) -> Result<usize, ConfigError> {
    let s = s.trim();
    // Find where digits end and suffix begins
    let (num_part, suffix) = split_number_suffix(s);
    let base: usize = num_part
        .parse()
        .map_err(|_| ConfigError::Value(format!("invalid size: {s}")))?;
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        _ => return Err(ConfigError::Value(format!("unknown size suffix: {suffix}"))),
    };
    Ok(base * multiplier)
}

/// Parse a value as seconds (u64). Bare integer = seconds, or string with `s`/`m`/`h` suffix.
fn parse_duration_secs(v: &Value) -> Result<u64, ConfigError> {
    match v {
        Value::Int(n) => {
            u64::try_from(*n).map_err(|_| ConfigError::Value(format!("duration out of range: {n}")))
        }
        Value::Str(s) => parse_duration_str(s),
        _ => Err(ConfigError::Value(format!("expected duration, got {v:?}"))),
    }
}

fn parse_duration_str(s: &str) -> Result<u64, ConfigError> {
    let s = s.trim();
    let (num_part, suffix) = split_number_suffix(s);
    let base: u64 = num_part
        .parse()
        .map_err(|_| ConfigError::Value(format!("invalid duration: {s}")))?;
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3600,
        _ => {
            return Err(ConfigError::Value(format!(
                "unknown duration suffix: {suffix}"
            )))
        }
    };
    Ok(base * multiplier)
}

/// Split "64mb" into ("64", "mb"). Works for purely numeric strings too.
fn split_number_suffix(s: &str) -> (&str, &str) {
    let end = s
        .bytes()
        .position(|b| !b.is_ascii_digit() && b != b'.')
        .unwrap_or(s.len());
    (&s[..end], &s[end..])
}

/// Extract a u16 value.
fn as_u16(v: &Value) -> Result<u16, ConfigError> {
    match v {
        Value::Int(n) => {
            u16::try_from(*n).map_err(|_| ConfigError::Value(format!("port out of range: {n}")))
        }
        Value::Str(s) => s
            .parse()
            .map_err(|_| ConfigError::Value(format!("invalid port: {s}"))),
        _ => Err(ConfigError::Value(format!("expected integer, got {v:?}"))),
    }
}

/// Extract a u32 value.
fn as_u32(v: &Value) -> Result<u32, ConfigError> {
    match v {
        Value::Int(n) => {
            u32::try_from(*n).map_err(|_| ConfigError::Value(format!("value out of range: {n}")))
        }
        Value::Str(s) => s
            .parse()
            .map_err(|_| ConfigError::Value(format!("invalid integer: {s}"))),
        _ => Err(ConfigError::Value(format!("expected integer, got {v:?}"))),
    }
}

/// Extract a usize value.
fn as_usize(v: &Value) -> Result<usize, ConfigError> {
    match v {
        Value::Int(n) => {
            usize::try_from(*n).map_err(|_| ConfigError::Value(format!("value out of range: {n}")))
        }
        Value::Str(s) => s
            .parse()
            .map_err(|_| ConfigError::Value(format!("invalid integer: {s}"))),
        _ => Err(ConfigError::Value(format!("expected integer, got {v:?}"))),
    }
}

/// Extract a string value.
fn as_string(v: &Value) -> Result<String, ConfigError> {
    match v {
        Value::Str(s) => Ok(s.clone()),
        Value::Int(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err(ConfigError::Value(format!("expected string, got {v:?}"))),
    }
}

// ---------------------------------------------------------------------------
// Listen address parsing
// ---------------------------------------------------------------------------

/// Parse a `listen` value like `"127.0.0.1:4225"` or just `"4225"`.
fn parse_listen(s: &str) -> Result<(Option<String>, Option<u16>), ConfigError> {
    let s = s.trim();
    if let Some(idx) = s.rfind(':') {
        let host = &s[..idx];
        let port_str = &s[idx + 1..];
        let port: u16 = port_str
            .parse()
            .map_err(|_| ConfigError::Value(format!("invalid port in: {s}")))?;
        Ok((Some(host.to_string()), Some(port)))
    } else if let Ok(port) = s.parse::<u16>() {
        Ok((None, Some(port)))
    } else {
        // Just a host
        Ok((Some(s.to_string()), None))
    }
}

// ---------------------------------------------------------------------------
// Go nats-server keys that we silently ignore (log at debug)
// ---------------------------------------------------------------------------

const IGNORED_KEYS: &[&str] = &[
    "jetstream",
    "cluster",
    "gateway",
    "accounts",
    "operator",
    "resolver",
    "system_account",
    "mappings",
    "debug",
    "trace",
    "logtime",
    "log_file",
    "pid_file",
    "write_deadline",
    "lame_duck_duration",
    "lame_duck_grace_period",
    "no_auth_user",
    "connect_error_reports",
    "reconnect_error_reports",
    "max_traced_msg_len",
    "trusted_keys",
    "trusted_operators",
    "no_sys_acc",
    "no_tls",
    "strict",
];

// ---------------------------------------------------------------------------
// Config builder
// ---------------------------------------------------------------------------

/// Load a Go nats-server `.conf` file and produce a [`LeafServerConfig`].
///
/// Unknown keys are logged at debug level and otherwise ignored, so this
/// parser can read real Go nats-server configs without erroring on features
/// open-wire does not support.
pub fn load_config(path: &Path) -> Result<LeafServerConfig, ConfigError> {
    let contents = std::fs::read_to_string(path)?;
    load_config_str(&contents)
}

/// Parse a config string (for testing).
pub fn load_config_str(input: &str) -> Result<LeafServerConfig, ConfigError> {
    let root = parse(input)?;
    build_config(&root)
}

fn build_config(root: &Value) -> Result<LeafServerConfig, ConfigError> {
    let entries = match root.as_map() {
        Some(e) => e,
        None => return Err(ConfigError::Value("top-level must be a map".into())),
    };

    let mut config = LeafServerConfig::default();
    let mut auth_token: Option<String> = None;
    let mut auth_user: Option<String> = None;
    let mut auth_pass: Option<String> = None;

    for (key, value) in entries {
        match key.as_str() {
            // --- Top-level scalars ---
            "listen" => {
                let s = as_string(value)?;
                let (host, port) = parse_listen(&s)?;
                if let Some(h) = host {
                    config.host = h;
                }
                if let Some(p) = port {
                    config.port = p;
                }
            }
            "host" => config.host = as_string(value)?,
            "port" => config.port = as_u16(value)?,
            "server_name" => config.server_name = as_string(value)?,
            "max_payload" => config.max_payload = parse_size(value)?,
            "max_pending" => config.max_pending = parse_size(value)?,
            "max_connections" => config.max_connections = as_usize(value)?,
            "max_control_line" => config.max_control_line = parse_size(value)?,
            "max_subscriptions" => config.max_subscriptions = as_usize(value)?,
            "ping_interval" => {
                let secs = parse_duration_secs(value)?;
                config.ping_interval = std::time::Duration::from_secs(secs);
            }
            "ping_max" => config.max_pings_outstanding = as_u32(value)?,

            // --- open-wire extensions ---
            "workers" => config.workers = as_usize(value)?,
            "max_read_buf" => config.max_read_buf_capacity = parse_size(value)?,
            "write_buf_size" => config.write_buf_capacity = parse_size(value)?,
            "metrics_port" => config.metrics_port = Some(as_u16(value)?),

            // --- authorization block ---
            "authorization" => {
                if let Some(entries) = value.as_map() {
                    for (akey, aval) in entries {
                        match akey.as_str() {
                            "token" => auth_token = Some(as_string(aval)?),
                            "user" | "username" => auth_user = Some(as_string(aval)?),
                            "password" | "pass" => auth_pass = Some(as_string(aval)?),
                            _ => {
                                tracing::debug!("ignoring authorization key: {akey}");
                            }
                        }
                    }
                }
            }

            // --- leafnodes block ---
            "leafnodes" => apply_leafnodes(&mut config, value)?,

            // --- websocket block ---
            "websocket" => {
                if let Some(entries) = value.as_map() {
                    for (wkey, wval) in entries {
                        match wkey.as_str() {
                            "listen" => {
                                let s = as_string(wval)?;
                                let (_, port) = parse_listen(&s)?;
                                if let Some(p) = port {
                                    config.ws_port = Some(p);
                                }
                            }
                            "port" => config.ws_port = Some(as_u16(wval)?),
                            "no_tls" => {
                                // Accepted and ignored — open-wire WS does not require TLS
                                tracing::debug!("ignoring websocket.no_tls");
                            }
                            _ => {
                                tracing::debug!("ignoring websocket key: {wkey}");
                            }
                        }
                    }
                }
            }

            // --- tls block ---
            "tls" => {
                if let Some(entries) = value.as_map() {
                    for (tkey, tval) in entries {
                        match tkey.as_str() {
                            "cert_file" => {
                                config.tls_cert = Some(std::path::PathBuf::from(as_string(tval)?));
                            }
                            "key_file" => {
                                config.tls_key = Some(std::path::PathBuf::from(as_string(tval)?));
                            }
                            _ => {
                                tracing::debug!("ignoring tls key: {tkey}");
                            }
                        }
                    }
                }
            }

            // --- Silently ignored Go keys ---
            k if IGNORED_KEYS.contains(&k) => {
                tracing::debug!("ignoring unsupported config key: {key}");
            }

            _ => {
                tracing::debug!("ignoring unknown config key: {key}");
            }
        }
    }

    // Build client auth
    if let Some(token) = auth_token {
        config.client_auth = ClientAuth::Token(token);
    } else if let (Some(user), Some(pass)) = (auth_user, auth_pass) {
        config.client_auth = ClientAuth::UserPass { user, pass };
    }

    Ok(config)
}

fn apply_leafnodes(config: &mut LeafServerConfig, value: &Value) -> Result<(), ConfigError> {
    let entries = match value.as_map() {
        Some(e) => e,
        None => return Ok(()),
    };

    for (lkey, lval) in entries {
        match lkey.as_str() {
            "remotes" => {
                if let Some(arr) = lval.as_array() {
                    if arr.len() > 1 {
                        tracing::warn!(
                            "config has {} leafnode remotes; only the first is used",
                            arr.len()
                        );
                    }
                    if let Some(remote) = arr.first() {
                        apply_remote(config, remote)?;
                    }
                }
            }
            "listen" => {
                // leafnodes.listen — this is for accepting inbound leaf
                // connections, which open-wire doesn't support yet. Log and skip.
                tracing::debug!("ignoring leafnodes.listen (inbound leaf not supported)");
            }
            _ => {
                tracing::debug!("ignoring leafnodes key: {lkey}");
            }
        }
    }
    Ok(())
}

fn apply_remote(config: &mut LeafServerConfig, remote: &Value) -> Result<(), ConfigError> {
    let entries = match remote.as_map() {
        Some(e) => e,
        None => return Ok(()),
    };

    let mut hub_creds = HubCredentials::default();
    let mut has_creds = false;

    for (rkey, rval) in entries {
        match rkey.as_str() {
            "url" | "urls" => {
                let url = as_string(rval)?;
                // Convert leaf:// scheme to nats:// for our upstream module
                let url = if let Some(rest) = url.strip_prefix("leaf://") {
                    format!("nats://{rest}")
                } else {
                    url
                };
                config.hub_url = Some(url);
            }
            "credentials" => {
                hub_creds.creds_file = Some(as_string(rval)?);
                has_creds = true;
            }
            _ => {
                tracing::debug!("ignoring remote key: {rkey}");
            }
        }
    }

    if has_creds {
        config.hub_credentials = Some(hub_creds);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Lexer tests ---

    fn lex_all(input: &str) -> Vec<Token> {
        let mut lexer = Lexer::new(input.as_bytes());
        let mut tokens = Vec::new();
        loop {
            let t = lexer.next_token().unwrap();
            if matches!(t, Token::Eof) {
                break;
            }
            tokens.push(t);
        }
        tokens
    }

    #[test]
    fn lex_basic_tokens() {
        let tokens = lex_all("{ } [ ] , = : ;");
        assert_eq!(
            tokens,
            vec![
                Token::LBrace,
                Token::RBrace,
                Token::LBracket,
                Token::RBracket,
                Token::Comma,
                Token::Equals,
                Token::Colon,
                Token::Semicolon,
            ]
        );
    }

    #[test]
    fn lex_strings() {
        let tokens = lex_all(r#""hello" 'world' bare"#);
        assert_eq!(
            tokens,
            vec![
                Token::Str("hello".into()),
                Token::Str("world".into()),
                Token::Str("bare".into()),
            ]
        );
    }

    #[test]
    fn lex_double_quote_escapes() {
        let tokens = lex_all(r#""hello\nworld""#);
        assert_eq!(tokens, vec![Token::Str("hello\nworld".into())]);
    }

    #[test]
    fn lex_numbers() {
        let tokens = lex_all("42 -7 2.72");
        assert_eq!(
            tokens,
            vec![Token::Integer(42), Token::Integer(-7), Token::Float(2.72),]
        );
    }

    #[test]
    fn lex_booleans() {
        let tokens = lex_all("true false yes no on off TRUE FALSE");
        assert_eq!(
            tokens,
            vec![
                Token::Bool(true),
                Token::Bool(false),
                Token::Bool(true),
                Token::Bool(false),
                Token::Bool(true),
                Token::Bool(false),
                Token::Bool(true),
                Token::Bool(false),
            ]
        );
    }

    #[test]
    fn lex_comments() {
        let tokens = lex_all("foo # comment\nbar // another\nbaz");
        // Newlines are tokens
        let strs: Vec<_> = tokens
            .iter()
            .filter_map(|t| match t {
                Token::Str(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(strs, vec!["foo", "bar", "baz"]);
    }

    // --- Size parsing tests ---

    #[test]
    fn parse_size_suffixes() {
        assert_eq!(parse_size_str("1024").unwrap(), 1024);
        assert_eq!(parse_size_str("1kb").unwrap(), 1024);
        assert_eq!(parse_size_str("1KB").unwrap(), 1024);
        assert_eq!(parse_size_str("1k").unwrap(), 1024);
        assert_eq!(parse_size_str("64mb").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_size_str("64MB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_size_str("8MiB").unwrap(), 8 * 1024 * 1024);
        assert_eq!(parse_size_str("10GiB").unwrap(), 10 * 1024 * 1024 * 1024);
        assert_eq!(parse_size_str("1gb").unwrap(), 1024 * 1024 * 1024);
    }

    // --- Duration parsing tests ---

    #[test]
    fn parse_duration_suffixes() {
        assert_eq!(parse_duration_str("120").unwrap(), 120);
        assert_eq!(parse_duration_str("120s").unwrap(), 120);
        assert_eq!(parse_duration_str("2m").unwrap(), 120);
        assert_eq!(parse_duration_str("1h").unwrap(), 3600);
    }

    // --- Parser tests ---

    #[test]
    fn parse_minimal_config() {
        let val = parse("port: 4222").unwrap();
        assert_eq!(val.get("port"), Some(&Value::Int(4222)));
    }

    #[test]
    fn parse_all_delimiters() {
        let input = "a = 1\nb: 2\nc 3";
        let val = parse(input).unwrap();
        assert_eq!(val.get("a"), Some(&Value::Int(1)));
        assert_eq!(val.get("b"), Some(&Value::Int(2)));
        assert_eq!(val.get("c"), Some(&Value::Int(3)));
    }

    #[test]
    fn parse_nested_block() {
        let input = r#"
leafnodes {
  remotes = [ { url: "leaf://127.0.0.1:7422" } ]
}
"#;
        let val = parse(input).unwrap();
        let ln = val.get("leafnodes").unwrap();
        let remotes = ln.get("remotes").unwrap();
        let arr = remotes.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        let remote = &arr[0];
        assert_eq!(
            remote.get("url").unwrap().as_str(),
            Some("leaf://127.0.0.1:7422")
        );
    }

    #[test]
    fn parse_bench_go_leaf_config() {
        let input = r#"
# Go native leaf node for benchmarks.
listen: 127.0.0.1:4225
leafnodes {
  remotes = [ { url: "leaf://127.0.0.1:7422" } ]
}
"#;
        let config = load_config_str(input).unwrap();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 4225);
        assert_eq!(config.hub_url.as_deref(), Some("nats://127.0.0.1:7422"));
    }

    #[test]
    fn parse_bench_go_leaf_ws_config() {
        let input = r#"
listen: 127.0.0.1:4225
leafnodes {
  remotes = [ { url: "leaf://127.0.0.1:7422" } ]
}
websocket {
  listen: "127.0.0.1:4226"
  no_tls: true
}
"#;
        let config = load_config_str(input).unwrap();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 4225);
        assert_eq!(config.hub_url.as_deref(), Some("nats://127.0.0.1:7422"));
        assert_eq!(config.ws_port, Some(4226));
    }

    #[test]
    fn parse_hub_config() {
        let input = r#"
listen: 127.0.0.1:4333
leafnodes {
  listen: 127.0.0.1:7422
}
"#;
        // This should parse without error; leafnodes.listen is ignored.
        let config = load_config_str(input).unwrap();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 4333);
        assert!(config.hub_url.is_none());
    }

    #[test]
    fn parse_jetstream_ignored() {
        let input = r#"
jetstream: {
  strict: true,
  max_mem_store:  8MiB,
  max_file_store: 10GiB
}
"#;
        // JetStream block should be silently ignored
        let config = load_config_str(input).unwrap();
        assert_eq!(config.port, 4222); // default
    }

    #[test]
    fn parse_full_config() {
        let input = r#"
server_name: "my-leaf"
listen: "10.0.0.1:5222"
max_payload: 2mb
max_pending: 128mb
max_connections: 1000
max_control_line: 8kb
max_subscriptions: 100
ping_interval: 30
ping_max: 5
workers: 4
metrics_port: 9090

authorization {
    user: admin
    password: "secret"
}

leafnodes {
    remotes = [
        {
            url: "nats://hub.example.com:7422"
            credentials: "/path/to/hub.creds"
        }
    ]
}

tls {
    cert_file: "/etc/tls/cert.pem"
    key_file: "/etc/tls/key.pem"
}

websocket {
    port: 8222
}
"#;
        let config = load_config_str(input).unwrap();
        assert_eq!(config.server_name, "my-leaf");
        assert_eq!(config.host, "10.0.0.1");
        assert_eq!(config.port, 5222);
        assert_eq!(config.max_payload, 2 * 1024 * 1024);
        assert_eq!(config.max_pending, 128 * 1024 * 1024);
        assert_eq!(config.max_connections, 1000);
        assert_eq!(config.max_control_line, 8 * 1024);
        assert_eq!(config.max_subscriptions, 100);
        assert_eq!(config.ping_interval, std::time::Duration::from_secs(30));
        assert_eq!(config.max_pings_outstanding, 5);
        assert_eq!(config.workers, 4);
        assert_eq!(config.metrics_port, Some(9090));
        assert!(
            matches!(config.client_auth, ClientAuth::UserPass { ref user, ref pass }
            if user == "admin" && pass == "secret")
        );
        assert_eq!(
            config.hub_url.as_deref(),
            Some("nats://hub.example.com:7422")
        );
        let creds = config.hub_credentials.as_ref().unwrap();
        assert_eq!(creds.creds_file.as_deref(), Some("/path/to/hub.creds"));
        assert_eq!(
            config.tls_cert.as_ref().unwrap().to_str().unwrap(),
            "/etc/tls/cert.pem"
        );
        assert_eq!(
            config.tls_key.as_ref().unwrap().to_str().unwrap(),
            "/etc/tls/key.pem"
        );
        assert_eq!(config.ws_port, Some(8222));
    }

    #[test]
    fn parse_token_auth() {
        let input = r#"
authorization {
    token: "s3cret-t0ken"
}
"#;
        let config = load_config_str(input).unwrap();
        assert!(matches!(config.client_auth, ClientAuth::Token(ref t) if t == "s3cret-t0ken"));
    }

    #[test]
    fn unknown_keys_do_not_error() {
        let input = r#"
port: 4222
some_future_feature: true
another_unknown {
    nested: value
}
"#;
        let config = load_config_str(input).unwrap();
        assert_eq!(config.port, 4222);
    }

    #[test]
    fn parse_real_bench_configs() {
        // Test against the actual config files in the repo
        let configs = [
            "tests/configs/bench_go_leaf.conf",
            "tests/configs/bench_go_leaf_ws.conf",
            "tests/configs/bench_hub.conf",
            "tests/configs/jetstream.conf",
        ];
        for path in &configs {
            let full = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
            if full.exists() {
                let result = load_config(&full);
                assert!(result.is_ok(), "failed to parse {path}: {:?}", result.err());
            }
        }
    }

    #[test]
    fn parse_listen_variants() {
        assert_eq!(parse_listen("4222").unwrap(), (None, Some(4222)));
        assert_eq!(
            parse_listen("127.0.0.1:4222").unwrap(),
            (Some("127.0.0.1".into()), Some(4222))
        );
        assert_eq!(
            parse_listen("0.0.0.0:4222").unwrap(),
            (Some("0.0.0.0".into()), Some(4222))
        );
    }

    #[test]
    fn parse_semicolons_as_separators() {
        let input = "port: 4222; host: \"0.0.0.0\"; max_connections: 100";
        let config = load_config_str(input).unwrap();
        assert_eq!(config.port, 4222);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.max_connections, 100);
    }

    #[test]
    fn parse_multiple_remotes_uses_first() {
        let input = r#"
leafnodes {
  remotes = [
    { url: "nats://first:7422" }
    { url: "nats://second:7422" }
  ]
}
"#;
        let config = load_config_str(input).unwrap();
        assert_eq!(config.hub_url.as_deref(), Some("nats://first:7422"));
    }

    #[test]
    fn parse_duration_variants() {
        let input = r#"ping_interval: "2m""#;
        let config = load_config_str(input).unwrap();
        assert_eq!(config.ping_interval, std::time::Duration::from_secs(120));
    }
}
