// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use bytes::{Buf, BytesMut};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

use async_nats::connection::AsyncReadWrite;
use async_nats::header::{HeaderMap, HeaderName, IntoHeaderValue};
use async_nats::subject::Subject;
use async_nats::{ClientOp, ConnectInfo, ServerInfo};

/// Server-side connection wrapper.
/// Owns the raw stream and its own read buffer for parsing client operations.
pub(crate) struct ServerConn {
    stream: Box<dyn AsyncReadWrite>,
    read_buf: BytesMut,
}

impl ServerConn {
    pub(crate) fn new(stream: Box<dyn AsyncReadWrite>) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(64 * 1024),
        }
    }

    /// Send INFO to connected client.
    pub(crate) async fn send_info(&mut self, info: &ServerInfo) -> io::Result<()> {
        let json = serde_json::to_string(info)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let line = format!("INFO {json}\r\n");
        self.write_flush(line.as_bytes()).await
    }

    /// Send MSG to connected client.
    pub(crate) async fn send_msg(
        &mut self,
        subject: &str,
        sid: u64,
        reply: Option<&str>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload.len();
                let line = match reply {
                    Some(r) => format!("HMSG {subject} {sid} {r} {hdr_len} {total_len}\r\n"),
                    None => format!("HMSG {subject} {sid} {hdr_len} {total_len}\r\n"),
                };
                let mut buf = Vec::with_capacity(line.len() + total_len + 2);
                buf.extend_from_slice(line.as_bytes());
                buf.extend_from_slice(&hdr_bytes);
                buf.extend_from_slice(payload);
                buf.extend_from_slice(b"\r\n");
                self.write_flush(&buf).await
            }
            _ => {
                let payload_len = payload.len();
                let line = match reply {
                    Some(r) => format!("MSG {subject} {sid} {r} {payload_len}\r\n"),
                    None => format!("MSG {subject} {sid} {payload_len}\r\n"),
                };
                let mut buf = Vec::with_capacity(line.len() + payload_len + 2);
                buf.extend_from_slice(line.as_bytes());
                buf.extend_from_slice(payload);
                buf.extend_from_slice(b"\r\n");
                self.write_flush(&buf).await
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn send_ping(&mut self) -> io::Result<()> {
        self.write_flush(b"PING\r\n").await
    }

    pub(crate) async fn send_pong(&mut self) -> io::Result<()> {
        self.write_flush(b"PONG\r\n").await
    }

    #[allow(dead_code)]
    pub(crate) async fn send_ok(&mut self) -> io::Result<()> {
        self.write_flush(b"+OK\r\n").await
    }

    pub(crate) async fn send_err(&mut self, msg: &str) -> io::Result<()> {
        let line = format!("-ERR '{msg}'\r\n");
        self.write_flush(line.as_bytes()).await
    }

    /// Read the next client operation from the wire.
    pub(crate) async fn read_client_op(&mut self) -> io::Result<Option<ClientOp>> {
        loop {
            if let Some(op) = self.try_parse_client_op()? {
                return Ok(Some(op));
            }
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                }
                return Err(io::ErrorKind::ConnectionReset.into());
            }
        }
    }

    fn try_parse_client_op(&mut self) -> io::Result<Option<ClientOp>> {
        let len = match memchr::memmem::find(&self.read_buf, b"\r\n") {
            Some(len) => len,
            None => return Ok(None),
        };

        if self.read_buf.starts_with(b"PING") {
            self.read_buf.advance(len + 2);
            return Ok(Some(ClientOp::Ping));
        }

        if self.read_buf.starts_with(b"PONG") {
            self.read_buf.advance(len + 2);
            return Ok(Some(ClientOp::Pong));
        }

        if self.read_buf.starts_with(b"CONNECT ") {
            let json_bytes = &self.read_buf[8..len];
            let info: ConnectInfo = serde_json::from_slice(json_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            self.read_buf.advance(len + 2);
            return Ok(Some(ClientOp::Connect(info)));
        }

        if self.read_buf.starts_with(b"SUB ") {
            let line = std::str::from_utf8(&self.read_buf[4..len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let mut args = line.split_whitespace();
            let op = match (args.next(), args.next(), args.next(), args.next()) {
                (Some(subject), Some(queue), Some(sid_str), None) => {
                    let sid = sid_str
                        .parse::<u64>()
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                    ClientOp::Subscribe {
                        sid,
                        subject: Subject::from(subject),
                        queue_group: Some(queue.to_string()),
                    }
                }
                (Some(subject), Some(sid_str), None, None) => {
                    let sid = sid_str
                        .parse::<u64>()
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                    ClientOp::Subscribe {
                        sid,
                        subject: Subject::from(subject),
                        queue_group: None,
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid SUB arguments",
                    ))
                }
            };
            self.read_buf.advance(len + 2);
            return Ok(Some(op));
        }

        if self.read_buf.starts_with(b"UNSUB ") {
            let line = std::str::from_utf8(&self.read_buf[6..len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let mut args = line.split_whitespace();
            let (sid_str, max_str) = (args.next(), args.next());
            let sid = sid_str
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing sid in UNSUB"))?
                .parse::<u64>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let max = max_str
                .map(|m| {
                    m.parse::<u64>()
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
                })
                .transpose()?;
            self.read_buf.advance(len + 2);
            return Ok(Some(ClientOp::Unsubscribe { sid, max }));
        }

        // HPUB subject [reply] hdr_len total_len\r\n[headers+payload]\r\n
        if self.read_buf.starts_with(b"HPUB ") {
            let line = std::str::from_utf8(&self.read_buf[5..len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let args: Vec<String> = line.split_whitespace().map(String::from).collect();

            let (subject, respond, hdr_len, total_len) = match args.len() {
                3 => {
                    let h = parse_usize(&args[1])?;
                    let t = parse_usize(&args[2])?;
                    (args[0].clone(), None, h, t)
                }
                4 => {
                    let h = parse_usize(&args[2])?;
                    let t = parse_usize(&args[3])?;
                    (args[0].clone(), Some(args[1].clone()), h, t)
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid HPUB arguments",
                    ))
                }
            };

            // Need full message: line\r\n + total_len bytes + \r\n
            if len + 2 + total_len + 2 > self.read_buf.len() {
                return Ok(None);
            }

            self.read_buf.advance(len + 2);
            let hdr_data = self.read_buf.split_to(hdr_len);
            let payload = self.read_buf.split_to(total_len - hdr_len).freeze();
            self.read_buf.advance(2);

            let headers = parse_headers(&hdr_data)?;

            return Ok(Some(ClientOp::Publish {
                subject: Subject::from(subject.as_str()),
                payload,
                respond: respond.map(|r| Subject::from(r.as_str())),
                headers: Some(headers),
            }));
        }

        // PUB subject [reply] size\r\n[payload]\r\n
        if self.read_buf.starts_with(b"PUB ") {
            let line = std::str::from_utf8(&self.read_buf[4..len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let args: Vec<String> = line.split_whitespace().map(String::from).collect();

            let (subject, respond, payload_len) = match args.len() {
                2 => {
                    let size = parse_usize(&args[1])?;
                    (args[0].clone(), None, size)
                }
                3 => {
                    let size = parse_usize(&args[2])?;
                    (args[0].clone(), Some(args[1].clone()), size)
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid PUB arguments",
                    ))
                }
            };

            if len + 2 + payload_len + 2 > self.read_buf.len() {
                return Ok(None);
            }

            self.read_buf.advance(len + 2);
            let payload = self.read_buf.split_to(payload_len).freeze();
            self.read_buf.advance(2);

            return Ok(Some(ClientOp::Publish {
                subject: Subject::from(subject.as_str()),
                payload,
                respond: respond.map(|r| Subject::from(r.as_str())),
                headers: None,
            }));
        }

        // Unknown command
        let unknown = self.read_buf.split_to(len + 2);
        let line = std::str::from_utf8(&unknown).unwrap_or("<invalid utf8>");
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown client operation: '{}'", line.trim()),
        ))
    }

    async fn write_flush(&mut self, data: &[u8]) -> io::Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        Ok(())
    }
}

fn parse_usize(s: &str) -> io::Result<usize> {
    s.parse::<usize>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}

pub(crate) fn parse_headers(data: &[u8]) -> io::Result<HeaderMap> {
    use std::str::FromStr;

    let text = std::str::from_utf8(data)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "header isn't valid utf-8"))?;

    let mut lines = text.lines().peekable();

    // Skip version line (NATS/1.0 ...)
    let _version = lines.next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "no header version line found")
    })?;

    let mut headers = HeaderMap::new();
    while let Some(line) = lines.next() {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid header line"))?;
        let name = HeaderName::from_str(name)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let mut value = value.trim_start().to_owned();
        while let Some(v) = lines.next_if(|s| s.starts_with(char::is_whitespace)) {
            value.push_str(v);
        }
        value.truncate(value.trim_end().len());
        headers.append(name, value.into_header_value());
    }

    Ok(headers)
}

/// Operations received from a hub over the leaf node protocol.
#[derive(Debug)]
pub(crate) enum LeafOp {
    Info(Box<ServerInfo>),
    Ping,
    Pong,
    Ok,
    Err(String),
    /// LS+ subject [queue]
    #[allow(dead_code)]
    LeafSub {
        subject: String,
        queue: Option<String>,
    },
    /// LS- subject [queue]
    #[allow(dead_code)]
    LeafUnsub {
        subject: String,
        queue: Option<String>,
    },
    /// LMSG – leaf message with optional reply and headers
    LeafMsg {
        subject: String,
        reply: Option<String>,
        headers: Option<HeaderMap>,
        payload: bytes::Bytes,
    },
}

/// Outgoing leaf node connection to a hub.
/// Owns the raw stream and its own read buffer for parsing hub operations.
pub(crate) struct LeafConn {
    stream: Box<dyn AsyncReadWrite>,
    read_buf: BytesMut,
}

impl LeafConn {
    pub(crate) fn new(stream: Box<dyn AsyncReadWrite>) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(64 * 1024),
        }
    }

    /// Read the next leaf operation from the hub.
    pub(crate) async fn read_leaf_op(&mut self) -> io::Result<Option<LeafOp>> {
        loop {
            if let Some(op) = self.try_parse_leaf_op()? {
                return Ok(Some(op));
            }
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                }
                return Err(io::ErrorKind::ConnectionReset.into());
            }
        }
    }

    fn try_parse_leaf_op(&mut self) -> io::Result<Option<LeafOp>> {
        let len = match memchr::memmem::find(&self.read_buf, b"\r\n") {
            Some(len) => len,
            None => return Ok(None),
        };

        if self.read_buf.starts_with(b"PING") {
            self.read_buf.advance(len + 2);
            return Ok(Some(LeafOp::Ping));
        }

        if self.read_buf.starts_with(b"PONG") {
            self.read_buf.advance(len + 2);
            return Ok(Some(LeafOp::Pong));
        }

        if self.read_buf.starts_with(b"+OK") {
            self.read_buf.advance(len + 2);
            return Ok(Some(LeafOp::Ok));
        }

        if self.read_buf.starts_with(b"-ERR") {
            let msg = std::str::from_utf8(&self.read_buf[5..len])
                .unwrap_or("<invalid utf8>")
                .trim_matches('\'')
                .to_string();
            self.read_buf.advance(len + 2);
            return Ok(Some(LeafOp::Err(msg)));
        }

        if self.read_buf.starts_with(b"INFO ") {
            let json_bytes = &self.read_buf[5..len];
            let info: ServerInfo = serde_json::from_slice(json_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.read_buf.advance(len + 2);
            return Ok(Some(LeafOp::Info(Box::new(info))));
        }

        if self.read_buf.starts_with(b"LS+ ") {
            let line = std::str::from_utf8(&self.read_buf[4..len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let mut args = line.split_whitespace();
            let subject = args
                .next()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing subject in LS+")
                })?
                .to_string();
            let queue = args.next().map(String::from);
            self.read_buf.advance(len + 2);
            return Ok(Some(LeafOp::LeafSub { subject, queue }));
        }

        if self.read_buf.starts_with(b"LS- ") {
            let line = std::str::from_utf8(&self.read_buf[4..len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let mut args = line.split_whitespace();
            let subject = args
                .next()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing subject in LS-")
                })?
                .to_string();
            let queue = args.next().map(String::from);
            self.read_buf.advance(len + 2);
            return Ok(Some(LeafOp::LeafUnsub { subject, queue }));
        }

        // LMSG subject [reply] <hdr_size total_size | size>\r\n<payload>\r\n
        if self.read_buf.starts_with(b"LMSG ") {
            let line = std::str::from_utf8(&self.read_buf[5..len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let args: Vec<&str> = line.split_whitespace().collect();

            return match args.len() {
                // LMSG subject size
                2 => {
                    let size = parse_usize(args[1])?;
                    if len + 2 + size + 2 > self.read_buf.len() {
                        return Ok(None);
                    }
                    let subject = args[0].to_string();
                    self.read_buf.advance(len + 2);
                    let payload = self.read_buf.split_to(size).freeze();
                    self.read_buf.advance(2);
                    Ok(Some(LeafOp::LeafMsg {
                        subject,
                        reply: None,
                        headers: None,
                        payload,
                    }))
                }
                // LMSG subject reply size  OR  LMSG subject hdr_size total_size
                3 => {
                    // Disambiguate: if both args[1] and args[2] parse as usize → headers
                    let a1 = args[1].parse::<usize>();
                    let a2 = args[2].parse::<usize>();
                    match (a1, a2) {
                        (Ok(hdr_size), Ok(total_size)) => {
                            // Headers variant (no reply)
                            if len + 2 + total_size + 2 > self.read_buf.len() {
                                return Ok(None);
                            }
                            let subject = args[0].to_string();
                            self.read_buf.advance(len + 2);
                            let hdr_data = self.read_buf.split_to(hdr_size);
                            let payload =
                                self.read_buf.split_to(total_size - hdr_size).freeze();
                            self.read_buf.advance(2);
                            let headers = parse_headers(&hdr_data)?;
                            Ok(Some(LeafOp::LeafMsg {
                                subject,
                                reply: None,
                                headers: Some(headers),
                                payload,
                            }))
                        }
                        _ => {
                            // Reply variant (no headers)
                            let size = parse_usize(args[2])?;
                            if len + 2 + size + 2 > self.read_buf.len() {
                                return Ok(None);
                            }
                            let subject = args[0].to_string();
                            let reply = args[1].to_string();
                            self.read_buf.advance(len + 2);
                            let payload = self.read_buf.split_to(size).freeze();
                            self.read_buf.advance(2);
                            Ok(Some(LeafOp::LeafMsg {
                                subject,
                                reply: Some(reply),
                                headers: None,
                                payload,
                            }))
                        }
                    }
                }
                // LMSG subject reply hdr_size total_size
                4 => {
                    let hdr_size = parse_usize(args[2])?;
                    let total_size = parse_usize(args[3])?;
                    if len + 2 + total_size + 2 > self.read_buf.len() {
                        return Ok(None);
                    }
                    let subject = args[0].to_string();
                    let reply = args[1].to_string();
                    self.read_buf.advance(len + 2);
                    let hdr_data = self.read_buf.split_to(hdr_size);
                    let payload = self.read_buf.split_to(total_size - hdr_size).freeze();
                    self.read_buf.advance(2);
                    let headers = parse_headers(&hdr_data)?;
                    Ok(Some(LeafOp::LeafMsg {
                        subject,
                        reply: Some(reply),
                        headers: Some(headers),
                        payload,
                    }))
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid LMSG arguments",
                )),
            };
        }

        // Unknown command
        let unknown = self.read_buf.split_to(len + 2);
        let line = std::str::from_utf8(&unknown).unwrap_or("<invalid utf8>");
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown leaf operation: '{}'", line.trim()),
        ))
    }

    /// Send CONNECT JSON to the hub.
    pub(crate) async fn send_connect(&mut self, info: &ConnectInfo) -> io::Result<()> {
        let json = serde_json::to_string(info)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let line = format!("CONNECT {json}\r\n");
        self.stream.write_all(line.as_bytes()).await
    }

    pub(crate) async fn send_ping(&mut self) -> io::Result<()> {
        self.stream.write_all(b"PING\r\n").await
    }

    pub(crate) async fn send_pong(&mut self) -> io::Result<()> {
        self.stream.write_all(b"PONG\r\n").await
    }

    /// Send LS+ subscription interest to the hub.
    pub(crate) async fn send_leaf_sub(&mut self, subject: &str) -> io::Result<()> {
        let line = format!("LS+ {subject}\r\n");
        self.stream.write_all(line.as_bytes()).await
    }

    /// Send LS- unsubscribe to the hub.
    pub(crate) async fn send_leaf_unsub(&mut self, subject: &str) -> io::Result<()> {
        let line = format!("LS- {subject}\r\n");
        self.stream.write_all(line.as_bytes()).await
    }

    /// Send LMSG to the hub.
    pub(crate) async fn send_leaf_msg(
        &mut self,
        subject: &str,
        reply: Option<&str>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload.len();
                let line = match reply {
                    Some(r) => format!("LMSG {subject} {r} {hdr_len} {total_len}\r\n"),
                    None => format!("LMSG {subject} {hdr_len} {total_len}\r\n"),
                };
                self.stream.write_all(line.as_bytes()).await?;
                self.stream.write_all(&hdr_bytes).await?;
                self.stream.write_all(payload).await?;
                self.stream.write_all(b"\r\n").await
            }
            _ => {
                let payload_len = payload.len();
                let line = match reply {
                    Some(r) => format!("LMSG {subject} {r} {payload_len}\r\n"),
                    None => format!("LMSG {subject} {payload_len}\r\n"),
                };
                self.stream.write_all(line.as_bytes()).await?;
                self.stream.write_all(payload).await?;
                self.stream.write_all(b"\r\n").await
            }
        }
    }

    /// Flush buffered writes to the wire.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio::io::duplex;

    async fn make_pair() -> (ServerConn, tokio::io::DuplexStream) {
        let (client_side, server_side) = duplex(8192);
        let conn = ServerConn::new(Box::new(server_side));
        (conn, client_side)
    }

    #[tokio::test]
    async fn test_send_info() {
        let (mut conn, mut client) = make_pair().await;
        let info = ServerInfo {
            server_id: "test".to_string(),
            max_payload: 1024 * 1024,
            proto: 1,
            headers: true,
            ..Default::default()
        };
        conn.send_info(&info).await.unwrap();

        let mut buf = BytesMut::with_capacity(4096);
        client.read_buf(&mut buf).await.unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        assert!(s.starts_with("INFO "));
        assert!(s.ends_with("\r\n"));
        assert!(s.contains("\"server_id\":\"test\""));
    }

    #[tokio::test]
    async fn test_parse_ping_pong() {
        let (mut conn, mut client) = make_pair().await;
        use tokio::io::AsyncWriteExt;
        client.write_all(b"PING\r\nPONG\r\n").await.unwrap();
        client.flush().await.unwrap();

        let op = conn.read_client_op().await.unwrap().unwrap();
        assert!(matches!(op, ClientOp::Ping));
        let op = conn.read_client_op().await.unwrap().unwrap();
        assert!(matches!(op, ClientOp::Pong));
    }

    #[tokio::test]
    async fn test_parse_sub() {
        let (mut conn, mut client) = make_pair().await;
        use tokio::io::AsyncWriteExt;
        client.write_all(b"SUB test.subject 1\r\n").await.unwrap();
        client
            .write_all(b"SUB test.queue myqueue 2\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();

        let op = conn.read_client_op().await.unwrap().unwrap();
        match op {
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                assert_eq!(sid, 1);
                assert_eq!(subject.as_str(), "test.subject");
                assert!(queue_group.is_none());
            }
            _ => panic!("expected Subscribe"),
        }

        let op = conn.read_client_op().await.unwrap().unwrap();
        match op {
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                assert_eq!(sid, 2);
                assert_eq!(subject.as_str(), "test.queue");
                assert_eq!(queue_group.as_deref(), Some("myqueue"));
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[tokio::test]
    async fn test_parse_pub() {
        let (mut conn, mut client) = make_pair().await;
        use tokio::io::AsyncWriteExt;
        client
            .write_all(b"PUB test.subject 5\r\nhello\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();

        let op = conn.read_client_op().await.unwrap().unwrap();
        match op {
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
            } => {
                assert_eq!(subject.as_str(), "test.subject");
                assert_eq!(payload.as_ref(), b"hello");
                assert!(respond.is_none());
                assert!(headers.is_none());
            }
            _ => panic!("expected Publish"),
        }
    }

    #[tokio::test]
    async fn test_parse_pub_with_reply() {
        let (mut conn, mut client) = make_pair().await;
        use tokio::io::AsyncWriteExt;
        client
            .write_all(b"PUB test.subject reply.to 5\r\nhello\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();

        let op = conn.read_client_op().await.unwrap().unwrap();
        match op {
            ClientOp::Publish {
                subject,
                respond,
                payload,
                ..
            } => {
                assert_eq!(subject.as_str(), "test.subject");
                assert_eq!(respond.as_ref().unwrap().as_str(), "reply.to");
                assert_eq!(payload.as_ref(), b"hello");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[tokio::test]
    async fn test_parse_connect() {
        let (mut conn, mut client) = make_pair().await;
        use tokio::io::AsyncWriteExt;
        client
            .write_all(
                b"CONNECT {\"verbose\":false,\"pedantic\":false,\"lang\":\"rust\",\"version\":\"0.1\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":true,\"tls_required\":false}\r\n",
            )
            .await
            .unwrap();
        client.flush().await.unwrap();

        let op = conn.read_client_op().await.unwrap().unwrap();
        match op {
            ClientOp::Connect(info) => {
                assert_eq!(info.lang, "rust");
                assert!(info.headers);
            }
            _ => panic!("expected Connect"),
        }
    }

    #[tokio::test]
    async fn test_parse_unsub() {
        let (mut conn, mut client) = make_pair().await;
        use tokio::io::AsyncWriteExt;
        client.write_all(b"UNSUB 1\r\n").await.unwrap();
        client.write_all(b"UNSUB 2 5\r\n").await.unwrap();
        client.flush().await.unwrap();

        let op = conn.read_client_op().await.unwrap().unwrap();
        assert!(matches!(op, ClientOp::Unsubscribe { sid: 1, max: None }));

        let op = conn.read_client_op().await.unwrap().unwrap();
        assert!(matches!(
            op,
            ClientOp::Unsubscribe {
                sid: 2,
                max: Some(5)
            }
        ));
    }

    #[tokio::test]
    async fn test_send_msg() {
        let (mut conn, mut client) = make_pair().await;
        conn.send_msg("test.sub", 1, None, None, b"hello")
            .await
            .unwrap();

        let mut buf = BytesMut::with_capacity(4096);
        client.read_buf(&mut buf).await.unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        assert_eq!(s, "MSG test.sub 1 5\r\nhello\r\n");
    }

    #[tokio::test]
    async fn test_send_msg_with_reply() {
        let (mut conn, mut client) = make_pair().await;
        conn.send_msg("test.sub", 1, Some("reply.to"), None, b"hi")
            .await
            .unwrap();

        let mut buf = BytesMut::with_capacity(4096);
        client.read_buf(&mut buf).await.unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        assert_eq!(s, "MSG test.sub 1 reply.to 2\r\nhi\r\n");
    }

    #[tokio::test]
    async fn test_eof_returns_none() {
        let (mut conn, client) = make_pair().await;
        drop(client);
        let result = conn.read_client_op().await.unwrap();
        assert!(result.is_none());
    }

    // --- LeafConn tests ---

    async fn make_leaf_pair() -> (LeafConn, tokio::io::DuplexStream) {
        let (hub_side, leaf_side) = duplex(8192);
        let conn = LeafConn::new(Box::new(leaf_side));
        (conn, hub_side)
    }

    #[tokio::test]
    async fn test_leaf_parse_info() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        use tokio::io::AsyncWriteExt;
        hub.write_all(b"INFO {\"server_id\":\"hub1\",\"max_payload\":1048576}\r\n")
            .await
            .unwrap();
        hub.flush().await.unwrap();

        let op = conn.read_leaf_op().await.unwrap().unwrap();
        match op {
            LeafOp::Info(info) => {
                assert_eq!(info.server_id, "hub1");
                assert_eq!(info.max_payload, 1048576);
            }
            _ => panic!("expected Info"),
        }
    }

    #[tokio::test]
    async fn test_leaf_parse_ping_pong_ok_err() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        use tokio::io::AsyncWriteExt;
        hub.write_all(b"PING\r\nPONG\r\n+OK\r\n-ERR 'test error'\r\n")
            .await
            .unwrap();
        hub.flush().await.unwrap();

        assert!(matches!(
            conn.read_leaf_op().await.unwrap().unwrap(),
            LeafOp::Ping
        ));
        assert!(matches!(
            conn.read_leaf_op().await.unwrap().unwrap(),
            LeafOp::Pong
        ));
        assert!(matches!(
            conn.read_leaf_op().await.unwrap().unwrap(),
            LeafOp::Ok
        ));
        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::Err(msg) => assert_eq!(msg, "test error"),
            _ => panic!("expected Err"),
        }
    }

    #[tokio::test]
    async fn test_leaf_parse_ls_sub_unsub() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        use tokio::io::AsyncWriteExt;
        hub.write_all(b"LS+ foo.bar\r\nLS+ baz.* myqueue\r\nLS- foo.bar\r\n")
            .await
            .unwrap();
        hub.flush().await.unwrap();

        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::LeafSub { subject, queue } => {
                assert_eq!(subject, "foo.bar");
                assert!(queue.is_none());
            }
            _ => panic!("expected LeafSub"),
        }
        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::LeafSub { subject, queue } => {
                assert_eq!(subject, "baz.*");
                assert_eq!(queue.as_deref(), Some("myqueue"));
            }
            _ => panic!("expected LeafSub"),
        }
        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::LeafUnsub { subject, queue } => {
                assert_eq!(subject, "foo.bar");
                assert!(queue.is_none());
            }
            _ => panic!("expected LeafUnsub"),
        }
    }

    #[tokio::test]
    async fn test_leaf_parse_lmsg_no_reply_no_headers() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        use tokio::io::AsyncWriteExt;
        hub.write_all(b"LMSG test.subject 5\r\nhello\r\n")
            .await
            .unwrap();
        hub.flush().await.unwrap();

        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(subject, "test.subject");
                assert!(reply.is_none());
                assert!(headers.is_none());
                assert_eq!(payload.as_ref(), b"hello");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[tokio::test]
    async fn test_leaf_parse_lmsg_with_reply() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        use tokio::io::AsyncWriteExt;
        hub.write_all(b"LMSG test.subject reply.to 5\r\nhello\r\n")
            .await
            .unwrap();
        hub.flush().await.unwrap();

        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(subject, "test.subject");
                assert_eq!(reply.as_deref(), Some("reply.to"));
                assert!(headers.is_none());
                assert_eq!(payload.as_ref(), b"hello");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[tokio::test]
    async fn test_leaf_parse_lmsg_with_headers() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        use tokio::io::AsyncWriteExt;
        let hdr = b"NATS/1.0\r\nX-Key: val\r\n\r\n";
        let payload = b"data";
        let hdr_len = hdr.len();
        let total_len = hdr_len + payload.len();
        let line = format!("LMSG test.subject {hdr_len} {total_len}\r\n");
        hub.write_all(line.as_bytes()).await.unwrap();
        hub.write_all(hdr).await.unwrap();
        hub.write_all(payload).await.unwrap();
        hub.write_all(b"\r\n").await.unwrap();
        hub.flush().await.unwrap();

        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(subject, "test.subject");
                assert!(reply.is_none());
                let hdrs = headers.unwrap();
                assert_eq!(
                    hdrs.get("X-Key").map(|v| v.to_string()),
                    Some("val".to_string())
                );
                assert_eq!(payload.as_ref(), b"data");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[tokio::test]
    async fn test_leaf_parse_lmsg_with_reply_and_headers() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        use tokio::io::AsyncWriteExt;
        let hdr = b"NATS/1.0\r\nFoo: bar\r\n\r\n";
        let payload = b"body";
        let hdr_len = hdr.len();
        let total_len = hdr_len + payload.len();
        let line = format!("LMSG test.subject reply.inbox {hdr_len} {total_len}\r\n");
        hub.write_all(line.as_bytes()).await.unwrap();
        hub.write_all(hdr).await.unwrap();
        hub.write_all(payload).await.unwrap();
        hub.write_all(b"\r\n").await.unwrap();
        hub.flush().await.unwrap();

        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(subject, "test.subject");
                assert_eq!(reply.as_deref(), Some("reply.inbox"));
                let hdrs = headers.unwrap();
                assert_eq!(
                    hdrs.get("Foo").map(|v| v.to_string()),
                    Some("bar".to_string())
                );
                assert_eq!(payload.as_ref(), b"body");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[tokio::test]
    async fn test_leaf_send_leaf_sub_unsub() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        conn.send_leaf_sub("foo.>").await.unwrap();
        conn.send_leaf_unsub("foo.>").await.unwrap();
        conn.flush().await.unwrap();

        let mut buf = BytesMut::with_capacity(4096);
        hub.read_buf(&mut buf).await.unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        assert_eq!(s, "LS+ foo.>\r\nLS- foo.>\r\n");
    }

    #[tokio::test]
    async fn test_leaf_send_lmsg_no_headers() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        conn.send_leaf_msg("test.sub", None, None, b"hello")
            .await
            .unwrap();
        conn.flush().await.unwrap();

        let mut buf = BytesMut::with_capacity(4096);
        hub.read_buf(&mut buf).await.unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        assert_eq!(s, "LMSG test.sub 5\r\nhello\r\n");
    }

    #[tokio::test]
    async fn test_leaf_send_lmsg_with_reply() {
        let (mut conn, mut hub) = make_leaf_pair().await;
        conn.send_leaf_msg("test.sub", Some("reply.to"), None, b"hi")
            .await
            .unwrap();
        conn.flush().await.unwrap();

        let mut buf = BytesMut::with_capacity(4096);
        hub.read_buf(&mut buf).await.unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        assert_eq!(s, "LMSG test.sub reply.to 2\r\nhi\r\n");
    }

    #[tokio::test]
    async fn test_leaf_eof_returns_none() {
        let (mut conn, hub) = make_leaf_pair().await;
        drop(hub);
        let result = conn.read_leaf_op().await.unwrap();
        assert!(result.is_none());
    }
}
