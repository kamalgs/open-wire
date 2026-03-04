// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use bytes::{Buf, BytesMut};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

use crate::connection::AsyncReadWrite;
use crate::header::{HeaderMap, HeaderName, IntoHeaderValue};
use crate::subject::Subject;
use crate::{ClientOp, ConnectInfo, ServerInfo};

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
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing sid in UNSUB")
                })?
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

fn parse_headers(data: &[u8]) -> io::Result<HeaderMap> {
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
        let (name, value) = line.split_once(':').ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid header line")
        })?;
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
}
