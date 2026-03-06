// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use bytes::BytesMut;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufWriter, ReadHalf, WriteHalf};

use async_nats::connection::AsyncReadWrite;
use async_nats::header::HeaderMap;
use async_nats::ServerInfo;

use crate::nats_proto::{self, MsgBuilder};

// Re-export parsed op types so the rest of the crate uses nats_proto's types.
pub(crate) use crate::nats_proto::ClientOp;
pub(crate) use crate::nats_proto::LeafOp;

/// Server-side connection wrapper.
/// Uses split read/write halves so the writer is wrapped in a BufWriter.
/// This ensures `write_msg` calls go into a memory buffer and only hit the
/// socket on `flush()`, dramatically reducing syscalls when batching.
pub(crate) struct ServerConn {
    reader: ReadHalf<Box<dyn AsyncReadWrite>>,
    writer: BufWriter<WriteHalf<Box<dyn AsyncReadWrite>>>,
    read_buf: BytesMut,
    msg_builder: MsgBuilder,
}

impl ServerConn {
    pub(crate) fn new(stream: Box<dyn AsyncReadWrite>) -> Self {
        let (reader, writer) = io::split(stream);
        Self {
            reader,
            writer: BufWriter::with_capacity(64 * 1024, writer),
            read_buf: BytesMut::with_capacity(64 * 1024),
            msg_builder: MsgBuilder::new(),
        }
    }

    /// Send INFO to connected client.
    pub(crate) async fn send_info(&mut self, info: &ServerInfo) -> io::Result<()> {
        let json = serde_json::to_string(info)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let line = format!("INFO {json}\r\n");
        self.write_flush(line.as_bytes()).await
    }

    /// Send MSG to connected client (write + flush).
    #[cfg(test)]
    pub(crate) async fn send_msg(
        &mut self,
        subject: &str,
        sid: u64,
        reply: Option<&str>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        self.write_msg(subject.as_bytes(), sid, reply.map(|r| r.as_bytes()), headers, payload)
            .await?;
        self.flush().await
    }

    /// Write a MSG to the client without flushing.
    /// Uses direct byte assembly — no `write!()` formatting.
    #[allow(dead_code)]
    pub(crate) async fn write_msg(
        &mut self,
        subject: &[u8],
        sid: u64,
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        let sid_bytes = nats_proto::sid_to_bytes(sid);
        let data = self
            .msg_builder
            .build_msg(subject, &sid_bytes, reply, headers, payload);
        self.writer.write_all(data).await
    }

    /// Write a MSG for a ClientMsg (Bytes-based subject/reply) without flushing.
    pub(crate) async fn write_client_msg(
        &mut self,
        msg: &crate::client_conn::ClientMsg,
    ) -> io::Result<()> {
        let data = self.msg_builder.build_msg(
            &msg.subject,
            &msg.sid_bytes,
            msg.reply.as_deref(),
            msg.headers.as_ref(),
            &msg.payload,
        );
        self.writer.write_all(data).await
    }

    /// Flush buffered writes to the wire.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
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
            let n = self.reader.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                }
                return Err(io::ErrorKind::ConnectionReset.into());
            }
        }
    }

    pub(crate) fn try_parse_client_op(&mut self) -> io::Result<Option<ClientOp>> {
        nats_proto::try_parse_client_op(&mut self.read_buf)
    }

    async fn write_flush(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data).await?;
        self.writer.flush().await?;
        Ok(())
    }
}

/// Outgoing leaf node connection to a hub.
/// Owns the raw stream and its own read buffer for parsing hub operations.
/// Used during the handshake phase; call `split()` to get independent
/// reader/writer halves for the I/O loop.
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

    /// Split into independent reader and writer halves.
    /// The writer is wrapped in a 64KB BufWriter for batched I/O.
    pub(crate) fn split(self) -> (LeafReader, LeafWriter) {
        let (reader, writer) = io::split(self.stream);
        (
            LeafReader {
                reader,
                read_buf: self.read_buf,
            },
            LeafWriter {
                writer: BufWriter::with_capacity(64 * 1024, writer),
                msg_builder: MsgBuilder::new(),
            },
        )
    }

    /// Read the next leaf operation from the hub.
    pub(crate) async fn read_leaf_op(&mut self) -> io::Result<Option<LeafOp>> {
        loop {
            if let Some(op) = nats_proto::try_parse_leaf_op(&mut self.read_buf)? {
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

    /// Send a leaf node CONNECT to the hub.
    pub(crate) async fn send_leaf_connect(
        &mut self,
        name: &str,
        headers: bool,
    ) -> io::Result<()> {
        let json = serde_json::json!({
            "verbose": false,
            "pedantic": false,
            "headers": headers,
            "no_responders": true,
            "name": name,
            "version": "0.1.0",
            "protocol": 1,
        });
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

    /// Flush buffered writes to the wire.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }
}

/// Read half of a leaf connection.
pub(crate) struct LeafReader {
    reader: ReadHalf<Box<dyn AsyncReadWrite>>,
    read_buf: BytesMut,
}

impl LeafReader {
    /// Read the next leaf operation from the hub.
    pub(crate) async fn read_leaf_op(&mut self) -> io::Result<Option<LeafOp>> {
        loop {
            if let Some(op) = nats_proto::try_parse_leaf_op(&mut self.read_buf)? {
                return Ok(Some(op));
            }
            let n = self.reader.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                }
                return Err(io::ErrorKind::ConnectionReset.into());
            }
        }
    }
}

/// Write half of a leaf connection, wrapped in BufWriter.
pub(crate) struct LeafWriter {
    writer: BufWriter<WriteHalf<Box<dyn AsyncReadWrite>>>,
    msg_builder: MsgBuilder,
}

impl LeafWriter {
    /// Send LS+ subscription interest to the hub.
    pub(crate) async fn send_leaf_sub(&mut self, subject: &[u8]) -> io::Result<()> {
        let data = self.msg_builder.build_leaf_sub(subject);
        self.writer.write_all(data).await
    }

    /// Send LS- unsubscribe to the hub.
    pub(crate) async fn send_leaf_unsub(&mut self, subject: &[u8]) -> io::Result<()> {
        let data = self.msg_builder.build_leaf_unsub(subject);
        self.writer.write_all(data).await
    }

    /// Send PONG to the hub.
    pub(crate) async fn send_pong(&mut self) -> io::Result<()> {
        self.writer.write_all(b"PONG\r\n").await
    }

    /// Send LMSG to the hub.
    pub(crate) async fn send_leaf_msg(
        &mut self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        let data = self
            .msg_builder
            .build_lmsg(subject, reply, headers, payload);
        self.writer.write_all(data).await
    }

    /// Flush buffered writes to the wire.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
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
                assert_eq!(&subject[..], b"test.subject");
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
                assert_eq!(&subject[..], b"test.queue");
                assert_eq!(&queue_group.unwrap()[..], b"myqueue");
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
                ..
            } => {
                assert_eq!(&subject[..], b"test.subject");
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
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&respond.unwrap()[..], b"reply.to");
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
                assert_eq!(&subject[..], b"foo.bar");
                assert!(queue.is_none());
            }
            _ => panic!("expected LeafSub"),
        }
        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::LeafSub { subject, queue } => {
                assert_eq!(&subject[..], b"baz.*");
                assert_eq!(&queue.unwrap()[..], b"myqueue");
            }
            _ => panic!("expected LeafSub"),
        }
        match conn.read_leaf_op().await.unwrap().unwrap() {
            LeafOp::LeafUnsub { subject, queue } => {
                assert_eq!(&subject[..], b"foo.bar");
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
                assert_eq!(&subject[..], b"test.subject");
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
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&reply.unwrap()[..], b"reply.to");
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
                assert_eq!(&subject[..], b"test.subject");
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
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&reply.unwrap()[..], b"reply.inbox");
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
        let (conn, mut hub) = make_leaf_pair().await;
        let (_reader, mut writer) = conn.split();
        writer.send_leaf_sub(b"foo.>").await.unwrap();
        writer.send_leaf_unsub(b"foo.>").await.unwrap();
        writer.flush().await.unwrap();

        let mut buf = BytesMut::with_capacity(4096);
        hub.read_buf(&mut buf).await.unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        assert_eq!(s, "LS+ foo.>\r\nLS- foo.>\r\n");
    }

    #[tokio::test]
    async fn test_leaf_send_lmsg_no_headers() {
        let (conn, mut hub) = make_leaf_pair().await;
        let (_reader, mut writer) = conn.split();
        writer
            .send_leaf_msg(b"test.sub", None, None, b"hello")
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let mut buf = BytesMut::with_capacity(4096);
        hub.read_buf(&mut buf).await.unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        assert_eq!(s, "LMSG test.sub 5\r\nhello\r\n");
    }

    #[tokio::test]
    async fn test_leaf_send_lmsg_with_reply() {
        let (conn, mut hub) = make_leaf_pair().await;
        let (_reader, mut writer) = conn.split();
        writer
            .send_leaf_msg(b"test.sub", Some(b"reply.to"), None, b"hi")
            .await
            .unwrap();
        writer.flush().await.unwrap();

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
