// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use async_nats::connection::AsyncReadWrite;

use crate::nats_proto;
use crate::protocol::{ClientOp, ServerConn};
use crate::server::ServerState;
use crate::sub_list::{ClientMsg, Subscription};
use crate::upstream::UpstreamCmd;

/// Per-client connection handler.
pub(crate) struct ClientConnection {
    id: u64,
    conn: ServerConn,
    state: Arc<ServerState>,
    /// Sender cloned into each Subscription for direct message delivery.
    msg_tx: mpsc::UnboundedSender<ClientMsg>,
    msg_rx: mpsc::UnboundedReceiver<ClientMsg>,
    /// Cached upstream sender — populated once after handshake to avoid
    /// RwLock read + Arc clone on every publish.
    upstream_tx: Option<mpsc::UnboundedSender<UpstreamCmd>>,
}

impl ClientConnection {
    pub(crate) fn new(
        id: u64,
        stream: Box<dyn AsyncReadWrite>,
        state: Arc<ServerState>,
    ) -> Self {
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        let conn = ServerConn::new(stream, state.buf_config);
        Self {
            id,
            conn,
            state,
            msg_tx,
            msg_rx,
            upstream_tx: None,
        }
    }

    /// Run the client connection handler.
    /// Returns when the client disconnects or an error occurs.
    pub(crate) async fn run(mut self) {
        if let Err(e) = self.handshake().await {
            warn!(id = self.id, error = %e, "client handshake failed");
            // Still need to cleanup even on handshake failure
            let id = self.id;
            let state = self.state.clone();
            drop(self);
            cleanup_conn(id, &state).await;
            return;
        }

        info!(id = self.id, "client connected");

        // Cache the upstream sender once — avoids RwLock read per publish
        self.upstream_tx = self.state.upstream_tx.read().unwrap().clone();

        let result = self.message_loop().await;
        if let Err(e) = &result {
            debug!(id = self.id, error = %e, "client disconnected with error");
        } else {
            debug!(id = self.id, "client disconnected");
        }

        let id = self.id;
        let state = self.state.clone();
        drop(self);
        cleanup_conn(id, &state).await;
    }

    async fn handshake(&mut self) -> std::io::Result<()> {
        // Send INFO
        let info = self.state.info.clone();
        self.conn.send_info(&info).await?;

        // Read CONNECT
        match self.conn.read_client_op().await? {
            Some(ClientOp::Connect(_connect_info)) => {
                debug!(id = self.id, "received CONNECT");
            }
            Some(other) => {
                self.conn.send_err("expected CONNECT").await.ok();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("expected CONNECT, got {:?}", other),
                ));
            }
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed before CONNECT",
                ));
            }
        }

        Ok(())
    }

    /// Check if publishes can be skipped (no subscribers, no upstream).
    /// Uses atomic flag — no lock acquisition.
    #[inline]
    fn can_skip_publish(&self) -> bool {
        self.upstream_tx.is_none()
            && !self.state.has_subs.load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn message_loop(&mut self) -> std::io::Result<()> {
        loop {
            let skip = self.can_skip_publish();
            tokio::select! {
                op = self.conn.read_client_op_inner(skip) => {
                    match op? {
                        Some(op) => {
                            self.handle_client_op(op).await?;
                            // Drain all remaining parseable ops from read buffer.
                            // Use skip path when no one is listening.
                            if skip {
                                while let Some(op) =
                                    self.conn.try_skip_or_parse_client_op()?
                                {
                                    self.handle_client_op(op).await?;
                                }
                            } else {
                                while let Some(op) = self.conn.try_parse_client_op()? {
                                    self.handle_client_op(op).await?;
                                }
                            }
                        }
                        None => return Ok(()), // clean disconnect
                    }
                }
                msg = self.msg_rx.recv() => {
                    match msg {
                        Some(msg) => {
                            // Write first message without flushing
                            self.conn.write_client_msg(&msg).await?;
                            // Drain any additional queued messages (batch write)
                            while let Ok(msg) = self.msg_rx.try_recv() {
                                self.conn.write_client_msg(&msg).await?;
                            }
                            // Single flush for the whole batch
                            self.conn.flush().await?;
                        }
                        None => return Ok(()), // server shutting down
                    }
                }
            }
        }
    }

    async fn handle_client_op(&mut self, op: ClientOp) -> std::io::Result<()> {
        match op {
            ClientOp::Ping => {
                self.conn.send_pong().await?;
            }
            ClientOp::Pong => {
                // Keepalive response, no action needed
            }
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                // SAFETY: NATS subjects are always valid UTF-8 (ASCII subset)
                let subject_str = bytes_to_str(&subject);
                let queue_str = queue_group.as_ref().map(|q| bytes_to_str(q).to_string());

                let sub = Subscription {
                    conn_id: self.id,
                    sid,
                    sid_bytes: nats_proto::sid_to_bytes(sid),
                    subject: subject_str.to_string(),
                    queue: queue_str,
                    msg_tx: self.msg_tx.clone(),
                };

                {
                    let mut subs = self.state.subs.write().unwrap();
                    subs.insert(sub);
                    self.state
                        .has_subs
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }

                // Notify upstream
                {
                    let mut upstream = self.state.upstream.write().await;
                    if let Some(ref mut up) = *upstream {
                        if let Err(e) = up.add_interest(subject_str.to_string()).await {
                            warn!(error = %e, "failed to add upstream interest");
                        }
                    }
                }

                debug!(id = self.id, sid, subject = %subject_str, "client subscribed");
            }
            ClientOp::Unsubscribe { sid, max: _ } => {
                let removed = {
                    let mut subs = self.state.subs.write().unwrap();
                    let r = subs.remove(self.id, sid);
                    self.state
                        .has_subs
                        .store(!subs.is_empty(), std::sync::atomic::Ordering::Relaxed);
                    r
                };

                if let Some(removed) = removed {
                    let mut upstream = self.state.upstream.write().await;
                    if let Some(ref mut up) = *upstream {
                        up.remove_interest(&removed.subject);
                    }
                    debug!(id = self.id, sid, subject = %removed.subject, "client unsubscribed");
                }
            }
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
                ..
            } => {
                // Route to local subscribers — direct send, no conns lookup
                {
                    let subject_str = bytes_to_str(&subject);
                    let subs = self.state.subs.read().unwrap();
                    subs.for_each_match(subject_str, |sub| {
                        let msg = ClientMsg {
                            subject: subject.clone(),
                            sid_bytes: sub.sid_bytes.clone(),
                            reply: respond.clone(),
                            headers: headers.clone(),
                            payload: payload.clone(),
                        };
                        // Non-blocking send; drop if the client is slow
                        let _ = sub.msg_tx.send(msg);
                    });
                }

                // Forward to upstream hub (cached sender, no lock)
                if let Some(ref tx) = self.upstream_tx {
                    if let Err(e) = tx.send(UpstreamCmd::Publish {
                        subject,
                        reply: respond,
                        headers,
                        payload,
                    }) {
                        warn!(error = %e, "failed to forward publish to upstream");
                    }
                }
            }
            ClientOp::Connect(_) => {
                // Duplicate CONNECT, ignore
            }
        }
        Ok(())
    }
}

/// Convert `Bytes` to `&str` without UTF-8 validation.
/// NATS subjects are restricted to ASCII printable characters.
#[inline]
fn bytes_to_str(b: &Bytes) -> &str {
    // SAFETY: NATS protocol subjects/reply-to are always ASCII
    unsafe { std::str::from_utf8_unchecked(b) }
}

async fn cleanup_conn(id: u64, state: &ServerState) {
    // Remove all subscriptions for this connection.
    // This also drops the msg_tx senders stored in each Subscription.
    let removed = {
        let mut subs = state.subs.write().unwrap();
        let r = subs.remove_conn(id);
        state
            .has_subs
            .store(!subs.is_empty(), std::sync::atomic::Ordering::Relaxed);
        r
    };

    // Update upstream interests
    if !removed.is_empty() {
        let mut upstream = state.upstream.write().await;
        if let Some(ref mut up) = *upstream {
            for sub in &removed {
                up.remove_interest(&sub.subject);
            }
        }
    }

    info!(id, "client cleaned up");
}

