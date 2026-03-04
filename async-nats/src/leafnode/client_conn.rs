// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::connection::AsyncReadWrite;
use crate::ClientOp;

use super::protocol::ServerConn;
use super::server::ServerState;
use super::sub_list::Subscription;
use super::upstream::ClientMsg;

/// Handle held by the server for sending messages to a client connection.
pub(crate) struct ClientHandle {
    pub msg_tx: mpsc::Sender<ClientMsg>,
}

/// Per-client connection handler.
pub(crate) struct ClientConnection {
    id: u64,
    conn: ServerConn,
    state: Arc<ServerState>,
    msg_rx: mpsc::Receiver<ClientMsg>,
}

impl ClientConnection {
    pub(crate) fn new(
        id: u64,
        stream: Box<dyn AsyncReadWrite>,
        state: Arc<ServerState>,
    ) -> (Self, ClientHandle) {
        let (msg_tx, msg_rx) = mpsc::channel(256);
        let conn = ServerConn::new(stream);
        let handle = ClientHandle { msg_tx };
        let client = Self {
            id,
            conn,
            state,
            msg_rx,
        };
        (client, handle)
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
                // Could validate auth, version, etc. here
                debug!(id = self.id, "received CONNECT");
            }
            Some(other) => {
                self.conn
                    .send_err("expected CONNECT")
                    .await
                    .ok();
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

        // Some clients send PING right after CONNECT
        // We'll handle that in the message loop

        Ok(())
    }

    async fn message_loop(&mut self) -> std::io::Result<()> {
        loop {
            tokio::select! {
                op = self.conn.read_client_op() => {
                    match op? {
                        Some(op) => self.handle_client_op(op).await?,
                        None => return Ok(()), // clean disconnect
                    }
                }
                msg = self.msg_rx.recv() => {
                    match msg {
                        Some(msg) => {
                            self.conn.send_msg(
                                &msg.subject,
                                msg.sid,
                                msg.reply.as_deref(),
                                msg.headers.as_ref(),
                                &msg.payload,
                            ).await?;
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
                let sub = Subscription {
                    conn_id: self.id,
                    sid,
                    subject: subject.to_string(),
                    queue: queue_group,
                };

                let subject_str = subject.to_string();

                {
                    let mut subs = self.state.subs.write().await;
                    subs.insert(sub);
                }

                // Notify upstream
                {
                    let mut upstream = self.state.upstream.write().await;
                    if let Some(ref mut up) = *upstream {
                        if let Err(e) = up.add_interest(subject_str.clone()).await {
                            warn!(error = %e, "failed to add upstream interest");
                        }
                    }
                }

                debug!(id = self.id, sid, subject = %subject_str, "client subscribed");
            }
            ClientOp::Unsubscribe { sid, max: _ } => {
                let removed = {
                    let mut subs = self.state.subs.write().await;
                    subs.remove(self.id, sid)
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
            } => {
                let subject_str = subject.to_string();

                // Route to local subscribers
                {
                    let subs = self.state.subs.read().await;
                    let matches = subs.match_subject(&subject_str);
                    let conns = self.state.conns.read().await;
                    for sub in &matches {
                        if let Some(handle) = conns.get(&sub.conn_id) {
                            let msg = ClientMsg {
                                subject: subject_str.clone(),
                                sid: sub.sid,
                                reply: respond.as_ref().map(|r| r.to_string()),
                                headers: headers.clone(),
                                payload: payload.clone(),
                            };
                            // Non-blocking send; drop if the client is slow
                            let _ = handle.msg_tx.try_send(msg);
                        }
                    }
                }

                // Forward to upstream hub
                {
                    let upstream = self.state.upstream.read().await;
                    if let Some(ref up) = *upstream {
                        if let Err(e) = up
                            .publish(
                                subject_str,
                                respond.map(|r| r.to_string()),
                                headers,
                                payload,
                            )
                            .await
                        {
                            warn!(error = %e, "failed to forward publish to upstream");
                        }
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

async fn cleanup_conn(id: u64, state: &ServerState) {
    // Remove all subscriptions for this connection
    let removed = {
        let mut subs = state.subs.write().await;
        subs.remove_conn(id)
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

    // Remove connection handle
    {
        let mut conns = state.conns.write().await;
        conns.remove(&id);
    }

    info!(id, "client cleaned up");
}
