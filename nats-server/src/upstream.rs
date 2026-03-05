// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use async_nats::header::HeaderMap;
use async_nats::{ConnectInfo, Protocol};

use crate::protocol::{LeafConn, LeafOp};
use crate::server::ServerState;

/// Commands sent from the Upstream handle to the background writer task.
enum UpstreamCmd {
    Subscribe(String),
    Unsubscribe(String),
    Publish {
        subject: String,
        reply: Option<String>,
        headers: Option<HeaderMap>,
        payload: Bytes,
    },
    #[allow(dead_code)]
    Pong,
}

/// Manages connection to an upstream NATS hub server using the leaf node protocol.
/// Sends LS+/LS- for subscription interest and LMSG for messages.
pub(crate) struct Upstream {
    cmd_tx: mpsc::UnboundedSender<UpstreamCmd>,
    /// subject → refcount
    interests: HashMap<String, u32>,
    task: JoinHandle<()>,
}

impl Upstream {
    /// Connect to the hub using the leaf node protocol.
    ///
    /// Performs the INFO/CONNECT/PING/PONG handshake, then spawns a background
    /// task that reads from the hub and writes commands batched together.
    pub(crate) async fn connect(
        hub_url: &str,
        state: Arc<ServerState>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let addr = parse_hub_addr(hub_url)?;
        let tcp = TcpStream::connect(&addr).await?;
        tcp.set_nodelay(true)?;

        let mut leaf = LeafConn::new(Box::new(tcp));

        // --- Handshake ---
        // 1. Read INFO from hub
        match leaf.read_leaf_op().await? {
            Some(LeafOp::Info(_info)) => {
                debug!("received INFO from hub");
            }
            Some(other) => {
                return Err(format!("expected INFO from hub, got: {other:?}").into());
            }
            None => {
                return Err("hub closed connection before INFO".into());
            }
        }

        // 2. Send CONNECT + PING
        let connect_info = ConnectInfo {
            verbose: false,
            pedantic: false,
            lang: "rust".to_string(),
            version: "0.1.0".to_string(),
            protocol: Protocol::Dynamic,
            echo: false,
            headers: true,
            no_responders: true,
            tls_required: false,
            ..Default::default()
        };
        leaf.send_connect(&connect_info).await?;
        leaf.send_ping().await?;
        leaf.flush().await?;

        // 3. Read until PONG (hub may send LS+ before PONG)
        loop {
            match leaf.read_leaf_op().await? {
                Some(LeafOp::Pong) => {
                    debug!("handshake complete");
                    break;
                }
                Some(LeafOp::Ping) => {
                    leaf.send_pong().await?;
                    leaf.flush().await?;
                }
                Some(LeafOp::LeafSub { .. }) | Some(LeafOp::LeafUnsub { .. }) => {
                    // Hub sending its interests; ignored for now
                }
                Some(LeafOp::Err(msg)) => {
                    return Err(format!("hub error during handshake: {msg}").into());
                }
                Some(LeafOp::Ok) => {
                    // Some hubs send +OK after CONNECT
                }
                Some(other) => {
                    return Err(
                        format!("unexpected op during handshake: {other:?}").into()
                    );
                }
                None => {
                    return Err("hub closed connection during handshake".into());
                }
            }
        }

        // 4. Send LS+ for any existing local subscriptions
        {
            let subs = state.subs.read().await;
            for subject in subs.unique_subjects() {
                leaf.send_leaf_sub(subject).await?;
            }
            leaf.flush().await?;
        }

        // 5. Split into reader/writer and spawn background task
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let task = tokio::spawn(run_leaf_io(leaf, cmd_rx, state));

        Ok(Self {
            cmd_tx,
            interests: HashMap::new(),
            task,
        })
    }

    /// Add a subscription interest for the given subject.
    /// If this is the first interest, sends LS+ to the hub.
    pub(crate) async fn add_interest(
        &mut self,
        subject: String,
    ) -> Result<(), async_nats::Error> {
        let count = self.interests.entry(subject.clone()).or_insert(0);
        *count += 1;
        if *count == 1 {
            self.cmd_tx
                .send(UpstreamCmd::Subscribe(subject))
                .map_err(|_| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "upstream task gone",
                    )) as async_nats::Error
                })?;
        }
        Ok(())
    }

    /// Remove a subscription interest. If refcount reaches zero, sends LS- to the hub.
    pub(crate) fn remove_interest(&mut self, subject: &str) {
        if let Some(count) = self.interests.get_mut(subject) {
            *count -= 1;
            if *count == 0 {
                self.interests.remove(subject);
                let _ = self
                    .cmd_tx
                    .send(UpstreamCmd::Unsubscribe(subject.to_string()));
            }
        }
    }

    /// Forward a publish from a local client to the hub as LMSG.
    pub(crate) async fn publish(
        &self,
        subject: String,
        reply: Option<String>,
        headers: Option<HeaderMap>,
        payload: Bytes,
    ) -> Result<(), async_nats::Error> {
        self.cmd_tx
            .send(UpstreamCmd::Publish {
                subject,
                reply,
                headers,
                payload,
            })
            .map_err(|_| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "upstream task gone",
                )) as async_nats::Error
            })?;
        Ok(())
    }
}

impl Drop for Upstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Background task: reads from the hub and writes commands to it.
///
/// Uses a single `LeafConn` that handles both reading and writing.
/// The writer side drains the command channel in a batch before flushing.
async fn run_leaf_io(
    mut leaf: LeafConn,
    mut cmd_rx: mpsc::UnboundedReceiver<UpstreamCmd>,
    state: Arc<ServerState>,
) {
    // We need to split reading and writing. Since LeafConn owns the stream,
    // we'll use a channel approach: the reader and writer share access via
    // an inner loop that alternates between reading and writing.
    loop {
        tokio::select! {
            biased;

            // Drain commands first (writer path)
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(cmd) => {
                        if let Err(e) = process_cmd(&mut leaf, &cmd).await {
                            error!(error = %e, "upstream write error");
                            break;
                        }
                        // Batch: drain any remaining commands without blocking
                        while let Ok(cmd) = cmd_rx.try_recv() {
                            if let Err(e) = process_cmd(&mut leaf, &cmd).await {
                                error!(error = %e, "upstream write error");
                                return;
                            }
                        }
                        if let Err(e) = leaf.flush().await {
                            error!(error = %e, "upstream flush error");
                            break;
                        }
                    }
                    None => {
                        debug!("upstream command channel closed");
                        break;
                    }
                }
            }

            // Read from hub (reader path)
            op = leaf.read_leaf_op() => {
                match op {
                    Ok(Some(leaf_op)) => {
                        if let Err(e) = handle_hub_op(&mut leaf, leaf_op, &state).await {
                            error!(error = %e, "error handling hub op");
                            break;
                        }
                    }
                    Ok(None) => {
                        warn!("hub connection closed");
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, "upstream read error");
                        break;
                    }
                }
            }
        }
    }
}

/// Process a single upstream command (write to hub, no flush).
async fn process_cmd(leaf: &mut LeafConn, cmd: &UpstreamCmd) -> std::io::Result<()> {
    match cmd {
        UpstreamCmd::Subscribe(subject) => {
            leaf.send_leaf_sub(subject).await?;
        }
        UpstreamCmd::Unsubscribe(subject) => {
            leaf.send_leaf_unsub(subject).await?;
        }
        UpstreamCmd::Publish {
            subject,
            reply,
            headers,
            payload,
        } => {
            leaf.send_leaf_msg(subject, reply.as_deref(), headers.as_ref(), payload)
                .await?;
        }
        UpstreamCmd::Pong => {
            leaf.send_pong().await?;
        }
    }
    Ok(())
}

/// Handle an operation received from the hub.
async fn handle_hub_op(
    leaf: &mut LeafConn,
    op: LeafOp,
    state: &ServerState,
) -> std::io::Result<()> {
    match op {
        LeafOp::LeafMsg {
            subject,
            reply,
            headers,
            payload,
        } => {
            let subs = state.subs.read().await;
            let matches = subs.match_subject(&subject);
            if matches.is_empty() {
                return Ok(());
            }

            let conns = state.conns.read().await;
            for sub in &matches {
                if let Some(handle) = conns.get(&sub.conn_id) {
                    let msg = ClientMsg {
                        subject: subject.clone(),
                        sid: sub.sid,
                        reply: reply.clone(),
                        headers: headers.clone(),
                        payload: payload.clone(),
                    };
                    let _ = handle.msg_tx.send(msg).await;
                }
            }
        }
        LeafOp::Ping => {
            leaf.send_pong().await?;
            leaf.flush().await?;
        }
        LeafOp::Pong | LeafOp::Ok => {
            // No action needed
        }
        LeafOp::LeafSub { .. } | LeafOp::LeafUnsub { .. } => {
            // Hub interest changes; ignored for now
        }
        LeafOp::Info(_) => {
            debug!("received updated INFO from hub");
        }
        LeafOp::Err(msg) => {
            warn!(msg = %msg, "hub sent error");
        }
    }
    Ok(())
}

/// Parse a hub URL like "nats://host:port" into "host:port".
fn parse_hub_addr(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let stripped = url
        .strip_prefix("nats://")
        .or_else(|| url.strip_prefix("nats-leaf://"))
        .unwrap_or(url);
    if stripped.contains(':') {
        Ok(stripped.to_string())
    } else {
        // Default leafnode port
        Ok(format!("{stripped}:7422"))
    }
}

/// A message to be delivered to a local client connection.
#[derive(Debug)]
pub(crate) struct ClientMsg {
    pub subject: String,
    pub sid: u64,
    pub reply: Option<String>,
    pub headers: Option<HeaderMap>,
    pub payload: Bytes,
}
