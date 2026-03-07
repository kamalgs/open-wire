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

use crate::sub_list::DirectWriter;
use crate::protocol::{LeafConn, LeafOp, LeafReader, LeafWriter};
use crate::server::ServerState;

/// Commands sent from the Upstream handle to the background writer task.
pub(crate) enum UpstreamCmd {
    Subscribe(String),
    Unsubscribe(String),
    Publish {
        subject: Bytes,
        reply: Option<Bytes>,
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

        let mut leaf = LeafConn::new(Box::new(tcp), state.buf_config);

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

        // 2. Send CONNECT (leaf-style, no `lang` field) + PING
        leaf.send_leaf_connect("rust-leaf", true).await?;
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
                Some(LeafOp::Info(_)) => {
                    // Hub may re-send INFO after CONNECT
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
            let subjects: Vec<String> = {
                let subs = state.subs.read().unwrap();
                subs.unique_subjects().into_iter().map(|s| s.to_string()).collect()
            };
            for subject in &subjects {
                leaf.send_leaf_sub(subject).await?;
            }
            leaf.flush().await?;
        }

        // 5. Split into independent reader/writer and spawn two tasks
        let (leaf_reader, leaf_writer) = leaf.split();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        // The reader sends Pong commands through the cmd channel
        let reader_cmd_tx = cmd_tx.clone();
        let reader_state = Arc::clone(&state);
        let reader_task = tokio::spawn(run_leaf_reader(leaf_reader, reader_cmd_tx, reader_state));
        let writer_task = tokio::spawn(run_leaf_writer(leaf_writer, cmd_rx));

        let task = tokio::spawn(async move {
            tokio::select! {
                _ = reader_task => {
                    debug!("leaf reader task finished");
                }
                _ = writer_task => {
                    debug!("leaf writer task finished");
                }
            }
        });

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

    /// Get a clone of the command sender for lock-free publish forwarding.
    pub(crate) fn sender(&self) -> mpsc::UnboundedSender<UpstreamCmd> {
        self.cmd_tx.clone()
    }

    /// Forward a publish from a local client to the hub as LMSG.
    #[allow(dead_code)]
    pub(crate) async fn publish(
        &self,
        subject: Bytes,
        reply: Option<Bytes>,
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

/// Background reader task: reads ops from the hub and dispatches them.
/// Runs independently from the writer, so hub PINGs are always answered
/// promptly even during write floods.
async fn run_leaf_reader(
    mut reader: LeafReader,
    cmd_tx: mpsc::UnboundedSender<UpstreamCmd>,
    state: Arc<ServerState>,
) {
    let mut dirty_writers: Vec<DirectWriter> = Vec::new();
    loop {
        match reader.read_leaf_op().await {
            Ok(Some(op)) => {
                if let Err(e) = handle_hub_op(op, &cmd_tx, &state, &mut dirty_writers) {
                    error!(error = %e, "error handling hub op");
                    break;
                }
                // Drain all remaining parseable ops from the read buffer
                // (pure in-memory parsing, no I/O) — same pattern as client_conn
                while let Some(op) = match reader.try_parse_leaf_op() {
                    Ok(op) => op,
                    Err(e) => {
                        error!(error = %e, "upstream parse error");
                        return;
                    }
                } {
                    if let Err(e) = handle_hub_op(op, &cmd_tx, &state, &mut dirty_writers) {
                        error!(error = %e, "error handling hub op");
                        return;
                    }
                }
                // Notify all dirty writers once after draining the batch
                for w in dirty_writers.drain(..) {
                    w.notify();
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

/// Background writer task: drains the command channel and writes to the hub.
/// Batches multiple commands before flushing for efficiency.
async fn run_leaf_writer(
    mut writer: LeafWriter,
    mut cmd_rx: mpsc::UnboundedReceiver<UpstreamCmd>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        if let Err(e) = process_cmd(&mut writer, &cmd).await {
            error!(error = %e, "upstream write error");
            break;
        }
        // Batch: drain any remaining commands without blocking
        while let Ok(cmd) = cmd_rx.try_recv() {
            if let Err(e) = process_cmd(&mut writer, &cmd).await {
                error!(error = %e, "upstream write error");
                return;
            }
        }
        if let Err(e) = writer.flush().await {
            error!(error = %e, "upstream flush error");
            break;
        }
    }
    debug!("upstream writer finished");
}

/// Process a single upstream command (write to hub, no flush).
async fn process_cmd(writer: &mut LeafWriter, cmd: &UpstreamCmd) -> std::io::Result<()> {
    match cmd {
        UpstreamCmd::Subscribe(subject) => {
            writer.send_leaf_sub(subject.as_bytes()).await?;
        }
        UpstreamCmd::Unsubscribe(subject) => {
            writer.send_leaf_unsub(subject.as_bytes()).await?;
        }
        UpstreamCmd::Publish {
            subject,
            reply,
            headers,
            payload,
        } => {
            writer
                .send_leaf_msg(subject, reply.as_deref(), headers.as_ref(), payload)
                .await?;
        }
        UpstreamCmd::Pong => {
            writer.send_pong().await?;
        }
    }
    Ok(())
}

/// Handle an operation received from the hub (reader side).
/// PING responses are sent via the command channel to the writer task.
/// Dirty writers (that had data written) are collected for batch notification.
fn handle_hub_op(
    op: LeafOp,
    cmd_tx: &mpsc::UnboundedSender<UpstreamCmd>,
    state: &ServerState,
    dirty_writers: &mut Vec<DirectWriter>,
) -> std::io::Result<()> {
    match op {
        LeafOp::LeafMsg {
            subject,
            reply,
            headers,
            payload,
        } => {
            // SAFETY: NATS subjects are always ASCII
            let subject_str = unsafe { std::str::from_utf8_unchecked(&subject) };
            let subs = state.subs.read().unwrap();
            subs.for_each_match(subject_str, |sub| {
                sub.writer.write_msg(
                    &subject,
                    &sub.sid_bytes,
                    reply.as_deref(),
                    headers.as_ref(),
                    &payload,
                );
                dirty_writers.push(sub.writer.clone());
            });
        }
        LeafOp::Ping => {
            // Send PONG via the writer task
            let _ = cmd_tx.send(UpstreamCmd::Pong);
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
