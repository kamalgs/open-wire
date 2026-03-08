// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::net::{Shutdown, TcpStream};
use std::sync::mpsc;
use std::sync::Arc;

use bytes::Bytes;
use tracing::{debug, error, warn};

use crate::types::HeaderMap;

use crate::protocol::{LeafConn, LeafOp, LeafReader, LeafWriter};
use crate::server::ServerState;
use crate::sub_list::DirectWriter;

/// Commands sent from the Upstream handle to the background writer thread.
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
    /// Signals the writer thread to shut down.
    Shutdown,
}

/// Manages connection to an upstream NATS hub server using the leaf node protocol.
/// Sends LS+/LS- for subscription interest and LMSG for messages.
pub(crate) struct Upstream {
    cmd_tx: mpsc::Sender<UpstreamCmd>,
    /// subject → refcount
    interests: HashMap<String, u32>,
    /// Kept for shutdown: closing this breaks the reader thread's blocking read.
    stream_shutdown: TcpStream,
}

impl Upstream {
    /// Connect to the hub using the leaf node protocol.
    ///
    /// Performs the INFO/CONNECT/PING/PONG handshake, then spawns background
    /// threads that read from the hub and write commands batched together.
    pub(crate) fn connect(
        hub_url: &str,
        state: Arc<ServerState>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let addr = parse_hub_addr(hub_url)?;
        let tcp = TcpStream::connect(&addr)?;
        tcp.set_nodelay(true)?;

        // Keep a clone for shutdown
        let stream_shutdown = tcp.try_clone()?;

        let mut leaf = LeafConn::new(tcp, state.buf_config);

        // --- Handshake ---
        // 1. Read INFO from hub
        match leaf.read_leaf_op()? {
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

        // 2. Send CONNECT (leaf-style) + PING
        leaf.send_leaf_connect("rust-leaf", true)?;
        leaf.send_ping()?;
        leaf.flush()?;

        // 3. Read until PONG (hub may send LS+ before PONG)
        loop {
            match leaf.read_leaf_op()? {
                Some(LeafOp::Pong) => {
                    debug!("handshake complete");
                    break;
                }
                Some(LeafOp::Ping) => {
                    leaf.send_pong()?;
                    leaf.flush()?;
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
                    return Err(format!("unexpected op during handshake: {other:?}").into());
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
                subs.unique_subjects()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect()
            };
            for subject in &subjects {
                leaf.send_leaf_sub(subject)?;
            }
            leaf.flush()?;
        }

        // 5. Split into independent reader/writer and spawn two threads
        let (leaf_reader, leaf_writer) = leaf.split()?;
        let (cmd_tx, cmd_rx) = mpsc::channel();

        // The reader sends Pong/Shutdown commands through the cmd channel
        let reader_cmd_tx = cmd_tx.clone();
        let reader_state = Arc::clone(&state);
        std::thread::Builder::new()
            .name("leaf-reader".into())
            .spawn(move || {
                run_leaf_reader(leaf_reader, reader_cmd_tx, reader_state);
            })
            .expect("failed to spawn leaf reader thread");

        std::thread::Builder::new()
            .name("leaf-writer".into())
            .spawn(move || {
                run_leaf_writer(leaf_writer, cmd_rx);
            })
            .expect("failed to spawn leaf writer thread");

        Ok(Self {
            cmd_tx,
            interests: HashMap::new(),
            stream_shutdown,
        })
    }

    /// Add a subscription interest for the given subject.
    /// If this is the first interest, sends LS+ to the hub.
    pub(crate) fn add_interest(
        &mut self,
        subject: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let count = self.interests.entry(subject.clone()).or_insert(0);
        *count += 1;
        if *count == 1 {
            self.cmd_tx
                .send(UpstreamCmd::Subscribe(subject))
                .map_err(|_| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "upstream thread gone",
                    )) as Box<dyn std::error::Error + Send + Sync>
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
    pub(crate) fn sender(&self) -> mpsc::Sender<UpstreamCmd> {
        self.cmd_tx.clone()
    }
}

impl Drop for Upstream {
    fn drop(&mut self) {
        // Shut down the TCP stream — breaks the reader thread's blocking read.
        self.stream_shutdown.shutdown(Shutdown::Both).ok();
        // Send shutdown to the writer thread (if channel still open).
        let _ = self.cmd_tx.send(UpstreamCmd::Shutdown);
        // Threads are detached — they'll exit on their own.
    }
}

/// Background reader: reads ops from the hub and dispatches them.
/// Runs independently from the writer, so hub PINGs are always answered
/// promptly even during write floods.
fn run_leaf_reader(
    mut reader: LeafReader,
    cmd_tx: mpsc::Sender<UpstreamCmd>,
    state: Arc<ServerState>,
) {
    let mut dirty_writers: Vec<DirectWriter> = Vec::new();
    loop {
        match reader.read_leaf_op() {
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
                        let _ = cmd_tx.send(UpstreamCmd::Shutdown);
                        return;
                    }
                } {
                    if let Err(e) = handle_hub_op(op, &cmd_tx, &state, &mut dirty_writers) {
                        error!(error = %e, "error handling hub op");
                        let _ = cmd_tx.send(UpstreamCmd::Shutdown);
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
    // Signal writer to shut down
    let _ = cmd_tx.send(UpstreamCmd::Shutdown);
}

/// Background writer: drains the command channel and writes to the hub.
/// Batches multiple commands before flushing for efficiency.
fn run_leaf_writer(mut writer: LeafWriter, cmd_rx: mpsc::Receiver<UpstreamCmd>) {
    while let Ok(cmd) = cmd_rx.recv() {
        if matches!(cmd, UpstreamCmd::Shutdown) {
            break;
        }
        if let Err(e) = process_cmd(&mut writer, &cmd) {
            error!(error = %e, "upstream write error");
            break;
        }
        // Batch: drain any remaining commands without blocking
        while let Ok(cmd) = cmd_rx.try_recv() {
            if matches!(cmd, UpstreamCmd::Shutdown) {
                debug!("upstream writer received shutdown");
                let _ = writer.flush();
                return;
            }
            if let Err(e) = process_cmd(&mut writer, &cmd) {
                error!(error = %e, "upstream write error");
                return;
            }
        }
        if let Err(e) = writer.flush() {
            error!(error = %e, "upstream flush error");
            break;
        }
    }
    debug!("upstream writer finished");
}

/// Process a single upstream command (write to hub, no flush).
fn process_cmd(writer: &mut LeafWriter, cmd: &UpstreamCmd) -> std::io::Result<()> {
    match cmd {
        UpstreamCmd::Subscribe(subject) => {
            writer.send_leaf_sub(subject.as_bytes())?;
        }
        UpstreamCmd::Unsubscribe(subject) => {
            writer.send_leaf_unsub(subject.as_bytes())?;
        }
        UpstreamCmd::Publish {
            subject,
            reply,
            headers,
            payload,
        } => {
            writer.send_leaf_msg(subject, reply.as_deref(), headers.as_ref(), payload)?;
        }
        UpstreamCmd::Pong => {
            writer.send_pong()?;
        }
        UpstreamCmd::Shutdown => {
            // Handled by caller
        }
    }
    Ok(())
}

/// Handle an operation received from the hub (reader side).
/// PING responses are sent via the command channel to the writer thread.
/// Dirty writers (that had data written) are collected for batch notification.
fn handle_hub_op(
    op: LeafOp,
    cmd_tx: &mpsc::Sender<UpstreamCmd>,
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
            // Send PONG via the writer thread
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
