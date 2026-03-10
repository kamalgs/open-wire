// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::{HashMap, HashSet};
use std::io::{self, Write as _};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tracing::{debug, error, info, warn};

use crate::interest::InterestPipeline;
use crate::types::HeaderMap;

use crate::protocol::{LeafConn, LeafOp, LeafReader, LeafWriter, UpstreamConnectCreds};
use crate::server::{HubCredentials, ServerState};
use crate::sub_list::DirectWriter;

/// Commands sent from the Upstream handle to the background writer thread.
#[derive(Debug)]
pub(crate) enum UpstreamCmd {
    Subscribe(String, Option<String>),
    Unsubscribe(String, Option<String>),
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

/// Exponential backoff with jitter for reconnection attempts.
pub(crate) struct Backoff {
    current: Duration,
    initial: Duration,
    max: Duration,
}

impl Backoff {
    /// Create a new backoff starting at `initial`, capping at `max`.
    pub(crate) fn new(initial: Duration, max: Duration) -> Self {
        Self {
            current: initial,
            initial,
            max,
        }
    }

    /// Return the next backoff duration (with ±25% jitter) and advance.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let base = self.current;
        // Double for next time, capped
        self.current = (self.current * 2).min(self.max);
        // Apply ±25% jitter
        let jitter_range = base.as_millis() as f64 * 0.25;
        let jitter = (rand::random::<f64>() - 0.5) * 2.0 * jitter_range;
        let ms = (base.as_millis() as f64 + jitter).max(1.0) as u64;
        Duration::from_millis(ms)
    }

    /// Reset backoff to the initial value (called on successful connect).
    pub(crate) fn reset(&mut self) {
        self.current = self.initial;
    }
}

/// Manages connection to an upstream NATS hub server using the leaf node protocol.
/// Sends LS+/LS- for subscription interest and LMSG for messages.
pub(crate) struct Upstream {
    cmd_tx: mpsc::Sender<UpstreamCmd>,
    /// (subject, queue) → refcount
    interests: HashMap<(String, Option<String>), u32>,
    /// Shutdown flag for the supervisor thread.
    shutdown: Arc<AtomicBool>,
    /// Kept for shutdown: closing this breaks the reader thread's blocking read.
    /// Wrapped in Option because it may not exist if not yet connected.
    stream_shutdown: Option<TcpStream>,
    /// Interest transform pipeline (mapping + collapse).
    pipeline: InterestPipeline,
}

impl Upstream {
    /// Connect to the hub using the leaf node protocol.
    ///
    /// Performs the INFO/CONNECT/PING/PONG handshake, then spawns background
    /// threads that read from the hub and write commands batched together.
    pub(crate) fn connect(
        hub_url: &str,
        config_creds: Option<&HubCredentials>,
        state: Arc<ServerState>,
        pipeline: InterestPipeline,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (cmd_tx, stream_shutdown) = connect_and_run(hub_url, config_creds, &state, &pipeline)?;

        // Store the sender in server state for workers
        *state.upstream_tx.write().unwrap() = Some(cmd_tx.clone());

        let shutdown = Arc::new(AtomicBool::new(false));

        Ok(Self {
            cmd_tx,
            interests: HashMap::new(),
            shutdown,
            stream_shutdown: Some(stream_shutdown),
            pipeline,
        })
    }

    /// Spawn a supervisor that connects to the hub with automatic reconnection.
    /// Initial connection failure is non-fatal — the supervisor keeps retrying.
    pub(crate) fn spawn_supervisor<F>(
        hub_url: String,
        config_creds: Option<HubCredentials>,
        state: Arc<ServerState>,
        build_pipeline: F,
    ) -> Self
    where
        F: Fn() -> InterestPipeline + Send + 'static,
    {
        let shutdown = Arc::new(AtomicBool::new(false));
        let (cmd_tx, cmd_rx) = mpsc::channel();

        let supervisor_shutdown = Arc::clone(&shutdown);
        let supervisor_state = Arc::clone(&state);
        let supervisor_tx = cmd_tx.clone();
        std::thread::Builder::new()
            .name("upstream-supervisor".into())
            .spawn(move || {
                run_supervisor(
                    hub_url,
                    config_creds,
                    supervisor_state,
                    supervisor_shutdown,
                    supervisor_tx,
                    cmd_rx,
                    &build_pipeline,
                );
            })
            .expect("failed to spawn upstream supervisor");

        Self {
            cmd_tx,
            interests: HashMap::new(),
            shutdown,
            stream_shutdown: None,
            pipeline: InterestPipeline::new(
                #[cfg(feature = "interest-collapse")]
                vec![],
                #[cfg(feature = "subject-mapping")]
                vec![],
            ),
        }
    }

    /// Add a subscription interest for the given subject and optional queue group.
    ///
    /// Tracks exact (subject, queue) refcounts for dedup. On the first occurrence
    /// of each unique subject, passes it through the interest pipeline (mapping +
    /// collapse) to determine what `LS+` to send upstream (if any).
    pub(crate) fn add_interest(
        &mut self,
        subject: String,
        queue: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = (subject.clone(), queue.clone());
        let count = self.interests.entry(key).or_insert(0);
        *count += 1;
        if *count > 1 {
            return Ok(()); // exact dedup
        }

        if let Some((subj, q)) = self.pipeline.on_subscribe(&subject, queue.as_deref()) {
            self.cmd_tx
                .send(UpstreamCmd::Subscribe(subj, q))
                .map_err(|_| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "upstream thread gone",
                    )) as Box<dyn std::error::Error + Send + Sync>
                })?;
        }
        Ok(())
    }

    /// Remove a subscription interest. Sends `LS-` upstream when the pipeline
    /// determines it is appropriate (last exact sub, or last collapsed sub).
    pub(crate) fn remove_interest(&mut self, subject: &str, queue: Option<&str>) {
        let key = (subject.to_string(), queue.map(|q| q.to_string()));
        if let Some(count) = self.interests.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                self.interests.remove(&key);
            } else {
                return; // still has exact refs
            }

            if let Some((subj, q)) = self.pipeline.on_unsubscribe(subject, queue) {
                let _ = self.cmd_tx.send(UpstreamCmd::Unsubscribe(subj, q));
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
        // Signal supervisor to stop.
        self.shutdown.store(true, Ordering::Release);
        // Shut down the TCP stream if we have one — breaks the reader thread.
        if let Some(ref stream) = self.stream_shutdown {
            stream.shutdown(Shutdown::Both).ok();
        }
        // Send shutdown to the writer thread (if channel still open).
        let _ = self.cmd_tx.send(UpstreamCmd::Shutdown);
        // Threads are detached — they'll exit on their own.
    }
}

/// Connect to hub, perform handshake, sync interests, spawn reader/writer threads.
/// Returns (cmd_tx, stream_for_shutdown) on success.
fn connect_and_run(
    hub_url: &str,
    config_creds: Option<&HubCredentials>,
    state: &Arc<ServerState>,
    pipeline: &InterestPipeline,
) -> Result<(mpsc::Sender<UpstreamCmd>, TcpStream), Box<dyn std::error::Error>> {
    let parsed = parse_hub_url(hub_url)?;
    let tcp = TcpStream::connect(&parsed.addr)?;
    tcp.set_nodelay(true)?;

    let stream_shutdown = tcp.try_clone()?;

    let mut leaf = if parsed.use_tls {
        let tls_config = crate::server::build_tls_client_config();
        let server_name = rustls_pki_types::ServerName::try_from(parsed.host.clone())
            .map_err(|e| format!("invalid TLS server name '{}': {e}", parsed.host))?;
        let tls_conn = rustls::ClientConnection::new(tls_config, server_name)
            .map_err(|e| format!("TLS client connection failed: {e}"))?;
        // Perform TLS handshake using blocking I/O via StreamOwned
        let mut tls_stream = rustls::StreamOwned::new(tls_conn, tcp);
        // Force handshake by flushing (rustls completes handshake on first I/O)
        tls_stream.flush()?;
        let (tls_conn, tcp) = tls_stream.into_parts();
        LeafConn::new_tls(tcp, tls_conn, state.buf_config)
    } else {
        LeafConn::new(tcp, state.buf_config)
    };

    let merged = merge_hub_credentials(&parsed.creds, config_creds);

    // --- Handshake ---
    let hub_nonce = match leaf.read_leaf_op()? {
        Some(LeafOp::Info(hub_info)) => {
            debug!("received INFO from hub");
            hub_info.nonce.clone()
        }
        Some(other) => {
            return Err(format!("expected INFO from hub, got: {other:?}").into());
        }
        None => {
            return Err("hub closed connection before INFO".into());
        }
    };

    let connect_creds = build_upstream_creds(&merged, &hub_nonce)?;
    let creds_ref = if has_any_creds(&connect_creds) {
        Some(&connect_creds)
    } else {
        None
    };
    leaf.send_leaf_connect("rust-leaf", true, creds_ref)?;
    leaf.send_ping()?;
    leaf.flush()?;

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
            Some(LeafOp::LeafSub { .. }) | Some(LeafOp::LeafUnsub { .. }) => {}
            Some(LeafOp::Err(msg)) => {
                return Err(format!("hub error during handshake: {msg}").into());
            }
            Some(LeafOp::Ok) | Some(LeafOp::Info(_)) => {}
            Some(other) => {
                return Err(format!("unexpected op during handshake: {other:?}").into());
            }
            None => {
                return Err("hub closed connection during handshake".into());
            }
        }
    }

    // Sync interests through the pipeline (mapping + collapse dedup)
    {
        let interests: Vec<(String, Option<String>)> = {
            let subs = state.subs.read().unwrap();
            subs.unique_interests()
                .into_iter()
                .map(|(s, q)| (s.to_string(), q.map(|q| q.to_string())))
                .collect()
        };

        let mut sent_collapse_keys: HashSet<String> = HashSet::new();
        for (subject, queue) in &interests {
            if let Some(q) = queue {
                // Queue subs: apply mapping only, no collapse
                let mapped = pipeline.transform_for_sync(subject);
                leaf.send_leaf_sub_queue(&mapped, q)?;
            } else {
                let mapped = pipeline.transform_for_sync(subject);
                if let Some(send_subj) =
                    pipeline.collapse_for_sync(&mapped, &mut sent_collapse_keys)
                {
                    leaf.send_leaf_sub(&send_subj)?;
                }
            }
        }
        leaf.flush()?;
    }

    // Spawn reader/writer threads
    let (leaf_reader, leaf_writer) = leaf.split()?;
    let (cmd_tx, cmd_rx) = mpsc::channel();

    let reader_cmd_tx = cmd_tx.clone();
    let reader_state = Arc::clone(state);
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

    Ok((cmd_tx, stream_shutdown))
}

/// Supervisor loop: connects to hub, re-connects on failure with backoff.
/// Forwards subscribe/unsubscribe/publish commands from `supervisor_rx` to the
/// active connection's cmd_tx when connected, drops them when disconnected.
fn run_supervisor(
    hub_url: String,
    config_creds: Option<HubCredentials>,
    state: Arc<ServerState>,
    shutdown: Arc<AtomicBool>,
    _supervisor_tx: mpsc::Sender<UpstreamCmd>,
    _supervisor_rx: mpsc::Receiver<UpstreamCmd>,
    build_pipeline: &dyn Fn() -> InterestPipeline,
) {
    let mut backoff = Backoff::new(Duration::from_millis(250), Duration::from_secs(30));

    loop {
        if shutdown.load(Ordering::Acquire) {
            debug!("upstream supervisor shutting down");
            return;
        }

        let pipeline = build_pipeline();
        match connect_and_run(&hub_url, config_creds.as_ref(), &state, &pipeline) {
            Ok((cmd_tx, stream_shutdown)) => {
                backoff.reset();
                info!("connected to upstream hub");

                // Update the global upstream_tx so workers send to this connection.
                *state.upstream_tx.write().unwrap() = Some(cmd_tx.clone());

                // Wait until the connection drops. We detect this by trying to
                // send periodic pings. The reader thread will shut down the writer
                // on disconnect, causing send() to fail.
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        stream_shutdown.shutdown(Shutdown::Both).ok();
                        let _ = cmd_tx.send(UpstreamCmd::Shutdown);
                        return;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                    if cmd_tx.send(UpstreamCmd::Pong).is_err() {
                        // Writer thread has exited — connection lost.
                        break;
                    }
                }

                warn!("upstream hub connection lost, will reconnect");
                *state.upstream_tx.write().unwrap() = None;
            }
            Err(e) => {
                warn!(error = %e, "failed to connect to upstream hub");
            }
        }

        if shutdown.load(Ordering::Acquire) {
            return;
        }

        let delay = backoff.next_delay();
        info!(delay_ms = delay.as_millis(), "reconnecting to upstream hub");

        // Sleep in small increments so we can check shutdown flag
        let end = std::time::Instant::now() + delay;
        while std::time::Instant::now() < end {
            if shutdown.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
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
        UpstreamCmd::Subscribe(subject, queue) => {
            if let Some(q) = queue {
                writer.send_leaf_sub_queue(subject.as_bytes(), q.as_bytes())?;
            } else {
                writer.send_leaf_sub(subject.as_bytes())?;
            }
        }
        UpstreamCmd::Unsubscribe(subject, queue) => {
            if let Some(q) = queue {
                writer.send_leaf_unsub_queue(subject.as_bytes(), q.as_bytes())?;
            } else {
                writer.send_leaf_unsub(subject.as_bytes())?;
            }
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
            let (_count, expired) = subs.for_each_match(subject_str, |sub| {
                sub.writer.write_msg(
                    &subject,
                    &sub.sid_bytes,
                    reply.as_deref(),
                    headers.as_ref(),
                    &payload,
                );
                dirty_writers.push(sub.writer.clone());
            });
            drop(subs);

            // Remove expired subs (reached max delivery limit).
            if !expired.is_empty() {
                let mut subs = state.subs.write().unwrap();
                for (conn_id, sid) in &expired {
                    if let Some(removed) = subs.remove(*conn_id, *sid) {
                        let mut upstream = state.upstream.write().unwrap();
                        if let Some(ref mut up) = *upstream {
                            up.remove_interest(&removed.subject, removed.queue.as_deref());
                        }
                    }
                }
                state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
            }
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

/// Parsed hub URL components.
struct ParsedHubUrl {
    addr: String,
    /// Hostname for TLS SNI (Server Name Indication).
    host: String,
    creds: HubCredentials,
    use_tls: bool,
}

/// Parse a hub URL like "nats://user:pass@host:port" into components.
///
/// Supported formats:
/// - `nats://host:port` — no credentials
/// - `tls://host:port` — TLS, no credentials
/// - `nats://token@host:port` — token auth (no colon in userinfo)
/// - `nats://user:pass@host:port` — user/pass auth
/// - `host:port` — bare address, no credentials
fn parse_hub_url(url: &str) -> Result<ParsedHubUrl, Box<dyn std::error::Error>> {
    let use_tls = url.starts_with("tls://") || url.starts_with("nats+tls://");
    let stripped = url
        .strip_prefix("tls://")
        .or_else(|| url.strip_prefix("nats+tls://"))
        .or_else(|| url.strip_prefix("nats://"))
        .or_else(|| url.strip_prefix("nats-leaf://"))
        .unwrap_or(url);

    let mut creds = HubCredentials::default();

    // Check for userinfo@ — use rfind('@') to handle passwords with '@'
    let host_port = if let Some(at_pos) = stripped.rfind('@') {
        let userinfo = &stripped[..at_pos];
        let rest = &stripped[at_pos + 1..];

        if let Some(colon_pos) = userinfo.find(':') {
            // user:pass
            creds.user = Some(userinfo[..colon_pos].to_string());
            creds.pass = Some(userinfo[colon_pos + 1..].to_string());
        } else {
            // token only
            creds.token = Some(userinfo.to_string());
        }
        rest
    } else {
        stripped
    };

    // Extract hostname (before port) for TLS SNI
    let host = if let Some(colon_pos) = host_port.rfind(':') {
        host_port[..colon_pos].to_string()
    } else {
        host_port.to_string()
    };

    let addr = if host_port.contains(':') {
        host_port.to_string()
    } else {
        // Default leafnode port
        format!("{host_port}:7422")
    };

    Ok(ParsedHubUrl {
        addr,
        host,
        creds,
        use_tls,
    })
}

/// Merge URL-extracted credentials with config-level credentials.
/// Config-level fields take precedence over URL fields.
fn merge_hub_credentials(
    url_creds: &HubCredentials,
    config_creds: Option<&HubCredentials>,
) -> HubCredentials {
    let config = match config_creds {
        Some(c) => c,
        None => return url_creds.clone(),
    };

    HubCredentials {
        user: config.user.clone().or_else(|| url_creds.user.clone()),
        pass: config.pass.clone().or_else(|| url_creds.pass.clone()),
        token: config.token.clone().or_else(|| url_creds.token.clone()),
        creds_file: config
            .creds_file
            .clone()
            .or_else(|| url_creds.creds_file.clone()),
    }
}

/// Parse a NATS `.creds` file containing a JWT and NKey seed.
///
/// The file format uses markers:
/// ```text
/// -----BEGIN NATS USER JWT-----
/// <jwt>
/// ------END NATS USER JWT------
/// -----BEGIN USER NKEY SEED-----
/// <seed>
/// ------END USER NKEY SEED------
/// ```
fn parse_creds_file(path: &str) -> io::Result<(String, nkeys::KeyPair)> {
    let contents = std::fs::read_to_string(path)?;

    let jwt = extract_between(
        &contents,
        "-----BEGIN NATS USER JWT-----",
        "------END NATS USER JWT------",
    )
    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing JWT in creds file"))?
    .trim()
    .to_string();

    let seed = extract_between(
        &contents,
        "-----BEGIN USER NKEY SEED-----",
        "------END USER NKEY SEED------",
    )
    .ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "missing NKey seed in creds file",
        )
    })?
    .trim();

    let kp = nkeys::KeyPair::from_seed(seed).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid NKey seed: {e}"),
        )
    })?;

    Ok((jwt, kp))
}

/// Extract text between two marker lines.
fn extract_between<'a>(text: &'a str, begin: &str, end: &str) -> Option<&'a str> {
    let start = text.find(begin)?;
    let after_begin = start + begin.len();
    let end_pos = text[after_begin..].find(end)?;
    Some(&text[after_begin..after_begin + end_pos])
}

/// Build `UpstreamConnectCreds` from merged hub credentials and the hub's nonce.
fn build_upstream_creds(
    creds: &HubCredentials,
    hub_nonce: &str,
) -> Result<UpstreamConnectCreds, Box<dyn std::error::Error>> {
    let mut out = UpstreamConnectCreds::default();

    if let Some(ref creds_path) = creds.creds_file {
        let (jwt, kp) = parse_creds_file(creds_path)?;
        let sig_bytes = kp
            .sign(hub_nonce.as_bytes())
            .map_err(|e| io::Error::other(format!("NKey sign failed: {e}")))?;
        out.jwt = Some(jwt);
        out.nkey = Some(kp.public_key());
        out.sig = Some(data_encoding::BASE64URL_NOPAD.encode(&sig_bytes));
    }

    // Explicit user/pass/token override creds-file fields
    if let Some(ref u) = creds.user {
        out.user = Some(u.clone());
    }
    if let Some(ref p) = creds.pass {
        out.pass = Some(p.clone());
    }
    if let Some(ref t) = creds.token {
        out.token = Some(t.clone());
    }

    Ok(out)
}

/// Check if any credential fields are set.
fn has_any_creds(c: &UpstreamConnectCreds) -> bool {
    c.user.is_some()
        || c.pass.is_some()
        || c.token.is_some()
        || c.jwt.is_some()
        || c.nkey.is_some()
        || c.sig.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_hub_url ---

    #[test]
    fn parse_bare_host_port() {
        let parsed = parse_hub_url("hub.example.com:7422").unwrap();
        assert_eq!(parsed.addr, "hub.example.com:7422");
        assert!(parsed.creds.user.is_none());
        assert!(parsed.creds.pass.is_none());
        assert!(parsed.creds.token.is_none());
        assert!(!parsed.use_tls);
    }

    #[test]
    fn parse_nats_scheme() {
        let parsed = parse_hub_url("nats://hub:7422").unwrap();
        assert_eq!(parsed.addr, "hub:7422");
        assert!(parsed.creds.token.is_none());
        assert!(!parsed.use_tls);
    }

    #[test]
    fn parse_token_url() {
        let parsed = parse_hub_url("nats://mytoken@hub:7422").unwrap();
        assert_eq!(parsed.addr, "hub:7422");
        assert_eq!(parsed.creds.token.as_deref(), Some("mytoken"));
        assert!(parsed.creds.user.is_none());
    }

    #[test]
    fn parse_userpass_url() {
        let parsed = parse_hub_url("nats://admin:secret@hub:7422").unwrap();
        assert_eq!(parsed.addr, "hub:7422");
        assert_eq!(parsed.creds.user.as_deref(), Some("admin"));
        assert_eq!(parsed.creds.pass.as_deref(), Some("secret"));
        assert!(parsed.creds.token.is_none());
    }

    #[test]
    fn parse_default_port() {
        let parsed = parse_hub_url("nats://hub").unwrap();
        assert_eq!(parsed.addr, "hub:7422");
    }

    #[test]
    fn parse_nats_leaf_scheme() {
        let parsed = parse_hub_url("nats-leaf://hub:7422").unwrap();
        assert_eq!(parsed.addr, "hub:7422");
    }

    #[test]
    fn parse_tls_scheme() {
        let parsed = parse_hub_url("tls://hub:7422").unwrap();
        assert_eq!(parsed.addr, "hub:7422");
        assert_eq!(parsed.host, "hub");
        assert!(parsed.use_tls);
    }

    #[test]
    fn parse_nats_tls_scheme() {
        let parsed = parse_hub_url("nats+tls://hub.example.com:7422").unwrap();
        assert_eq!(parsed.addr, "hub.example.com:7422");
        assert_eq!(parsed.host, "hub.example.com");
        assert!(parsed.use_tls);
    }

    #[test]
    fn parse_host_extracted() {
        let parsed = parse_hub_url("nats://myhost:4222").unwrap();
        assert_eq!(parsed.host, "myhost");
        assert!(!parsed.use_tls);
    }

    // --- merge_hub_credentials ---

    #[test]
    fn merge_config_wins() {
        let url = HubCredentials {
            user: Some("url_user".into()),
            pass: Some("url_pass".into()),
            ..Default::default()
        };
        let cfg = HubCredentials {
            user: Some("cfg_user".into()),
            ..Default::default()
        };
        let merged = merge_hub_credentials(&url, Some(&cfg));
        assert_eq!(merged.user.as_deref(), Some("cfg_user"));
        // pass falls back to URL
        assert_eq!(merged.pass.as_deref(), Some("url_pass"));
    }

    #[test]
    fn merge_no_config() {
        let url = HubCredentials {
            token: Some("t".into()),
            ..Default::default()
        };
        let merged = merge_hub_credentials(&url, None);
        assert_eq!(merged.token.as_deref(), Some("t"));
    }

    // --- parse_creds_file ---

    #[test]
    fn parse_creds_file_valid() {
        let kp = nkeys::KeyPair::new_user();
        let seed = kp.seed().unwrap();
        let content = format!(
            "-----BEGIN NATS USER JWT-----\n\
             eyJhbGciOiJlZDI1NTE5LW5rZXkifQ.test.jwt\n\
             ------END NATS USER JWT------\n\
             \n\
             -----BEGIN USER NKEY SEED-----\n\
             {seed}\n\
             ------END USER NKEY SEED------\n"
        );

        let dir = std::env::temp_dir().join("open_wire_test_creds");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.creds");
        std::fs::write(&path, &content).unwrap();

        let (jwt, parsed_kp) = parse_creds_file(path.to_str().unwrap()).unwrap();
        assert_eq!(jwt, "eyJhbGciOiJlZDI1NTE5LW5rZXkifQ.test.jwt");
        assert_eq!(parsed_kp.public_key(), kp.public_key());

        std::fs::remove_file(&path).ok();
    }

    // --- extract_between ---

    #[test]
    fn extract_between_works() {
        let text = "AAA---BEGIN---\nhello\n---END---BBB";
        let result = extract_between(text, "---BEGIN---", "---END---");
        assert_eq!(result, Some("\nhello\n"));
    }

    #[test]
    fn extract_between_missing() {
        let result = extract_between("no markers", "---BEGIN---", "---END---");
        assert!(result.is_none());
    }

    // --- Backoff ---

    #[test]
    fn backoff_initial_delay() {
        let mut b = Backoff::new(Duration::from_millis(250), Duration::from_secs(30));
        let d = b.next_delay();
        // Should be ~250ms ±25% (187..312)
        assert!(d.as_millis() >= 187, "too small: {}ms", d.as_millis());
        assert!(d.as_millis() <= 313, "too large: {}ms", d.as_millis());
    }

    #[test]
    fn backoff_doubles() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_secs(30));
        let _ = b.next_delay(); // 100ms base
        let d2 = b.next_delay(); // 200ms base
                                 // ~200ms ±25%
        assert!(d2.as_millis() >= 150, "too small: {}ms", d2.as_millis());
        assert!(d2.as_millis() <= 250, "too large: {}ms", d2.as_millis());
    }

    #[test]
    fn backoff_caps_at_max() {
        let mut b = Backoff::new(Duration::from_secs(20), Duration::from_secs(30));
        let _ = b.next_delay(); // 20s base
        let d2 = b.next_delay(); // should cap at 30s base (not 40s)
        assert!(
            d2.as_millis() <= 37500,
            "exceeded cap: {}ms",
            d2.as_millis()
        );
    }

    #[test]
    fn backoff_reset() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_secs(30));
        let _ = b.next_delay();
        let _ = b.next_delay();
        b.reset();
        let d = b.next_delay();
        // Back to ~100ms ±25%
        assert!(d.as_millis() >= 75, "too small: {}ms", d.as_millis());
        assert!(d.as_millis() <= 125, "too large: {}ms", d.as_millis());
    }

    // --- add_interest / remove_interest with pipeline ---

    use crate::interest::InterestPipeline;

    /// Helper: create an Upstream with a pipeline but no real connection.
    fn test_upstream(
        #[cfg(feature = "interest-collapse")] templates: Vec<String>,
    ) -> (Upstream, mpsc::Receiver<UpstreamCmd>) {
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let pipeline = InterestPipeline::new(
            #[cfg(feature = "interest-collapse")]
            templates,
            #[cfg(feature = "subject-mapping")]
            vec![],
        );
        let upstream = Upstream {
            cmd_tx,
            interests: HashMap::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            stream_shutdown: None,
            pipeline,
        };
        (upstream, cmd_rx)
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn add_interest_collapse_first_sends_wildcard() {
        let (mut up, rx) = test_upstream(vec!["app.*.sessions.>".to_string()]);

        up.add_interest("app.n1.sessions.s1".into(), None).unwrap();
        // Should send the collapsed wildcard
        match rx.try_recv().unwrap() {
            UpstreamCmd::Subscribe(subj, queue) => {
                assert_eq!(subj, "app.n1.sessions.>");
                assert!(queue.is_none());
            }
            other => panic!("expected Subscribe, got {other:?}"),
        }

        // Second matching sub should NOT send another LS+
        up.add_interest("app.n1.sessions.s2".into(), None).unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn remove_interest_collapse_last_sends_unsub() {
        let (mut up, rx) = test_upstream(vec!["app.*.sessions.>".to_string()]);

        up.add_interest("app.n1.sessions.s1".into(), None).unwrap();
        up.add_interest("app.n1.sessions.s2".into(), None).unwrap();
        // Drain the subscribe
        let _ = rx.try_recv();

        up.remove_interest("app.n1.sessions.s1", None);
        // Still one left — no LS-
        assert!(rx.try_recv().is_err());

        up.remove_interest("app.n1.sessions.s2", None);
        // Last one — should send LS-
        match rx.try_recv().unwrap() {
            UpstreamCmd::Unsubscribe(subj, queue) => {
                assert_eq!(subj, "app.n1.sessions.>");
                assert!(queue.is_none());
            }
            other => panic!("expected Unsubscribe, got {other:?}"),
        }
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn queue_group_bypasses_collapse() {
        let (mut up, rx) = test_upstream(vec!["app.*.sessions.>".to_string()]);

        // Queue group subs should use exact interest even if subject matches
        up.add_interest("app.n1.sessions.s1".into(), Some("q1".into()))
            .unwrap();
        match rx.try_recv().unwrap() {
            UpstreamCmd::Subscribe(subj, queue) => {
                assert_eq!(subj, "app.n1.sessions.s1");
                assert_eq!(queue.as_deref(), Some("q1"));
            }
            other => panic!("expected exact Subscribe for queue, got {other:?}"),
        }
    }

    #[test]
    fn no_collapse_templates_uses_exact() {
        let (mut up, rx) = test_upstream(
            #[cfg(feature = "interest-collapse")]
            vec![],
        );

        up.add_interest("app.n1.sessions.s1".into(), None).unwrap();
        match rx.try_recv().unwrap() {
            UpstreamCmd::Subscribe(subj, queue) => {
                assert_eq!(subj, "app.n1.sessions.s1");
                assert!(queue.is_none());
            }
            other => panic!("expected exact Subscribe, got {other:?}"),
        }
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn different_collapse_keys_tracked_independently() {
        let (mut up, rx) = test_upstream(vec!["app.*.sessions.>".to_string()]);

        // Two different nodes → two different collapse keys
        up.add_interest("app.n1.sessions.s1".into(), None).unwrap();
        up.add_interest("app.n2.sessions.s1".into(), None).unwrap();

        let sub1 = rx.try_recv().unwrap();
        let sub2 = rx.try_recv().unwrap();

        match (sub1, sub2) {
            (UpstreamCmd::Subscribe(s1, _), UpstreamCmd::Subscribe(s2, _)) => {
                let mut keys = vec![s1, s2];
                keys.sort();
                assert_eq!(keys, vec!["app.n1.sessions.>", "app.n2.sessions.>"]);
            }
            _ => panic!("expected two Subscribe commands"),
        }
    }
}
