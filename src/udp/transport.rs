//! ENet host wrapper — service thread for UDP binary transport.
//!
//! Manages a single enet `Host<UdpSocket>` on a dedicated thread. The thread
//! alternates between polling enet for incoming packets and draining the
//! `UdpCmd` channel for outgoing messages.

use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, error, info, warn};

use crate::handler::{deliver_to_subs_upstream_inner, handle_expired_subs_upstream};
use crate::server::ServerState;
use crate::sub_list::DirectWriter;

use super::codec::{self, BatchEncoder};
use super::UdpCmd;

/// A UDP data channel to a single route peer.
/// Spawns a service thread that drives an enet Host.
pub(crate) struct UdpTransport {
    cmd_tx: mpsc::Sender<UdpCmd>,
    shutdown: Arc<AtomicBool>,
}

impl UdpTransport {
    /// Create a new UDP transport and spawn the service thread.
    ///
    /// `local_port` is the UDP port to bind on this host.
    /// `peer_addr` is the remote peer's UDP address (host:port).
    /// `state` is the shared server state for delivering received messages.
    /// `is_server` controls which side initiates the enet connection:
    ///   - `true`: listen for incoming enet connection (inbound route)
    ///   - `false`: connect to peer (outbound route)
    pub fn new(
        local_port: u16,
        peer_addr: SocketAddr,
        state: Arc<ServerState>,
        is_server: bool,
    ) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], local_port)))?;
        socket.set_nonblocking(true)?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let (cmd_tx, cmd_rx) = mpsc::channel();

        let thread_shutdown = Arc::clone(&shutdown);
        let thread_name = format!(
            "udp-transport-{}",
            if is_server { "server" } else { "client" }
        );

        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                if let Err(e) =
                    run_service(socket, peer_addr, state, cmd_rx, thread_shutdown, is_server)
                {
                    error!(error = %e, "UDP transport service thread exited with error");
                }
            })?;

        Ok(Self { cmd_tx, shutdown })
    }

    /// Get a clone of the command sender for forwarding messages.
    pub fn cmd_tx(&self) -> mpsc::Sender<UdpCmd> {
        self.cmd_tx.clone()
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = self.cmd_tx.send(UdpCmd::Shutdown);
    }
}

impl Drop for UdpTransport {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Default batch buffer capacity — room for ~10 messages with 128B payloads.
const DEFAULT_BATCH_CAP: usize = 2048;

/// Maximum time to wait for enet events before checking commands (milliseconds).
const SERVICE_POLL_MS: u64 = 1;

/// enet channel for message forwarding (unreliable sequenced).
const DATA_CHANNEL: u8 = 0;

/// Main service loop. Runs on a dedicated thread.
fn run_service(
    socket: UdpSocket,
    peer_addr: SocketAddr,
    state: Arc<ServerState>,
    cmd_rx: mpsc::Receiver<UdpCmd>,
    shutdown: Arc<AtomicBool>,
    is_server: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut host = rusty_enet::Host::new(
        socket,
        rusty_enet::HostSettings {
            peer_limit: 1,
            channel_limit: 1,
            compressor: Some(Box::new(rusty_enet::RangeCoder::new())),
            checksum: Some(Box::new(rusty_enet::crc32)),
            ..Default::default()
        },
    )
    .map_err(|e| format!("enet host creation failed: {e:?}"))?;

    // Client side initiates the enet connection to the server side.
    let peer_id = if !is_server {
        let peer = host
            .connect(peer_addr, 1, 0)
            .map_err(|e| format!("enet connect failed: {e:?}"))?;
        Some(peer.id())
    } else {
        None
    };

    let mut connected = false;
    let mut encoder = BatchEncoder::with_capacity(DEFAULT_BATCH_CAP);
    let mut dirty_writers: Vec<DirectWriter> = Vec::new();

    debug!(
        peer = %peer_addr,
        role = if is_server { "server" } else { "client" },
        "UDP transport service started"
    );

    loop {
        if shutdown.load(Ordering::Relaxed) {
            // Flush any pending enet data.
            host.flush();
            debug!("UDP transport shutting down");
            return Ok(());
        }

        // 1. Service enet — process incoming packets and connection events.
        while let Some(event) = host.service()? {
            match event {
                rusty_enet::Event::Connect { peer, .. } => {
                    connected = true;
                    debug!(peer_id = peer.id().0, "enet peer connected");
                }
                rusty_enet::Event::Disconnect { peer, .. } => {
                    connected = false;
                    warn!(peer_id = peer.id().0, "enet peer disconnected");
                }
                rusty_enet::Event::Receive { packet, .. } => {
                    handle_incoming_batch(packet.data(), &state, &mut dirty_writers);
                }
            }
        }

        // Notify all dirty writers from this batch of received messages.
        for w in dirty_writers.drain(..) {
            w.notify();
        }

        // 2. Drain command channel — accumulate outgoing messages into batches.
        if connected {
            drain_and_send(&cmd_rx, &mut encoder, &mut host, peer_id);
        } else {
            // Not connected yet — discard messages (at-most-once semantics).
            while cmd_rx.try_recv().is_ok() {}
        }

        // Brief sleep to yield CPU when idle. ENet's internal timers handle
        // retransmit/keepalive timing; we just need to call service() frequently.
        std::thread::sleep(Duration::from_millis(SERVICE_POLL_MS));
    }
}

/// Drain the command channel and send batched messages via enet.
fn drain_and_send(
    cmd_rx: &mpsc::Receiver<UdpCmd>,
    encoder: &mut BatchEncoder,
    host: &mut rusty_enet::Host<UdpSocket>,
    peer_id: Option<rusty_enet::PeerID>,
) {
    encoder.clear();

    while let Ok(cmd) = cmd_rx.try_recv() {
        match cmd {
            UdpCmd::Send {
                subject,
                reply,
                headers,
                payload,
            } => {
                let hdr_bytes = headers.as_ref().map(|h| h.to_bytes());
                encoder.push(&subject, reply.as_deref(), hdr_bytes.as_deref(), &payload);

                // Flush batch if approaching a reasonable size.
                // ENet handles fragmentation, but smaller packets are more efficient.
                if encoder.encoded_size() >= 8000 || encoder.len() == 255 {
                    flush_batch(encoder, host, peer_id);
                    encoder.clear();
                }
            }
            UdpCmd::Shutdown => return,
        }
    }

    // Flush remaining messages.
    if !encoder.is_empty() {
        flush_batch(encoder, host, peer_id);
    }

    // Let enet send queued packets.
    host.flush();
}

/// Encode and send one batch via enet.
fn flush_batch(
    encoder: &mut BatchEncoder,
    host: &mut rusty_enet::Host<UdpSocket>,
    peer_id: Option<rusty_enet::PeerID>,
) {
    let data = encoder.finish();
    let packet = rusty_enet::Packet::unreliable(data);

    if let Some(pid) = peer_id {
        if let Some(peer) = host.get_peer_mut(pid) {
            if let Err(e) = peer.send(DATA_CHANNEL, &packet) {
                debug!(error = ?e, "enet send failed");
            }
        }
    } else {
        // Server side — broadcast to all connected peers (should be exactly 1).
        host.broadcast(DATA_CHANNEL, &packet);
    }
}

/// Decode an incoming binary batch and deliver messages to local subscribers.
fn handle_incoming_batch(data: &[u8], state: &ServerState, dirty_writers: &mut Vec<DirectWriter>) {
    let iter = match codec::decode_batch(data) {
        Ok(iter) => iter,
        Err(e) => {
            warn!(error = %e, "failed to decode UDP batch");
            return;
        }
    };

    for result in iter {
        let msg = match result {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "failed to decode UDP message entry");
                return;
            }
        };

        let subject_str = match std::str::from_utf8(msg.subject) {
            Ok(s) => s,
            Err(_) => {
                warn!("invalid UTF-8 in UDP message subject");
                continue;
            }
        };

        // Parse header bytes back into HeaderMap if present.
        let headers = msg.headers.and_then(|h| {
            crate::nats_proto::parse_headers(h)
                .map_err(|e| warn!(error = %e, "failed to parse headers from UDP message"))
                .ok()
        });

        // One-hop: skip route subs — messages from UDP transport are never
        // re-forwarded to other route peers.
        let (_delivered, expired) = deliver_to_subs_upstream_inner(
            state,
            msg.subject,
            subject_str,
            msg.reply,
            headers.as_ref(),
            msg.payload,
            dirty_writers,
            true, // skip_routes (one-hop rule)
            #[cfg(feature = "gateway")]
            false, // don't skip gateways
            #[cfg(feature = "accounts")]
            0, // default account
        );
        handle_expired_subs_upstream(
            &expired,
            state,
            #[cfg(feature = "accounts")]
            0,
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// UdpListener — shared server-side enet Host accepting inbound connections
// ────────────────────────────────────────────────────────────────────────────

/// Maximum number of concurrent inbound enet peers.
const MAX_PEERS: usize = 64;

/// A shared server-side enet listener that binds on `cluster_udp_port` and
/// accepts incoming connections from outbound `UdpTransport` clients.
///
/// Runs a service thread that polls enet for incoming packets, decodes binary
/// batches, and delivers messages to local subscribers. Does not send messages
/// (outbound transport handles that direction).
pub(crate) struct UdpListener {
    shutdown: Arc<AtomicBool>,
}

impl UdpListener {
    /// Bind on `port` and spawn the server-side enet service thread.
    pub fn new(port: u16, state: Arc<ServerState>) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], port)))?;
        socket.set_nonblocking(true)?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);

        std::thread::Builder::new()
            .name("udp-listener".into())
            .spawn(move || {
                if let Err(e) = run_listener(socket, state, thread_shutdown) {
                    error!(error = %e, "UDP listener thread exited with error");
                }
            })?;

        info!(port, "UDP listener started");
        Ok(Self { shutdown })
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

impl Drop for UdpListener {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Server-side enet service loop. Accepts connections and receives messages.
fn run_listener(
    socket: UdpSocket,
    state: Arc<ServerState>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut host = rusty_enet::Host::new(
        socket,
        rusty_enet::HostSettings {
            peer_limit: MAX_PEERS,
            channel_limit: 1,
            compressor: Some(Box::new(rusty_enet::RangeCoder::new())),
            checksum: Some(Box::new(rusty_enet::crc32)),
            ..Default::default()
        },
    )
    .map_err(|e| format!("enet listener creation failed: {e:?}"))?;

    let mut dirty_writers: Vec<DirectWriter> = Vec::new();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            host.flush();
            debug!("UDP listener shutting down");
            return Ok(());
        }

        while let Some(event) = host.service()? {
            match event {
                rusty_enet::Event::Connect { peer, .. } => {
                    debug!(
                        peer_id = peer.id().0,
                        addr = ?peer.address(),
                        "enet inbound peer connected"
                    );
                }
                rusty_enet::Event::Disconnect { peer, .. } => {
                    debug!(peer_id = peer.id().0, "enet inbound peer disconnected");
                }
                rusty_enet::Event::Receive { packet, .. } => {
                    handle_incoming_batch(packet.data(), &state, &mut dirty_writers);
                }
            }
        }

        for w in dirty_writers.drain(..) {
            w.notify();
        }

        std::thread::sleep(Duration::from_millis(SERVICE_POLL_MS));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_encoder_flush_threshold() {
        let mut encoder = BatchEncoder::with_capacity(DEFAULT_BATCH_CAP);
        // Push messages until we'd exceed 8000 bytes.
        let subject = b"test.subject.long.name";
        let payload = [0u8; 512];
        let mut count = 0;
        while encoder.encoded_size() < 8000 && encoder.len() < 255 {
            encoder.push(subject, None, None, &payload);
            count += 1;
        }
        // Should have accumulated multiple messages.
        assert!(count > 10);
        assert!(encoder.encoded_size() >= 8000 || encoder.len() == 255);
    }

    #[test]
    fn test_batch_size_calculation() {
        let encoder = BatchEncoder::with_capacity(256);
        // Empty batch = 3 bytes (header only).
        assert_eq!(encoder.encoded_size(), codec::BATCH_HEADER_SIZE);
    }
}
