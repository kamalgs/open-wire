//! Outbound route connection manager for full mesh clustering.
//!
//! Each outbound route connection spawns a reader thread that processes
//! RS+/RS-/RMSG from the remote peer, and a writer thread that drains
//! the DirectWriter buffer to TCP.
//!
//! One-hop rule: messages received from a route are never re-forwarded
//! to other routes (enforced by deliver_to_subs_upstream skipping route subs).

use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tracing::{debug, error, info, warn};

use crate::handler::{deliver_to_subs_upstream_inner, handle_expired_subs_upstream};
use crate::nats_proto::{self, MsgBuilder, RouteOp};
use crate::server::ServerState;
use crate::sub_list::{DirectWriter, Subscription};
use crate::upstream::Backoff;

/// Virtual connection ID range for outbound route connections.
/// Uses high IDs to avoid collision with inbound connection IDs.
const ROUTE_CONN_ID_BASE: u64 = 1 << 48;

/// Global counter for outbound route connection IDs.
static ROUTE_CONN_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_route_conn_id() -> u64 {
    ROUTE_CONN_ID_BASE + ROUTE_CONN_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Manages all outbound route connections to seed peers.
pub(crate) struct RouteConnManager {
    shutdown: Arc<AtomicBool>,
}

impl RouteConnManager {
    /// Spawn outbound route connections to all configured seed peers,
    /// plus a coordinator thread that handles gossip-discovered URLs.
    pub(crate) fn spawn(state: Arc<ServerState>) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let seeds = state.cluster_seeds.clone();

        // Create the coordinator channel and store sender in state.
        let (coord_tx, coord_rx) = std::sync::mpsc::channel::<String>();
        {
            let mut tx_lock = state.route_connect_tx.lock().unwrap();
            *tx_lock = Some(coord_tx);
        }

        // Spawn seed supervisors.
        for seed_url in &seeds {
            let st = Arc::clone(&state);
            let sd = Arc::clone(&shutdown);
            let url = seed_url.clone();

            std::thread::Builder::new()
                .name(format!("route-{}", url))
                .spawn(move || {
                    run_route_supervisor(url, st, sd);
                })
                .expect("failed to spawn route supervisor");
        }

        // Spawn coordinator thread for gossip-discovered routes.
        {
            let st = Arc::clone(&state);
            let sd = Arc::clone(&shutdown);

            std::thread::Builder::new()
                .name("route-coordinator".into())
                .spawn(move || {
                    run_route_coordinator(coord_rx, seeds, st, sd);
                })
                .expect("failed to spawn route coordinator");
        }

        Self { shutdown }
    }
}

impl Drop for RouteConnManager {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

/// Coordinator thread: receives new gossip-discovered URLs and spawns
/// supervisors for each new unique route peer.
fn run_route_coordinator(
    rx: std::sync::mpsc::Receiver<String>,
    seeds: Vec<String>,
    state: Arc<ServerState>,
    shutdown: Arc<AtomicBool>,
) {
    // Track which URLs already have active supervisors.
    let mut active_urls: HashSet<String> = HashSet::new();
    for seed in &seeds {
        active_urls.insert(normalize_route_url(seed));
    }

    // Also add own cluster address.
    if let Some(cp) = state.cluster_port {
        active_urls.insert(format!("0.0.0.0:{cp}"));
        active_urls.insert(format!("127.0.0.1:{cp}"));
    }

    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }

        // Block with timeout so we can check shutdown.
        let url = match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(url) => url,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
        };

        let normalized = normalize_route_url(&url);

        // Skip own address.
        if let Some(cp) = state.cluster_port {
            if normalized == format!("0.0.0.0:{cp}")
                || normalized == format!("127.0.0.1:{cp}")
                || normalized == format!("{host}:{cp}", host = state.info.host)
            {
                continue;
            }
        }

        if active_urls.contains(&normalized) {
            continue;
        }

        info!(url = %normalized, "gossip: discovered new route peer, connecting");
        active_urls.insert(normalized.clone());

        let st = Arc::clone(&state);
        let sd = Arc::clone(&shutdown);

        std::thread::Builder::new()
            .name(format!("route-gossip-{}", normalized))
            .spawn(move || {
                run_route_supervisor(normalized, st, sd);
            })
            .expect("failed to spawn gossip route supervisor");
    }
}

/// Supervisor loop for a single outbound route connection.
fn run_route_supervisor(seed_url: String, state: Arc<ServerState>, shutdown: Arc<AtomicBool>) {
    let mut backoff = Backoff::new(Duration::from_millis(250), Duration::from_secs(30));

    loop {
        if shutdown.load(Ordering::Acquire) {
            debug!(seed = %seed_url, "route supervisor shutting down");
            return;
        }

        match connect_route(&seed_url, &state, &shutdown) {
            Ok(()) => {
                backoff.reset();
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                warn!(seed = %seed_url, "route connection lost, will reconnect");
            }
            Err(e) => {
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                warn!(seed = %seed_url, error = %e, "route connection failed");
            }
        }

        if shutdown.load(Ordering::Acquire) {
            return;
        }

        let delay = backoff.next_delay();
        debug!(seed = %seed_url, delay_ms = delay.as_millis(), "reconnecting to route peer");

        let end = std::time::Instant::now() + delay;
        while std::time::Instant::now() < end {
            if shutdown.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// Normalize a route URL: strip scheme, ensure host:port format.
pub(crate) fn normalize_route_url(url: &str) -> String {
    parse_route_url(url)
}

/// Parse a route URL like "nats-route://host:port" into a TCP address.
fn parse_route_url(url: &str) -> String {
    let stripped = url
        .strip_prefix("nats-route://")
        .or_else(|| url.strip_prefix("nats://"))
        .unwrap_or(url);

    if stripped.contains(':') {
        stripped.to_string()
    } else {
        format!("{stripped}:4248")
    }
}

/// Process `connect_urls` from a peer's INFO, adding new URLs to
/// `known_urls` and sending them to the coordinator channel.
pub(crate) fn process_gossip_urls(state: &ServerState, connect_urls: &[String]) {
    let mut peers = state.route_peers.lock().unwrap();
    let tx = state.route_connect_tx.lock().unwrap();
    for url in connect_urls {
        let normalized = normalize_route_url(url);
        if peers.known_urls.insert(normalized.clone()) {
            // New URL discovered via gossip — notify coordinator.
            if let Some(ref sender) = *tx {
                let _ = sender.send(normalized);
            }
        }
    }
}

/// Connect to a route peer, perform handshake, run reader/writer loop.
fn connect_route(
    seed_url: &str,
    state: &Arc<ServerState>,
    shutdown: &Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = parse_route_url(seed_url);
    let tcp = TcpStream::connect(&addr)?;
    tcp.set_nodelay(true)?;

    let conn_id = next_route_conn_id();
    info!(addr = %addr, conn_id, "outbound route connection established");

    let mut read_buf = BytesMut::with_capacity(state.buf_config.max_read_buf);
    let tcp_writer = tcp.try_clone()?;
    let tcp_shutdown = tcp.try_clone()?;

    // --- Read peer's INFO ---
    read_into_buf(&tcp, &mut read_buf)?;
    let peer_info = match nats_proto::try_parse_route_op(&mut read_buf)? {
        Some(RouteOp::Info(info)) => {
            debug!(peer_id = %info.server_id, "received route INFO from peer");
            // Self-connect check
            if info.server_id == state.info.server_id {
                debug!("detected self-connect on route, closing");
                return Err("self-connect on route".into());
            }
            info
        }
        Some(other) => {
            return Err(format!("expected INFO from route peer, got: {other:?}").into());
        }
        None => {
            return Err("route peer closed connection before INFO".into());
        }
    };

    // --- server_id dedup check ---
    let peer_server_id = peer_info.server_id.clone();
    {
        let peers = state.route_peers.lock().unwrap();
        if peers.connected.contains_key(&peer_server_id) {
            debug!(
                peer_id = %peer_server_id,
                "duplicate outbound route connection, closing"
            );
            return Err("route peer already connected (dedup)".into());
        }
    }

    // Process connect_urls from peer INFO.
    if !peer_info.connect_urls.is_empty() {
        process_gossip_urls(state, &peer_info.connect_urls);
    }

    // --- Send our INFO + CONNECT + PING ---
    {
        let mut w = io::BufWriter::new(&tcp_writer);
        w.write_all(build_route_info(state).as_bytes())?;
        w.write_all(build_route_connect(state).as_bytes())?;
        w.write_all(b"PING\r\n")?;
        w.flush()?;
    }

    // --- Wait for PONG (process handshake ops) ---
    loop {
        match try_parse_or_read(&tcp, &mut read_buf)? {
            RouteOp::Pong => {
                debug!("route handshake complete");
                break;
            }
            RouteOp::Ping => {
                let mut w = io::BufWriter::new(&tcp_writer);
                w.write_all(b"PONG\r\n")?;
                w.flush()?;
            }
            RouteOp::Info(info) => {
                if !info.connect_urls.is_empty() {
                    process_gossip_urls(state, &info.connect_urls);
                }
            }
            RouteOp::Connect(_) => {}
            RouteOp::RouteSub { .. } | RouteOp::RouteUnsub { .. } => {}
            other => {
                return Err(format!("unexpected op during route handshake: {other:?}").into());
            }
        }
    }

    // --- Register peer in RoutePeerRegistry (double-check for races) ---
    {
        let mut peers = state.route_peers.lock().unwrap();
        if peers.connected.contains_key(&peer_server_id) {
            debug!(
                peer_id = %peer_server_id,
                "duplicate outbound route (race dedup), closing"
            );
            return Err("route peer already connected (race dedup)".into());
        }
        peers.connected.insert(peer_server_id.clone(), addr.clone());
    }

    // --- Create DirectWriter for this route ---
    let direct_writer = DirectWriter::new_dummy();

    // Register in route_writers
    {
        let mut writers = state.route_writers.write().unwrap();
        writers.insert(conn_id, direct_writer.clone());
    }

    // --- Send RS+ for existing local subs ---
    {
        let mut w = io::BufWriter::new(&tcp_writer);
        let subs = state.subs.read().unwrap();
        let mut builder = MsgBuilder::new();
        for (subject, queue) in subs.local_interests() {
            let data = if let Some(q) = queue {
                builder.build_route_sub_queue(subject.as_bytes(), q.as_bytes())
            } else {
                builder.build_route_sub(subject.as_bytes())
            };
            w.write_all(data)?;
        }
        drop(subs);
        w.flush()?;
    }

    // --- Broadcast updated INFO to all route peers (topology changed) ---
    broadcast_route_info(state);

    // --- Spawn writer thread ---
    // The writer thread polls the DirectWriter's eventfd and drains buffered
    // RMSG/RS+ data to the TCP socket.
    let writer_dw = direct_writer.clone();
    let writer_shutdown = Arc::clone(shutdown);
    let writer_handle = std::thread::Builder::new()
        .name(format!("route-writer-{}", conn_id))
        .spawn(move || {
            run_route_writer(tcp_writer, writer_dw, writer_shutdown);
        })
        .expect("failed to spawn route writer");

    // --- Run reader loop ---
    let result = run_route_reader(&tcp, &mut read_buf, conn_id, state, &direct_writer);

    // --- Cleanup ---
    tcp_shutdown.shutdown(Shutdown::Both).ok();
    let _ = writer_handle.join();

    // Remove all route subs for this connection from SubList
    {
        let mut subs = state.subs.write().unwrap();
        subs.remove_conn(conn_id);
        state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
    }

    // Remove from route_writers
    {
        let mut writers = state.route_writers.write().unwrap();
        writers.remove(&conn_id);
    }

    // Remove from RoutePeerRegistry
    {
        let mut peers = state.route_peers.lock().unwrap();
        peers.connected.remove(&peer_server_id);
    }

    info!(conn_id, "outbound route connection closed");
    result
}

/// Writer thread: waits on DirectWriter's eventfd and flushes buffered data to TCP.
fn run_route_writer(tcp: TcpStream, dw: DirectWriter, shutdown: Arc<AtomicBool>) {
    let efd = dw.event_raw_fd();
    let mut pfds = [libc::pollfd {
        fd: efd,
        events: libc::POLLIN,
        revents: 0,
    }];
    let mut tcp_out = io::BufWriter::new(tcp);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            // Final drain
            if let Some(data) = dw.drain() {
                let _ = tcp_out.write_all(&data);
                let _ = tcp_out.flush();
            }
            return;
        }

        pfds[0].revents = 0;
        let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 1, 500) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            error!(error = %err, "route writer poll error");
            return;
        }

        if pfds[0].revents & libc::POLLIN != 0 {
            // Consume eventfd
            let mut val: u64 = 0;
            unsafe {
                libc::read(efd, &mut val as *mut u64 as *mut libc::c_void, 8);
            }
        }

        // Drain any pending data
        if let Some(data) = dw.drain() {
            if let Err(e) = tcp_out.write_all(&data) {
                debug!(error = %e, "route writer TCP error");
                return;
            }
            if let Err(e) = tcp_out.flush() {
                debug!(error = %e, "route writer flush error");
                return;
            }
        }
    }
}

/// Main reader loop for an outbound route connection.
fn run_route_reader(
    tcp: &TcpStream,
    read_buf: &mut BytesMut,
    conn_id: u64,
    state: &ServerState,
    direct_writer: &DirectWriter,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut dirty_writers: Vec<DirectWriter> = Vec::new();
    let mut route_sid_counter: u64 = 0;
    let mut route_sids: HashMap<(bytes::Bytes, Option<bytes::Bytes>), u64> = HashMap::new();
    let mut tmp = [0u8; 65536];

    loop {
        // Try to parse all ops already in the buffer
        while let Some(op) = nats_proto::try_parse_route_op(read_buf)? {
            handle_route_op(
                op,
                conn_id,
                state,
                direct_writer,
                tcp,
                &mut dirty_writers,
                &mut route_sid_counter,
                &mut route_sids,
            )?;
        }

        // Notify all dirty writers from this batch
        for w in dirty_writers.drain(..) {
            w.notify();
        }

        // Read more data from TCP
        let n = (&*tcp).read(&mut tmp)?;
        if n == 0 {
            return Ok(());
        }
        read_buf.extend_from_slice(&tmp[..n]);
    }
}

/// Handle a single route protocol operation from a peer.
#[allow(clippy::too_many_arguments)]
fn handle_route_op(
    op: RouteOp,
    conn_id: u64,
    state: &ServerState,
    direct_writer: &DirectWriter,
    tcp: &TcpStream,
    dirty_writers: &mut Vec<DirectWriter>,
    route_sid_counter: &mut u64,
    route_sids: &mut HashMap<(bytes::Bytes, Option<bytes::Bytes>), u64>,
) -> io::Result<()> {
    match op {
        RouteOp::RouteSub { subject, queue, .. } => {
            *route_sid_counter += 1;
            let sid = *route_sid_counter;
            route_sids.insert((subject.clone(), queue.clone()), sid);

            let subject_str = unsafe { std::str::from_utf8_unchecked(&subject) };
            let queue_str = queue
                .as_ref()
                .map(|q| unsafe { std::str::from_utf8_unchecked(q) }.to_string());

            let sub = Subscription {
                conn_id,
                sid,
                sid_bytes: nats_proto::sid_to_bytes(sid),
                subject: subject_str.to_string(),
                queue: queue_str,
                writer: direct_writer.clone(),
                max_msgs: AtomicU64::new(0),
                delivered: AtomicU64::new(0),
                is_leaf: false,
                is_route: true,
                #[cfg(feature = "gateway")]
                is_gateway: false,
            };

            let mut subs = state.subs.write().unwrap();
            subs.insert(sub);
            state.has_subs.store(true, Ordering::Relaxed);

            debug!(conn_id, sid, subject = %subject_str, "outbound route sub");
        }
        RouteOp::RouteUnsub { subject } => {
            // RS- doesn't carry queue; remove matching (subject, None) entry
            let key = (subject.clone(), None);
            if let Some(sid) = route_sids.remove(&key) {
                let mut subs = state.subs.write().unwrap();
                subs.remove(conn_id, sid);
                state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
            }
        }
        RouteOp::RouteMsg {
            subject,
            reply,
            headers,
            payload,
            ..
        } => {
            let subject_str = unsafe { std::str::from_utf8_unchecked(&subject) };
            // One-hop: skip route subs — messages from a route peer are never
            // re-forwarded to other route peers.
            let (_delivered, expired) = deliver_to_subs_upstream_inner(
                state,
                &subject,
                subject_str,
                reply.as_deref(),
                headers.as_ref(),
                &payload,
                dirty_writers,
                true, // skip_routes
                #[cfg(feature = "gateway")]
                false, // don't skip gateways — route msgs forward to gateway peers
            );
            handle_expired_subs_upstream(&expired, state);
        }
        RouteOp::Ping => {
            let mut w = io::BufWriter::new(tcp);
            w.write_all(b"PONG\r\n")?;
            w.flush()?;
        }
        RouteOp::Pong => {}
        RouteOp::Info(info) => {
            // Gossip: process connect_urls from active-phase INFO updates.
            if !info.connect_urls.is_empty() {
                process_gossip_urls(state, &info.connect_urls);
            }
            debug!("received updated INFO from route peer");
        }
        RouteOp::Connect(_) => {
            debug!("received CONNECT from route peer");
        }
    }
    Ok(())
}

/// Build INFO JSON for route protocol, including `connect_urls` from known peers.
pub(crate) fn build_route_info(state: &ServerState) -> String {
    let cluster_name = state.cluster_name.as_deref().unwrap_or("default");
    let cluster_port = state.cluster_port.unwrap_or(0);

    // Collect known route URLs for gossip.
    let connect_urls = {
        let peers = state.route_peers.lock().unwrap();
        let urls: Vec<&str> = peers.known_urls.iter().map(|s| s.as_str()).collect();
        if urls.is_empty() {
            String::new()
        } else {
            let items: Vec<String> = urls
                .iter()
                .map(|u| format!("\"nats-route://{}\"", u))
                .collect();
            format!(",\"connect_urls\":[{}]", items.join(","))
        }
    };

    format!(
        "INFO {{\"server_id\":\"{}\",\"server_name\":\"{}\",\"version\":\"{}\",\
         \"host\":\"{}\",\"port\":{},\"max_payload\":{},\"proto\":1,\
         \"cluster\":\"{}\",\"cluster_port\":{}{}}}\r\n",
        state.info.server_id,
        state.info.server_name,
        state.info.version,
        state.info.host,
        state.info.port,
        state.info.max_payload,
        cluster_name,
        cluster_port,
        connect_urls,
    )
}

/// Build CONNECT JSON for route protocol.
fn build_route_connect(state: &ServerState) -> String {
    let cluster_name = state.cluster_name.as_deref().unwrap_or("default");
    format!(
        "CONNECT {{\"server_id\":\"{}\",\"name\":\"{}\",\"cluster\":\"{}\"}}\r\n",
        state.info.server_id, state.info.server_name, cluster_name,
    )
}

/// Broadcast updated INFO (with current `connect_urls`) to all connected route peers.
pub(crate) fn broadcast_route_info(state: &ServerState) {
    let info_line = build_route_info(state);
    let info_bytes = info_line.as_bytes();
    let writers = state.route_writers.read().unwrap();
    for writer in writers.values() {
        writer.write_raw(info_bytes);
        writer.notify();
    }
}

/// Read from TCP into the buffer, blocking until data is available.
fn read_into_buf(tcp: &TcpStream, buf: &mut BytesMut) -> io::Result<()> {
    let mut tmp = [0u8; 4096];
    let n = (&*tcp).read(&mut tmp)?;
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed",
        ));
    }
    buf.extend_from_slice(&tmp[..n]);
    Ok(())
}

/// Parse the next route op from the buffer, reading more data from TCP if needed.
fn try_parse_or_read(
    tcp: &TcpStream,
    buf: &mut BytesMut,
) -> Result<RouteOp, Box<dyn std::error::Error>> {
    loop {
        if let Some(op) = nats_proto::try_parse_route_op(buf)? {
            return Ok(op);
        }
        read_into_buf(tcp, buf)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_route_url_with_scheme() {
        assert_eq!(parse_route_url("nats-route://host1:4248"), "host1:4248");
    }

    #[test]
    fn parse_route_url_bare() {
        assert_eq!(parse_route_url("host2:4248"), "host2:4248");
    }

    #[test]
    fn parse_route_url_default_port() {
        assert_eq!(parse_route_url("host3"), "host3:4248");
    }

    #[test]
    fn parse_route_url_nats_scheme() {
        assert_eq!(parse_route_url("nats://host4:6248"), "host4:6248");
    }

    #[test]
    fn route_conn_id_is_high() {
        let id = next_route_conn_id();
        assert!(id >= ROUTE_CONN_ID_BASE);
    }

    #[test]
    fn normalize_route_url_strips_scheme() {
        assert_eq!(normalize_route_url("nats-route://host:4248"), "host:4248");
        assert_eq!(normalize_route_url("host:4248"), "host:4248");
        assert_eq!(normalize_route_url("host"), "host:4248");
    }
}
