//! Outbound route connection manager for full mesh clustering.
//!
//! Each outbound route connection spawns a reader thread that processes
//! RS+/RS-/RMSG from the remote peer, and a writer thread that drains
//! the MsgWriter buffer to TCP.
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

use crate::core::server::ServerState;
use crate::handler::{
    deliver_to_subs_upstream_inner, handle_expired_subs_upstream, DeliveryScope, Msg,
};
use crate::nats_proto::{self, MsgBuilder, RouteOp};
use crate::sub_list::{MsgWriter, SubKind, Subscription};

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
        let seeds = state.cluster.seeds.clone();

        let (coord_tx, coord_rx) = std::sync::mpsc::channel::<String>();
        {
            let mut tx_lock = state.cluster.connect_tx.lock().expect("connect_tx lock");
            *tx_lock = Some(coord_tx);
        }

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
    let mut active_urls: HashSet<String> = HashSet::new();
    for seed in &seeds {
        active_urls.insert(normalize_route_url(seed));
    }

    if let Some(cp) = state.cluster.port {
        active_urls.insert(format!("0.0.0.0:{cp}"));
        active_urls.insert(format!("127.0.0.1:{cp}"));
    }

    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }

        let url = match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(url) => url,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
        };

        let normalized = normalize_route_url(&url);

        if let Some(cp) = state.cluster.port {
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
    crate::connector::common::run_supervisor(&seed_url, &shutdown, || {
        connect_route(&seed_url, &state, &shutdown)
    });
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
    let mut peers = state.cluster.route_peers.lock().expect("route_peers lock");
    let tx = state.cluster.connect_tx.lock().expect("connect_tx lock");
    for url in connect_urls {
        let normalized = normalize_route_url(url);
        if peers.known_urls.insert(normalized.clone()) {
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

    let peer_server_id = peer_info.server_id.clone();
    let use_binary = peer_info.open_wire == Some(1);
    {
        let peers = state.cluster.route_peers.lock().expect("route_peers lock");
        if peers.connected.contains_key(&peer_server_id) {
            debug!(
                peer_id = %peer_server_id,
                "duplicate outbound route connection, closing"
            );
            return Err("route peer already connected (dedup)".into());
        }
    }

    if !peer_info.connect_urls.is_empty() {
        process_gossip_urls(state, &peer_info.connect_urls);
    }

    {
        let mut w = io::BufWriter::new(&tcp_writer);
        w.write_all(build_route_info(state).as_bytes())?;
        w.write_all(build_route_connect(state).as_bytes())?;
        w.write_all(b"PING\r\n")?;
        w.flush()?;
    }

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

    {
        let mut peers = state.cluster.route_peers.lock().expect("route_peers lock");
        if peers.connected.contains_key(&peer_server_id) {
            debug!(
                peer_id = %peer_server_id,
                "duplicate outbound route (race dedup), closing"
            );
            return Err("route peer already connected (race dedup)".into());
        }
        peers.connected.insert(peer_server_id.clone(), addr.clone());
    }

    let direct_writer = if use_binary {
        MsgWriter::new_binary_dummy()
    } else {
        MsgWriter::new_dummy()
    };

    {
        let mut writers = state
            .cluster
            .route_writers
            .write()
            .expect("route_writers write lock");
        writers.insert(conn_id, direct_writer.clone());
    }

    {
        let mut w = io::BufWriter::new(&tcp_writer);
        let subs = state
            .get_subs(
                #[cfg(feature = "accounts")]
                0,
            )
            .read()
            .expect("subs read lock");
        if use_binary {
            // Binary mode: encode initial sub sync as binary Sub frames.
            let mut bin_buf = BytesMut::new();
            for (subject, queue) in subs.local_interests() {
                let queue_slice: &[u8] = queue.map(|q| q.as_bytes()).unwrap_or(b"");
                crate::protocol::bin_proto::write_sub(
                    subject.as_bytes(),
                    queue_slice,
                    b"$G",
                    &mut bin_buf,
                );
            }
            drop(subs);
            w.write_all(&bin_buf)?;
        } else {
            let mut builder = MsgBuilder::new();
            for (subject, queue) in subs.local_interests() {
                let data = if let Some(q) = queue {
                    builder.build_route_sub_queue(
                        subject.as_bytes(),
                        q.as_bytes(),
                        #[cfg(feature = "accounts")]
                        b"$G".as_slice(),
                    )
                } else {
                    builder.build_route_sub(
                        subject.as_bytes(),
                        #[cfg(feature = "accounts")]
                        b"$G".as_slice(),
                    )
                };
                w.write_all(data)?;
            }
            drop(subs);
        }
        w.flush()?;
    }

    broadcast_route_info(state);

    // The writer thread polls the MsgWriter's eventfd and drains buffered
    // RMSG/RS+ data to the TCP socket.
    let writer_dw = direct_writer.clone();
    let writer_shutdown = Arc::clone(shutdown);
    let max_pending_route = state.buf_config.max_pending_route;
    let writer_handle = std::thread::Builder::new()
        .name(format!("route-writer-{}", conn_id))
        .spawn(move || {
            run_route_writer(tcp_writer, writer_dw, writer_shutdown, max_pending_route);
        })
        .expect("failed to spawn route writer");

    let result = run_route_reader(
        &tcp,
        &mut read_buf,
        conn_id,
        state,
        &direct_writer,
        use_binary,
    );

    tcp_shutdown.shutdown(Shutdown::Both).ok();
    let _ = writer_handle.join();

    {
        #[cfg(feature = "accounts")]
        {
            for account_subs in &state.account_subs {
                let mut subs = account_subs.write().expect("subs write lock");
                subs.remove_conn(conn_id);
            }
            state.has_subs.store(
                state
                    .account_subs
                    .iter()
                    .any(|s| !s.read().expect("subs read lock").is_empty()),
                Ordering::Relaxed,
            );
        }
        #[cfg(not(feature = "accounts"))]
        {
            let mut subs = state.subs.write().expect("subs write lock");
            subs.remove_conn(conn_id);
            state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
        }
    }

    {
        let mut writers = state
            .cluster
            .route_writers
            .write()
            .expect("route_writers write lock");
        writers.remove(&conn_id);
    }

    {
        let mut peers = state.cluster.route_peers.lock().expect("route_peers lock");
        peers.connected.remove(&peer_server_id);
    }

    info!(conn_id, "outbound route connection closed");
    result
}

/// Writer thread: waits on MsgWriter's eventfd and flushes buffered data to TCP.
///
/// Sets the route's congestion level after each drain cycle so publishers on
/// other workers can reduce their read budget (non-blocking backpressure).
/// A 2-second write deadline matches Go nats-server's default; on timeout
/// the congestion level is set to maximum but the route is not disconnected
/// immediately — only when the buffer exceeds `max_pending_route`.
fn run_route_writer(
    tcp: TcpStream,
    dw: MsgWriter,
    shutdown: Arc<AtomicBool>,
    max_pending_route: usize,
) {
    // 2-second write deadline, matching Go nats-server.
    tcp.set_write_timeout(Some(std::time::Duration::from_secs(2)))
        .ok();

    let efd = dw.event_raw_fd();
    let mut pfds = [libc::pollfd {
        fd: efd,
        events: libc::POLLIN,
        revents: 0,
    }];
    let mut tcp_out = io::BufWriter::new(tcp);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            if let Some(data) = dw.drain() {
                let _ = tcp_out.write_all(&data);
                let _ = tcp_out.flush();
            }
            dw.set_congestion(0);
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
            let mut val: u64 = 0;
            unsafe {
                libc::read(efd, &mut val as *mut u64 as *mut libc::c_void, 8);
            }
        }

        if let Some(data) = dw.drain() {
            if let Err(e) = tcp_out.write_all(&data) {
                if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock {
                    // Write deadline expired — signal heavy congestion but keep trying.
                    dw.set_congestion(2);
                    warn!("route writer stalled (write deadline exceeded)");
                    continue;
                }
                debug!(error = %e, "route writer TCP error");
                return;
            }
            if let Err(e) = tcp_out.flush() {
                if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock {
                    dw.set_congestion(2);
                    warn!("route writer flush stalled (write deadline exceeded)");
                    continue;
                }
                debug!(error = %e, "route writer flush error");
                return;
            }
        }

        // Update congestion level based on remaining buffer after flush.
        let buf_len = dw.buf_len();
        if buf_len > max_pending_route {
            warn!(
                pending_bytes = buf_len,
                max = max_pending_route,
                "route writer slow consumer, disconnecting"
            );
            return;
        }
        let ratio = buf_len as f64 / max_pending_route as f64;
        let level = if ratio < 0.25 {
            0
        } else if ratio < 0.75 {
            1
        } else {
            2
        };
        dw.set_congestion(level);
    }
}

/// Main reader loop for an outbound route connection.
fn run_route_reader(
    tcp: &TcpStream,
    read_buf: &mut BytesMut,
    conn_id: u64,
    state: &ServerState,
    direct_writer: &MsgWriter,
    binary: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut dirty_writers: Vec<MsgWriter> = Vec::new();
    let mut route_sid_counter: u64 = 0;
    let mut route_sids: HashMap<(bytes::Bytes, Option<bytes::Bytes>), u64> = HashMap::new();
    let mut tmp = [0u8; 65536];

    loop {
        if binary {
            while let Some(frame) = crate::protocol::bin_proto::try_decode(read_buf) {
                handle_bin_frame(
                    frame,
                    conn_id,
                    state,
                    direct_writer,
                    tcp,
                    &mut dirty_writers,
                    &mut route_sid_counter,
                    &mut route_sids,
                )?;
            }
        } else {
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
        }

        for w in dirty_writers.drain(..) {
            w.notify();
        }

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
    direct_writer: &MsgWriter,
    tcp: &TcpStream,
    dirty_writers: &mut Vec<MsgWriter>,
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

            let sub = Subscription::new(
                conn_id,
                sid,
                subject_str.to_string(),
                queue_str,
                direct_writer.clone(),
                SubKind::Route,
                #[cfg(feature = "accounts")]
                0,
            );

            let mut subs = state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    0,
                )
                .write()
                .expect("subs write lock");
            subs.insert(sub);
            state.has_subs.store(true, Ordering::Relaxed);

            debug!(conn_id, sid, subject = %subject_str, "outbound route sub");
        }
        RouteOp::RouteUnsub { subject, .. } => {
            let key = (subject.clone(), None);
            if let Some(sid) = route_sids.remove(&key) {
                let mut subs = state
                    .get_subs(
                        #[cfg(feature = "accounts")]
                        0,
                    )
                    .write()
                    .expect("subs write lock");
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
            let msg = Msg::new(
                subject.clone(),
                reply.clone(),
                headers.as_ref(),
                payload.clone(),
            );
            // One-hop: skip route subs — messages from a route peer are never
            // re-forwarded to other route peers.
            let (_delivered, expired) = deliver_to_subs_upstream_inner(
                state,
                &msg,
                dirty_writers,
                &DeliveryScope::from_route(),
                #[cfg(feature = "accounts")]
                0, // account_id — will use actual account from wire in Phase 4
            );
            handle_expired_subs_upstream(
                &expired,
                state,
                #[cfg(feature = "accounts")]
                0,
            );
        }
        RouteOp::Ping => {
            let mut w = io::BufWriter::new(tcp);
            w.write_all(b"PONG\r\n")?;
            w.flush()?;
        }
        RouteOp::Pong => {}
        RouteOp::Info(info) => {
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

/// Handle a single binary frame from an outbound route peer.
///
/// Maps binary frames to the same logic as `handle_route_op`:
/// - `Sub`/`Unsub` register/unregister route subscriptions.
/// - `Msg`/`HMsg` deliver to local subscribers.
/// - `Ping` replies with a binary PONG.
/// - `Pong` is a no-op.
#[allow(clippy::too_many_arguments)]
fn handle_bin_frame(
    frame: crate::protocol::bin_proto::BinFrame,
    conn_id: u64,
    state: &ServerState,
    direct_writer: &MsgWriter,
    tcp: &TcpStream,
    dirty_writers: &mut Vec<MsgWriter>,
    route_sid_counter: &mut u64,
    route_sids: &mut HashMap<(bytes::Bytes, Option<bytes::Bytes>), u64>,
) -> io::Result<()> {
    use crate::protocol::bin_proto::BinOp;

    match frame.op {
        BinOp::Sub => {
            *route_sid_counter += 1;
            let sid = *route_sid_counter;
            let subject_b = frame.subject.clone();
            let queue_b = if frame.reply.is_empty() {
                None
            } else {
                Some(frame.reply.clone())
            };
            route_sids.insert((subject_b.clone(), queue_b.clone()), sid);

            let subject_str = unsafe { std::str::from_utf8_unchecked(&subject_b) };
            let queue_str = queue_b
                .as_ref()
                .map(|q| unsafe { std::str::from_utf8_unchecked(q) }.to_string());

            let sub = Subscription::new(
                conn_id,
                sid,
                subject_str.to_string(),
                queue_str,
                direct_writer.clone(),
                SubKind::Route,
                #[cfg(feature = "accounts")]
                0,
            );

            let mut subs = state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    0,
                )
                .write()
                .expect("subs write lock");
            subs.insert(sub);
            state.has_subs.store(true, Ordering::Relaxed);

            debug!(conn_id, sid, subject = %subject_str, "outbound binary route sub");
        }
        BinOp::Unsub => {
            let key = (frame.subject.clone(), None);
            if let Some(sid) = route_sids.remove(&key) {
                let mut subs = state
                    .get_subs(
                        #[cfg(feature = "accounts")]
                        0,
                    )
                    .write()
                    .expect("subs write lock");
                subs.remove(conn_id, sid);
                state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
            }
        }
        BinOp::Msg => {
            let reply = if frame.reply.is_empty() {
                None
            } else {
                Some(frame.reply.clone())
            };
            let msg = Msg::new(frame.subject.clone(), reply, None, frame.payload.clone());
            let (_delivered, expired) = deliver_to_subs_upstream_inner(
                state,
                &msg,
                dirty_writers,
                &DeliveryScope::from_route(),
                #[cfg(feature = "accounts")]
                0,
            );
            handle_expired_subs_upstream(
                &expired,
                state,
                #[cfg(feature = "accounts")]
                0,
            );
        }
        BinOp::HMsg => {
            // Payload layout: [4B hdr_len LE][hdr_bytes][body]
            if frame.payload.len() < 4 {
                return Ok(());
            }
            let hdr_len = u32::from_le_bytes([
                frame.payload[0],
                frame.payload[1],
                frame.payload[2],
                frame.payload[3],
            ]) as usize;
            if frame.payload.len() < 4 + hdr_len {
                return Ok(());
            }
            let hdr_bytes = &frame.payload[4..4 + hdr_len];
            let headers = match nats_proto::parse_headers(hdr_bytes) {
                Ok(h) => h,
                Err(_) => return Ok(()),
            };
            let payload = frame.payload.slice(4 + hdr_len..);
            let reply = if frame.reply.is_empty() {
                None
            } else {
                Some(frame.reply.clone())
            };
            let msg = Msg::new(frame.subject.clone(), reply, Some(&headers), payload);
            let (_delivered, expired) = deliver_to_subs_upstream_inner(
                state,
                &msg,
                dirty_writers,
                &DeliveryScope::from_route(),
                #[cfg(feature = "accounts")]
                0,
            );
            handle_expired_subs_upstream(
                &expired,
                state,
                #[cfg(feature = "accounts")]
                0,
            );
        }
        BinOp::Ping => {
            let mut w = io::BufWriter::new(tcp);
            let mut pong_buf = BytesMut::new();
            crate::protocol::bin_proto::write_pong(&mut pong_buf);
            w.write_all(&pong_buf)?;
            w.flush()?;
        }
        BinOp::Pong => {}
    }
    Ok(())
}

/// Build INFO JSON for route protocol, including `connect_urls` from known peers.
pub(crate) fn build_route_info(state: &ServerState) -> String {
    let cluster_name = state.cluster.name.as_deref().unwrap_or("default");
    let cluster_port = state.cluster.port.unwrap_or(0);

    let connect_urls = {
        let peers = state.cluster.route_peers.lock().expect("route_peers lock");
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
         \"cluster\":\"{}\",\"cluster_port\":{},\"open_wire\":1{}}}\r\n",
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
    let cluster_name = state.cluster.name.as_deref().unwrap_or("default");
    format!(
        "CONNECT {{\"server_id\":\"{}\",\"name\":\"{}\",\"cluster\":\"{}\",\"open_wire\":1}}\r\n",
        state.info.server_id, state.info.server_name, cluster_name,
    )
}

/// Broadcast updated INFO (with current `connect_urls`) to all connected route peers.
pub(crate) fn broadcast_route_info(state: &ServerState) {
    let info_line = build_route_info(state);
    let info_bytes = info_line.as_bytes();
    let writers = state
        .cluster
        .route_writers
        .read()
        .expect("route_writers read lock");
    for writer in writers.values() {
        // Binary-mode connections don't use text INFO gossip.
        if writer.is_binary() {
            continue;
        }
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
