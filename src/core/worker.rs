//! N-worker epoll event loop.
//!
//! Each worker owns one epoll instance multiplexing many client connections.
//! MsgWriter notifies the *worker's* single eventfd, so fan-out to 100
//! connections on the same worker costs 1 eventfd write, not 100.

use std::collections::HashMap;

use rustc_hash::FxHashMap;
use std::io::{self, Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::{Buf, BufMut, BytesMut};
use metrics::{counter, gauge};
use tracing::{debug, info, warn};

use crate::buf::RouteOp;
use crate::buf::{AdaptiveBuf, ClientOp};

use crate::connector::gateway::GatewayHandler;

use crate::connector::leaf::LeafHandler;

use crate::connector::leaf::UpstreamCmd;

use crate::connector::mesh::RouteHandler;
use crate::core::server::ServerState;
use crate::handler::client::ClientHandler;

use crate::handler::propagation::send_existing_route_subs;

use crate::handler::propagation::send_existing_subs;
use crate::handler::{
    handle_expired_subs, ConnCtx, ConnExt, ConnKind, ConnectionHandler, HandleResult,
    MessageDeliveryHub,
};
use crate::nats_proto;
use crate::sub_list::{create_eventfd, DirectBuf, MsgWriter};

use crate::sub_list::{BinSeg, BinSegBuf};
use crate::websocket::{self, DecodeStatus, WsCodec};

use rustls::ServerConnection;

/// Sentinel key in epoll_event.u64 for the worker's eventfd.
const EVENT_FD_KEY: u64 = 0;

// --- Worker commands ---

pub(crate) enum WorkerCmd {
    NewConn {
        id: u64,
        stream: TcpStream,
        addr: SocketAddr,
        is_websocket: bool,
    },
    /// Accept an inbound leaf node connection (hub mode).
    NewLeafConn {
        id: u64,
        stream: TcpStream,
        addr: SocketAddr,
    },
    /// Accept an inbound route connection (cluster mode).
    NewRouteConn {
        id: u64,
        stream: TcpStream,
        addr: SocketAddr,
    },
    /// Accept an inbound gateway connection (gateway mode).
    NewGatewayConn {
        id: u64,
        stream: TcpStream,
        addr: SocketAddr,
    },
    /// Accept an inbound binary-protocol client connection.
    NewBinaryConn {
        id: u64,
        stream: TcpStream,
        addr: SocketAddr,
    },
    /// Broadcast INFO line with ldm:true to all active connections.
    LameDuck(Vec<u8>),
    /// Drain all connections: flush pending data, remove subs, disconnect.
    Drain,
    Shutdown,
}

/// Handle for the acceptor to send connections and commands to a worker.
pub(crate) struct WorkerHandle {
    pub tx: mpsc::Sender<WorkerCmd>,
    event_fd: Arc<OwnedFd>,
    join_handle: Option<std::thread::JoinHandle<()>>,
}

impl WorkerHandle {
    /// Send a new connection to this worker and wake it.
    pub fn send_conn(&self, id: u64, stream: TcpStream, addr: SocketAddr, is_websocket: bool) {
        let _ = self.tx.send(WorkerCmd::NewConn {
            id,
            stream,
            addr,
            is_websocket,
        });
        self.wake();
    }

    /// Send a new inbound leaf node connection to this worker and wake it.
    pub fn send_leaf_conn(&self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let _ = self.tx.send(WorkerCmd::NewLeafConn { id, stream, addr });
        self.wake();
    }

    /// Send a new inbound route connection to this worker and wake it.
    pub fn send_route_conn(&self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let _ = self.tx.send(WorkerCmd::NewRouteConn { id, stream, addr });
        self.wake();
    }

    /// Send a new inbound gateway connection to this worker and wake it.
    pub fn send_gateway_conn(&self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let _ = self.tx.send(WorkerCmd::NewGatewayConn { id, stream, addr });
        self.wake();
    }

    /// Send a new binary-protocol client connection to this worker and wake it.
    pub fn send_binary_conn(&self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let _ = self.tx.send(WorkerCmd::NewBinaryConn { id, stream, addr });
        self.wake();
    }

    /// Send lame duck INFO line to all active connections.
    pub fn send_lame_duck(&self, info_line: Vec<u8>) {
        let _ = self.tx.send(WorkerCmd::LameDuck(info_line));
        self.wake();
    }

    /// Send drain command to all connections and wake the worker.
    pub fn send_drain(&self) {
        let _ = self.tx.send(WorkerCmd::Drain);
        self.wake();
    }

    /// Send shutdown command and wake the worker.
    pub fn shutdown(&self) {
        let _ = self.tx.send(WorkerCmd::Shutdown);
        self.wake();
    }

    /// Wait for the worker thread to finish (with a bounded timeout).
    pub fn join(mut self) {
        if let Some(handle) = self.join_handle.take() {
            // Give the worker up to 5 seconds to finish cleanup.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                if handle.is_finished() {
                    let _ = handle.join();
                    return;
                }
                if std::time::Instant::now() >= deadline {
                    warn!("worker thread did not exit within timeout");
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }

    fn wake(&self) {
        let val: u64 = 1;
        unsafe {
            libc::write(
                self.event_fd.as_raw_fd(),
                &val as *const u64 as *const libc::c_void,
                8,
            );
        }
    }
}

// --- Worker ---

pub(crate) struct Worker {
    epoll_fd: OwnedFd,
    event_fd: Arc<OwnedFd>,
    conns: FxHashMap<u64, ClientState>,
    fd_to_conn: FxHashMap<RawFd, u64>,
    rx: mpsc::Receiver<WorkerCmd>,
    state: Arc<ServerState>,
    info_line: Vec<u8>,
    /// INFO line for inbound leaf connections (port set to leafnode_port).
    hub_info_line: Vec<u8>,
    shutdown: bool,
    /// Accumulated eventfd notifications. Flushed after processing a read batch.
    /// Deduplicates across multiple PUBs in the same read buffer.
    pending_notify: [RawFd; 16],
    pending_notify_count: usize,
    /// Worker index label for metrics.
    worker_label: String,
    /// Thread-local metric accumulators — plain u64, no atomics.
    /// Flushed to the global `metrics` recorder once per epoll batch.
    msgs_received: u64,
    msgs_received_bytes: u64,
    msgs_delivered: u64,
    msgs_delivered_bytes: u64,
    /// Worker index within the worker pool (0-based).
    #[cfg(feature = "worker-affinity")]
    worker_index: usize,
}

// --- Connection state machine ---

#[derive(Clone, Copy)]
enum ConnPhase {
    /// TLS: handshake in progress.
    TlsHandshake,
    /// WebSocket: waiting for HTTP upgrade request.
    WsHandshake,
    /// INFO queued in write_buf, waiting to be flushed.
    SendInfo,
    /// INFO sent, waiting for CONNECT from client.
    WaitConnect,
    /// Normal operation.
    Active,
    /// Connection is draining — no new subs, flush pending, then disconnect.
    Draining,
}

/// Transport layer for a client connection.
enum Transport {
    /// Raw TCP — NATS protocol bytes directly on the socket.
    Raw,
    /// WebSocket — NATS protocol inside WebSocket binary frames.
    WebSocket {
        codec: WsCodec,
        /// Raw bytes read from the socket (WebSocket-framed).
        raw_buf: BytesMut,
        /// Encoded WebSocket frames ready to write to the socket.
        ws_out: BytesMut,
    },
    /// TLS — NATS protocol over encrypted transport using rustls.
    Tls(Box<TlsTransport>),
}

/// TLS transport state (boxed to keep `Transport` enum small).
struct TlsTransport {
    tls_conn: ServerConnection,
    /// Encrypted bytes read from socket, pending TLS processing.
    enc_in: BytesMut,
    /// Encrypted bytes ready to write to socket.
    enc_out: BytesMut,
}

pub(crate) struct ClientState {
    fd: RawFd,
    _stream: TcpStream,
    read_buf: AdaptiveBuf,
    write_buf: BytesMut,
    direct_writer: MsgWriter,
    direct_buf: Arc<Mutex<DirectBuf>>,
    has_pending: Arc<AtomicBool>,
    phase: ConnPhase,
    transport: Transport,
    ext: ConnExt,

    upstream_txs: Vec<mpsc::Sender<UpstreamCmd>>,
    /// Whether EPOLLOUT is currently registered for this fd.
    epoll_has_out: bool,
    /// When false, suppress delivery of the client's own published messages.
    echo: bool,
    /// When true, send 503 no-responders status for request-reply with zero subscribers.
    no_responders: bool,
    /// When this connection was accepted (for auth timeout).
    accepted_at: Instant,
    /// Last time activity was seen on this connection.
    last_activity: Instant,
    /// Number of server-initiated PINGs sent without a PONG response.
    pings_outstanding: u32,
    /// Number of active subscriptions for this connection.
    pub(crate) sub_count: usize,
    /// Per-user permissions (set after CONNECT for Users auth).
    permissions: Option<crate::core::server::Permissions>,
    /// Account this connection belongs to. 0 = `$G` (global/default).
    #[cfg(feature = "accounts")]
    account_id: crate::core::server::AccountId,
    /// Maximum bytes to read per EPOLLIN event. Shrinks when this client publishes
    /// to a congested route (TCP flow control throttles the publisher naturally).
    /// `usize::MAX` means unlimited (default).
    read_budget: usize,
}

impl ClientState {
    /// Build a per-connection context view for protocol handlers.
    fn conn_ctx(&mut self, conn_id: u64) -> ConnCtx<'_> {
        ConnCtx {
            conn_id,
            write_buf: &mut self.write_buf,
            direct_writer: &self.direct_writer,
            echo: self.echo,
            no_responders: self.no_responders,
            sub_count: &mut self.sub_count,

            upstream_txs: &mut self.upstream_txs,
            permissions: &self.permissions,
            ext: &mut self.ext,
            draining: matches!(self.phase, ConnPhase::Draining),
            #[cfg(feature = "accounts")]
            account_id: self.account_id,
        }
    }
}

impl Worker {
    /// Spawn a worker thread. Returns a handle for sending commands.
    pub(crate) fn spawn(index: usize, state: Arc<ServerState>) -> WorkerHandle {
        let (tx, rx) = mpsc::channel();
        let event_fd = Arc::new(create_eventfd());
        let info_json =
            serde_json::to_string(&state.info).expect("failed to serialize server info");
        let info_line = format!("INFO {info_json}\r\n").into_bytes();

        // Build a separate INFO line for inbound leaf connections.
        // Must have: port = leafnode_port, client_id != 0, leafnode_urls present.
        // The Go nats-server checks CID != 0 && leafnode_urls != nil to confirm
        // it connected to a leafnode port (not a client port).

        let hub_info_line = if let Some(lp) = state.leaf.port {
            let mut leaf_info = state.info.clone();
            leaf_info.port = lp;
            leaf_info.client_id = 1; // non-zero signals leafnode port
            leaf_info.leafnode_urls = Some(vec![format!("{}:{}", leaf_info.host, lp)]);
            let json = serde_json::to_string(&leaf_info).expect("failed to serialize leaf info");
            format!("INFO {json}\r\n").into_bytes()
        } else {
            info_line.clone()
        };

        let event_fd_clone = Arc::clone(&event_fd);
        let join_handle = std::thread::Builder::new()
            .name(format!("worker-{index}"))
            .spawn(move || {
                let epoll_fd = unsafe { libc::epoll_create1(0) };
                assert!(epoll_fd >= 0, "epoll_create1 failed");
                let epoll_fd = unsafe { OwnedFd::from_raw_fd(epoll_fd) };

                let mut ev = libc::epoll_event {
                    events: libc::EPOLLIN as u32,
                    u64: EVENT_FD_KEY,
                };
                let ret = unsafe {
                    libc::epoll_ctl(
                        epoll_fd.as_raw_fd(),
                        libc::EPOLL_CTL_ADD,
                        event_fd.as_raw_fd(),
                        &mut ev,
                    )
                };
                assert!(ret == 0, "epoll_ctl ADD eventfd failed");

                let mut worker = Worker {
                    epoll_fd,
                    event_fd,
                    conns: FxHashMap::default(),
                    fd_to_conn: FxHashMap::default(),
                    rx,
                    state,
                    info_line,

                    hub_info_line,
                    shutdown: false,
                    pending_notify: [-1; 16],
                    pending_notify_count: 0,
                    worker_label: index.to_string(),
                    msgs_received: 0,
                    msgs_received_bytes: 0,
                    msgs_delivered: 0,
                    msgs_delivered_bytes: 0,
                    #[cfg(feature = "worker-affinity")]
                    worker_index: index,
                };
                worker.run();
            })
            .expect("failed to spawn worker thread");

        WorkerHandle {
            tx,
            event_fd: event_fd_clone,
            join_handle: Some(join_handle),
        }
    }

    fn run(&mut self) {
        let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 256];

        // Compute epoll timeout for periodic keepalive and auth timeout checks.
        // Use half the ping interval (capped at 30s) so we check reasonably often.
        // If auth timeout is enabled, ensure we wake up often enough to enforce it.
        let ping_interval_ms = self.state.ping_interval_ms.load(Ordering::Relaxed);
        let auth_timeout_ms = self.state.auth_timeout_ms.load(Ordering::Relaxed);
        let epoll_timeout_ms = {
            let ping_half = if ping_interval_ms == 0 {
                u64::MAX
            } else {
                (ping_interval_ms / 2).min(30_000)
            };
            let auth_half = if auth_timeout_ms == 0 || !self.state.auth.is_required() {
                u64::MAX
            } else {
                (auth_timeout_ms / 2).clamp(100, 30_000)
            };
            let min = ping_half.min(auth_half);
            if min == u64::MAX {
                -1i32
            } else {
                min as i32
            }
        };

        loop {
            let n = unsafe {
                libc::epoll_wait(
                    self.epoll_fd.as_raw_fd(),
                    events.as_mut_ptr(),
                    events.len() as i32,
                    epoll_timeout_ms,
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                warn!(error = %err, "epoll_wait failed");
                break;
            }

            for ev in events.iter().take(n as usize) {
                let ev = *ev;
                let key = ev.u64;

                if key == EVENT_FD_KEY {
                    self.handle_eventfd();
                } else {
                    let conn_id = key;
                    if ev.events & libc::EPOLLERR as u32 != 0 {
                        self.remove_conn(conn_id);
                        continue;
                    }
                    if ev.events & libc::EPOLLIN as u32 != 0 {
                        self.handle_read(conn_id);
                    }
                    if ev.events & libc::EPOLLOUT as u32 != 0 {
                        self.handle_write(conn_id);
                    }
                }
            }

            // Flush any pending MsgWriter data after processing all events.
            // This handles local delivery (pub on this worker → sub on this worker)
            // without needing an eventfd round-trip.
            self.flush_pending();

            self.flush_metrics();

            if epoll_timeout_ms > 0 {
                self.check_pings();
                self.check_auth_timeout();
            }

            if self.shutdown {
                break;
            }
        }

        let conn_ids: Vec<u64> = self.conns.keys().copied().collect();
        for conn_id in conn_ids {
            self.remove_conn(conn_id);
        }
    }

    fn handle_eventfd(&mut self) {
        let mut val: u64 = 0;
        unsafe {
            libc::read(
                self.event_fd.as_raw_fd(),
                &mut val as *mut u64 as *mut libc::c_void,
                8,
            );
        }

        while let Ok(cmd) = self.rx.try_recv() {
            match cmd {
                WorkerCmd::NewConn {
                    id,
                    stream,
                    addr,
                    is_websocket,
                } => {
                    self.add_conn(id, stream, addr, is_websocket);
                }

                WorkerCmd::NewLeafConn { id, stream, addr } => {
                    self.add_leaf_conn(id, stream, addr);
                }

                WorkerCmd::NewRouteConn { id, stream, addr } => {
                    self.add_route_conn(id, stream, addr);
                }

                WorkerCmd::NewGatewayConn { id, stream, addr } => {
                    self.add_gateway_conn(id, stream, addr);
                }

                WorkerCmd::NewBinaryConn { id, stream, addr } => {
                    self.add_binary_conn(id, stream, addr);
                }
                WorkerCmd::LameDuck(info_line) => {
                    self.handle_lame_duck(&info_line);
                }
                WorkerCmd::Drain => {
                    self.handle_drain();
                }
                WorkerCmd::Shutdown => {
                    self.shutdown = true;
                    return;
                }
            }
        }
        // Note: flush_pending is called after the event loop iteration in run(),
        // so we don't need to call it here.
    }

    fn add_conn(&mut self, id: u64, stream: TcpStream, addr: SocketAddr, is_websocket: bool) {
        stream.set_nonblocking(true).ok();
        stream.set_nodelay(true).ok();
        let fd = stream.as_raw_fd();

        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: id,
        };
        let ret =
            unsafe { libc::epoll_ctl(self.epoll_fd.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if ret != 0 {
            warn!(id, error = %io::Error::last_os_error(), "epoll_ctl ADD failed");
            return;
        }

        let direct_buf = Arc::new(Mutex::new(DirectBuf::Text(BytesMut::with_capacity(65536))));
        let has_pending = Arc::new(AtomicBool::new(false));
        let direct_writer = MsgWriter::new(
            Arc::clone(&direct_buf),
            Arc::clone(&has_pending),
            Arc::clone(&self.event_fd),
        );

        let (phase, transport, write_buf) = if is_websocket {
            (
                ConnPhase::WsHandshake,
                Transport::WebSocket {
                    codec: WsCodec::new(),
                    raw_buf: BytesMut::with_capacity(4096),
                    ws_out: BytesMut::with_capacity(4096),
                },
                BytesMut::with_capacity(4096),
            )
        } else if let Some(ref tls_config) = self.state.tls_config {
            let tls_conn = ServerConnection::new(Arc::clone(tls_config))
                .expect("failed to create TLS connection");
            (
                ConnPhase::TlsHandshake,
                Transport::Tls(Box::new(TlsTransport {
                    tls_conn,
                    enc_in: BytesMut::with_capacity(8192),
                    enc_out: BytesMut::with_capacity(8192),
                })),
                BytesMut::with_capacity(4096),
            )
        } else {
            let mut wb = BytesMut::with_capacity(4096);
            wb.extend_from_slice(&self.info_line);
            (ConnPhase::SendInfo, Transport::Raw, wb)
        };

        let client = ClientState {
            fd,
            _stream: stream,
            read_buf: AdaptiveBuf::new(self.state.buf_config.max_read_buf),
            write_buf,
            direct_writer,
            direct_buf,
            has_pending,
            phase,
            transport,
            ext: ConnExt::Client,

            upstream_txs: Vec::new(),
            epoll_has_out: false,
            echo: true,
            no_responders: false,
            accepted_at: Instant::now(),
            last_activity: Instant::now(),
            pings_outstanding: 0,
            sub_count: 0,
            permissions: None,
            #[cfg(feature = "accounts")]
            account_id: 0,
            read_budget: usize::MAX,
        };

        self.fd_to_conn.insert(fd, id);
        self.conns.insert(id, client);

        counter!("connections_total", "worker" => self.worker_label.clone()).increment(1);
        gauge!("connections_active", "worker" => self.worker_label.clone())
            .set(self.conns.len() as f64);

        if is_websocket {
            debug!(id, addr = %addr, "accepted websocket connection on worker");
        } else {
            debug!(id, addr = %addr, "accepted connection on worker");
            self.try_flush_conn(id);
        }
    }

    /// Shared setup for inbound leaf/route/gateway connections.
    ///
    /// Configures the socket, registers with epoll, creates MsgWriter,
    /// queues the INFO line, and inserts into the connection map.
    fn register_conn(
        &mut self,
        id: u64,
        stream: TcpStream,
        addr: SocketAddr,
        info_bytes: &[u8],
        ext: ConnExt,
    ) {
        let kind = ext.kind();

        stream.set_nonblocking(true).ok();
        stream.set_nodelay(true).ok();
        let fd = stream.as_raw_fd();

        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: id,
        };
        let ret =
            unsafe { libc::epoll_ctl(self.epoll_fd.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if ret != 0 {
            warn!(id, error = %io::Error::last_os_error(), "epoll_ctl ADD failed for {kind} conn");
            return;
        }

        let direct_buf = Arc::new(Mutex::new(DirectBuf::Text(BytesMut::with_capacity(65536))));
        let has_pending = Arc::new(AtomicBool::new(false));
        let direct_writer = MsgWriter::new(
            Arc::clone(&direct_buf),
            Arc::clone(&has_pending),
            Arc::clone(&self.event_fd),
        );

        let mut write_buf = BytesMut::with_capacity(4096);
        write_buf.extend_from_slice(info_bytes);

        let client = ClientState {
            fd,
            _stream: stream,
            read_buf: AdaptiveBuf::new(self.state.buf_config.max_read_buf),
            write_buf,
            direct_writer,
            direct_buf,
            has_pending,
            phase: ConnPhase::SendInfo,
            transport: Transport::Raw,
            ext,

            upstream_txs: Vec::new(),
            epoll_has_out: false,
            echo: true,
            no_responders: false,
            accepted_at: Instant::now(),
            last_activity: Instant::now(),
            pings_outstanding: 0,
            sub_count: 0,
            permissions: None,
            #[cfg(feature = "accounts")]
            account_id: 0,
            read_budget: usize::MAX,
        };

        self.fd_to_conn.insert(fd, id);
        self.conns.insert(id, client);

        let metric = format!("{kind}_connections_total");
        counter!(metric, "worker" => self.worker_label.clone()).increment(1);
        gauge!("connections_active", "worker" => self.worker_label.clone())
            .set(self.conns.len() as f64);

        debug!(id, addr = %addr, "accepted inbound {kind} connection");
        self.try_flush_conn(id);
    }

    fn add_leaf_conn(&mut self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let info = self.hub_info_line.clone();
        self.register_conn(
            id,
            stream,
            addr,
            &info,
            ConnExt::Leaf {
                leaf_sid_counter: 0,
                leaf_sids: HashMap::new(),
            },
        );
    }

    fn add_route_conn(&mut self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let info = crate::connector::mesh::build_route_info(&self.state);
        self.register_conn(
            id,
            stream,
            addr,
            info.as_bytes(),
            ConnExt::Route {
                route_sid_counter: 0,
                route_sids: HashMap::new(),
                peer_server_id: None,
                binary: false,
            },
        );
    }

    fn add_gateway_conn(&mut self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let info = crate::connector::gateway::get_gateway_info(&self.state);
        self.register_conn(
            id,
            stream,
            addr,
            info.as_bytes(),
            ConnExt::Gateway {
                gateway_sid_counter: 0,
                gateway_sids: HashMap::new(),
                gateway_sids_by_subject: HashMap::new(),
                peer_gateway_name: None,
            },
        );
    }

    /// Create a binary-protocol client connection — no handshake, active immediately.
    fn add_binary_conn(&mut self, id: u64, stream: TcpStream, addr: SocketAddr) {
        // Binary clients use register_conn with an empty INFO (no handshake)
        // and then upgrade to Active phase with binary DirectBuf.
        self.register_conn(id, stream, addr, b"", ConnExt::BinaryClient);

        // Upgrade: replace text DirectBuf with binary, set phase to Active.
        if let Some(client) = self.conns.get_mut(&id) {
            let direct_buf = Arc::new(Mutex::new(DirectBuf::Binary(BinSegBuf::new())));
            let has_pending = Arc::new(AtomicBool::new(false));
            client.direct_writer = MsgWriter::new_binary_shared(
                Arc::clone(&direct_buf),
                Arc::clone(&has_pending),
                Arc::clone(&self.event_fd),
            );
            client.direct_buf = direct_buf;
            client.has_pending = has_pending;
            client.phase = ConnPhase::Active;
            client.echo = false;
        }
    }

    fn remove_conn(&mut self, conn_id: u64) {
        if let Some(client) = self.conns.remove(&conn_id) {
            unsafe {
                libc::epoll_ctl(
                    self.epoll_fd.as_raw_fd(),
                    libc::EPOLL_CTL_DEL,
                    client.fd,
                    std::ptr::null_mut(),
                );
            }
            self.fd_to_conn.remove(&client.fd);
            self.state
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);

            if client.ext.is_leaf() {
                self.state
                    .leaf
                    .inbound_writers
                    .write()
                    .expect("inbound_writers write lock")
                    .remove(&conn_id);
            }

            if client.ext.is_route() {
                self.state
                    .cluster
                    .route_writers
                    .write()
                    .expect("route_writers write lock")
                    .remove(&conn_id);
                if let ConnExt::Route {
                    peer_server_id: Some(ref sid),
                    ..
                } = client.ext
                {
                    self.state
                        .cluster
                        .route_peers
                        .lock()
                        .expect("route_peers lock")
                        .connected
                        .remove(sid);
                }
            }

            if client.ext.is_gateway() {
                self.state
                    .gateway
                    .writers
                    .write()
                    .expect("gateway writers write lock")
                    .remove(&conn_id);
                if let ConnExt::Gateway {
                    peer_gateway_name: Some(ref name),
                    ..
                } = client.ext
                {
                    let mut peers = self.state.gateway.peers.lock().expect("gateway peers lock");
                    if let Some(ids) = peers.connected.get_mut(name) {
                        ids.remove(&conn_id);
                        if ids.is_empty() {
                            peers.connected.remove(name);
                        }
                    }
                }
            }

            cleanup_conn(conn_id, &self.state);
            gauge!("connections_active", "worker" => self.worker_label.clone())
                .set(self.conns.len() as f64);
            debug!(conn_id, "connection removed");
        }
    }

    /// Scan all connections for pending MsgWriter data, drain into write_buf,
    /// and try to flush to socket.
    fn flush_pending(&mut self) {
        let epoll_fd = self.epoll_fd.as_raw_fd();
        let mut to_remove: Vec<u64> = Vec::new();
        // Tracks the highest congestion level seen this pass (0.0 = at HWM, 1.0 = at max_pending).
        // Used to apply graduated backpressure after the scan loop.
        let mut max_congestion: f64 = 0.0;

        for (conn_id, client) in &mut self.conns {
            if !client.has_pending.load(Ordering::Acquire) {
                continue;
            }
            client.has_pending.store(false, Ordering::Release);
            // Drain the per-connection direct buffer.
            // For Raw+Text: O(1) split, then writev with write_buf.
            // For Raw+Binary: take segment list, build scatter-gather iovecs (zero-copy).
            // For WS/TLS: materialise into write_buf for further encoding.
            enum DrainResult {
                Empty,
                FlatBytes(BytesMut),

                Segs { segs: Vec<BinSeg>, total_len: usize },
            }
            let drained = {
                let mut dbuf = client.direct_buf.lock().expect("direct_buf lock");
                if dbuf.is_empty() {
                    DrainResult::Empty
                } else if matches!(client.transport, Transport::Raw) {
                    match &mut *dbuf {
                        DirectBuf::Text(b) => DrainResult::FlatBytes(b.split()),

                        DirectBuf::Binary(seg_buf) => {
                            let total_len = seg_buf.total_len;
                            DrainResult::Segs {
                                segs: seg_buf.take(),
                                total_len,
                            }
                        }
                    }
                } else {
                    // WS/TLS: flatten everything into write_buf
                    match &mut *dbuf {
                        DirectBuf::Text(b) => {
                            client.write_buf.extend_from_slice(b);
                            b.clear();
                        }

                        DirectBuf::Binary(seg_buf) => {
                            seg_buf.materialize_into(&mut client.write_buf);
                        }
                    }
                    DrainResult::Empty
                }
            };
            // Materialise DrainResult: text path uses direct_data; binary uses segs_drain.
            let direct_data: BytesMut;

            let segs_drain: Option<(Vec<BinSeg>, usize)>;
            match drained {
                DrainResult::Empty => {
                    direct_data = BytesMut::new();
                    segs_drain = None;
                }
                DrainResult::FlatBytes(b) => {
                    direct_data = b;
                    segs_drain = None;
                }

                DrainResult::Segs { segs, total_len } => {
                    direct_data = BytesMut::new();
                    segs_drain = Some((segs, total_len));
                }
            }

            // Slow consumer detection and graduated backpressure.
            //
            // Hard limit (max_pending): disconnect the connection to protect server memory.
            // Soft limit (max_pending / 2): start applying a proportional sleep after the
            // scan loop so the worker stops reading client data, letting kernel TCP flow
            // control propagate backpressure to the publisher naturally.
            let is_route = client.ext.is_route();
            let max_pend = if is_route {
                self.state.buf_config.max_pending_route
            } else {
                self.state.buf_config.max_pending
            };
            if max_pend > 0 {
                let extra = if let Some((_, tl)) = &segs_drain {
                    *tl
                } else {
                    direct_data.len()
                };
                let pending = match &client.transport {
                    Transport::Raw => client.write_buf.len() + extra,
                    Transport::WebSocket { ws_out, .. } => client.write_buf.len() + ws_out.len(),
                    Transport::Tls(ref tls) => client.write_buf.len() + tls.enc_out.len(),
                };
                if pending > max_pend {
                    warn!(
                        conn_id = *conn_id,
                        conn_type = client.ext.kind(),
                        pending_bytes = pending,
                        max = max_pend,
                        "slow consumer, disconnecting"
                    );
                    counter!("slow_consumers_total", "worker" => self.worker_label.clone())
                        .increment(1);
                    self.state
                        .stats
                        .slow_consumers
                        .fetch_add(1, Ordering::Relaxed);
                    to_remove.push(*conn_id);
                    continue;
                }
                // Update route congestion level for cross-worker backpressure.
                if is_route {
                    let ratio = pending as f64 / max_pend as f64;
                    let level = if ratio < 0.25 {
                        0
                    } else if ratio < 0.75 {
                        1
                    } else {
                        2
                    };
                    client.direct_writer.set_congestion(level);
                }
                // Track max congestion across all connections (for metrics).
                let hwm = max_pend / 2;
                if pending > hwm {
                    let congestion = (pending - hwm) as f64 / (max_pend - hwm) as f64;
                    if congestion > max_congestion {
                        max_congestion = congestion;
                    }
                }
            }

            // For TLS: encrypt write_buf into enc_out, then write enc_out
            if let Transport::Tls(ref mut tls) = &mut client.transport {
                if !client.write_buf.is_empty() {
                    let _ = tls.tls_conn.writer().write_all(&client.write_buf);
                    client.write_buf.clear();
                }
                // Extract encrypted records
                loop {
                    let mut tmp = [0u8; 8192];
                    match tls.tls_conn.write_tls(&mut io::Cursor::new(&mut tmp[..])) {
                        Ok(0) => break,
                        Ok(n) => tls.enc_out.extend_from_slice(&tmp[..n]),
                        Err(_) => break,
                    }
                }
            }

            // For WebSocket: encode write_buf into ws_out, then write ws_out
            // For Raw: we use writev with write_buf + direct_data (zero-copy)
            let is_raw = matches!(client.transport, Transport::Raw);
            if !is_raw {
                match &mut client.transport {
                    Transport::WebSocket { ws_out, .. } => {
                        if !client.write_buf.is_empty() {
                            WsCodec::encode(&client.write_buf, ws_out);
                            client.write_buf.clear();
                        }
                    }
                    Transport::Tls(_) => {} // already handled above
                    Transport::Raw => unreachable!(),
                }
            }

            // Inline flush
            let mut error = false;
            if is_raw {
                // ── Binary segment path (zero-copy scatter-gather) ──────────

                if let Some((segs, _total_len)) = segs_drain {
                    // Build iovec array: write_buf (if any) + per-segment iovecs.
                    // Each Msg segment contributes up to 4 iovecs; Inline contributes 1.
                    // IOV_MAX on Linux is 1024 — for very large batches materialise first.
                    let wb_len = client.write_buf.len();
                    let max_iovs = (if wb_len > 0 { 1 } else { 0 }) + segs.len() * 4;
                    if max_iovs > 1024 {
                        // Safety valve: materialise segs into write_buf and fall through.
                        for seg in segs {
                            match seg {
                                BinSeg::Inline(b) => client.write_buf.extend_from_slice(&b),
                                BinSeg::Msg(f) => {
                                    client.write_buf.extend_from_slice(&f.header);
                                    client.write_buf.extend_from_slice(&f.subject);
                                    client.write_buf.extend_from_slice(&f.reply);
                                    client.write_buf.extend_from_slice(&f.payload);
                                }
                            }
                        }
                        // Fall through to the standard write_buf-only write below.
                    } else {
                        // Build iovecs without holding a borrow across the write call.
                        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(max_iovs);
                        if wb_len > 0 {
                            iovecs.push(libc::iovec {
                                iov_base: client.write_buf.as_ptr() as *mut libc::c_void,
                                iov_len: wb_len,
                            });
                        }
                        for seg in &segs {
                            match seg {
                                BinSeg::Inline(b) if !b.is_empty() => {
                                    iovecs.push(libc::iovec {
                                        iov_base: b.as_ptr() as *mut libc::c_void,
                                        iov_len: b.len(),
                                    });
                                }
                                BinSeg::Msg(f) => {
                                    iovecs.push(libc::iovec {
                                        iov_base: f.header.as_ptr() as *mut libc::c_void,
                                        iov_len: 9,
                                    });
                                    if !f.subject.is_empty() {
                                        iovecs.push(libc::iovec {
                                            iov_base: f.subject.as_ptr() as *mut libc::c_void,
                                            iov_len: f.subject.len(),
                                        });
                                    }
                                    if !f.reply.is_empty() {
                                        iovecs.push(libc::iovec {
                                            iov_base: f.reply.as_ptr() as *mut libc::c_void,
                                            iov_len: f.reply.len(),
                                        });
                                    }
                                    if !f.payload.is_empty() {
                                        iovecs.push(libc::iovec {
                                            iov_base: f.payload.as_ptr() as *mut libc::c_void,
                                            iov_len: f.payload.len(),
                                        });
                                    }
                                }
                                _ => {}
                            }
                        }
                        let total_bytes: usize = iovecs.iter().map(|v| v.iov_len).sum();
                        // SAFETY: iovecs point into BytesMut / Bytes buffers that remain
                        // valid until end of this block; fd is the connection socket.
                        let n = unsafe {
                            libc::writev(client.fd, iovecs.as_ptr(), iovecs.len() as i32)
                        };
                        if n < 0 {
                            let err = io::Error::last_os_error();
                            if err.kind() == io::ErrorKind::WouldBlock {
                                // Materialise everything into write_buf for later retry.
                                // (write_buf content is already there; append segs.)
                                for seg in segs {
                                    match seg {
                                        BinSeg::Inline(b) => client.write_buf.extend_from_slice(&b),
                                        BinSeg::Msg(f) => {
                                            client.write_buf.extend_from_slice(&f.header);
                                            client.write_buf.extend_from_slice(&f.subject);
                                            client.write_buf.extend_from_slice(&f.reply);
                                            client.write_buf.extend_from_slice(&f.payload);
                                        }
                                    }
                                }
                                if !client.epoll_has_out {
                                    epoll_mod(epoll_fd, client.fd, *conn_id, true);
                                    client.epoll_has_out = true;
                                }
                            } else {
                                error = true;
                            }
                        } else {
                            let written = n as usize;
                            if written >= total_bytes {
                                client.write_buf.clear();
                            } else {
                                // Partial write: flatten all bytes, skip written, keep rest.
                                // Rebuild write_buf from scratch = [wb | segs], advance written.
                                let mut all = BytesMut::with_capacity(total_bytes);
                                all.extend_from_slice(&client.write_buf);
                                for seg in segs {
                                    match seg {
                                        BinSeg::Inline(b) => all.extend_from_slice(&b),
                                        BinSeg::Msg(f) => {
                                            all.extend_from_slice(&f.header);
                                            all.extend_from_slice(&f.subject);
                                            all.extend_from_slice(&f.reply);
                                            all.extend_from_slice(&f.payload);
                                        }
                                    }
                                }
                                all.advance(written);
                                client.write_buf = all;
                                if !client.epoll_has_out {
                                    epoll_mod(epoll_fd, client.fd, *conn_id, true);
                                    client.epoll_has_out = true;
                                }
                            }
                        }
                        if error {
                            to_remove.push(*conn_id);
                        }
                        continue;
                    }
                }

                // ── Text / flat-buffer path ─────────────────────────────────
                // writev path: write_buf and direct_data as two iovecs
                let wb_len = client.write_buf.len();
                let dd_len = direct_data.len();
                let total = wb_len + dd_len;
                let mut written_total = 0usize;

                while written_total < total {
                    // SAFETY: pointers are valid slices from BytesMut, offsets
                    // are bounds-checked by the while condition.
                    let n = unsafe {
                        if wb_len > 0 && dd_len > 0 && written_total < wb_len {
                            // Both buffers have data and we haven't finished write_buf
                            let iovs = [
                                libc::iovec {
                                    iov_base: client.write_buf.as_ptr().add(written_total)
                                        as *mut libc::c_void,
                                    iov_len: wb_len - written_total,
                                },
                                libc::iovec {
                                    iov_base: direct_data.as_ptr() as *mut libc::c_void,
                                    iov_len: dd_len,
                                },
                            ];
                            libc::writev(client.fd, iovs.as_ptr(), 2)
                        } else if written_total < wb_len {
                            // Only write_buf has data (or direct_data is empty)
                            libc::write(
                                client.fd,
                                client.write_buf.as_ptr().add(written_total) as *const libc::c_void,
                                wb_len - written_total,
                            )
                        } else {
                            // Past write_buf, writing from direct_data
                            let dd_offset = written_total - wb_len;
                            libc::write(
                                client.fd,
                                direct_data.as_ptr().add(dd_offset) as *const libc::c_void,
                                dd_len - dd_offset,
                            )
                        }
                    };
                    if n < 0 {
                        let err = io::Error::last_os_error();
                        if err.kind() == io::ErrorKind::WouldBlock {
                            if !client.epoll_has_out {
                                epoll_mod(epoll_fd, client.fd, *conn_id, true);
                                client.epoll_has_out = true;
                            }
                            break;
                        }
                        error = true;
                        break;
                    }
                    written_total += n as usize;
                }

                // Advance/merge buffers based on how much was written
                if written_total >= wb_len {
                    client.write_buf.clear();
                    let dd_consumed = written_total - wb_len;
                    if dd_consumed < dd_len {
                        // Partial write into direct_data — merge remainder into write_buf
                        client
                            .write_buf
                            .extend_from_slice(&direct_data[dd_consumed..]);
                    }
                } else {
                    // Partial write within write_buf — merge all of direct_data
                    client.write_buf.advance(written_total);
                    if !direct_data.is_empty() {
                        client.write_buf.extend_from_slice(&direct_data);
                    }
                }
            } else {
                // WS/TLS: single-buffer write
                let (write_ptr, write_len) = match &mut client.transport {
                    Transport::WebSocket { ws_out, .. } => (ws_out.as_ptr(), ws_out.len()),
                    Transport::Tls(ref tls) => (tls.enc_out.as_ptr(), tls.enc_out.len()),
                    Transport::Raw => unreachable!(),
                };
                let mut written_total = 0usize;
                let total = write_len;
                while written_total < total {
                    let n = unsafe {
                        libc::write(
                            client.fd,
                            write_ptr.add(written_total) as *const libc::c_void,
                            total - written_total,
                        )
                    };
                    if n < 0 {
                        let err = io::Error::last_os_error();
                        if err.kind() == io::ErrorKind::WouldBlock {
                            if !client.epoll_has_out {
                                epoll_mod(epoll_fd, client.fd, *conn_id, true);
                                client.epoll_has_out = true;
                            }
                            break;
                        }
                        error = true;
                        break;
                    }
                    written_total += n as usize;
                }
                match &mut client.transport {
                    Transport::WebSocket { ws_out, .. } => ws_out.advance(written_total),
                    Transport::Tls(ref mut tls) => tls.enc_out.advance(written_total),
                    Transport::Raw => unreachable!(),
                }
            }

            if error {
                to_remove.push(*conn_id);
                continue;
            }

            let is_empty = match &client.transport {
                Transport::Raw => client.write_buf.is_empty(),
                Transport::WebSocket { ws_out, .. } => ws_out.is_empty(),
                Transport::Tls(ref tls) => tls.enc_out.is_empty(),
            };
            if is_empty && client.epoll_has_out {
                epoll_mod(epoll_fd, client.fd, *conn_id, false);
                client.epoll_has_out = false;
            }
        }

        for conn_id in to_remove {
            self.remove_conn(conn_id);
        }
    }

    /// Send accumulated eventfd notifications to remote workers, then clear.
    fn flush_notifications(&mut self) {
        let val: u64 = 1;
        for i in 0..self.pending_notify_count {
            unsafe {
                libc::write(
                    self.pending_notify[i],
                    &val as *const u64 as *const libc::c_void,
                    8,
                );
            }
        }
        self.pending_notify_count = 0;
    }

    /// Flush thread-local metric accumulators to the global recorder.
    /// Called once per epoll batch — amortises atomic overhead across all
    /// messages processed in a single `epoll_wait` wake-up.
    fn flush_metrics(&mut self) {
        if self.msgs_received > 0 {
            counter!("messages_received_total", "worker" => self.worker_label.clone())
                .increment(self.msgs_received);
            counter!("messages_received_bytes", "worker" => self.worker_label.clone())
                .increment(self.msgs_received_bytes);
            self.state
                .stats
                .in_msgs
                .fetch_add(self.msgs_received, Ordering::Relaxed);
            self.state
                .stats
                .in_bytes
                .fetch_add(self.msgs_received_bytes, Ordering::Relaxed);
            self.msgs_received = 0;
            self.msgs_received_bytes = 0;
        }
        if self.msgs_delivered > 0 {
            counter!("messages_delivered_total", "worker" => self.worker_label.clone())
                .increment(self.msgs_delivered);
            counter!("messages_delivered_bytes", "worker" => self.worker_label.clone())
                .increment(self.msgs_delivered_bytes);
            self.state
                .stats
                .out_msgs
                .fetch_add(self.msgs_delivered, Ordering::Relaxed);
            self.state
                .stats
                .out_bytes
                .fetch_add(self.msgs_delivered_bytes, Ordering::Relaxed);
            self.msgs_delivered = 0;
            self.msgs_delivered_bytes = 0;
        }
    }

    /// Send keepalive PINGs to idle connections and close unresponsive ones.
    fn check_pings(&mut self) {
        let now = Instant::now();
        let interval_ms = self.state.ping_interval_ms.load(Ordering::Relaxed);
        let interval = std::time::Duration::from_millis(interval_ms);
        let max_pings = self.state.max_pings_outstanding.load(Ordering::Relaxed);
        let epoll_fd = self.epoll_fd.as_raw_fd();
        let mut to_remove: Vec<u64> = Vec::new();

        for (conn_id, client) in &mut self.conns {
            if !matches!(client.phase, ConnPhase::Active) {
                continue;
            }
            let elapsed = now.duration_since(client.last_activity);
            if elapsed < interval {
                continue;
            }
            if client.pings_outstanding >= max_pings {
                warn!(conn_id = *conn_id, "connection stale, closing");
                to_remove.push(*conn_id);
                continue;
            }
            client.write_buf.extend_from_slice(b"PING\r\n");
            client.pings_outstanding += 1;
            client.last_activity = now;
            if !client.epoll_has_out {
                epoll_mod(epoll_fd, client.fd, *conn_id, true);
                client.epoll_has_out = true;
            }
        }

        for conn_id in to_remove {
            self.remove_conn(conn_id);
        }
    }

    /// Disconnect clients that haven't completed authentication within the deadline.
    fn check_auth_timeout(&mut self) {
        let timeout_ms = self.state.auth_timeout_ms.load(Ordering::Relaxed);
        if timeout_ms == 0 || !self.state.auth.is_required() {
            return;
        }
        let timeout = std::time::Duration::from_millis(timeout_ms);
        let now = Instant::now();
        let epoll_fd = self.epoll_fd.as_raw_fd();
        let mut to_remove: Vec<u64> = Vec::new();

        for (conn_id, client) in &mut self.conns {
            if matches!(client.phase, ConnPhase::Active) {
                continue;
            }
            if now.duration_since(client.accepted_at) > timeout {
                warn!(conn_id = *conn_id, "authentication timeout");
                counter!("auth_timeout_total", "worker" => self.worker_label.clone()).increment(1);
                client
                    .write_buf
                    .extend_from_slice(b"-ERR 'Authentication Timeout'\r\n");
                if !client.epoll_has_out {
                    epoll_mod(epoll_fd, client.fd, *conn_id, true);
                    client.epoll_has_out = true;
                }
                to_remove.push(*conn_id);
            }
        }

        for conn_id in to_remove {
            self.try_flush_conn(conn_id);
            self.remove_conn(conn_id);
        }
    }

    /// Broadcast INFO with ldm:true to all active connections.
    fn handle_lame_duck(&mut self, info_line: &[u8]) {
        let epoll_fd = self.epoll_fd.as_raw_fd();
        for (conn_id, client) in &mut self.conns {
            if !matches!(client.phase, ConnPhase::Active) {
                continue;
            }
            client.write_buf.extend_from_slice(info_line);
            if !client.epoll_has_out {
                epoll_mod(epoll_fd, client.fd, *conn_id, true);
                client.epoll_has_out = true;
            }
        }
    }

    /// Drain all connections: flush pending data, remove subscriptions, disconnect.
    fn handle_drain(&mut self) {
        let conn_ids: Vec<u64> = self.conns.keys().copied().collect();
        for conn_id in conn_ids {
            let is_active = matches!(
                self.conns.get(&conn_id).map(|c| &c.phase),
                Some(ConnPhase::Active)
            );
            if !is_active {
                self.remove_conn(conn_id);
                continue;
            }
            if let Some(client) = self.conns.get_mut(&conn_id) {
                client.phase = ConnPhase::Draining;
                {
                    let mut dbuf = client.direct_buf.lock().expect("direct_buf lock");
                    if !dbuf.is_empty() {
                        match &mut *dbuf {
                            DirectBuf::Text(b) => {
                                client.write_buf.extend_from_slice(b);
                                b.clear();
                            }

                            DirectBuf::Binary(seg_buf) => {
                                seg_buf.materialize_into(&mut client.write_buf);
                            }
                        }
                    }
                }
            }
            self.try_flush_conn(conn_id);
            cleanup_conn(conn_id, &self.state);
            self.remove_conn(conn_id);
        }
    }

    /// Try to flush write_buf to the socket. Registers/removes EPOLLOUT as needed.
    /// For WebSocket connections, write_buf is encoded into ws_out first.
    /// For TLS connections, delegates to tls_flush_encrypted.
    fn try_flush_conn(&mut self, conn_id: u64) {
        if matches!(
            self.conns.get(&conn_id).map(|c| &c.transport),
            Some(Transport::Tls(..))
        ) {
            self.tls_flush_encrypted(conn_id);
            if let Some(client) = self.conns.get_mut(&conn_id) {
                if matches!(client.phase, ConnPhase::SendInfo) {
                    if let Transport::Tls(ref tls) = client.transport {
                        if tls.enc_out.is_empty() {
                            client.phase = ConnPhase::WaitConnect;
                        }
                    }
                }
            }
            return;
        }

        let epoll_fd = self.epoll_fd.as_raw_fd();

        let client = match self.conns.get_mut(&conn_id) {
            Some(c) => c,
            None => return,
        };

        // For WebSocket: encode pending write_buf into ws_out
        // Exception: during WsHandshake, write_buf contains raw HTTP response
        if matches!(client.transport, Transport::WebSocket { .. })
            && !matches!(client.phase, ConnPhase::WsHandshake)
            && !client.write_buf.is_empty()
        {
            if let Transport::WebSocket { ws_out, .. } = &mut client.transport {
                WsCodec::encode(&client.write_buf, ws_out);
                client.write_buf.clear();
            }
        }

        // Determine which buffer to flush
        let flush_ws = matches!(client.transport, Transport::WebSocket { .. })
            && !matches!(client.phase, ConnPhase::WsHandshake);

        let buf = if flush_ws {
            if let Transport::WebSocket { ws_out, .. } = &mut client.transport {
                ws_out
            } else {
                unreachable!()
            }
        } else {
            &mut client.write_buf
        };

        while !buf.is_empty() {
            let n =
                unsafe { libc::write(client.fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    if !client.epoll_has_out {
                        epoll_mod(epoll_fd, client.fd, conn_id, true);
                        client.epoll_has_out = true;
                    }
                    return;
                }
                // Write error — remove connection
                let _ = buf;
                self.remove_conn(conn_id);
                return;
            }
            buf.advance(n as usize);
        }

        // All data flushed
        if client.epoll_has_out {
            epoll_mod(epoll_fd, client.fd, conn_id, false);
            client.epoll_has_out = false;
        }

        if matches!(client.phase, ConnPhase::SendInfo) {
            client.phase = ConnPhase::WaitConnect;
        }
    }

    fn handle_read(&mut self, conn_id: u64) {
        let transport_type = match self.conns.get(&conn_id).map(|c| &c.transport) {
            Some(Transport::WebSocket { .. }) => 1,
            Some(Transport::Tls(..)) => 2,
            _ => 0,
        };

        match transport_type {
            1 => self.handle_read_ws(conn_id),
            2 => self.handle_read_tls(conn_id),
            _ => self.handle_read_raw(conn_id),
        }
    }

    fn handle_read_raw(&mut self, conn_id: u64) {
        let mut got_eof = false;
        let mut total_read = 0usize;
        loop {
            let client = match self.conns.get_mut(&conn_id) {
                Some(c) => c,
                None => return,
            };
            let budget = client.read_budget;
            // Cap total bytes read across all iterations of this loop.
            let remaining = budget.saturating_sub(total_read);
            if remaining == 0 {
                break;
            }
            match client.read_buf.read_from_fd(client.fd, remaining) {
                Ok(0) => {
                    got_eof = true;
                    break;
                }
                Ok(n) => {
                    client.read_buf.after_read(n);
                    total_read += n;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => {
                    self.remove_conn(conn_id);
                    return;
                }
            }
        }

        if let Some(client) = self.conns.get_mut(&conn_id) {
            client.last_activity = Instant::now();
        }
        self.process_read_buf(conn_id);
        self.flush_notifications();

        if got_eof {
            self.remove_conn(conn_id);
        }
    }

    fn handle_read_ws(&mut self, conn_id: u64) {
        let is_handshake = matches!(
            self.conns.get(&conn_id).map(|c| &c.phase),
            Some(ConnPhase::WsHandshake)
        );

        if is_handshake {
            loop {
                let client = match self.conns.get_mut(&conn_id) {
                    Some(c) => c,
                    None => return,
                };
                match client.read_buf.read_from_fd(client.fd, usize::MAX) {
                    Ok(0) => {
                        self.remove_conn(conn_id);
                        return;
                    }
                    Ok(n) => {
                        client.read_buf.after_read(n);
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        self.remove_conn(conn_id);
                        return;
                    }
                }
            }
            self.process_read_buf(conn_id);
            return;
        }

        loop {
            let client = match self.conns.get_mut(&conn_id) {
                Some(c) => c,
                None => return,
            };
            let raw_buf = if let Transport::WebSocket { raw_buf, .. } = &mut client.transport {
                raw_buf
            } else {
                return;
            };
            raw_buf.reserve(4096);
            let spare = raw_buf.spare_capacity_mut();
            let n = unsafe {
                libc::read(
                    client.fd,
                    spare.as_mut_ptr() as *mut libc::c_void,
                    spare.len(),
                )
            };
            if n == 0 {
                self.remove_conn(conn_id);
                return;
            }
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    break;
                }
                self.remove_conn(conn_id);
                return;
            }
            unsafe { raw_buf.set_len(raw_buf.len() + n as usize) };
        }

        let mut close = false;
        let mut decode_err = false;
        if let Some(client) = self.conns.get_mut(&conn_id) {
            if let Transport::WebSocket { codec, raw_buf, .. } = &mut client.transport {
                loop {
                    match codec.decode(raw_buf, &mut client.read_buf) {
                        Ok(DecodeStatus::Complete) => continue,
                        Ok(DecodeStatus::NeedMore) => break,
                        Ok(DecodeStatus::Close) => {
                            close = true;
                            break;
                        }
                        Err(_) => {
                            decode_err = true;
                            break;
                        }
                    }
                }
            }
        }

        if decode_err {
            self.remove_conn(conn_id);
            return;
        }

        if close {
            // Send close frame back
            if let Some(client) = self.conns.get_mut(&conn_id) {
                if let Transport::WebSocket { ws_out, .. } = &mut client.transport {
                    WsCodec::encode_close(ws_out);
                }
            }
            self.try_flush_conn(conn_id);
            self.remove_conn(conn_id);
            return;
        }

        if let Some(client) = self.conns.get_mut(&conn_id) {
            client.last_activity = Instant::now();
        }
        self.process_read_buf(conn_id);
        self.flush_notifications();
    }

    fn handle_read_tls(&mut self, conn_id: u64) {
        loop {
            let client = match self.conns.get_mut(&conn_id) {
                Some(c) => c,
                None => return,
            };
            let tls = if let Transport::Tls(ref mut tls) = client.transport {
                tls
            } else {
                return;
            };
            tls.enc_in.reserve(8192);
            let spare = tls.enc_in.spare_capacity_mut();
            let n = unsafe {
                libc::read(
                    client.fd,
                    spare.as_mut_ptr() as *mut libc::c_void,
                    spare.len(),
                )
            };
            if n == 0 {
                self.remove_conn(conn_id);
                return;
            }
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    break;
                }
                self.remove_conn(conn_id);
                return;
            }
            unsafe { tls.enc_in.set_len(tls.enc_in.len() + n as usize) };
        }

        let mut error = false;
        if let Some(client) = self.conns.get_mut(&conn_id) {
            if let Transport::Tls(ref mut tls) = client.transport {
                loop {
                    if tls.enc_in.is_empty() {
                        break;
                    }
                    match tls.tls_conn.read_tls(&mut io::Cursor::new(&tls.enc_in[..])) {
                        Ok(0) => break,
                        Ok(n) => {
                            tls.enc_in.advance(n);
                        }
                        Err(e) => {
                            warn!(conn_id, error = %e, "TLS read_tls error");
                            error = true;
                            break;
                        }
                    }
                }

                if !error {
                    match tls.tls_conn.process_new_packets() {
                        Ok(state) => {
                            if state.plaintext_bytes_to_read() > 0 {
                                let n = state.plaintext_bytes_to_read();
                                client.read_buf.reserve(n);
                                let chunk = client.read_buf.chunk_mut();
                                let buf = unsafe {
                                    std::slice::from_raw_parts_mut(chunk.as_mut_ptr(), chunk.len())
                                };
                                match tls.tls_conn.reader().read(buf) {
                                    Ok(read) => {
                                        unsafe { client.read_buf.advance_mut(read) };
                                    }
                                    Err(e) => {
                                        warn!(conn_id, error = %e, "TLS plaintext read error");
                                        error = true;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(conn_id, error = %e, "TLS process error");
                            error = true;
                        }
                    }
                }
            }
        }

        if error {
            // Try to write any TLS alert before disconnecting
            self.tls_flush_encrypted(conn_id);
            self.remove_conn(conn_id);
            return;
        }

        self.tls_flush_encrypted(conn_id);

        if let Some(client) = self.conns.get_mut(&conn_id) {
            client.last_activity = Instant::now();
        }
        self.process_read_buf(conn_id);
        self.flush_notifications();
    }

    /// Encrypt pending write_buf data via TLS and flush encrypted output to socket.
    fn tls_flush_encrypted(&mut self, conn_id: u64) {
        let epoll_fd = self.epoll_fd.as_raw_fd();
        let client = match self.conns.get_mut(&conn_id) {
            Some(c) => c,
            None => return,
        };

        if let Transport::Tls(ref mut tls) = client.transport {
            if !client.write_buf.is_empty() {
                if let Err(e) = tls.tls_conn.writer().write_all(&client.write_buf) {
                    warn!(conn_id, error = %e, "TLS write error");
                }
                client.write_buf.clear();
            }

            loop {
                let mut tmp = [0u8; 8192];
                match tls.tls_conn.write_tls(&mut io::Cursor::new(&mut tmp[..])) {
                    Ok(0) => break,
                    Ok(n) => {
                        tls.enc_out.extend_from_slice(&tmp[..n]);
                    }
                    Err(_) => break,
                }
            }

            while !tls.enc_out.is_empty() {
                let n = unsafe {
                    libc::write(
                        client.fd,
                        tls.enc_out.as_ptr() as *const libc::c_void,
                        tls.enc_out.len(),
                    )
                };
                if n < 0 {
                    let err = io::Error::last_os_error();
                    if err.kind() == io::ErrorKind::WouldBlock {
                        if !client.epoll_has_out {
                            epoll_mod(epoll_fd, client.fd, conn_id, true);
                            client.epoll_has_out = true;
                        }
                        return;
                    }
                    return;
                }
                tls.enc_out.advance(n as usize);
            }

            if tls.enc_out.is_empty() && client.epoll_has_out {
                epoll_mod(epoll_fd, client.fd, conn_id, false);
                client.epoll_has_out = false;
            }
        }
    }

    fn process_read_buf(&mut self, conn_id: u64) {
        loop {
            // Single lookup: read phase, kind, and can_skip together to avoid
            // separate conns.get calls in dispatch_active and can_skip_publishes.
            let (phase, kind, can_skip) = match self.conns.get(&conn_id) {
                None => return,
                Some(c) => {
                    let phase = c.phase;
                    let kind = c.ext.kind_tag();
                    // Only compute can_skip for Client connections — Route/Leaf/Gateway
                    // never use it, and the check accesses upstream_txs + atomic loads.
                    let can_skip = matches!(
                        (phase, &kind),
                        (ConnPhase::Active | ConnPhase::Draining, ConnKind::Client)
                    ) && {
                        let has_gw = self.state.gateway.has_interest.load(Ordering::Acquire);
                        c.upstream_txs.is_empty()
                            && !self.state.has_subs.load(Ordering::Acquire)
                            && !has_gw
                    };
                    (phase, kind, can_skip)
                }
            };
            match phase {
                ConnPhase::SendInfo => return,
                ConnPhase::TlsHandshake => {
                    self.process_tls_handshake(conn_id);
                    return;
                }
                ConnPhase::WsHandshake => {
                    if !self.process_ws_handshake(conn_id) {
                        return;
                    }
                }
                ConnPhase::WaitConnect => {
                    if self.check_max_control_line(conn_id) {
                        return;
                    }
                    if !self.process_wait_connect(conn_id) {
                        return;
                    }
                }
                ConnPhase::Active | ConnPhase::Draining => {
                    if !self.dispatch_active(conn_id, kind, can_skip) {
                        return;
                    }
                }
            }
        }
    }

    /// Process TLS handshake phase: check completion, queue INFO, flush encrypted.
    fn process_tls_handshake(&mut self, conn_id: u64) {
        let handshake_done = match self.conns.get(&conn_id) {
            Some(c) => {
                if let Transport::Tls(ref tls) = c.transport {
                    !tls.tls_conn.is_handshaking()
                } else {
                    false
                }
            }
            None => return,
        };
        if handshake_done {
            if let Some(c) = self.conns.get(&conn_id) {
                if let Transport::Tls(ref tls) = c.transport {
                    if let Some(certs) = tls.tls_conn.peer_certificates() {
                        if let Some(cert) = certs.first() {
                            tracing::info!(
                                conn_id,
                                cert_len = cert.len(),
                                "client certificate verified"
                            );
                        }
                    }
                }
            }
            let Some(client) = self.conns.get_mut(&conn_id) else {
                return;
            };
            client.write_buf.extend_from_slice(&self.info_line);
            client.phase = ConnPhase::SendInfo;
            self.tls_flush_encrypted(conn_id);
            let flushed = match self.conns.get(&conn_id) {
                Some(c) => {
                    if let Transport::Tls(ref tls) = c.transport {
                        tls.enc_out.is_empty()
                    } else {
                        true
                    }
                }
                None => return,
            };
            if flushed {
                if let Some(c) = self.conns.get_mut(&conn_id) {
                    c.phase = ConnPhase::WaitConnect;
                }
            }
        }
    }

    /// Process WebSocket handshake: parse HTTP upgrade, send accept response, queue INFO.
    ///
    /// Returns `true` if the caller should `continue` the loop, `false` to `return`.
    fn process_ws_handshake(&mut self, conn_id: u64) -> bool {
        let result = {
            let Some(client) = self.conns.get(&conn_id) else {
                return false;
            };
            websocket::parse_ws_upgrade(&client.read_buf)
        };
        match result {
            None => false, // need more data
            Some(Err(_)) => {
                if let Some(client) = self.conns.get_mut(&conn_id) {
                    client
                        .write_buf
                        .extend_from_slice(b"HTTP/1.1 400 Bad Request\r\n\r\n");
                }
                self.try_flush_conn(conn_id);
                self.remove_conn(conn_id);
                false
            }
            Some(Ok((upgrade, consumed))) => {
                let response = websocket::build_ws_accept_response(&upgrade.key);
                let Some(client) = self.conns.get_mut(&conn_id) else {
                    return false;
                };
                client.read_buf.advance(consumed);
                if let Transport::WebSocket { ws_out, .. } = &mut client.transport {
                    ws_out.extend_from_slice(&response);
                }
                let info_line = self.info_line.clone();
                let Some(client) = self.conns.get_mut(&conn_id) else {
                    return false;
                };
                client.write_buf.extend_from_slice(&info_line);
                client.phase = ConnPhase::SendInfo;
                self.try_flush_conn(conn_id);
                true // continue loop
            }
        }
    }

    /// Check if read buffer exceeds `max_control_line` without a newline.
    ///
    /// Returns `true` if exceeded (caller should return), `false` if OK.
    fn check_max_control_line(&mut self, conn_id: u64) -> bool {
        let max_ctrl = self.state.max_control_line.load(Ordering::Relaxed);
        if max_ctrl > 0 {
            let exceeded = {
                let Some(client) = self.conns.get(&conn_id) else {
                    return false;
                };
                let buf: &[u8] = &client.read_buf;
                buf.len() > max_ctrl && memchr::memchr(b'\n', &buf[..max_ctrl]).is_none()
            };
            if exceeded {
                warn!(
                    conn_id,
                    max_control_line = max_ctrl,
                    "maximum control line exceeded"
                );
                counter!("connections_rejected_total").increment(1);
                if let Some(client) = self.conns.get_mut(&conn_id) {
                    client
                        .write_buf
                        .extend_from_slice(b"-ERR 'Maximum Control Line Exceeded'\r\n");
                }
                self.try_flush_conn(conn_id);
                self.remove_conn(conn_id);
                return true;
            }
        }
        false
    }

    /// Process WaitConnect phase: parse CONNECT from client/leaf/route/gateway.
    ///
    /// Returns `true` if the caller should `continue` the loop, `false` to `return`.
    fn process_wait_connect(&mut self, conn_id: u64) -> bool {
        // Route connections use the route protocol parser (INFO+CONNECT+PING).

        let is_route_conn = self
            .conns
            .get(&conn_id)
            .map(|c| c.ext.is_route())
            .unwrap_or(false);

        if is_route_conn {
            let route_op = {
                let Some(client) = self.conns.get_mut(&conn_id) else {
                    return false;
                };
                match nats_proto::try_parse_route_op(&mut client.read_buf) {
                    Ok(op) => op,
                    Err(_) => {
                        self.remove_conn(conn_id);
                        return false;
                    }
                }
            };
            match route_op {
                Some(RouteOp::Info(peer_info)) => {
                    if !peer_info.connect_urls.is_empty() {
                        crate::connector::mesh::process_gossip_urls(
                            &self.state,
                            &peer_info.connect_urls,
                        );
                    }
                    return true; // continue
                }
                Some(RouteOp::Connect(connect_info)) => {
                    let peer_sid = connect_info.server_id.clone();

                    if let Some(ref sid) = peer_sid {
                        if *sid == self.state.info.server_id {
                            debug!(conn_id, "self-connect on inbound route, closing");
                            self.remove_conn(conn_id);
                            return false;
                        }
                        // Dedup: if an outbound (or earlier inbound) route to this peer
                        // is already active, drop this connection.  Whichever direction
                        // arrives first wins; the other is silently closed.  This prevents
                        // bidirectional seed configs from creating two route connections
                        // per pair, which would double-deliver every routed message.
                        let is_duplicate = {
                            let mut peers = self
                                .state
                                .cluster
                                .route_peers
                                .lock()
                                .expect("route_peers lock");
                            if peers.connected.contains_key(sid.as_str()) {
                                true
                            } else {
                                // Register so subsequent connections from this peer are dropped.
                                peers.connected.insert(sid.clone(), "inbound".to_string());
                                false
                            }
                        };
                        if is_duplicate {
                            debug!(
                                conn_id,
                                peer_id = %sid,
                                "duplicate inbound route, dropping"
                            );
                            self.remove_conn(conn_id);
                            return false;
                        }
                    }

                    let use_binary =
                        connect_info.open_wire == Some(1) && self.state.info.open_wire == Some(1);

                    // Peer's CONNECT — go Active, register, exchange subs.
                    let Some(client) = self.conns.get_mut(&conn_id) else {
                        return false;
                    };
                    client.phase = ConnPhase::Active;
                    client.echo = false;

                    {
                        client.upstream_txs = self
                            .state
                            .leaf
                            .upstream_txs
                            .read()
                            .expect("upstream_txs read lock")
                            .clone();
                    }
                    // Always respond with text PONG — the handshake stays in text.
                    // Binary framing only activates for Active-phase data frames.
                    client.write_buf.extend_from_slice(b"PONG\r\n");
                    // Ensure flush_pending sends PONG even when there are no existing
                    // subs to sync (send_existing_route_subs wouldn't set has_pending).
                    client.has_pending.store(true, Ordering::Release);

                    if let ConnExt::Route {
                        ref mut peer_server_id,
                        ref mut binary,
                        ..
                    } = client.ext
                    {
                        *peer_server_id = peer_sid.clone();
                        *binary = use_binary;
                    }

                    // If binary mode, switch the direct buffer to segment mode and replace
                    // the direct_writer with a binary-mode writer sharing the same Arcs.
                    if use_binary {
                        {
                            let mut dbuf = client.direct_buf.lock().expect("direct_buf lock");
                            *dbuf = DirectBuf::Binary(BinSegBuf::new());
                        }
                        let binary_dw = crate::sub_list::MsgWriter::new_binary_shared(
                            Arc::clone(&client.direct_buf),
                            Arc::clone(&client.has_pending),
                            Arc::clone(&self.event_fd),
                        );
                        client.direct_writer = binary_dw;
                    }

                    let dw = client.direct_writer.clone();
                    self.state
                        .cluster
                        .route_writers
                        .write()
                        .expect("route_writers write lock")
                        .insert(conn_id, dw);

                    send_existing_route_subs(&self.state, &client.direct_writer);

                    crate::connector::mesh::broadcast_route_info(&self.state);

                    info!(conn_id, use_binary, "inbound route connected");
                }
                Some(RouteOp::Ping) => {
                    if let Some(client) = self.conns.get_mut(&conn_id) {
                        client.write_buf.extend_from_slice(b"PONG\r\n");
                    }
                    return true; // continue
                }
                Some(_) => {
                    self.remove_conn(conn_id);
                    return false;
                }
                None => return false, // need more data
            }
            // After route handshake ops, continue the outer loop to process
            // remaining data in the read buffer.
            return true;
        }

        // Gateway connections use the gateway protocol parser (INFO+CONNECT+PING).

        let is_gateway_conn = self
            .conns
            .get(&conn_id)
            .map(|c| c.ext.is_gateway())
            .unwrap_or(false);

        if is_gateway_conn {
            let gw_op = {
                let Some(client) = self.conns.get_mut(&conn_id) else {
                    return false;
                };
                match nats_proto::try_parse_gateway_op(&mut client.read_buf) {
                    Ok(op) => op,
                    Err(_) => {
                        self.remove_conn(conn_id);
                        return false;
                    }
                }
            };
            match gw_op {
                Some(nats_proto::GatewayOp::Info(peer_info)) => {
                    if let Some(ref urls) = peer_info.gateway_urls {
                        if !urls.is_empty() {
                            let tx = self
                                .state
                                .gateway
                                .connect_tx
                                .lock()
                                .expect("gateway connect_tx lock");
                            let mut peers =
                                self.state.gateway.peers.lock().expect("gateway peers lock");
                            for url in urls {
                                if peers.known_urls.insert(url.clone()) {
                                    if let Some(ref sender) = *tx {
                                        let _ = sender.send(url.clone());
                                    }
                                }
                            }
                        }
                    }
                    return true; // continue
                }
                Some(nats_proto::GatewayOp::Connect(connect_info)) => {
                    let peer_gw_name = connect_info.gateway.clone();

                    // Peer's CONNECT — go Active, register, exchange subs.
                    let Some(client) = self.conns.get_mut(&conn_id) else {
                        return false;
                    };
                    client.phase = ConnPhase::Active;
                    client.echo = false;

                    {
                        client.upstream_txs = self
                            .state
                            .leaf
                            .upstream_txs
                            .read()
                            .expect("upstream_txs read lock")
                            .clone();
                    }
                    client.write_buf.extend_from_slice(b"PONG\r\n");

                    if let ConnExt::Gateway {
                        ref mut peer_gateway_name,
                        ..
                    } = client.ext
                    {
                        *peer_gateway_name = peer_gw_name.clone();
                    }

                    let dw = client.direct_writer.clone();
                    self.state
                        .gateway
                        .writers
                        .write()
                        .expect("gateway writers write lock")
                        .insert(conn_id, dw);

                    if let Some(ref name) = peer_gw_name {
                        let mut peers =
                            self.state.gateway.peers.lock().expect("gateway peers lock");
                        peers
                            .connected
                            .entry(name.clone())
                            .or_default()
                            .insert(conn_id);
                    }

                    send_existing_route_subs(&self.state, &client.direct_writer);

                    crate::connector::gateway::broadcast_gateway_info(&self.state);

                    info!(
                        conn_id,
                        peer_gateway = ?peer_gw_name,
                        "inbound gateway connected"
                    );
                }
                Some(nats_proto::GatewayOp::Ping) => {
                    if let Some(client) = self.conns.get_mut(&conn_id) {
                        client.write_buf.extend_from_slice(b"PONG\r\n");
                    }
                    return true; // continue
                }
                Some(_) => {
                    self.remove_conn(conn_id);
                    return false;
                }
                None => return false, // need more data
            }
            // After gateway handshake ops, continue the outer loop.
            return true;
        }

        let op = {
            let Some(client) = self.conns.get_mut(&conn_id) else {
                return false;
            };
            match nats_proto::try_parse_client_op_cursor(&mut client.read_buf) {
                Ok(op) => op,
                Err(_) => {
                    self.remove_conn(conn_id);
                    return false;
                }
            }
        };
        match op {
            Some(ClientOp::Connect(connect_info)) => {
                let is_leaf = self
                    .conns
                    .get(&conn_id)
                    .map(|c| c.ext.is_leaf())
                    .unwrap_or(false);

                if is_leaf {
                    // Inbound leaf node — validate leaf auth, send PING, go Active.

                    {
                        let leaf_perms = self.state.leaf.inbound_auth.validate(&connect_info);
                        let leaf_perms = match leaf_perms {
                            Some(p) => p.map(std::sync::Arc::new),
                            None => {
                                warn!(conn_id, "leaf authorization violation");
                                counter!(
                                    "auth_failures_total",
                                    "worker" => self.worker_label.clone()
                                )
                                .increment(1);
                                if let Some(client) = self.conns.get_mut(&conn_id) {
                                    client
                                        .write_buf
                                        .extend_from_slice(b"-ERR 'Authorization Violation'\r\n");
                                }
                                self.try_flush_conn(conn_id);
                                self.remove_conn(conn_id);
                                return false;
                            }
                        };

                        let Some(client) = self.conns.get_mut(&conn_id) else {
                            return false;
                        };
                        client.phase = ConnPhase::Active;
                        client.echo = false; // suppress echo for leaf conns
                        client.permissions = leaf_perms.as_ref().map(|p| p.as_ref().clone());

                        {
                            client.upstream_txs = self
                                .state
                                .leaf
                                .upstream_txs
                                .read()
                                .expect("upstream_txs read lock")
                                .clone();
                        }
                        client.write_buf.extend_from_slice(b"PING\r\n");

                        let dw = client.direct_writer.clone();
                        self.state
                            .leaf
                            .inbound_writers
                            .write()
                            .expect("inbound_writers write lock")
                            .insert(conn_id, (dw, leaf_perms.clone()));

                        send_existing_subs(&self.state, &client.direct_writer, &leaf_perms);

                        info!(conn_id, "inbound leaf connected");
                    }
                } else {
                    if !self
                        .state
                        .auth
                        .validate(&connect_info, &self.state.info.nonce)
                    {
                        warn!(conn_id, "authorization violation");
                        counter!(
                            "auth_failures_total",
                            "worker" => self.worker_label.clone()
                        )
                        .increment(1);
                        if let Some(client) = self.conns.get_mut(&conn_id) {
                            client
                                .write_buf
                                .extend_from_slice(b"-ERR 'Authorization Violation'\r\n");
                        }
                        self.try_flush_conn(conn_id);
                        self.remove_conn(conn_id);
                        return false;
                    }
                    let perms = self.state.auth.lookup_permissions(&connect_info);
                    #[cfg(feature = "accounts")]
                    let acct_id = self.state.lookup_account(connect_info.user.as_deref());
                    let Some(client) = self.conns.get_mut(&conn_id) else {
                        return false;
                    };
                    client.phase = ConnPhase::Active;
                    client.echo = connect_info.echo;
                    client.no_responders = connect_info.no_responders && connect_info.headers;
                    client.permissions = perms;
                    #[cfg(feature = "accounts")]
                    {
                        client.account_id = acct_id;
                    }

                    {
                        client.upstream_txs = self
                            .state
                            .leaf
                            .upstream_txs
                            .read()
                            .expect("upstream_txs read lock")
                            .clone();
                    }
                    info!(conn_id, "client connected");
                }
            }
            Some(_) => {
                if let Some(client) = self.conns.get_mut(&conn_id) {
                    client
                        .write_buf
                        .extend_from_slice(b"-ERR 'expected CONNECT'\r\n");
                }
                self.try_flush_conn(conn_id);
                self.remove_conn(conn_id);
                return false;
            }
            None => return false, // need more data
        }
        true // continue loop
    }

    /// Dispatch to the appropriate handler based on connection kind.
    ///
    /// `kind` and `can_skip` are pre-computed by the caller in the same
    /// `conns.get` that read the connection phase, avoiding a second lookup.
    fn dispatch_active(&mut self, conn_id: u64, kind: ConnKind, can_skip: bool) -> bool {
        match kind {
            ConnKind::Leaf => self.process_active::<LeafHandler>(conn_id),

            ConnKind::Route => {
                let is_binary = self
                    .conns
                    .get(&conn_id)
                    .map(|c| matches!(&c.ext, ConnExt::Route { binary: true, .. }))
                    .unwrap_or(false);
                if is_binary {
                    self.process_active_route_binary(conn_id)
                } else {
                    self.process_active::<RouteHandler>(conn_id)
                }
            }

            ConnKind::Gateway => self.process_active::<GatewayHandler>(conn_id),

            ConnKind::BinaryClient => self.process_active_binary_client(conn_id),
            ConnKind::Client => self.process_active_client(conn_id, can_skip),
        }
    }

    /// Generic active-phase processing: parse → handle → cleanup.
    ///
    /// Works for any connection type that implements `ConnectionHandler`.
    /// The client path uses its own method due to the `can_skip` optimization.
    fn process_active<H: ConnectionHandler>(&mut self, conn_id: u64) -> bool {
        let op = match self.parse_conn_op::<H>(conn_id) {
            Some(op) => op,
            None => return false,
        };
        self.dispatch_and_apply::<H>(conn_id, op)
    }

    /// Parse the next protocol operation from a connection's read buffer.
    ///
    /// Returns `Some(op)` on success. On `None` (need more data) shrinks the
    /// buffer and checks the max-control-line limit; on error disconnects.
    fn parse_conn_op<H: ConnectionHandler>(&mut self, conn_id: u64) -> Option<H::Op> {
        let max_ctrl = self.state.max_control_line.load(Ordering::Relaxed);
        let result = {
            let client = self.conns.get_mut(&conn_id)?;
            H::parse_op(&mut client.read_buf)
        };
        match result {
            Ok(Some(op)) => Some(op),
            Ok(None) => {
                // Check max control line only when incomplete and buffer is large.
                // In the hot path buf.len() <= max_ctrl so this is a no-op.
                let exceeded = max_ctrl > 0
                    && self.conns.get(&conn_id).is_some_and(|c| {
                        let buf: &[u8] = &c.read_buf;
                        buf.len() > max_ctrl && memchr::memchr(b'\n', &buf[..max_ctrl]).is_none()
                    });
                if exceeded {
                    warn!(
                        conn_id,
                        max_control_line = max_ctrl,
                        "maximum control line exceeded"
                    );
                    counter!("connections_rejected_total").increment(1);
                    if let Some(client) = self.conns.get_mut(&conn_id) {
                        client
                            .write_buf
                            .extend_from_slice(b"-ERR 'Maximum Control Line Exceeded'\r\n");
                    }
                    self.try_flush_conn(conn_id);
                    self.remove_conn(conn_id);
                } else if let Some(client) = self.conns.get_mut(&conn_id) {
                    client.read_buf.try_shrink();
                }
                None
            }
            Err(_) => {
                self.remove_conn(conn_id);
                None
            }
        }
    }

    /// Dispatch a parsed op through its handler, then apply the result.
    fn dispatch_and_apply<H: ConnectionHandler>(&mut self, conn_id: u64, op: H::Op) -> bool {
        let (result, expired, congested) = {
            let Some(client) = self.conns.get_mut(&conn_id) else {
                return false;
            };
            let mut conn_ctx = client.conn_ctx(conn_id);
            let mut worker_ctx = MessageDeliveryHub {
                state: &self.state,
                event_fd: self.event_fd.as_raw_fd(),
                pending_notify: &mut self.pending_notify,
                pending_notify_count: &mut self.pending_notify_count,
                msgs_received: &mut self.msgs_received,
                msgs_received_bytes: &mut self.msgs_received_bytes,
                msgs_delivered: &mut self.msgs_delivered,
                msgs_delivered_bytes: &mut self.msgs_delivered_bytes,
                worker_label: &self.worker_label,
                #[cfg(feature = "worker-affinity")]
                worker_index: self.worker_index,
                route_congested: false,
            };
            let (result, expired) = H::handle_op(&mut conn_ctx, &mut worker_ctx, op);
            (result, expired, worker_ctx.route_congested)
        };
        self.adjust_read_budget(conn_id, congested);
        let cont = self.apply_result(conn_id, result, expired);
        // Stop processing more ops from this client when a route is congested.
        // Remaining data stays in the read buffer; the next event loop iteration
        // will resume parsing with the reduced read budget in effect.
        if congested {
            return false;
        }
        cont
    }

    /// Adjust a connection's read budget based on route congestion.
    ///
    /// When a publish hits a congested route, shrink the budget so less data
    /// is read from this client on the next EPOLLIN. The kernel TCP receive
    /// buffer fills up, TCP flow control kicks in, and the publisher slows
    /// down — all without blocking the worker event loop.
    #[inline]
    fn adjust_read_budget(&mut self, conn_id: u64, route_congested: bool) {
        if let Some(client) = self.conns.get_mut(&conn_id) {
            if route_congested {
                // On first congestion, snap from unlimited to a concrete cap.
                // Then halve on each subsequent congestion signal.
                if client.read_budget > 65536 {
                    client.read_budget = 65536;
                } else {
                    client.read_budget = (client.read_budget / 2).max(256);
                }
            } else if client.read_budget < usize::MAX {
                // Double toward unlimited when congestion clears.
                client.read_budget = client.read_budget.saturating_mul(2);
            }
        }
    }

    /// Handle expired subs cleanup and flush/disconnect from a `HandleResult`.
    fn apply_result(
        &mut self,
        conn_id: u64,
        result: HandleResult,
        expired: Vec<(u64, u64)>,
    ) -> bool {
        #[cfg(feature = "accounts")]
        let acct = self.conns.get(&conn_id).map(|c| c.account_id).unwrap_or(0);
        handle_expired_subs(
            &expired,
            &self.state,
            &mut self.conns,
            &self.worker_label,
            #[cfg(feature = "accounts")]
            acct,
        );
        match result {
            HandleResult::Ok => true,
            HandleResult::Flush => {
                self.try_flush_conn(conn_id);
                true
            }
            HandleResult::Disconnect => {
                self.try_flush_conn(conn_id);
                self.remove_conn(conn_id);
                false
            }
        }
    }

    /// Client-specific active processing with `can_skip` optimization.
    ///
    /// `can_skip` is pre-computed by the caller alongside the phase/kind read,
    /// avoiding a separate conns.get call here.
    fn process_active_client(&mut self, conn_id: u64, can_skip: bool) -> bool {
        let op = match self.parse_client_op(conn_id, can_skip) {
            Some(op) => op,
            None => return false,
        };
        match op {
            ClientOp::Pong if can_skip => true,
            ClientOp::Pong => {
                if let Some(client) = self.conns.get_mut(&conn_id) {
                    client.pings_outstanding = 0;
                    client.last_activity = Instant::now();
                }
                true
            }
            op => self.dispatch_and_apply::<ClientHandler>(conn_id, op),
        }
    }

    /// Parse a client op, optionally using the skip-publish fast path.
    fn parse_client_op(&mut self, conn_id: u64, can_skip: bool) -> Option<ClientOp> {
        let max_ctrl = self.state.max_control_line.load(Ordering::Relaxed);
        let result = {
            let client = self.conns.get_mut(&conn_id)?;
            if can_skip {
                nats_proto::try_skip_or_parse_client_op_cursor(&mut client.read_buf)
            } else {
                nats_proto::try_parse_client_op_cursor(&mut client.read_buf)
            }
        };
        match result {
            Ok(Some(op)) => Some(op),
            Ok(None) => {
                // Check max control line only when incomplete and buffer is large.
                // In the hot path buf.len() <= max_ctrl so this is a no-op.
                let exceeded = max_ctrl > 0
                    && self.conns.get(&conn_id).is_some_and(|c| {
                        let buf: &[u8] = &c.read_buf;
                        buf.len() > max_ctrl && memchr::memchr(b'\n', &buf[..max_ctrl]).is_none()
                    });
                if exceeded {
                    warn!(
                        conn_id,
                        max_control_line = max_ctrl,
                        "maximum control line exceeded"
                    );
                    counter!("connections_rejected_total").increment(1);
                    if let Some(client) = self.conns.get_mut(&conn_id) {
                        client
                            .write_buf
                            .extend_from_slice(b"-ERR 'Maximum Control Line Exceeded'\r\n");
                    }
                    self.try_flush_conn(conn_id);
                    self.remove_conn(conn_id);
                } else if let Some(client) = self.conns.get_mut(&conn_id) {
                    client.read_buf.try_shrink();
                }
                None
            }
            Err(_) => {
                self.remove_conn(conn_id);
                None
            }
        }
    }

    fn handle_write(&mut self, conn_id: u64) {
        self.try_flush_conn(conn_id);
    }

    /// Parse and dispatch a single binary frame from an inbound binary-mode route connection.
    ///
    /// Maps `BinOp::Ping/Pong` in-place, converts `Msg/HMsg/Sub/Unsub` to `RouteOp`
    /// and delegates to `RouteHandler` via `dispatch_and_apply`.
    fn process_active_route_binary(&mut self, conn_id: u64) -> bool {
        use crate::protocol::bin_proto::{self, BinOp};

        // The outbound side sends INFO+CONNECT+PING as text in one batch.
        // After we transition to binary Active, the text PING\r\n may still
        // be sitting at the front of the read buffer. Consume it silently —
        // we already sent PONG when processing CONNECT, so a second PONG
        // would arrive as invalid bytes in the peer's binary parser.
        {
            let Some(client) = self.conns.get_mut(&conn_id) else {
                return false;
            };
            let buf = client.read_buf.bytes_mut();
            if buf.starts_with(b"PING\r\n") {
                buf.advance(6);
                return true;
            }
        }

        let frame = {
            let Some(client) = self.conns.get_mut(&conn_id) else {
                return false;
            };
            bin_proto::try_decode(client.read_buf.bytes_mut())
        };
        let frame = match frame {
            Some(f) => f,
            None => return false,
        };

        match frame.op {
            BinOp::Ping => {
                let Some(client) = self.conns.get_mut(&conn_id) else {
                    return false;
                };
                bin_proto::write_pong(&mut client.write_buf);
                return true;
            }
            BinOp::Pong => return true,
            _ => {}
        }

        let route_op = bin_frame_to_route_op(frame);
        match route_op {
            Some(op) => self.dispatch_and_apply::<RouteHandler>(conn_id, op),
            None => true,
        }
    }

    /// Parse and dispatch frames from a binary-protocol client connection.
    ///
    /// - `Msg`: publish to local subscribers (and propagate via routes/gateways/leaf).
    /// - `Sub`: register a subscription; subject=pattern, reply=queue, payload=SID (u32 LE).
    /// - `Unsub`: remove a subscription; subject=SID (u32 LE).
    /// - `Ping`/`Pong`: keepalive.
    fn process_active_binary_client(&mut self, conn_id: u64) -> bool {
        use crate::handler::propagation::propagate_all_interest;
        use crate::handler::{DeliveryScope, Msg};
        use crate::protocol::bin_proto::{self, BinOp};
        use crate::sub_list::{SubKind, Subscription};

        let frame = {
            let Some(client) = self.conns.get_mut(&conn_id) else {
                return false;
            };
            bin_proto::try_decode(client.read_buf.bytes_mut())
        };
        let frame = match frame {
            Some(f) => f,
            None => return false,
        };

        match frame.op {
            BinOp::Ping => {
                let Some(client) = self.conns.get_mut(&conn_id) else {
                    return false;
                };
                bin_proto::write_pong(&mut client.write_buf);
                return true;
            }
            BinOp::Pong => return true,
            BinOp::Msg | BinOp::HMsg => {
                let reply = if frame.reply.is_empty() {
                    None
                } else {
                    Some(frame.reply)
                };
                let msg = Msg::new(frame.subject, reply, None, frame.payload);
                let (expired, congested) = {
                    let Some(client) = self.conns.get_mut(&conn_id) else {
                        return false;
                    };
                    let conn_ctx = client.conn_ctx(conn_id);
                    let mut wctx = MessageDeliveryHub {
                        state: &self.state,
                        event_fd: self.event_fd.as_raw_fd(),
                        pending_notify: &mut self.pending_notify,
                        pending_notify_count: &mut self.pending_notify_count,
                        msgs_received: &mut self.msgs_received,
                        msgs_received_bytes: &mut self.msgs_received_bytes,
                        msgs_delivered: &mut self.msgs_delivered,
                        msgs_delivered_bytes: &mut self.msgs_delivered_bytes,
                        worker_label: &self.worker_label,
                        #[cfg(feature = "worker-affinity")]
                        worker_index: self.worker_index,
                        route_congested: false,
                    };
                    *wctx.msgs_received += 1;
                    *wctx.msgs_received_bytes += msg.payload.len() as u64;
                    let (_, expired) = wctx.publish(
                        &msg,
                        conn_ctx.conn_id,
                        &DeliveryScope::local(false),
                        #[cfg(feature = "accounts")]
                        conn_ctx.account_id,
                    );
                    let congested = wctx.route_congested;
                    (expired, congested)
                };
                self.adjust_read_budget(conn_id, congested);
                self.apply_result(conn_id, crate::handler::HandleResult::Ok, expired);
                // Stop processing more frames from this client when a route is
                // congested — remaining data stays in the read buffer and will be
                // processed on the next event loop iteration, giving the route
                // writer time to drain.
                if congested {
                    return false;
                }
            }
            BinOp::Sub => {
                // subject=pattern, reply=queue, payload=SID as u32 LE (4 bytes)
                let sid = if frame.payload.len() >= 4 {
                    u32::from_le_bytes([
                        frame.payload[0],
                        frame.payload[1],
                        frame.payload[2],
                        frame.payload[3],
                    ]) as u64
                } else {
                    return true; // malformed, ignore
                };
                let subject_str = std::str::from_utf8(&frame.subject).unwrap_or("");
                let queue_str = if frame.reply.is_empty() {
                    None
                } else {
                    Some(std::str::from_utf8(&frame.reply).unwrap_or("").to_string())
                };

                let Some(writer) = self.conns.get(&conn_id).map(|c| c.direct_writer.clone()) else {
                    return true;
                };
                let sub = Subscription::new(
                    conn_id,
                    sid,
                    subject_str.to_string(),
                    queue_str,
                    writer,
                    SubKind::BinaryClient,
                    #[cfg(feature = "accounts")]
                    0,
                );
                {
                    let mut subs = self
                        .state
                        .get_subs(
                            #[cfg(feature = "accounts")]
                            0,
                        )
                        .write()
                        .expect("subs write lock");
                    subs.insert(sub);
                    self.state.has_subs.store(true, Ordering::Release);
                }
                if let Some(c) = self.conns.get_mut(&conn_id) {
                    c.sub_count += 1;
                }
                propagate_all_interest(
                    &self.state,
                    frame.subject.as_ref(),
                    if frame.reply.is_empty() {
                        None
                    } else {
                        Some(frame.reply.as_ref())
                    },
                    true,
                    #[cfg(feature = "accounts")]
                    b"$G",
                );
            }
            BinOp::Unsub => {
                // subject=SID as u32 LE (4 bytes)
                let sid = if frame.subject.len() >= 4 {
                    u32::from_le_bytes([
                        frame.subject[0],
                        frame.subject[1],
                        frame.subject[2],
                        frame.subject[3],
                    ]) as u64
                } else {
                    return true;
                };
                let removed = {
                    let mut subs = self
                        .state
                        .get_subs(
                            #[cfg(feature = "accounts")]
                            0,
                        )
                        .write()
                        .expect("subs write lock");
                    let r = subs.remove(conn_id, sid);
                    self.state
                        .has_subs
                        .store(!subs.is_empty(), Ordering::Release);
                    r
                };
                if removed.is_some() {
                    if let Some(c) = self.conns.get_mut(&conn_id) {
                        c.sub_count = c.sub_count.saturating_sub(1);
                    }
                }
            }
        }
        true
    }
}

/// Convert a decoded `BinFrame` to a `RouteOp` for dispatch via `RouteHandler`.
///
/// `Ping` and `Pong` are handled in-place by `process_active_route_binary` and
/// must not be passed to this function.
fn bin_frame_to_route_op(frame: crate::protocol::bin_proto::BinFrame) -> Option<RouteOp> {
    use crate::protocol::bin_proto::BinOp;
    match frame.op {
        BinOp::Msg => Some(RouteOp::RouteMsg {
            #[cfg(feature = "accounts")]
            account: bytes::Bytes::from_static(b"$G"),
            subject: frame.subject,
            reply: if frame.reply.is_empty() {
                None
            } else {
                Some(frame.reply)
            },
            headers: None,
            payload: frame.payload,
        }),
        BinOp::HMsg => {
            // Payload layout: [4B hdr_len LE][hdr_bytes][body]
            if frame.payload.len() < 4 {
                return None;
            }
            let hdr_len = u32::from_le_bytes([
                frame.payload[0],
                frame.payload[1],
                frame.payload[2],
                frame.payload[3],
            ]) as usize;
            if frame.payload.len() < 4 + hdr_len {
                return None;
            }
            let hdr_bytes = &frame.payload[4..4 + hdr_len];
            let headers = crate::nats_proto::parse_headers(hdr_bytes).ok()?;
            let payload = frame.payload.slice(4 + hdr_len..);
            Some(RouteOp::RouteMsg {
                #[cfg(feature = "accounts")]
                account: bytes::Bytes::from_static(b"$G"),
                subject: frame.subject,
                reply: if frame.reply.is_empty() {
                    None
                } else {
                    Some(frame.reply)
                },
                headers: Some(headers),
                payload,
            })
        }
        BinOp::Sub => Some(RouteOp::RouteSub {
            #[cfg(feature = "accounts")]
            account: frame.payload,
            subject: frame.subject,
            queue: if frame.reply.is_empty() {
                None
            } else {
                Some(frame.reply)
            },
        }),
        BinOp::Unsub => Some(RouteOp::RouteUnsub {
            #[cfg(feature = "accounts")]
            account: frame.payload,
            subject: frame.subject,
        }),
        BinOp::Ping | BinOp::Pong => None, // handled in-place
    }
}

/// Modify epoll registration for a client socket.
fn epoll_mod(epoll_fd: RawFd, client_fd: RawFd, conn_id: u64, enable_out: bool) {
    let events = if enable_out {
        libc::EPOLLIN as u32 | libc::EPOLLOUT as u32
    } else {
        libc::EPOLLIN as u32
    };
    let mut ev = libc::epoll_event {
        events,
        u64: conn_id,
    };
    unsafe {
        libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_MOD, client_fd, &mut ev);
    }
}

fn cleanup_conn(id: u64, state: &ServerState) {
    let removed = {
        #[cfg(feature = "accounts")]
        {
            let mut all_removed = Vec::new();
            for account_sub in &state.account_subs {
                let mut subs = account_sub.write().expect("subs write lock");
                all_removed.extend(subs.remove_conn(id));
            }
            state.has_subs.store(
                state
                    .account_subs
                    .iter()
                    .any(|s| !s.read().expect("subs read lock").is_empty()),
                std::sync::atomic::Ordering::Release,
            );
            all_removed
        }
        #[cfg(not(feature = "accounts"))]
        {
            let mut subs = state.subs.write().expect("subs write lock");
            let r = subs.remove_conn(id);
            state.has_subs.store(!subs.is_empty(), Ordering::Release);
            r
        }
    };

    if !removed.is_empty() {
        let mut upstreams = state.leaf.upstreams.write().expect("upstreams write lock");
        for up in upstreams.iter_mut() {
            for sub in &removed {
                up.remove_interest(&sub.subject, sub.queue.as_deref());
            }
        }
    }

    info!(id, "client cleaned up");
}

/// Tests for `bin_frame_to_route_op`: binary frame → RouteOp conversion.
#[cfg(test)]
mod bin_frame_tests {
    use super::*;
    use crate::protocol::bin_proto::{self, BinOp};
    use bytes::{Bytes, BytesMut};

    /// Encode a frame with the given fields and decode it back.
    fn frame(op: BinOp, subject: &[u8], reply: &[u8], payload: &[u8]) -> bin_proto::BinFrame {
        let mut buf = BytesMut::new();
        bin_proto::encode_into(op, subject, reply, payload, &mut buf);
        bin_proto::try_decode(&mut buf).expect("decode failed")
    }

    #[test]
    fn msg_no_reply() {
        match bin_frame_to_route_op(frame(BinOp::Msg, b"foo.bar", b"", b"hello")).unwrap() {
            RouteOp::RouteMsg {
                subject,
                reply,
                payload,
                headers,
                ..
            } => {
                assert_eq!(&subject[..], b"foo.bar");
                assert!(reply.is_none());
                assert_eq!(&payload[..], b"hello");
                assert!(headers.is_none());
            }
            other => panic!("expected RouteMsg, got {other:?}"),
        }
    }

    #[test]
    fn msg_with_reply() {
        match bin_frame_to_route_op(frame(BinOp::Msg, b"foo", b"_INBOX.1", b"")).unwrap() {
            RouteOp::RouteMsg { reply, .. } => {
                assert_eq!(reply.unwrap().as_ref(), b"_INBOX.1");
            }
            other => panic!("expected RouteMsg, got {other:?}"),
        }
    }

    #[test]
    fn hmsg_round_trip() {
        let hdr = b"NATS/1.0\r\nX-Src: node-a\r\n\r\n";
        let body = b"payload";
        let mut buf = BytesMut::new();
        bin_proto::write_hmsg(b"events.v1", b"", hdr, body, &mut buf);
        let f = bin_proto::try_decode(&mut buf).unwrap();
        match bin_frame_to_route_op(f).unwrap() {
            RouteOp::RouteMsg {
                subject,
                reply,
                headers,
                payload,
                ..
            } => {
                assert_eq!(&subject[..], b"events.v1");
                assert!(reply.is_none());
                assert!(headers.is_some(), "headers must be parsed");
                assert_eq!(&payload[..], body);
            }
            other => panic!("expected RouteMsg, got {other:?}"),
        }
    }

    #[test]
    fn hmsg_payload_too_short_returns_none() {
        // Craft an HMsg where the embedded hdr_len field claims more bytes than exist.
        let f = bin_proto::BinFrame {
            op: BinOp::HMsg,
            subject: Bytes::from_static(b"sub"),
            reply: Bytes::new(),
            // [4B hdr_len=50][only 3 bytes] — truncated header
            payload: Bytes::from_static(b"\x32\x00\x00\x00abc"),
        };
        assert!(
            bin_frame_to_route_op(f).is_none(),
            "truncated HMsg payload must return None"
        );
    }

    #[test]
    fn hmsg_payload_empty_returns_none() {
        let f = bin_proto::BinFrame {
            op: BinOp::HMsg,
            subject: Bytes::from_static(b"sub"),
            reply: Bytes::new(),
            payload: Bytes::new(), // no hdr_len bytes at all
        };
        assert!(bin_frame_to_route_op(f).is_none());
    }

    #[test]
    fn sub_no_queue() {
        match bin_frame_to_route_op(frame(BinOp::Sub, b"foo.>", b"", b"$G")).unwrap() {
            RouteOp::RouteSub { subject, queue, .. } => {
                assert_eq!(&subject[..], b"foo.>");
                assert!(queue.is_none());
            }
            other => panic!("expected RouteSub, got {other:?}"),
        }
    }

    #[test]
    fn sub_with_queue() {
        match bin_frame_to_route_op(frame(BinOp::Sub, b"events.>", b"workers", b"$G")).unwrap() {
            RouteOp::RouteSub { subject, queue, .. } => {
                assert_eq!(&subject[..], b"events.>");
                assert_eq!(queue.unwrap().as_ref(), b"workers");
            }
            other => panic!("expected RouteSub, got {other:?}"),
        }
    }

    #[test]
    fn unsub() {
        match bin_frame_to_route_op(frame(BinOp::Unsub, b"foo.>", b"", b"$G")).unwrap() {
            RouteOp::RouteUnsub { subject, .. } => {
                assert_eq!(&subject[..], b"foo.>");
            }
            other => panic!("expected RouteUnsub, got {other:?}"),
        }
    }

    #[test]
    fn ping_returns_none() {
        assert!(bin_frame_to_route_op(frame(BinOp::Ping, b"", b"", b"")).is_none());
    }

    #[test]
    fn pong_returns_none() {
        assert!(bin_frame_to_route_op(frame(BinOp::Pong, b"", b"", b"")).is_none());
    }
}
