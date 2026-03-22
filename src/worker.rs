//! N-worker epoll event loop.
//!
//! Each worker owns one epoll instance multiplexing many client connections.
//! DirectWriter notifies the *worker's* single eventfd, so fan-out to 100
//! connections on the same worker costs 1 eventfd write, not 100.

use std::collections::HashMap;
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

use crate::client_handler::ClientHandler;
#[cfg(feature = "gateway")]
use crate::gateway_handler::GatewayHandler;
#[cfg(feature = "hub")]
use crate::handler::send_existing_subs;
#[cfg(feature = "gateway")]
use crate::handler::send_existing_subs_to_gateway;
#[cfg(feature = "cluster")]
use crate::handler::send_existing_subs_to_route;
use crate::handler::{handle_expired_subs, ConnCtx, ConnExt, HandleResult, WorkerCtx};
#[cfg(feature = "hub")]
use crate::leaf_handler::LeafHandler;
use crate::nats_proto;
#[cfg(feature = "cluster")]
use crate::protocol::RouteOp;
use crate::protocol::{AdaptiveBuf, ClientOp};
#[cfg(feature = "cluster")]
use crate::route_handler::RouteHandler;
use crate::server::ServerState;
use crate::sub_list::{create_eventfd, DirectWriter};
#[cfg(feature = "leaf")]
use crate::upstream::UpstreamCmd;
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
    #[cfg(feature = "hub")]
    NewLeafConn {
        id: u64,
        stream: TcpStream,
        addr: SocketAddr,
    },
    /// Accept an inbound route connection (cluster mode).
    #[cfg(feature = "cluster")]
    NewRouteConn {
        id: u64,
        stream: TcpStream,
        addr: SocketAddr,
    },
    /// Accept an inbound gateway connection (gateway mode).
    #[cfg(feature = "gateway")]
    NewGatewayConn {
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
    #[cfg(feature = "hub")]
    pub fn send_leaf_conn(&self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let _ = self.tx.send(WorkerCmd::NewLeafConn { id, stream, addr });
        self.wake();
    }

    /// Send a new inbound route connection to this worker and wake it.
    #[cfg(feature = "cluster")]
    pub fn send_route_conn(&self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let _ = self.tx.send(WorkerCmd::NewRouteConn { id, stream, addr });
        self.wake();
    }

    /// Send a new inbound gateway connection to this worker and wake it.
    #[cfg(feature = "gateway")]
    pub fn send_gateway_conn(&self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let _ = self.tx.send(WorkerCmd::NewGatewayConn { id, stream, addr });
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

// --- Connection state machine ---

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
    direct_writer: DirectWriter,
    direct_buf: Arc<Mutex<BytesMut>>,
    has_pending: Arc<AtomicBool>,
    phase: ConnPhase,
    transport: Transport,
    #[allow(dead_code)]
    conn_id: u64,
    ext: ConnExt,
    #[cfg(feature = "leaf")]
    upstream_tx: Option<mpsc::Sender<UpstreamCmd>>,
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
    permissions: Option<crate::server::Permissions>,
    /// Account this connection belongs to. 0 = `$G` (global/default).
    #[cfg(feature = "accounts")]
    account_id: crate::server::AccountId,
}

// --- Worker ---

pub(crate) struct Worker {
    epoll_fd: OwnedFd,
    event_fd: Arc<OwnedFd>,
    conns: HashMap<u64, ClientState>,
    fd_to_conn: HashMap<RawFd, u64>,
    rx: mpsc::Receiver<WorkerCmd>,
    state: Arc<ServerState>,
    info_line: Vec<u8>,
    /// INFO line for inbound leaf connections (port set to leafnode_port).
    #[cfg(feature = "hub")]
    leaf_info_line: Vec<u8>,
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
        #[cfg(feature = "hub")]
        let leaf_info_line = if let Some(lp) = state.leafnode_port {
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

                // Register eventfd with epoll (level-triggered)
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
                    conns: HashMap::new(),
                    fd_to_conn: HashMap::new(),
                    rx,
                    state,
                    info_line,
                    #[cfg(feature = "hub")]
                    leaf_info_line,
                    shutdown: false,
                    pending_notify: [-1; 16],
                    pending_notify_count: 0,
                    worker_label: index.to_string(),
                    msgs_received: 0,
                    msgs_received_bytes: 0,
                    msgs_delivered: 0,
                    msgs_delivered_bytes: 0,
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

            // Flush any pending DirectWriter data after processing all events.
            // This handles local delivery (pub on this worker → sub on this worker)
            // without needing an eventfd round-trip.
            self.flush_pending();

            // Flush accumulated message counters to the global metrics recorder.
            self.flush_metrics();

            // Check for idle connections, keepalive PINGs, and auth timeouts.
            if epoll_timeout_ms > 0 {
                self.check_pings();
                self.check_auth_timeout();
            }

            if self.shutdown {
                break;
            }
        }

        // Clean up all connections on shutdown.
        let conn_ids: Vec<u64> = self.conns.keys().copied().collect();
        for conn_id in conn_ids {
            self.remove_conn(conn_id);
        }
    }

    fn handle_eventfd(&mut self) {
        // Consume eventfd counter
        let mut val: u64 = 0;
        unsafe {
            libc::read(
                self.event_fd.as_raw_fd(),
                &mut val as *mut u64 as *mut libc::c_void,
                8,
            );
        }

        // Check for new connections / shutdown / lame duck
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
                #[cfg(feature = "hub")]
                WorkerCmd::NewLeafConn { id, stream, addr } => {
                    self.add_leaf_conn(id, stream, addr);
                }
                #[cfg(feature = "cluster")]
                WorkerCmd::NewRouteConn { id, stream, addr } => {
                    self.add_route_conn(id, stream, addr);
                }
                #[cfg(feature = "gateway")]
                WorkerCmd::NewGatewayConn { id, stream, addr } => {
                    self.add_gateway_conn(id, stream, addr);
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

        // Register with epoll (EPOLLIN, level-triggered)
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

        let direct_buf = Arc::new(Mutex::new(BytesMut::with_capacity(65536)));
        let has_pending = Arc::new(AtomicBool::new(false));
        let direct_writer = DirectWriter::new(
            Arc::clone(&direct_buf),
            Arc::clone(&has_pending),
            Arc::clone(&self.event_fd),
        );

        let (phase, transport, write_buf) = if is_websocket {
            // WS: wait for HTTP upgrade before sending INFO
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
            // TLS: perform handshake before sending INFO
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
            // Raw TCP: send INFO immediately
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
            conn_id: id,
            ext: ConnExt::Client,
            #[cfg(feature = "leaf")]
            upstream_tx: None,
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
            // Try to flush INFO immediately
            self.try_flush_conn(id);
        }
    }

    /// Add an inbound leaf node connection (hub mode).
    /// Sends INFO immediately and waits for CONNECT from the leaf.
    #[cfg(feature = "hub")]
    fn add_leaf_conn(&mut self, id: u64, stream: TcpStream, addr: SocketAddr) {
        stream.set_nonblocking(true).ok();
        stream.set_nodelay(true).ok();
        let fd = stream.as_raw_fd();

        // Register with epoll (EPOLLIN, level-triggered)
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: id,
        };
        let ret =
            unsafe { libc::epoll_ctl(self.epoll_fd.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if ret != 0 {
            warn!(id, error = %io::Error::last_os_error(), "epoll_ctl ADD failed for leaf conn");
            return;
        }

        let direct_buf = Arc::new(Mutex::new(BytesMut::with_capacity(65536)));
        let has_pending = Arc::new(AtomicBool::new(false));
        let direct_writer = DirectWriter::new(
            Arc::clone(&direct_buf),
            Arc::clone(&has_pending),
            Arc::clone(&self.event_fd),
        );

        // Queue INFO in write_buf — use leaf_info_line (port = leafnode_port).
        let mut write_buf = BytesMut::with_capacity(4096);
        write_buf.extend_from_slice(&self.leaf_info_line);

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
            conn_id: id,
            ext: ConnExt::Leaf {
                leaf_sid_counter: 0,
                leaf_sids: HashMap::new(),
            },
            #[cfg(feature = "leaf")]
            upstream_tx: None,
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
        };

        self.fd_to_conn.insert(fd, id);
        self.conns.insert(id, client);

        counter!("leaf_connections_total", "worker" => self.worker_label.clone()).increment(1);
        gauge!("connections_active", "worker" => self.worker_label.clone())
            .set(self.conns.len() as f64);

        debug!(id, addr = %addr, "accepted inbound leaf connection on worker");
        // Try to flush INFO immediately
        self.try_flush_conn(id);
    }

    /// Add an inbound route connection (cluster mode).
    /// Sends INFO immediately and waits for CONNECT from the route peer.
    #[cfg(feature = "cluster")]
    fn add_route_conn(&mut self, id: u64, stream: TcpStream, addr: SocketAddr) {
        stream.set_nonblocking(true).ok();
        stream.set_nodelay(true).ok();
        let fd = stream.as_raw_fd();

        // Register with epoll (EPOLLIN, level-triggered)
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: id,
        };
        let ret =
            unsafe { libc::epoll_ctl(self.epoll_fd.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if ret != 0 {
            warn!(id, error = %io::Error::last_os_error(), "epoll_ctl ADD failed for route conn");
            return;
        }

        let direct_buf = Arc::new(Mutex::new(BytesMut::with_capacity(65536)));
        let has_pending = Arc::new(AtomicBool::new(false));
        let direct_writer = DirectWriter::new(
            Arc::clone(&direct_buf),
            Arc::clone(&has_pending),
            Arc::clone(&self.event_fd),
        );

        // Queue INFO in write_buf — use dynamic route INFO (includes connect_urls).
        let mut write_buf = BytesMut::with_capacity(4096);
        let info_str = crate::route_conn::build_route_info(&self.state);
        // Use the dynamic info
        write_buf.extend_from_slice(info_str.as_bytes());

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
            conn_id: id,
            ext: ConnExt::Route {
                route_sid_counter: 0,
                route_sids: std::collections::HashMap::new(),
                peer_server_id: None,
            },
            #[cfg(feature = "leaf")]
            upstream_tx: None,
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
        };

        self.fd_to_conn.insert(fd, id);
        self.conns.insert(id, client);

        counter!("route_connections_total", "worker" => self.worker_label.clone()).increment(1);
        gauge!("connections_active", "worker" => self.worker_label.clone())
            .set(self.conns.len() as f64);

        debug!(id, addr = %addr, "accepted inbound route connection on worker");
        // Try to flush INFO immediately
        self.try_flush_conn(id);
    }

    /// Add an inbound gateway connection (gateway mode).
    /// Sends INFO immediately and waits for CONNECT from the gateway peer.
    #[cfg(feature = "gateway")]
    fn add_gateway_conn(&mut self, id: u64, stream: TcpStream, addr: SocketAddr) {
        stream.set_nonblocking(true).ok();
        stream.set_nodelay(true).ok();
        let fd = stream.as_raw_fd();

        // Register with epoll (EPOLLIN, level-triggered)
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: id,
        };
        let ret =
            unsafe { libc::epoll_ctl(self.epoll_fd.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if ret != 0 {
            warn!(id, error = %io::Error::last_os_error(), "epoll_ctl ADD failed for gateway conn");
            return;
        }

        let direct_buf = Arc::new(Mutex::new(BytesMut::with_capacity(65536)));
        let has_pending = Arc::new(AtomicBool::new(false));
        let direct_writer = DirectWriter::new(
            Arc::clone(&direct_buf),
            Arc::clone(&has_pending),
            Arc::clone(&self.event_fd),
        );

        // Queue INFO in write_buf — use dynamic gateway INFO.
        let mut write_buf = BytesMut::with_capacity(4096);
        let info_str = crate::gateway_conn::get_gateway_info(&self.state);
        write_buf.extend_from_slice(info_str.as_bytes());

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
            conn_id: id,
            ext: ConnExt::Gateway {
                gateway_sid_counter: 0,
                gateway_sids: std::collections::HashMap::new(),
                gateway_sids_by_subject: std::collections::HashMap::new(),
                peer_gateway_name: None,
            },
            #[cfg(feature = "leaf")]
            upstream_tx: None,
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
        };

        self.fd_to_conn.insert(fd, id);
        self.conns.insert(id, client);

        counter!("gateway_connections_total", "worker" => self.worker_label.clone()).increment(1);
        gauge!("connections_active", "worker" => self.worker_label.clone())
            .set(self.conns.len() as f64);

        debug!(id, addr = %addr, "accepted inbound gateway connection on worker");
        // Try to flush INFO immediately
        self.try_flush_conn(id);
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

            // Unregister leaf DirectWriter from shared registry.
            #[cfg(feature = "hub")]
            if client.ext.is_leaf() {
                self.state.leaf_writers.write().unwrap().remove(&conn_id);
            }
            // Unregister route DirectWriter and peer from shared registries.
            #[cfg(feature = "cluster")]
            if client.ext.is_route() {
                self.state.route_writers.write().unwrap().remove(&conn_id);
                if let ConnExt::Route {
                    peer_server_id: Some(ref sid),
                    ..
                } = client.ext
                {
                    self.state.route_peers.lock().unwrap().connected.remove(sid);
                }
            }
            // Unregister gateway DirectWriter and peer from shared registries.
            #[cfg(feature = "gateway")]
            if client.ext.is_gateway() {
                self.state.gateway_writers.write().unwrap().remove(&conn_id);
                if let ConnExt::Gateway {
                    peer_gateway_name: Some(ref name),
                    ..
                } = client.ext
                {
                    let mut peers = self.state.gateway_peers.lock().unwrap();
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

    /// Scan all connections for pending DirectWriter data, drain into write_buf,
    /// and try to flush to socket.
    fn flush_pending(&mut self) {
        let epoll_fd = self.epoll_fd.as_raw_fd();
        let mut to_remove: Vec<u64> = Vec::new();

        for (conn_id, client) in &mut self.conns {
            if !client.has_pending.load(Ordering::Acquire) {
                continue;
            }
            client.has_pending.store(false, Ordering::Relaxed);
            let direct_data = {
                let mut dbuf = client.direct_buf.lock().unwrap();
                if !dbuf.is_empty() {
                    if matches!(client.transport, Transport::Raw) {
                        // O(1) take for Raw — we'll use writev to avoid copying
                        dbuf.split()
                    } else {
                        // WS/TLS need the data in write_buf for transformation
                        client.write_buf.extend_from_slice(&dbuf);
                        dbuf.clear();
                        BytesMut::new()
                    }
                } else {
                    BytesMut::new()
                }
            };

            // Slow consumer detection: if pending data exceeds max_pending,
            // disconnect the client to protect server memory.
            let max_pending = self.state.buf_config.max_pending;
            if max_pending > 0 {
                let pending = match &client.transport {
                    Transport::Raw => client.write_buf.len() + direct_data.len(),
                    Transport::WebSocket { ws_out, .. } => client.write_buf.len() + ws_out.len(),
                    Transport::Tls(ref tls) => client.write_buf.len() + tls.enc_out.len(),
                };
                if pending > max_pending {
                    warn!(
                        conn_id = *conn_id,
                        pending_bytes = pending,
                        max = max_pending,
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
            // Send PING and register EPOLLOUT
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
                // Non-active connections: disconnect immediately
                self.remove_conn(conn_id);
                continue;
            }
            // Active connections: flush direct_buf, try to flush socket, then remove
            if let Some(client) = self.conns.get_mut(&conn_id) {
                client.phase = ConnPhase::Draining;
                // Drain direct_buf into write_buf
                {
                    let mut dbuf = client.direct_buf.lock().unwrap();
                    if !dbuf.is_empty() {
                        client.write_buf.extend_from_slice(&dbuf);
                        dbuf.clear();
                    }
                }
            }
            self.try_flush_conn(conn_id);
            // Remove all subs for this connection
            cleanup_conn(conn_id, &self.state);
            self.remove_conn(conn_id);
        }
    }

    /// Try to flush write_buf to the socket. Registers/removes EPOLLOUT as needed.
    /// For WebSocket connections, write_buf is encoded into ws_out first.
    /// For TLS connections, delegates to tls_flush_encrypted.
    fn try_flush_conn(&mut self, conn_id: u64) {
        // TLS connections use their own flush path
        if matches!(
            self.conns.get(&conn_id).map(|c| &c.transport),
            Some(Transport::Tls(..))
        ) {
            self.tls_flush_encrypted(conn_id);
            // Phase transition: SendInfo → WaitConnect
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

        // Phase transition: SendInfo → WaitConnect
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
        // Read from socket in a loop until WouldBlock to drain the kernel buffer.
        let mut got_eof = false;
        loop {
            let client = match self.conns.get_mut(&conn_id) {
                Some(c) => c,
                None => return,
            };
            match client.read_buf.read_from_fd(client.fd) {
                Ok(0) => {
                    got_eof = true;
                    break;
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

        if let Some(client) = self.conns.get_mut(&conn_id) {
            client.last_activity = Instant::now();
        }
        self.process_read_buf(conn_id);
        self.flush_notifications();

        // Remove connection after processing any remaining data in the buffer.
        if got_eof {
            self.remove_conn(conn_id);
        }
    }

    fn handle_read_ws(&mut self, conn_id: u64) {
        // For WsHandshake phase, read directly into read_buf (raw HTTP bytes)
        let is_handshake = matches!(
            self.conns.get(&conn_id).map(|c| &c.phase),
            Some(ConnPhase::WsHandshake)
        );

        if is_handshake {
            // During handshake, read HTTP bytes into read_buf directly
            loop {
                let client = match self.conns.get_mut(&conn_id) {
                    Some(c) => c,
                    None => return,
                };
                match client.read_buf.read_from_fd(client.fd) {
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

        // Active WS: read into raw_buf, decode frames into read_buf
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
            // Read from socket into raw_buf
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

        // Decode WS frames into read_buf
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
        // Read encrypted bytes from socket
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

        // Feed encrypted data to TLS and extract plaintext
        let mut error = false;
        if let Some(client) = self.conns.get_mut(&conn_id) {
            if let Transport::Tls(ref mut tls) = client.transport {
                // Feed encrypted data to rustls
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
                    // Process TLS state (handshake / app data)
                    match tls.tls_conn.process_new_packets() {
                        Ok(state) => {
                            // Extract plaintext
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

        // Flush any TLS handshake/alert data
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
            // Encrypt pending plaintext writes
            if !client.write_buf.is_empty() {
                if let Err(e) = tls.tls_conn.writer().write_all(&client.write_buf) {
                    warn!(conn_id, error = %e, "TLS write error");
                }
                client.write_buf.clear();
            }

            // Extract encrypted TLS records to enc_out
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

            // Write enc_out to socket
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
            let phase = match self.conns.get(&conn_id) {
                Some(c) => match c.phase {
                    ConnPhase::TlsHandshake => 3,
                    ConnPhase::WsHandshake => 0,
                    ConnPhase::SendInfo => return,
                    ConnPhase::WaitConnect => 1,
                    ConnPhase::Active | ConnPhase::Draining => 2,
                },
                None => return,
            };

            if phase == 3 {
                // TlsHandshake: check if handshake is complete
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
                    // Log client certificate if present (mTLS)
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
                    let client = self.conns.get_mut(&conn_id).unwrap();
                    // Queue INFO in write_buf (will be encrypted by tls_flush_encrypted)
                    client.write_buf.extend_from_slice(&self.info_line);
                    client.phase = ConnPhase::SendInfo;
                    self.tls_flush_encrypted(conn_id);
                    // Check if all encrypted data was flushed
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
                return;
            }

            if phase == 0 {
                // WsHandshake: parse HTTP upgrade request
                let result = {
                    let client = self.conns.get(&conn_id).unwrap();
                    websocket::parse_ws_upgrade(&client.read_buf)
                };
                match result {
                    None => return, // need more data
                    Some(Err(_)) => {
                        // Bad upgrade request
                        if let Some(client) = self.conns.get_mut(&conn_id) {
                            client
                                .write_buf
                                .extend_from_slice(b"HTTP/1.1 400 Bad Request\r\n\r\n");
                        }
                        self.try_flush_conn(conn_id);
                        self.remove_conn(conn_id);
                        return;
                    }
                    Some(Ok((upgrade, consumed))) => {
                        let response = websocket::build_ws_accept_response(&upgrade.key);
                        let client = self.conns.get_mut(&conn_id).unwrap();
                        // Consume HTTP request from read_buf
                        client.read_buf.advance(consumed);
                        // Write raw HTTP 101 response directly to ws_out
                        // (bypassing WS framing since this is the upgrade response)
                        if let Transport::WebSocket { ws_out, .. } = &mut client.transport {
                            ws_out.extend_from_slice(&response);
                        }
                        // Queue INFO in write_buf (will be WS-encoded by try_flush_conn)
                        let info_line = self.info_line.clone();
                        let client = self.conns.get_mut(&conn_id).unwrap();
                        client.write_buf.extend_from_slice(&info_line);
                        // Now transition to SendInfo
                        client.phase = ConnPhase::SendInfo;
                        self.try_flush_conn(conn_id);
                    }
                }
                continue;
            }

            // Max control line check: if the buffer is larger than the limit
            // and there is no newline within the first max_control_line bytes,
            // the client is sending an oversized control line.
            if phase == 1 || phase == 2 {
                let max_ctrl = self.state.max_control_line.load(Ordering::Relaxed);
                if max_ctrl > 0 {
                    let exceeded = {
                        let client = self.conns.get(&conn_id).unwrap();
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
                        return;
                    }
                }
            }

            if phase == 1 {
                // WaitConnect: parse CONNECT (or route INFO+CONNECT for route conns)

                // Route connections use the route protocol parser (INFO+CONNECT+PING).
                #[cfg(feature = "cluster")]
                let is_route_conn = self
                    .conns
                    .get(&conn_id)
                    .map(|c| c.ext.is_route())
                    .unwrap_or(false);
                #[cfg(not(feature = "cluster"))]
                let _is_route_conn = false;

                #[cfg(feature = "cluster")]
                if is_route_conn {
                    let route_op = {
                        let client = self.conns.get_mut(&conn_id).unwrap();
                        match nats_proto::try_parse_route_op(&mut client.read_buf) {
                            Ok(op) => op,
                            Err(_) => {
                                self.remove_conn(conn_id);
                                return;
                            }
                        }
                    };
                    match route_op {
                        Some(RouteOp::Info(peer_info)) => {
                            // Process gossip connect_urls from peer's INFO.
                            if !peer_info.connect_urls.is_empty() {
                                crate::route_conn::process_gossip_urls(
                                    &self.state,
                                    &peer_info.connect_urls,
                                );
                            }
                            continue;
                        }
                        Some(RouteOp::Connect(connect_info)) => {
                            let peer_sid = connect_info.server_id.clone();

                            // Self-connect check.
                            if let Some(ref sid) = peer_sid {
                                if *sid == self.state.info.server_id {
                                    debug!(conn_id, "self-connect on inbound route, closing");
                                    self.remove_conn(conn_id);
                                    return;
                                }
                            }

                            // Peer's CONNECT — go Active, register, exchange subs.
                            let client = self.conns.get_mut(&conn_id).unwrap();
                            client.phase = ConnPhase::Active;
                            client.echo = false;
                            #[cfg(feature = "leaf")]
                            {
                                client.upstream_tx = self.state.upstream_tx.read().unwrap().clone();
                            }
                            client.write_buf.extend_from_slice(b"PONG\r\n");

                            // Store peer_server_id for cleanup.
                            if let ConnExt::Route {
                                ref mut peer_server_id,
                                ..
                            } = client.ext
                            {
                                *peer_server_id = peer_sid.clone();
                            }

                            // Note: we intentionally do NOT register inbound peers
                            // in route_peers.connected here. The outbound side handles
                            // its own registration in connect_route(). Having both an
                            // inbound and outbound route to the same peer is harmless
                            // (one-hop enforcement prevents message loops) and avoids
                            // a retry storm when outbound dedup rejects connections.

                            // Register route DirectWriter.
                            let dw = client.direct_writer.clone();
                            self.state
                                .route_writers
                                .write()
                                .unwrap()
                                .insert(conn_id, dw);

                            // Send existing local subscriptions as RS+ to the new route.
                            send_existing_subs_to_route(&self.state, &client.direct_writer);

                            // Broadcast updated INFO to all routes (gossip re-broadcast).
                            crate::route_conn::broadcast_route_info(&self.state);

                            info!(conn_id, "inbound route connected");
                        }
                        Some(RouteOp::Ping) => {
                            // Peer sent PING during handshake — respond with PONG.
                            if let Some(client) = self.conns.get_mut(&conn_id) {
                                client.write_buf.extend_from_slice(b"PONG\r\n");
                            }
                            continue;
                        }
                        Some(_) => {
                            self.remove_conn(conn_id);
                            return;
                        }
                        None => return, // need more data
                    }
                    // After route handshake ops, continue the outer loop to process
                    // remaining data in the read buffer.
                    continue;
                }

                // Gateway connections use the gateway protocol parser (INFO+CONNECT+PING).
                #[cfg(feature = "gateway")]
                let is_gateway_conn = self
                    .conns
                    .get(&conn_id)
                    .map(|c| c.ext.is_gateway())
                    .unwrap_or(false);

                #[cfg(feature = "gateway")]
                if is_gateway_conn {
                    let gw_op = {
                        let client = self.conns.get_mut(&conn_id).unwrap();
                        match nats_proto::try_parse_gateway_op(&mut client.read_buf) {
                            Ok(op) => op,
                            Err(_) => {
                                self.remove_conn(conn_id);
                                return;
                            }
                        }
                    };
                    match gw_op {
                        Some(nats_proto::GatewayOp::Info(peer_info)) => {
                            // Process gateway_urls from peer's INFO for gossip.
                            if let Some(ref urls) = peer_info.gateway_urls {
                                if !urls.is_empty() {
                                    let tx = self.state.gateway_connect_tx.lock().unwrap();
                                    let mut peers = self.state.gateway_peers.lock().unwrap();
                                    for url in urls {
                                        if peers.known_urls.insert(url.clone()) {
                                            if let Some(ref sender) = *tx {
                                                let _ = sender.send(url.clone());
                                            }
                                        }
                                    }
                                }
                            }
                            continue;
                        }
                        Some(nats_proto::GatewayOp::Connect(connect_info)) => {
                            // Extract gateway name from CONNECT.
                            let peer_gw_name = connect_info.gateway.clone();

                            // Peer's CONNECT — go Active, register, exchange subs.
                            let client = self.conns.get_mut(&conn_id).unwrap();
                            client.phase = ConnPhase::Active;
                            client.echo = false;
                            #[cfg(feature = "leaf")]
                            {
                                client.upstream_tx = self.state.upstream_tx.read().unwrap().clone();
                            }
                            client.write_buf.extend_from_slice(b"PONG\r\n");

                            // Store peer gateway name for cleanup.
                            if let ConnExt::Gateway {
                                ref mut peer_gateway_name,
                                ..
                            } = client.ext
                            {
                                *peer_gateway_name = peer_gw_name.clone();
                            }

                            // Register gateway DirectWriter.
                            let dw = client.direct_writer.clone();
                            self.state
                                .gateway_writers
                                .write()
                                .unwrap()
                                .insert(conn_id, dw);

                            // Register in gateway_peers.
                            if let Some(ref name) = peer_gw_name {
                                let mut peers = self.state.gateway_peers.lock().unwrap();
                                peers
                                    .connected
                                    .entry(name.clone())
                                    .or_default()
                                    .insert(conn_id);
                            }

                            // Send existing local subscriptions as RS+ to the
                            // new gateway (interest-only mode).
                            send_existing_subs_to_gateway(&self.state, &client.direct_writer);

                            // Broadcast updated INFO to all gateways (gossip).
                            crate::gateway_conn::broadcast_gateway_info(&self.state);

                            info!(
                                conn_id,
                                peer_gateway = ?peer_gw_name,
                                "inbound gateway connected"
                            );
                        }
                        Some(nats_proto::GatewayOp::Ping) => {
                            // Peer sent PING during handshake — respond with PONG.
                            if let Some(client) = self.conns.get_mut(&conn_id) {
                                client.write_buf.extend_from_slice(b"PONG\r\n");
                            }
                            continue;
                        }
                        Some(_) => {
                            self.remove_conn(conn_id);
                            return;
                        }
                        None => return, // need more data
                    }
                    // After gateway handshake ops, continue the outer loop.
                    continue;
                }

                let op = {
                    let client = self.conns.get_mut(&conn_id).unwrap();
                    match nats_proto::try_parse_client_op(&mut client.read_buf) {
                        Ok(op) => op,
                        Err(_) => {
                            self.remove_conn(conn_id);
                            return;
                        }
                    }
                };
                match op {
                    Some(ClientOp::Connect(connect_info)) => {
                        #[cfg(feature = "hub")]
                        let is_leaf = self
                            .conns
                            .get(&conn_id)
                            .map(|c| c.ext.is_leaf())
                            .unwrap_or(false);
                        #[cfg(not(feature = "hub"))]
                        let is_leaf = false;

                        if is_leaf {
                            // Inbound leaf node — validate leaf auth, send PING, go Active.
                            #[cfg(feature = "hub")]
                            {
                                // Validate leaf auth before taking mutable borrow on conns.
                                let leaf_perms = self.state.leaf_auth.validate(&connect_info);
                                let leaf_perms = match leaf_perms {
                                    Some(p) => p.map(std::sync::Arc::new),
                                    None => {
                                        // Auth failed — disconnect.
                                        warn!(conn_id, "leaf authorization violation");
                                        counter!(
                                            "auth_failures_total",
                                            "worker" => self.worker_label.clone()
                                        )
                                        .increment(1);
                                        if let Some(client) = self.conns.get_mut(&conn_id) {
                                            client.write_buf.extend_from_slice(
                                                b"-ERR 'Authorization Violation'\r\n",
                                            );
                                        }
                                        self.try_flush_conn(conn_id);
                                        self.remove_conn(conn_id);
                                        return;
                                    }
                                };

                                let client = self.conns.get_mut(&conn_id).unwrap();
                                client.phase = ConnPhase::Active;
                                client.echo = false; // suppress echo for leaf conns
                                                     // Store permissions on the connection for filtering
                                                     // in leaf_handler (LMSG publish check, LS+ subscribe check).
                                client.permissions =
                                    leaf_perms.as_ref().map(|p| p.as_ref().clone());
                                #[cfg(feature = "leaf")]
                                {
                                    client.upstream_tx =
                                        self.state.upstream_tx.read().unwrap().clone();
                                }
                                client.write_buf.extend_from_slice(b"PING\r\n");

                                // Register leaf DirectWriter so interest can be propagated.
                                let dw = client.direct_writer.clone();
                                self.state
                                    .leaf_writers
                                    .write()
                                    .unwrap()
                                    .insert(conn_id, (dw, leaf_perms.clone()));

                                // Send existing subscriptions as LS+ to the new leaf.
                                send_existing_subs(&self.state, &client.direct_writer, &leaf_perms);

                                info!(conn_id, "inbound leaf connected");
                            }
                        } else {
                            // Regular client connection — validate auth.
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
                                return;
                            }
                            let perms = self.state.auth.lookup_permissions(&connect_info);
                            #[cfg(feature = "accounts")]
                            let acct_id = self.state.lookup_account(connect_info.user.as_deref());
                            let client = self.conns.get_mut(&conn_id).unwrap();
                            client.phase = ConnPhase::Active;
                            client.echo = connect_info.echo;
                            client.no_responders =
                                connect_info.no_responders && connect_info.headers;
                            client.permissions = perms;
                            #[cfg(feature = "accounts")]
                            {
                                client.account_id = acct_id;
                            }
                            #[cfg(feature = "leaf")]
                            {
                                client.upstream_tx = self.state.upstream_tx.read().unwrap().clone();
                            }
                            info!(conn_id, "client connected");
                        }
                    }
                    Some(_) => {
                        // Wrong op
                        if let Some(client) = self.conns.get_mut(&conn_id) {
                            client
                                .write_buf
                                .extend_from_slice(b"-ERR 'expected CONNECT'\r\n");
                        }
                        self.try_flush_conn(conn_id);
                        self.remove_conn(conn_id);
                        return;
                    }
                    None => return, // need more data
                }
            } else {
                // Active phase: parse ops (client or leaf protocol).
                #[cfg(feature = "hub")]
                let is_leaf = self
                    .conns
                    .get(&conn_id)
                    .map(|c| c.ext.is_leaf())
                    .unwrap_or(false);
                #[cfg(not(feature = "hub"))]
                let is_leaf = false;

                #[cfg(feature = "cluster")]
                let is_route = self
                    .conns
                    .get(&conn_id)
                    .map(|c| c.ext.is_route())
                    .unwrap_or(false);
                #[cfg(not(feature = "cluster"))]
                let is_route = false;

                #[cfg(feature = "gateway")]
                let is_gateway = self
                    .conns
                    .get(&conn_id)
                    .map(|c| c.ext.is_gateway())
                    .unwrap_or(false);
                #[cfg(not(feature = "gateway"))]
                let is_gateway = false;

                if is_leaf {
                    // Leaf connection: parse leaf protocol ops (LS+, LS-, LMSG, PING, PONG).
                    #[cfg(feature = "hub")]
                    {
                        let op = {
                            let client = self.conns.get_mut(&conn_id).unwrap();
                            match nats_proto::try_parse_leaf_op(&mut client.read_buf) {
                                Ok(op) => op,
                                Err(_) => {
                                    self.remove_conn(conn_id);
                                    return;
                                }
                            }
                        };
                        match op {
                            Some(op) => {
                                let (result, expired) = {
                                    let client = self.conns.get_mut(&conn_id).unwrap();
                                    let draining = matches!(client.phase, ConnPhase::Draining);
                                    let mut conn_ctx = ConnCtx {
                                        conn_id,
                                        write_buf: &mut client.write_buf,
                                        direct_writer: &client.direct_writer,
                                        echo: client.echo,
                                        no_responders: client.no_responders,
                                        sub_count: &mut client.sub_count,
                                        #[cfg(feature = "leaf")]
                                        upstream_tx: &mut client.upstream_tx,
                                        permissions: &client.permissions,
                                        ext: &mut client.ext,
                                        draining,
                                        #[cfg(feature = "accounts")]
                                        account_id: client.account_id,
                                    };
                                    let mut worker_ctx = WorkerCtx {
                                        state: &self.state,
                                        event_fd: self.event_fd.as_raw_fd(),
                                        pending_notify: &mut self.pending_notify,
                                        pending_notify_count: &mut self.pending_notify_count,
                                        msgs_received: &mut self.msgs_received,
                                        msgs_received_bytes: &mut self.msgs_received_bytes,
                                        msgs_delivered: &mut self.msgs_delivered,
                                        msgs_delivered_bytes: &mut self.msgs_delivered_bytes,
                                        worker_label: &self.worker_label,
                                    };
                                    LeafHandler::handle_op(&mut conn_ctx, &mut worker_ctx, op)
                                };
                                {
                                    #[cfg(feature = "accounts")]
                                    let acct =
                                        self.conns.get(&conn_id).map(|c| c.account_id).unwrap_or(0);
                                    handle_expired_subs(
                                        &expired,
                                        &self.state,
                                        &mut self.conns,
                                        &self.worker_label,
                                        #[cfg(feature = "accounts")]
                                        acct,
                                    );
                                }
                                match result {
                                    HandleResult::Ok => {}
                                    HandleResult::Flush => self.try_flush_conn(conn_id),
                                    HandleResult::Disconnect => {
                                        self.try_flush_conn(conn_id);
                                        self.remove_conn(conn_id);
                                        return;
                                    }
                                }
                            }
                            None => {
                                if let Some(client) = self.conns.get_mut(&conn_id) {
                                    client.read_buf.try_shrink();
                                }
                                return;
                            }
                        }
                    }
                } else if is_route {
                    // Route connection: parse route protocol ops (RS+, RS-, RMSG, PING, PONG).
                    #[cfg(feature = "cluster")]
                    {
                        let op = {
                            let client = self.conns.get_mut(&conn_id).unwrap();
                            match nats_proto::try_parse_route_op(&mut client.read_buf) {
                                Ok(op) => op,
                                Err(_) => {
                                    self.remove_conn(conn_id);
                                    return;
                                }
                            }
                        };
                        match op {
                            Some(op) => {
                                let (result, expired) = {
                                    let client = self.conns.get_mut(&conn_id).unwrap();
                                    let draining = matches!(client.phase, ConnPhase::Draining);
                                    let mut conn_ctx = ConnCtx {
                                        conn_id,
                                        write_buf: &mut client.write_buf,
                                        direct_writer: &client.direct_writer,
                                        echo: client.echo,
                                        no_responders: client.no_responders,
                                        sub_count: &mut client.sub_count,
                                        #[cfg(feature = "leaf")]
                                        upstream_tx: &mut client.upstream_tx,
                                        permissions: &client.permissions,
                                        ext: &mut client.ext,
                                        draining,
                                        #[cfg(feature = "accounts")]
                                        account_id: client.account_id,
                                    };
                                    let mut worker_ctx = WorkerCtx {
                                        state: &self.state,
                                        event_fd: self.event_fd.as_raw_fd(),
                                        pending_notify: &mut self.pending_notify,
                                        pending_notify_count: &mut self.pending_notify_count,
                                        msgs_received: &mut self.msgs_received,
                                        msgs_received_bytes: &mut self.msgs_received_bytes,
                                        msgs_delivered: &mut self.msgs_delivered,
                                        msgs_delivered_bytes: &mut self.msgs_delivered_bytes,
                                        worker_label: &self.worker_label,
                                    };
                                    RouteHandler::handle_op(&mut conn_ctx, &mut worker_ctx, op)
                                };
                                {
                                    #[cfg(feature = "accounts")]
                                    let acct =
                                        self.conns.get(&conn_id).map(|c| c.account_id).unwrap_or(0);
                                    handle_expired_subs(
                                        &expired,
                                        &self.state,
                                        &mut self.conns,
                                        &self.worker_label,
                                        #[cfg(feature = "accounts")]
                                        acct,
                                    );
                                }
                                match result {
                                    HandleResult::Ok => {}
                                    HandleResult::Flush => self.try_flush_conn(conn_id),
                                    HandleResult::Disconnect => {
                                        self.try_flush_conn(conn_id);
                                        self.remove_conn(conn_id);
                                        return;
                                    }
                                }
                            }
                            None => {
                                if let Some(client) = self.conns.get_mut(&conn_id) {
                                    client.read_buf.try_shrink();
                                }
                                return;
                            }
                        }
                    }
                } else if is_gateway {
                    // Gateway connection: parse gateway protocol ops.
                    #[cfg(feature = "gateway")]
                    {
                        let op = {
                            let client = self.conns.get_mut(&conn_id).unwrap();
                            match nats_proto::try_parse_gateway_op(&mut client.read_buf) {
                                Ok(op) => op,
                                Err(_) => {
                                    self.remove_conn(conn_id);
                                    return;
                                }
                            }
                        };
                        match op {
                            Some(op) => {
                                let (result, expired) = {
                                    let client = self.conns.get_mut(&conn_id).unwrap();
                                    let draining = matches!(client.phase, ConnPhase::Draining);
                                    let mut conn_ctx = ConnCtx {
                                        conn_id,
                                        write_buf: &mut client.write_buf,
                                        direct_writer: &client.direct_writer,
                                        echo: client.echo,
                                        no_responders: client.no_responders,
                                        sub_count: &mut client.sub_count,
                                        #[cfg(feature = "leaf")]
                                        upstream_tx: &mut client.upstream_tx,
                                        permissions: &client.permissions,
                                        ext: &mut client.ext,
                                        draining,
                                        #[cfg(feature = "accounts")]
                                        account_id: client.account_id,
                                    };
                                    let mut worker_ctx = WorkerCtx {
                                        state: &self.state,
                                        event_fd: self.event_fd.as_raw_fd(),
                                        pending_notify: &mut self.pending_notify,
                                        pending_notify_count: &mut self.pending_notify_count,
                                        msgs_received: &mut self.msgs_received,
                                        msgs_received_bytes: &mut self.msgs_received_bytes,
                                        msgs_delivered: &mut self.msgs_delivered,
                                        msgs_delivered_bytes: &mut self.msgs_delivered_bytes,
                                        worker_label: &self.worker_label,
                                    };
                                    GatewayHandler::handle_op(&mut conn_ctx, &mut worker_ctx, op)
                                };
                                {
                                    #[cfg(feature = "accounts")]
                                    let acct =
                                        self.conns.get(&conn_id).map(|c| c.account_id).unwrap_or(0);
                                    handle_expired_subs(
                                        &expired,
                                        &self.state,
                                        &mut self.conns,
                                        &self.worker_label,
                                        #[cfg(feature = "accounts")]
                                        acct,
                                    );
                                }
                                match result {
                                    HandleResult::Ok => {}
                                    HandleResult::Flush => self.try_flush_conn(conn_id),
                                    HandleResult::Disconnect => {
                                        self.try_flush_conn(conn_id);
                                        self.remove_conn(conn_id);
                                        return;
                                    }
                                }
                            }
                            None => {
                                if let Some(client) = self.conns.get_mut(&conn_id) {
                                    client.read_buf.try_shrink();
                                }
                                return;
                            }
                        }
                    }
                } else {
                    // Client connection: parse client protocol ops.
                    let can_skip = {
                        #[cfg(feature = "gateway")]
                        let has_gw = self.state.has_gateway_interest.load(Ordering::Relaxed);
                        #[cfg(not(feature = "gateway"))]
                        let has_gw = false;

                        #[cfg(feature = "leaf")]
                        {
                            let client = self.conns.get(&conn_id).unwrap();
                            client.upstream_tx.is_none()
                                && !self.state.has_subs.load(Ordering::Relaxed)
                                && !has_gw
                        }
                        #[cfg(not(feature = "leaf"))]
                        {
                            !self.state.has_subs.load(Ordering::Relaxed) && !has_gw
                        }
                    };

                    let op = {
                        let client = self.conns.get_mut(&conn_id).unwrap();
                        let result = if can_skip {
                            nats_proto::try_skip_or_parse_client_op(&mut client.read_buf)
                        } else {
                            nats_proto::try_parse_client_op(&mut client.read_buf)
                        };
                        match result {
                            Ok(op) => op,
                            Err(_) => {
                                self.remove_conn(conn_id);
                                return;
                            }
                        }
                    };

                    match op {
                        Some(ClientOp::Pong) if can_skip => {
                            // Skipped PUB/HPUB, continue parsing
                            continue;
                        }
                        Some(ClientOp::Pong) => {
                            if let Some(client) = self.conns.get_mut(&conn_id) {
                                client.pings_outstanding = 0;
                                client.last_activity = Instant::now();
                            }
                        }
                        Some(op) => {
                            let (result, expired) = {
                                let client = self.conns.get_mut(&conn_id).unwrap();
                                let draining = matches!(client.phase, ConnPhase::Draining);
                                let mut conn_ctx = ConnCtx {
                                    conn_id,
                                    write_buf: &mut client.write_buf,
                                    direct_writer: &client.direct_writer,
                                    echo: client.echo,
                                    no_responders: client.no_responders,
                                    sub_count: &mut client.sub_count,
                                    #[cfg(feature = "leaf")]
                                    upstream_tx: &mut client.upstream_tx,
                                    permissions: &client.permissions,
                                    ext: &mut client.ext,
                                    draining,
                                    #[cfg(feature = "accounts")]
                                    account_id: client.account_id,
                                };
                                let mut worker_ctx = WorkerCtx {
                                    state: &self.state,
                                    event_fd: self.event_fd.as_raw_fd(),
                                    pending_notify: &mut self.pending_notify,
                                    pending_notify_count: &mut self.pending_notify_count,
                                    msgs_received: &mut self.msgs_received,
                                    msgs_received_bytes: &mut self.msgs_received_bytes,
                                    msgs_delivered: &mut self.msgs_delivered,
                                    msgs_delivered_bytes: &mut self.msgs_delivered_bytes,
                                    worker_label: &self.worker_label,
                                };
                                ClientHandler::handle_op(&mut conn_ctx, &mut worker_ctx, op)
                            };
                            {
                                #[cfg(feature = "accounts")]
                                let acct =
                                    self.conns.get(&conn_id).map(|c| c.account_id).unwrap_or(0);
                                handle_expired_subs(
                                    &expired,
                                    &self.state,
                                    &mut self.conns,
                                    &self.worker_label,
                                    #[cfg(feature = "accounts")]
                                    acct,
                                );
                            }
                            match result {
                                HandleResult::Ok => {}
                                HandleResult::Flush => self.try_flush_conn(conn_id),
                                HandleResult::Disconnect => {
                                    self.try_flush_conn(conn_id);
                                    self.remove_conn(conn_id);
                                    return;
                                }
                            }
                        }
                        None => {
                            // No more complete ops — try_shrink and return
                            if let Some(client) = self.conns.get_mut(&conn_id) {
                                client.read_buf.try_shrink();
                            }
                            return;
                        }
                    }
                }
            }
        }
    }

    fn handle_write(&mut self, conn_id: u64) {
        self.try_flush_conn(conn_id);
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
                let mut subs = account_sub.write().unwrap();
                all_removed.extend(subs.remove_conn(id));
            }
            state.has_subs.store(
                state
                    .account_subs
                    .iter()
                    .any(|s| !s.read().unwrap().is_empty()),
                std::sync::atomic::Ordering::Relaxed,
            );
            all_removed
        }
        #[cfg(not(feature = "accounts"))]
        {
            let mut subs = state.subs.write().unwrap();
            let r = subs.remove_conn(id);
            state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
            r
        }
    };

    #[cfg(feature = "leaf")]
    if !removed.is_empty() {
        let mut upstream = state.upstream.write().unwrap();
        if let Some(ref mut up) = *upstream {
            for sub in &removed {
                up.remove_interest(&sub.subject, sub.queue.as_deref());
            }
        }
    }
    #[cfg(not(feature = "leaf"))]
    let _ = &removed;

    info!(id, "client cleaned up");
}
