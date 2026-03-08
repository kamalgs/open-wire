// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! N-worker epoll event loop.
//!
//! Each worker owns one epoll instance multiplexing many client connections.
//! DirectWriter notifies the *worker's* single eventfd, so fan-out to 100
//! connections on the same worker costs 1 eventfd write, not 100.

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes, BytesMut};
use tracing::{debug, info, warn};

use crate::nats_proto;
use crate::protocol::{AdaptiveBuf, ClientOp};
use crate::server::ServerState;
use crate::sub_list::{create_eventfd, DirectWriter, Subscription};
use crate::upstream::UpstreamCmd;
use crate::websocket::{self, DecodeStatus, WsCodec};

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
    Shutdown,
}

/// Handle for the acceptor to send connections and commands to a worker.
pub(crate) struct WorkerHandle {
    pub tx: mpsc::Sender<WorkerCmd>,
    event_fd: Arc<OwnedFd>,
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

    /// Send shutdown command and wake the worker.
    pub fn shutdown(&self) {
        let _ = self.tx.send(WorkerCmd::Shutdown);
        self.wake();
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
    /// WebSocket: waiting for HTTP upgrade request.
    WsHandshake,
    /// INFO queued in write_buf, waiting to be flushed.
    SendInfo,
    /// INFO sent, waiting for CONNECT from client.
    WaitConnect,
    /// Normal operation.
    Active,
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
}

struct ClientState {
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
    upstream_tx: Option<mpsc::Sender<UpstreamCmd>>,
    /// Whether EPOLLOUT is currently registered for this fd.
    epoll_has_out: bool,
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
    shutdown: bool,
    /// Accumulated eventfd notifications. Flushed after processing a read batch.
    /// Deduplicates across multiple PUBs in the same read buffer.
    pending_notify: [RawFd; 16],
    pending_notify_count: usize,
}

impl Worker {
    /// Spawn a worker thread. Returns a handle for sending commands.
    pub(crate) fn spawn(index: usize, state: Arc<ServerState>) -> WorkerHandle {
        let (tx, rx) = mpsc::channel();
        let event_fd = Arc::new(create_eventfd());
        let handle = WorkerHandle {
            tx,
            event_fd: Arc::clone(&event_fd),
        };

        let info_json =
            serde_json::to_string(&state.info).expect("failed to serialize server info");
        let info_line = format!("INFO {info_json}\r\n").into_bytes();

        std::thread::Builder::new()
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
                    shutdown: false,
                    pending_notify: [-1; 16],
                    pending_notify_count: 0,
                };
                worker.run();
            })
            .expect("failed to spawn worker thread");

        handle
    }

    fn run(&mut self) {
        let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 256];

        loop {
            let n = unsafe {
                libc::epoll_wait(
                    self.epoll_fd.as_raw_fd(),
                    events.as_mut_ptr(),
                    events.len() as i32,
                    -1,
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

            for i in 0..n as usize {
                let ev = events[i];
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

            if self.shutdown {
                break;
            }
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

        // Check for new connections / shutdown
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
            upstream_tx: None,
            epoll_has_out: false,
        };

        self.fd_to_conn.insert(fd, id);
        self.conns.insert(id, client);

        if is_websocket {
            debug!(id, addr = %addr, "accepted websocket connection on worker");
        } else {
            debug!(id, addr = %addr, "accepted connection on worker");
            // Try to flush INFO immediately
            self.try_flush_conn(id);
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
            cleanup_conn(conn_id, &self.state);
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
            {
                let mut dbuf = client.direct_buf.lock().unwrap();
                if !dbuf.is_empty() {
                    client.write_buf.extend_from_slice(&dbuf);
                    dbuf.clear();
                }
            }

            // Slow consumer detection: if pending data exceeds max_pending,
            // disconnect the client to protect server memory.
            let max_pending = self.state.buf_config.max_pending;
            if max_pending > 0 {
                let pending = match &client.transport {
                    Transport::Raw => client.write_buf.len(),
                    Transport::WebSocket { ws_out, .. } => client.write_buf.len() + ws_out.len(),
                };
                if pending > max_pending {
                    warn!(
                        conn_id = *conn_id,
                        pending_bytes = pending,
                        max = max_pending,
                        "slow consumer, disconnecting"
                    );
                    to_remove.push(*conn_id);
                    continue;
                }
            }

            // For WebSocket: encode write_buf into ws_out, then write ws_out
            let (write_ptr, write_len) = match &mut client.transport {
                Transport::Raw => (client.write_buf.as_ptr(), client.write_buf.len()),
                Transport::WebSocket { ws_out, .. } => {
                    if !client.write_buf.is_empty() {
                        WsCodec::encode(&client.write_buf, ws_out);
                        client.write_buf.clear();
                    }
                    (ws_out.as_ptr(), ws_out.len())
                }
            };

            // Inline flush
            let mut error = false;
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

            // Advance the appropriate buffer
            match &mut client.transport {
                Transport::Raw => {
                    client.write_buf.advance(written_total);
                }
                Transport::WebSocket { ws_out, .. } => {
                    ws_out.advance(written_total);
                }
            }

            if error {
                to_remove.push(*conn_id);
                continue;
            }

            let is_empty = match &client.transport {
                Transport::Raw => client.write_buf.is_empty(),
                Transport::WebSocket { ws_out, .. } => ws_out.is_empty(),
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

    /// Try to flush write_buf to the socket. Registers/removes EPOLLOUT as needed.
    /// For WebSocket connections, write_buf is encoded into ws_out first.
    fn try_flush_conn(&mut self, conn_id: u64) {
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
        let is_ws = matches!(
            self.conns.get(&conn_id).map(|c| &c.transport),
            Some(Transport::WebSocket { .. })
        );

        if is_ws {
            self.handle_read_ws(conn_id);
        } else {
            self.handle_read_raw(conn_id);
        }
    }

    fn handle_read_raw(&mut self, conn_id: u64) {
        // Read from socket in a loop until WouldBlock to drain the kernel buffer.
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
        self.flush_notifications();
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
                    match codec.decode(raw_buf, &mut *client.read_buf) {
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

        self.process_read_buf(conn_id);
        self.flush_notifications();
    }

    fn process_read_buf(&mut self, conn_id: u64) {
        loop {
            let phase = match self.conns.get(&conn_id) {
                Some(c) => match c.phase {
                    ConnPhase::WsHandshake => 0,
                    ConnPhase::SendInfo => return,
                    ConnPhase::WaitConnect => 1,
                    ConnPhase::Active => 2,
                },
                None => return,
            };

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

            if phase == 1 {
                // WaitConnect: parse CONNECT
                let op = {
                    let client = self.conns.get_mut(&conn_id).unwrap();
                    match nats_proto::try_parse_client_op(&mut *client.read_buf) {
                        Ok(op) => op,
                        Err(_) => {
                            self.remove_conn(conn_id);
                            return;
                        }
                    }
                };
                match op {
                    Some(ClientOp::Connect(connect_info)) => {
                        if !self
                            .state
                            .auth
                            .validate(&connect_info, &self.state.info.nonce)
                        {
                            warn!(conn_id, "authorization violation");
                            if let Some(client) = self.conns.get_mut(&conn_id) {
                                client
                                    .write_buf
                                    .extend_from_slice(b"-ERR 'Authorization Violation'\r\n");
                            }
                            self.try_flush_conn(conn_id);
                            self.remove_conn(conn_id);
                            return;
                        }
                        let client = self.conns.get_mut(&conn_id).unwrap();
                        client.phase = ConnPhase::Active;
                        client.upstream_tx = self.state.upstream_tx.read().unwrap().clone();
                        info!(conn_id, "client connected");
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
                // Active: parse client ops
                let can_skip = {
                    let client = self.conns.get(&conn_id).unwrap();
                    client.upstream_tx.is_none() && !self.state.has_subs.load(Ordering::Relaxed)
                };

                let op = {
                    let client = self.conns.get_mut(&conn_id).unwrap();
                    let result = if can_skip {
                        nats_proto::try_skip_or_parse_client_op(&mut *client.read_buf)
                    } else {
                        nats_proto::try_parse_client_op(&mut *client.read_buf)
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
                    Some(op) => {
                        if self.handle_client_op(conn_id, op).is_err() {
                            self.remove_conn(conn_id);
                            return;
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

    fn handle_write(&mut self, conn_id: u64) {
        self.try_flush_conn(conn_id);
    }

    fn handle_client_op(&mut self, conn_id: u64, op: ClientOp) -> io::Result<()> {
        match op {
            ClientOp::Ping => {
                if let Some(client) = self.conns.get_mut(&conn_id) {
                    client.write_buf.extend_from_slice(b"PONG\r\n");
                }
                self.try_flush_conn(conn_id);
            }
            ClientOp::Pong => {}
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                let subject_str = bytes_to_str(&subject);
                let queue_str = queue_group.as_ref().map(|q| bytes_to_str(q).to_string());

                let direct_writer = match self.conns.get(&conn_id) {
                    Some(c) => c.direct_writer.clone(),
                    None => return Ok(()),
                };

                let sub = Subscription {
                    conn_id,
                    sid,
                    sid_bytes: nats_proto::sid_to_bytes(sid),
                    subject: subject_str.to_string(),
                    queue: queue_str,
                    writer: direct_writer,
                };

                {
                    let mut subs = self.state.subs.write().unwrap();
                    subs.insert(sub);
                    self.state.has_subs.store(true, Ordering::Relaxed);
                }

                {
                    let mut upstream = self.state.upstream.write().unwrap();
                    if let Some(ref mut up) = *upstream {
                        if let Err(e) = up.add_interest(subject_str.to_string()) {
                            warn!(error = %e, "failed to add upstream interest");
                        }
                    }
                }

                debug!(conn_id, sid, subject = %subject_str, "client subscribed");
            }
            ClientOp::Unsubscribe { sid, max: _ } => {
                let removed = {
                    let mut subs = self.state.subs.write().unwrap();
                    let r = subs.remove(conn_id, sid);
                    self.state
                        .has_subs
                        .store(!subs.is_empty(), Ordering::Relaxed);
                    r
                };

                if let Some(removed) = removed {
                    let mut upstream = self.state.upstream.write().unwrap();
                    if let Some(ref mut up) = *upstream {
                        up.remove_interest(&removed.subject);
                    }
                    debug!(conn_id, sid, subject = %removed.subject, "client unsubscribed");
                }
            }
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
                ..
            } => {
                {
                    let my_event_fd = self.event_fd.as_raw_fd();
                    let pending_notify = &mut self.pending_notify;
                    let pending_count = &mut self.pending_notify_count;

                    let subject_str = bytes_to_str(&subject);
                    let subs = self.state.subs.read().unwrap();
                    subs.for_each_match(subject_str, |sub| {
                        sub.writer.write_msg(
                            &subject,
                            &sub.sid_bytes,
                            respond.as_deref(),
                            headers.as_ref(),
                            &payload,
                        );
                        // Skip notification for our own worker — flush_pending
                        // runs after the event loop iteration.
                        let fd = sub.writer.event_raw_fd();
                        if fd == my_event_fd {
                            return;
                        }
                        // Accumulate notification (deduplicated across entire batch).
                        if !pending_notify[..*pending_count].contains(&fd) {
                            if *pending_count < pending_notify.len() {
                                pending_notify[*pending_count] = fd;
                                *pending_count += 1;
                            }
                        }
                    });
                }

                let upstream_tx = self.conns.get(&conn_id).and_then(|c| c.upstream_tx.clone());
                if let Some(ref tx) = upstream_tx {
                    if let Err(e) = tx.send(UpstreamCmd::Publish {
                        subject,
                        reply: respond,
                        headers,
                        payload,
                    }) {
                        warn!(error = %e, "failed to forward publish to upstream");
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

/// Convert `Bytes` to `&str` without UTF-8 validation.
/// NATS subjects are restricted to ASCII printable characters.
#[inline]
fn bytes_to_str(b: &Bytes) -> &str {
    // SAFETY: NATS protocol subjects/reply-to are always ASCII
    unsafe { std::str::from_utf8_unchecked(b) }
}

fn cleanup_conn(id: u64, state: &ServerState) {
    let removed = {
        let mut subs = state.subs.write().unwrap();
        let r = subs.remove_conn(id);
        state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
        r
    };

    if !removed.is_empty() {
        let mut upstream = state.upstream.write().unwrap();
        if let Some(ref mut up) = *upstream {
            for sub in &removed {
                up.remove_interest(&sub.subject);
            }
        }
    }

    info!(id, "client cleaned up");
}
