// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{error, info};

use async_nats::ServerInfo;

use crate::client_conn::{ClientConnection, ClientHandle};
use crate::protocol::BufConfig;
use crate::sub_list::SubList;
use crate::upstream::{Upstream, UpstreamCmd};

/// Configuration for the leaf node server.
#[derive(Debug, Clone)]
pub struct LeafServerConfig {
    /// Address to listen on (e.g., "0.0.0.0").
    pub host: String,
    /// Port to listen on.
    pub port: u16,
    /// Optional WebSocket port. When set, a second listener accepts
    /// WebSocket connections on this port.
    #[cfg(feature = "websockets")]
    pub ws_port: Option<u16>,
    /// Optional upstream hub URL (e.g., "nats://hub:4222").
    pub hub_url: Option<String>,
    /// Server name.
    pub server_name: String,
    /// Max per-client read buffer capacity in bytes (default: 64 KB).
    /// The buffer starts small (512B) and grows adaptively up to this limit.
    pub max_read_buf_capacity: usize,
    /// Per-client write buffer capacity in bytes (default: 64 KB).
    pub write_buf_capacity: usize,
}

impl Default for LeafServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 4222,
            #[cfg(feature = "websockets")]
            ws_port: None,
            hub_url: None,
            server_name: "leaf-node".to_string(),
            max_read_buf_capacity: 65536,
            write_buf_capacity: 65536,
        }
    }
}

/// Shared server state accessible by all client connections.
pub(crate) struct ServerState {
    pub info: ServerInfo,
    pub conns: std::sync::RwLock<HashMap<u64, ClientHandle>>,
    pub subs: std::sync::RwLock<SubList>,
    pub upstream: tokio::sync::RwLock<Option<Upstream>>,
    /// Lock-free sender for forwarding publishes to the upstream hub.
    /// Set once after upstream connects; read without locking on every publish.
    pub upstream_tx: std::sync::RwLock<Option<mpsc::UnboundedSender<UpstreamCmd>>>,
    pub buf_config: BufConfig,
    next_cid: AtomicU64,
}

impl ServerState {
    fn new(info: ServerInfo, buf_config: BufConfig) -> Self {
        Self {
            info,
            conns: std::sync::RwLock::new(HashMap::new()),
            subs: std::sync::RwLock::new(SubList::new()),
            upstream: tokio::sync::RwLock::new(None),
            upstream_tx: std::sync::RwLock::new(None),
            buf_config,
            next_cid: AtomicU64::new(1),
        }
    }

    pub(crate) fn next_client_id(&self) -> u64 {
        self.next_cid.fetch_add(1, Ordering::Relaxed)
    }
}

/// A NATS leaf node gateway server.
///
/// Accepts local client connections, routes messages between them,
/// and optionally forwards traffic to an upstream NATS hub.
pub struct LeafServer {
    config: LeafServerConfig,
    state: Arc<ServerState>,
}

impl LeafServer {
    /// Create a new leaf server with the given configuration.
    pub fn new(config: LeafServerConfig) -> Self {
        let info = ServerInfo {
            server_id: format!("LEAF_{}", rand::random::<u32>()),
            server_name: config.server_name.clone(),
            version: "0.1.0".to_string(),
            proto: 1,
            max_payload: 1024 * 1024, // 1MB
            headers: true,
            host: config.host.clone(),
            port: config.port,
            ..Default::default()
        };

        let buf_config = BufConfig {
            max_read_buf: config.max_read_buf_capacity,
            write_buf: config.write_buf_capacity,
        };

        Self {
            config,
            state: Arc::new(ServerState::new(info, buf_config)),
        }
    }

    /// Connect to the upstream hub if configured, using the leaf node protocol.
    async fn connect_upstream(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref hub_url) = self.config.hub_url {
            info!(url = %hub_url, "connecting to upstream hub (leaf protocol)");
            match Upstream::connect(hub_url, Arc::clone(&self.state)).await {
                Ok(upstream) => {
                    let sender = upstream.sender();
                    *self.state.upstream.write().await = Some(upstream);
                    *self.state.upstream_tx.write().unwrap() = Some(sender);
                    info!("connected to upstream hub");
                }
                Err(e) => {
                    error!(error = %e, "failed to connect to upstream hub");
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Register a new client connection and spawn its handler task.
    async fn spawn_client(
        &self,
        stream: Box<dyn async_nats::connection::AsyncReadWrite>,
        addr: std::net::SocketAddr,
        transport: &str,
    ) {
        let cid = self.state.next_client_id();
        let state = Arc::clone(&self.state);

        info!(cid, addr = %addr, transport, "accepted connection");

        let (client_conn, handle) = ClientConnection::new(cid, stream, state.clone());

        {
            let mut conns = state.conns.write().unwrap();
            conns.insert(cid, handle);
        }

        tokio::spawn(async move {
            client_conn.run().await;
        });
    }

    /// Accept a TCP connection and register it.
    async fn accept_tcp(&self, tcp_stream: TcpStream, addr: std::net::SocketAddr) {
        tcp_stream.set_nodelay(true).ok();
        self.spawn_client(Box::new(tcp_stream), addr, "tcp").await;
    }

    /// Accept a WebSocket connection (upgrade from TCP) and register it.
    #[cfg(feature = "websockets")]
    async fn accept_ws(&self, tcp_stream: TcpStream, addr: std::net::SocketAddr) {
        tcp_stream.set_nodelay(true).ok();
        match tokio_websockets::ServerBuilder::new()
            .accept(tcp_stream)
            .await
        {
            Ok(ws_stream) => {
                let adapter = async_nats::connection::WebSocketAdapter::new(ws_stream);
                self.spawn_client(Box::new(adapter), addr, "websocket")
                    .await;
            }
            Err(e) => {
                error!(addr = %addr, error = %e, "websocket upgrade failed");
            }
        }
    }

    /// Run the leaf server. Listens for connections and optionally
    /// connects to the upstream hub.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.connect_upstream().await?;

        let bind_addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(addr = %bind_addr, "leaf server listening (tcp)");

        #[cfg(feature = "websockets")]
        let ws_listener = if let Some(ws_port) = self.config.ws_port {
            let ws_addr = format!("{}:{}", self.config.host, ws_port);
            let l = TcpListener::bind(&ws_addr).await?;
            info!(addr = %ws_addr, "leaf server listening (websocket)");
            Some(l)
        } else {
            None
        };

        loop {
            #[cfg(feature = "websockets")]
            {
                if let Some(ref ws_l) = ws_listener {
                    tokio::select! {
                        result = listener.accept() => {
                            match result {
                                Ok((tcp_stream, addr)) => self.accept_tcp(tcp_stream, addr).await,
                                Err(e) => error!(error = %e, "failed to accept tcp connection"),
                            }
                        }
                        result = ws_l.accept() => {
                            match result {
                                Ok((tcp_stream, addr)) => self.accept_ws(tcp_stream, addr).await,
                                Err(e) => error!(error = %e, "failed to accept websocket connection"),
                            }
                        }
                    }
                    continue;
                }
            }

            // TCP-only path (no WS listener or websockets feature disabled)
            match listener.accept().await {
                Ok((tcp_stream, addr)) => self.accept_tcp(tcp_stream, addr).await,
                Err(e) => error!(error = %e, "failed to accept connection"),
            }
        }
    }

    /// Run the leaf server with graceful shutdown support.
    pub async fn run_until_shutdown(
        &self,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.connect_upstream().await?;

        let bind_addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(addr = %bind_addr, "leaf server listening (tcp)");

        #[cfg(feature = "websockets")]
        let ws_listener = if let Some(ws_port) = self.config.ws_port {
            let ws_addr = format!("{}:{}", self.config.host, ws_port);
            let l = TcpListener::bind(&ws_addr).await?;
            info!(addr = %ws_addr, "leaf server listening (websocket)");
            Some(l)
        } else {
            None
        };

        loop {
            #[cfg(feature = "websockets")]
            {
                if let Some(ref ws_l) = ws_listener {
                    tokio::select! {
                        result = listener.accept() => {
                            match result {
                                Ok((tcp_stream, addr)) => self.accept_tcp(tcp_stream, addr).await,
                                Err(e) => error!(error = %e, "failed to accept tcp connection"),
                            }
                        }
                        result = ws_l.accept() => {
                            match result {
                                Ok((tcp_stream, addr)) => self.accept_ws(tcp_stream, addr).await,
                                Err(e) => error!(error = %e, "failed to accept websocket connection"),
                            }
                        }
                        _ = shutdown.recv() => {
                            info!("shutting down leaf server");
                            break;
                        }
                    }
                    continue;
                }
            }

            // TCP-only path
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((tcp_stream, addr)) => self.accept_tcp(tcp_stream, addr).await,
                        Err(e) => error!(error = %e, "failed to accept connection"),
                    }
                }
                _ = shutdown.recv() => {
                    info!("shutting down leaf server");
                    break;
                }
            }
        }

        // Cleanup: drop all connection handles to signal clients
        {
            let mut conns = self.state.conns.write().unwrap();
            conns.clear();
        }

        // Drop upstream
        {
            *self.state.upstream_tx.write().unwrap() = None;
            let mut upstream = self.state.upstream.write().await;
            *upstream = None;
        }

        Ok(())
    }
}
