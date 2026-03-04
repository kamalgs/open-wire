// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::ServerInfo;

use super::client_conn::{ClientConnection, ClientHandle};
use super::sub_list::SubList;
use super::upstream::Upstream;

/// Configuration for the leaf node server.
#[derive(Debug, Clone)]
pub struct LeafServerConfig {
    /// Address to listen on (e.g., "0.0.0.0").
    pub host: String,
    /// Port to listen on.
    pub port: u16,
    /// Optional upstream hub URL (e.g., "nats://hub:4222").
    pub hub_url: Option<String>,
    /// Server name.
    pub server_name: String,
}

impl Default for LeafServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 4222,
            hub_url: None,
            server_name: "leaf-node".to_string(),
        }
    }
}

/// Shared server state accessible by all client connections.
pub(crate) struct ServerState {
    pub info: ServerInfo,
    pub conns: RwLock<HashMap<u64, ClientHandle>>,
    pub subs: RwLock<SubList>,
    pub upstream: RwLock<Option<Upstream>>,
    next_cid: AtomicU64,
}

impl ServerState {
    fn new(info: ServerInfo) -> Self {
        Self {
            info,
            conns: RwLock::new(HashMap::new()),
            subs: RwLock::new(SubList::new()),
            upstream: RwLock::new(None),
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

        Self {
            config,
            state: Arc::new(ServerState::new(info)),
        }
    }

    /// Run the leaf server. Listens for connections and optionally
    /// connects to the upstream hub.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Connect to upstream hub if configured
        if let Some(ref hub_url) = self.config.hub_url {
            info!(url = %hub_url, "connecting to upstream hub");
            match crate::connect(hub_url).await {
                Ok(client) => {
                    let upstream = Upstream::new(client, Arc::clone(&self.state));
                    *self.state.upstream.write().await = Some(upstream);
                    info!("connected to upstream hub");
                }
                Err(e) => {
                    error!(error = %e, "failed to connect to upstream hub");
                    return Err(e.into());
                }
            }
        }

        let bind_addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(addr = %bind_addr, "leaf server listening");

        loop {
            match listener.accept().await {
                Ok((tcp_stream, addr)) => {
                    tcp_stream.set_nodelay(true).ok();
                    let cid = self.state.next_client_id();
                    let state = Arc::clone(&self.state);

                    info!(cid, addr = %addr, "accepted connection");

                    let (client_conn, handle) =
                        ClientConnection::new(cid, Box::new(tcp_stream), state.clone());

                    {
                        let mut conns = state.conns.write().await;
                        conns.insert(cid, handle);
                    }

                    tokio::spawn(async move {
                        client_conn.run().await;
                    });
                }
                Err(e) => {
                    error!(error = %e, "failed to accept connection");
                }
            }
        }
    }

    /// Run the leaf server with graceful shutdown support.
    pub async fn run_until_shutdown(
        &self,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Connect to upstream hub if configured
        if let Some(ref hub_url) = self.config.hub_url {
            info!(url = %hub_url, "connecting to upstream hub");
            match crate::connect(hub_url).await {
                Ok(client) => {
                    let upstream = Upstream::new(client, Arc::clone(&self.state));
                    *self.state.upstream.write().await = Some(upstream);
                    info!("connected to upstream hub");
                }
                Err(e) => {
                    error!(error = %e, "failed to connect to upstream hub");
                    return Err(e.into());
                }
            }
        }

        let bind_addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(addr = %bind_addr, "leaf server listening");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((tcp_stream, addr)) => {
                            tcp_stream.set_nodelay(true).ok();
                            let cid = self.state.next_client_id();
                            let state = Arc::clone(&self.state);

                            info!(cid, addr = %addr, "accepted connection");

                            let (client_conn, handle) =
                                ClientConnection::new(cid, Box::new(tcp_stream), state.clone());

                            {
                                let mut conns = state.conns.write().await;
                                conns.insert(cid, handle);
                            }

                            tokio::spawn(async move {
                                client_conn.run().await;
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "failed to accept connection");
                        }
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
            let mut conns = self.state.conns.write().await;
            conns.clear();
        }

        // Drop upstream
        {
            let mut upstream = self.state.upstream.write().await;
            *upstream = None;
        }

        Ok(())
    }
}
