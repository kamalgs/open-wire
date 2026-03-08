// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

use tracing::{error, info};

use crate::types::{ConnectInfo, ServerInfo};

use crate::protocol::BufConfig;
use crate::sub_list::SubList;
use crate::upstream::{Upstream, UpstreamCmd};
use crate::worker::{Worker, WorkerHandle};

/// Downstream client authentication configuration.
#[derive(Debug, Clone, Default)]
pub enum ClientAuth {
    /// No authentication required (default).
    #[default]
    None,
    /// Single token. Client sends `auth_token` in CONNECT.
    Token(String),
    /// Single user/password pair.
    UserPass { user: String, pass: String },
    /// NKey public key allowlist. Server sends nonce in INFO;
    /// client signs nonce and sends `nkey` + `sig` in CONNECT.
    NKey(Vec<String>),
}

impl ClientAuth {
    /// Returns `true` if authentication is required.
    pub fn is_required(&self) -> bool {
        !matches!(self, ClientAuth::None)
    }

    /// Returns `true` if the auth mode requires a nonce in INFO.
    pub fn needs_nonce(&self) -> bool {
        matches!(self, ClientAuth::NKey(_))
    }

    /// Validate a client's CONNECT info against the configured auth.
    pub fn validate(&self, info: &ConnectInfo, nonce: &str) -> bool {
        match self {
            ClientAuth::None => true,
            ClientAuth::Token(t) => info.auth_token.as_deref() == Some(t.as_str()),
            ClientAuth::UserPass { user, pass } => {
                info.user.as_deref() == Some(user.as_str())
                    && info.pass.as_deref() == Some(pass.as_str())
            }
            ClientAuth::NKey(allowed_keys) => {
                let nkey = match info.nkey.as_deref() {
                    Some(k) => k,
                    None => return false,
                };
                if !allowed_keys.iter().any(|k| k == nkey) {
                    return false;
                }
                let sig = match info.signature.as_deref() {
                    Some(s) => s,
                    None => return false,
                };
                let sig_bytes = match data_encoding::BASE64URL_NOPAD.decode(sig.as_bytes()) {
                    Ok(b) => b,
                    Err(_) => {
                        // Try standard base64 as fallback (some clients use it)
                        match data_encoding::BASE64.decode(sig.as_bytes()) {
                            Ok(b) => b,
                            Err(_) => return false,
                        }
                    }
                };
                match nkeys::KeyPair::from_public_key(nkey) {
                    Ok(kp) => kp.verify(nonce.as_bytes(), &sig_bytes).is_ok(),
                    Err(_) => false,
                }
            }
        }
    }
}

/// Credentials for connecting to an upstream hub server.
#[derive(Debug, Clone, Default)]
pub struct HubCredentials {
    /// Username for user/password auth.
    pub user: Option<String>,
    /// Password for user/password auth.
    pub pass: Option<String>,
    /// Token for token auth.
    pub token: Option<String>,
    /// Path to a `.creds` file (JWT + NKey seed) for NKey/JWT auth.
    pub creds_file: Option<String>,
}

/// Generate a random nonce for NKey challenge-response auth.
fn generate_nonce() -> String {
    let mut data = [0u8; 11];
    rand::Rng::fill(&mut rand::thread_rng(), &mut data);
    data_encoding::BASE64URL_NOPAD.encode(&data)
}

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
    /// Max per-client read buffer capacity in bytes (default: 64 KB).
    /// The buffer starts small (512B) and grows adaptively up to this limit.
    pub max_read_buf_capacity: usize,
    /// Per-client write buffer capacity in bytes (default: 64 KB).
    pub write_buf_capacity: usize,
    /// Number of worker threads (default: available parallelism or 4).
    pub workers: usize,
    /// Optional WebSocket port. When set, a second listener accepts WebSocket
    /// connections on this port (NATS protocol over WebSocket binary frames).
    pub ws_port: Option<u16>,
    /// Maximum pending write bytes per connection before disconnecting as a
    /// slow consumer (default: 64 MB, matching Go nats-server). 0 = unlimited.
    pub max_pending: usize,
    /// Client authentication configuration.
    pub client_auth: ClientAuth,
    /// Credentials for connecting to the upstream hub.
    pub hub_credentials: Option<HubCredentials>,
}

impl Default for LeafServerConfig {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            host: "0.0.0.0".to_string(),
            port: 4222,
            hub_url: None,
            server_name: "leaf-node".to_string(),
            max_read_buf_capacity: 65536,
            write_buf_capacity: 65536,
            workers,
            ws_port: None,
            max_pending: 64 * 1024 * 1024,
            client_auth: ClientAuth::None,
            hub_credentials: None,
        }
    }
}

/// Shared server state accessible by all client connections.
pub(crate) struct ServerState {
    pub info: ServerInfo,
    pub auth: ClientAuth,
    pub subs: std::sync::RwLock<SubList>,
    pub upstream: std::sync::RwLock<Option<Upstream>>,
    /// Lock-free sender for forwarding publishes to the upstream hub.
    /// Set once after upstream connects; read without locking on every publish.
    pub upstream_tx: std::sync::RwLock<Option<mpsc::Sender<UpstreamCmd>>>,
    /// Lock-free flag: true when at least one subscription exists.
    /// Updated on subscribe/unsubscribe. Avoids taking subs lock on every publish
    /// just to check emptiness.
    pub has_subs: AtomicBool,
    pub buf_config: BufConfig,
    next_cid: AtomicU64,
}

impl ServerState {
    fn new(info: ServerInfo, auth: ClientAuth, buf_config: BufConfig) -> Self {
        Self {
            info,
            auth,
            subs: std::sync::RwLock::new(SubList::new()),
            upstream: std::sync::RwLock::new(None),
            upstream_tx: std::sync::RwLock::new(None),
            has_subs: AtomicBool::new(false),
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
        let nonce = if config.client_auth.needs_nonce() {
            generate_nonce()
        } else {
            String::new()
        };

        let info = ServerInfo {
            server_id: format!("LEAF_{}", rand::random::<u32>()),
            server_name: config.server_name.clone(),
            version: "0.5.0".to_string(),
            proto: 1,
            max_payload: 1024 * 1024, // 1MB
            headers: true,
            host: config.host.clone(),
            port: config.port,
            auth_required: config.client_auth.is_required(),
            nonce,
            ..Default::default()
        };

        let buf_config = BufConfig {
            max_read_buf: config.max_read_buf_capacity,
            write_buf: config.write_buf_capacity,
            max_pending: config.max_pending,
        };

        let auth = config.client_auth.clone();
        Self {
            config,
            state: Arc::new(ServerState::new(info, auth, buf_config)),
        }
    }

    /// Connect to the upstream hub if configured, using the leaf node protocol.
    fn connect_upstream(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref hub_url) = self.config.hub_url {
            info!(url = %hub_url, "connecting to upstream hub (leaf protocol)");
            match Upstream::connect(
                hub_url,
                self.config.hub_credentials.as_ref(),
                Arc::clone(&self.state),
            ) {
                Ok(upstream) => {
                    let sender = upstream.sender();
                    *self.state.upstream.write().unwrap() = Some(upstream);
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

    /// Spawn N worker threads and return their handles.
    fn spawn_workers(&self) -> Vec<WorkerHandle> {
        let n = self.config.workers.max(1);
        info!(workers = n, "spawning worker threads");
        (0..n)
            .map(|i| Worker::spawn(i, Arc::clone(&self.state)))
            .collect()
    }

    /// Distribute a new TCP connection to the next worker (round-robin).
    fn accept_tcp(
        &self,
        tcp_stream: TcpStream,
        addr: std::net::SocketAddr,
        workers: &[WorkerHandle],
        next_worker: &mut usize,
        is_websocket: bool,
    ) {
        let cid = self.state.next_client_id();
        let idx = *next_worker % workers.len();
        *next_worker = idx + 1;
        workers[idx].send_conn(cid, tcp_stream, addr, is_websocket);
    }

    /// Run the leaf server. Listens for connections and optionally
    /// connects to the upstream hub. Blocks forever.
    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.connect_upstream()?;

        let workers = self.spawn_workers();
        let mut next_worker = 0usize;

        let bind_addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&bind_addr)?;
        info!(addr = %bind_addr, "leaf server listening (tcp)");

        let ws_listener = if let Some(ws_port) = self.config.ws_port {
            let ws_addr = format!("{}:{}", self.config.host, ws_port);
            let wl = TcpListener::bind(&ws_addr)?;
            info!(addr = %ws_addr, "leaf server listening (websocket)");
            Some(wl)
        } else {
            None
        };

        if let Some(ref ws_listener) = ws_listener {
            // Poll both listeners
            listener.set_nonblocking(true)?;
            ws_listener.set_nonblocking(true)?;
            let tcp_fd = listener.as_raw_fd();
            let ws_fd = ws_listener.as_raw_fd();
            let mut pfds = [
                libc::pollfd {
                    fd: tcp_fd,
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: ws_fd,
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];

            loop {
                pfds[0].revents = 0;
                pfds[1].revents = 0;
                let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };
                if ret < 0 {
                    let err = std::io::Error::last_os_error();
                    if err.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(err.into());
                }
                if pfds[0].revents & libc::POLLIN != 0 {
                    while let Ok((stream, addr)) = listener.accept() {
                        self.accept_tcp(stream, addr, &workers, &mut next_worker, false);
                    }
                }
                if pfds[1].revents & libc::POLLIN != 0 {
                    while let Ok((stream, addr)) = ws_listener.accept() {
                        self.accept_tcp(stream, addr, &workers, &mut next_worker, true);
                    }
                }
            }
        } else {
            loop {
                match listener.accept() {
                    Ok((tcp_stream, addr)) => {
                        self.accept_tcp(tcp_stream, addr, &workers, &mut next_worker, false);
                    }
                    Err(e) => error!(error = %e, "failed to accept connection"),
                }
            }
        }
    }

    /// Run the leaf server with graceful shutdown support.
    ///
    /// Uses `poll()` on the listener fd with a timeout so we can periodically
    /// check the shutdown flag without spinning.
    pub fn run_until_shutdown(
        &self,
        shutdown: Arc<AtomicBool>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.connect_upstream()?;

        let workers = self.spawn_workers();
        let mut next_worker = 0usize;

        let bind_addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&bind_addr)?;
        listener.set_nonblocking(true)?;
        info!(addr = %bind_addr, "leaf server listening (tcp)");

        let ws_listener = if let Some(ws_port) = self.config.ws_port {
            let ws_addr = format!("{}:{}", self.config.host, ws_port);
            let wl = TcpListener::bind(&ws_addr)?;
            wl.set_nonblocking(true)?;
            info!(addr = %ws_addr, "leaf server listening (websocket)");
            Some(wl)
        } else {
            None
        };

        let nfds = if ws_listener.is_some() { 2 } else { 1 };
        let mut pfds = [
            libc::pollfd {
                fd: listener.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: ws_listener.as_ref().map(|l| l.as_raw_fd()).unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];

        loop {
            if shutdown.load(Ordering::Relaxed) {
                info!("shutting down leaf server");
                break;
            }

            pfds[0].revents = 0;
            pfds[1].revents = 0;
            let ret = unsafe { libc::poll(pfds.as_mut_ptr(), nfds as libc::nfds_t, 1000) };

            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err.into());
            }

            if ret > 0 && pfds[0].revents & libc::POLLIN != 0 {
                while let Ok((tcp_stream, addr)) = listener.accept() {
                    self.accept_tcp(tcp_stream, addr, &workers, &mut next_worker, false);
                }
            }

            if ret > 0 && pfds[1].revents & libc::POLLIN != 0 {
                if let Some(ref ws_listener) = ws_listener {
                    while let Ok((tcp_stream, addr)) = ws_listener.accept() {
                        self.accept_tcp(tcp_stream, addr, &workers, &mut next_worker, true);
                    }
                }
            }
        }

        // Shutdown workers
        for w in &workers {
            w.shutdown();
        }

        // Cleanup: clear all subscriptions.
        {
            let mut subs = self.state.subs.write().unwrap();
            *subs = SubList::new();
        }

        // Drop upstream
        {
            *self.state.upstream_tx.write().unwrap() = None;
            let mut upstream = self.state.upstream.write().unwrap();
            *upstream = None;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- is_required / needs_nonce ---

    #[test]
    fn none_not_required() {
        let auth = ClientAuth::None;
        assert!(!auth.is_required());
        assert!(!auth.needs_nonce());
    }

    #[test]
    fn token_required_no_nonce() {
        let auth = ClientAuth::Token("secret".into());
        assert!(auth.is_required());
        assert!(!auth.needs_nonce());
    }

    #[test]
    fn userpass_required_no_nonce() {
        let auth = ClientAuth::UserPass {
            user: "u".into(),
            pass: "p".into(),
        };
        assert!(auth.is_required());
        assert!(!auth.needs_nonce());
    }

    #[test]
    fn nkey_required_needs_nonce() {
        let auth = ClientAuth::NKey(vec!["UABC".into()]);
        assert!(auth.is_required());
        assert!(auth.needs_nonce());
    }

    // --- ClientAuth::None ---

    #[test]
    fn none_always_passes() {
        let auth = ClientAuth::None;
        let info = ConnectInfo::default();
        assert!(auth.validate(&info, ""));
    }

    // --- ClientAuth::Token ---

    #[test]
    fn token_match() {
        let auth = ClientAuth::Token("secret".into());
        let info = ConnectInfo {
            auth_token: Some("secret".into()),
            ..Default::default()
        };
        assert!(auth.validate(&info, ""));
    }

    #[test]
    fn token_mismatch() {
        let auth = ClientAuth::Token("secret".into());
        let info = ConnectInfo {
            auth_token: Some("wrong".into()),
            ..Default::default()
        };
        assert!(!auth.validate(&info, ""));
    }

    #[test]
    fn token_absent() {
        let auth = ClientAuth::Token("secret".into());
        let info = ConnectInfo::default();
        assert!(!auth.validate(&info, ""));
    }

    // --- ClientAuth::UserPass ---

    #[test]
    fn userpass_match() {
        let auth = ClientAuth::UserPass {
            user: "admin".into(),
            pass: "password".into(),
        };
        let info = ConnectInfo {
            user: Some("admin".into()),
            pass: Some("password".into()),
            ..Default::default()
        };
        assert!(auth.validate(&info, ""));
    }

    #[test]
    fn userpass_wrong_pass() {
        let auth = ClientAuth::UserPass {
            user: "admin".into(),
            pass: "password".into(),
        };
        let info = ConnectInfo {
            user: Some("admin".into()),
            pass: Some("wrong".into()),
            ..Default::default()
        };
        assert!(!auth.validate(&info, ""));
    }

    #[test]
    fn userpass_missing_user() {
        let auth = ClientAuth::UserPass {
            user: "admin".into(),
            pass: "password".into(),
        };
        let info = ConnectInfo {
            pass: Some("password".into()),
            ..Default::default()
        };
        assert!(!auth.validate(&info, ""));
    }

    // --- ClientAuth::NKey ---

    #[test]
    fn nkey_valid_signature() {
        let kp = nkeys::KeyPair::new_user();
        let pub_key = kp.public_key();
        let nonce = "testnonce123";
        let sig = kp.sign(nonce.as_bytes()).unwrap();
        let sig_b64 = data_encoding::BASE64URL_NOPAD.encode(&sig);

        let auth = ClientAuth::NKey(vec![pub_key.clone()]);
        let info = ConnectInfo {
            nkey: Some(pub_key),
            signature: Some(sig_b64),
            ..Default::default()
        };
        assert!(auth.validate(&info, nonce));
    }

    #[test]
    fn nkey_wrong_key_rejected() {
        let kp = nkeys::KeyPair::new_user();
        let other_kp = nkeys::KeyPair::new_user();
        let nonce = "testnonce";
        let sig = kp.sign(nonce.as_bytes()).unwrap();
        let sig_b64 = data_encoding::BASE64URL_NOPAD.encode(&sig);

        // Allowlist has a different key
        let auth = ClientAuth::NKey(vec![other_kp.public_key()]);
        let info = ConnectInfo {
            nkey: Some(kp.public_key()),
            signature: Some(sig_b64),
            ..Default::default()
        };
        assert!(!auth.validate(&info, nonce));
    }

    #[test]
    fn nkey_bad_signature_rejected() {
        let kp = nkeys::KeyPair::new_user();
        let pub_key = kp.public_key();
        let auth = ClientAuth::NKey(vec![pub_key.clone()]);

        let info = ConnectInfo {
            nkey: Some(pub_key),
            signature: Some("badsig".into()),
            ..Default::default()
        };
        assert!(!auth.validate(&info, "nonce"));
    }

    #[test]
    fn nkey_missing_sig() {
        let kp = nkeys::KeyPair::new_user();
        let pub_key = kp.public_key();
        let auth = ClientAuth::NKey(vec![pub_key.clone()]);
        let info = ConnectInfo {
            nkey: Some(pub_key),
            ..Default::default()
        };
        assert!(!auth.validate(&info, "nonce"));
    }

    #[test]
    fn nkey_missing_key() {
        let auth = ClientAuth::NKey(vec!["UABC".into()]);
        let info = ConnectInfo::default();
        assert!(!auth.validate(&info, "nonce"));
    }

    // --- generate_nonce ---

    #[test]
    fn generate_nonce_length() {
        let nonce = generate_nonce();
        // 11 bytes base64url-no-pad = ceil(11*4/3) = 15 chars
        assert_eq!(nonce.len(), 15);
        // Should be valid base64url characters
        assert!(nonce
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }
}
