#[cfg(any(feature = "hub", feature = "cluster", feature = "gateway"))]
use std::collections::HashMap;
#[cfg(any(feature = "cluster", feature = "gateway"))]
use std::collections::HashSet;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
#[cfg(feature = "leaf")]
use std::sync::mpsc;
use std::sync::Arc;
#[cfg(any(feature = "cluster", feature = "gateway"))]
use std::sync::Mutex;
use std::time::Instant;

use metrics::counter;
use tracing::{error, info, warn};

use crate::types::{ConnectInfo, ServerInfo};

use crate::protocol::BufConfig;
#[cfg(any(feature = "hub", feature = "cluster", feature = "gateway"))]
use crate::sub_list::DirectWriter;
use crate::sub_list::SubList;
#[cfg(feature = "leaf")]
use crate::upstream::{Upstream, UpstreamCmd};
use crate::worker::{Worker, WorkerHandle};

/// Per-subject permission rule with allow/deny lists.
#[derive(Debug, Clone, Default)]
pub struct Permission {
    /// Subjects allowed (NATS wildcard patterns). Empty = allow all.
    pub allow: Vec<String>,
    /// Subjects denied (NATS wildcard patterns). Deny takes precedence.
    pub deny: Vec<String>,
}

impl Permission {
    /// Check whether the given subject is permitted.
    /// Deny takes precedence over allow. Empty allow list = allow all.
    pub fn is_allowed(&self, subject: &str) -> bool {
        if self
            .deny
            .iter()
            .any(|p| crate::sub_list::subject_matches(p, subject))
        {
            return false;
        }
        if self.allow.is_empty() {
            return true;
        }
        self.allow
            .iter()
            .any(|p| crate::sub_list::subject_matches(p, subject))
    }
}

/// Per-user publish/subscribe permissions.
#[derive(Debug, Clone, Default)]
pub struct Permissions {
    /// Publish permission rules.
    pub publish: Permission,
    /// Subscribe permission rules.
    pub subscribe: Permission,
}

/// A user entry with credentials and optional permissions.
#[derive(Debug, Clone)]
pub struct UserConfig {
    /// Username.
    pub user: String,
    /// Password.
    pub pass: String,
    /// Optional per-user permissions.
    pub permissions: Option<Permissions>,
}

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
    /// Multi-user with per-user credentials and optional permissions.
    Users(Vec<UserConfig>),
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

    /// Look up permissions for a successfully authenticated client.
    /// Returns `None` if the auth mode doesn't support per-user permissions
    /// or the user has no permissions configured.
    pub fn lookup_permissions(&self, info: &ConnectInfo) -> Option<Permissions> {
        match self {
            ClientAuth::Users(users) => {
                let u = info.user.as_deref()?;
                let p = info.pass.as_deref()?;
                users
                    .iter()
                    .find(|uc| uc.user == u && uc.pass == p)
                    .and_then(|uc| uc.permissions.clone())
            }
            _ => None,
        }
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
            ClientAuth::Users(users) => {
                let u = match info.user.as_deref() {
                    Some(u) => u,
                    None => return false,
                };
                let p = match info.pass.as_deref() {
                    Some(p) => p,
                    None => return false,
                };
                users.iter().any(|uc| uc.user == u && uc.pass == p)
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
#[cfg(feature = "leaf")]
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

/// Compute a 6-character base36 hash from a string using FNV-1a.
#[cfg(feature = "gateway")]
fn fnv_hash_base36(s: &str) -> String {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0100_0000_01b3;
    let mut h = FNV_OFFSET;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    // Convert to base36, take first 6 chars
    let mut buf = String::with_capacity(6);
    let mut val = h;
    for _ in 0..6 {
        let digit = (val % 36) as u8;
        let c = if digit < 10 {
            b'0' + digit
        } else {
            b'a' + digit - 10
        };
        buf.push(c as char);
        val /= 36;
    }
    buf
}

/// Configuration for the leaf node server.
#[derive(Debug, Clone)]
pub struct LeafServerConfig {
    /// Address to listen on (e.g., "0.0.0.0").
    pub host: String,
    /// Port to listen on.
    pub port: u16,
    /// Optional upstream hub URL (e.g., "nats://hub:4222").
    #[cfg(feature = "leaf")]
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
    /// Optional leafnode listen port. When set, open-wire acts as a hub and
    /// accepts inbound leaf node connections on this port.
    #[cfg(feature = "hub")]
    pub leafnode_port: Option<u16>,
    /// Maximum pending write bytes per connection before disconnecting as a
    /// slow consumer (default: 64 MB, matching Go nats-server). 0 = unlimited.
    pub max_pending: usize,
    /// Maximum message payload size in bytes (default: 1 MB, matching Go nats-server).
    /// Advertised in INFO and enforced on PUB/HPUB.
    pub max_payload: usize,
    /// Maximum simultaneous client connections (default: 65536). 0 = unlimited.
    pub max_connections: usize,
    /// Maximum control line length in bytes (default: 4096, matching Go nats-server).
    pub max_control_line: usize,
    /// Maximum subscriptions per client connection (default: 0 = unlimited).
    pub max_subscriptions: usize,
    /// Interval between server-initiated PING keepalives (default: 2 minutes).
    /// Set to `Duration::ZERO` to disable keepalive.
    pub ping_interval: std::time::Duration,
    /// Maximum outstanding PINGs before closing a connection (default: 2).
    pub max_pings_outstanding: u32,
    /// Client authentication configuration.
    pub client_auth: ClientAuth,
    /// Credentials for connecting to the upstream hub.
    #[cfg(feature = "leaf")]
    pub hub_credentials: Option<HubCredentials>,
    /// Port for Prometheus metrics HTTP endpoint. `None` = disabled.
    pub metrics_port: Option<u16>,
    /// Path to TLS certificate file (PEM). When set with `tls_key`, enables TLS.
    pub tls_cert: Option<PathBuf>,
    /// Path to TLS private key file (PEM).
    pub tls_key: Option<PathBuf>,
    /// Path to CA certificate file (PEM) for client certificate verification (mTLS).
    pub tls_ca_cert: Option<PathBuf>,
    /// When `true` (and `tls_ca_cert` is set), require and verify client certificates.
    pub tls_verify: bool,
    /// Path to write the server PID file. Removed on shutdown.
    pub pid_file: Option<PathBuf>,
    /// Path to the log file. When set, tracing output is directed here.
    pub log_file: Option<PathBuf>,
    /// Timeout for clients to send CONNECT after connection (default: 2s).
    /// Set to `Duration::ZERO` to disable. Only enforced when auth is required.
    pub auth_timeout: std::time::Duration,
    /// Duration of lame duck mode before shutdown (default: 30s).
    pub lame_duck_duration: std::time::Duration,
    /// Grace period before lame duck starts closing connections (default: 10s).
    pub lame_duck_grace_period: std::time::Duration,
    /// Port for the monitoring HTTP server (/varz, /healthz). `None` = disabled.
    pub monitoring_port: Option<u16>,
    /// Interest collapse templates for upstream leaf subscriptions.
    ///
    /// Each template is a NATS subject pattern (e.g., `"app.*.sessions.>"`).
    /// When a client subscribes to a subject matching a template, the leaf sends
    /// a single collapsed wildcard `LS+` to the hub instead of per-subject `LS+`.
    /// This reduces upstream interest propagation from O(N) to O(1) per template.
    #[cfg(all(feature = "leaf", feature = "interest-collapse"))]
    pub interest_collapse: Vec<String>,
    /// Subject mapping rules for upstream leaf subscriptions.
    ///
    /// Each mapping rewrites subjects matching the `from` pattern to the `to` pattern
    /// before sending upstream. Supports prefix mappings (`local.>` → `prod.>`) and
    /// exact mappings.
    #[cfg(all(feature = "leaf", feature = "subject-mapping"))]
    pub subject_mappings: Vec<crate::interest::SubjectMapping>,
    /// Port for cluster route connections. When set, the server listens for
    /// inbound route connections and participates in full mesh clustering.
    #[cfg(feature = "cluster")]
    pub cluster_port: Option<u16>,
    /// Seed route URLs for outbound connections (e.g., `["nats-route://host2:4248"]`).
    #[cfg(feature = "cluster")]
    pub cluster_seeds: Vec<String>,
    /// Cluster name. All nodes in a cluster must use the same name.
    #[cfg(feature = "cluster")]
    pub cluster_name: Option<String>,
    /// Port for gateway connections. When set, the server listens for
    /// inbound gateway connections from other clusters.
    #[cfg(feature = "gateway")]
    pub gateway_port: Option<u16>,
    /// This cluster's gateway name (required for gateway mode).
    #[cfg(feature = "gateway")]
    pub gateway_name: Option<String>,
    /// Remote clusters to connect to via gateways.
    #[cfg(feature = "gateway")]
    pub gateway_remotes: Vec<GatewayRemote>,
}

/// A remote cluster for gateway connections.
#[derive(Debug, Clone)]
pub struct GatewayRemote {
    /// Remote cluster name.
    pub name: String,
    /// Seed URLs for that cluster.
    pub urls: Vec<String>,
}

impl Default for LeafServerConfig {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            host: "0.0.0.0".to_string(),
            port: 4222,
            #[cfg(feature = "leaf")]
            hub_url: None,
            server_name: "open-wire".to_string(),
            max_read_buf_capacity: 65536,
            write_buf_capacity: 65536,
            workers,
            ws_port: None,
            #[cfg(feature = "hub")]
            leafnode_port: None,
            max_pending: 64 * 1024 * 1024,
            max_payload: 1_048_576,
            max_connections: 65_536,
            max_control_line: 4_096,
            max_subscriptions: 0,
            ping_interval: std::time::Duration::from_secs(120),
            max_pings_outstanding: 2,
            client_auth: ClientAuth::None,
            #[cfg(feature = "leaf")]
            hub_credentials: None,
            metrics_port: None,
            tls_cert: None,
            tls_key: None,
            tls_ca_cert: None,
            tls_verify: false,
            pid_file: None,
            log_file: None,
            auth_timeout: std::time::Duration::from_secs(2),
            lame_duck_duration: std::time::Duration::from_secs(30),
            lame_duck_grace_period: std::time::Duration::from_secs(10),
            monitoring_port: None,
            #[cfg(all(feature = "leaf", feature = "interest-collapse"))]
            interest_collapse: Vec::new(),
            #[cfg(all(feature = "leaf", feature = "subject-mapping"))]
            subject_mappings: Vec::new(),
            #[cfg(feature = "cluster")]
            cluster_port: None,
            #[cfg(feature = "cluster")]
            cluster_seeds: Vec::new(),
            #[cfg(feature = "cluster")]
            cluster_name: None,
            #[cfg(feature = "gateway")]
            gateway_port: None,
            #[cfg(feature = "gateway")]
            gateway_name: None,
            #[cfg(feature = "gateway")]
            gateway_remotes: Vec::new(),
        }
    }
}

/// Install the Prometheus metrics exporter on the given port.
///
/// Spawns a background HTTP listener thread serving `/metrics` in Prometheus
/// text format. Uses `metrics-exporter-prometheus` which is sync-compatible.
fn install_metrics_exporter(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder.with_http_listener(([0, 0, 0, 0], port)).install()?;
    Ok(())
}

/// Spawn a monitoring HTTP server on the given port.
/// Serves `/varz` (JSON stats) and `/healthz` (health check).
fn spawn_monitoring_server(port: u16, state: Arc<ServerState>) {
    std::thread::Builder::new()
        .name("monitoring".into())
        .spawn(move || {
            let listener = match TcpListener::bind(("0.0.0.0", port)) {
                Ok(l) => l,
                Err(e) => {
                    error!(port, error = %e, "failed to bind monitoring port");
                    return;
                }
            };
            info!(port, "monitoring endpoint listening");

            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        let _ = handle_monitoring_request(&mut stream, &state);
                    }
                    Err(e) => {
                        warn!(error = %e, "monitoring accept error");
                    }
                }
            }
        })
        .expect("failed to spawn monitoring thread");
}

/// Handle a single HTTP request on the monitoring port.
fn handle_monitoring_request(stream: &mut TcpStream, state: &ServerState) -> io::Result<()> {
    use std::io::{BufRead, BufReader, Write};

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    // Parse the path from "GET /path HTTP/1.x"
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");

    // Consume remaining headers
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            break;
        }
    }

    let (status, content_type, body) = match path {
        "/healthz" => (
            "200 OK",
            "application/json",
            r#"{"status":"ok"}"#.to_string(),
        ),
        "/varz" => {
            let uptime = state.stats.start_time.elapsed();
            let subs_count = {
                let subs = state.subs.read().unwrap();
                subs.unique_subjects().len()
            };
            let body = format!(
                concat!(
                    "{{",
                    "\"server_id\":\"{}\",",
                    "\"server_name\":\"{}\",",
                    "\"version\":\"{}\",",
                    "\"host\":\"{}\",",
                    "\"port\":{},",
                    "\"uptime_sec\":{},",
                    "\"connections\":{},",
                    "\"total_connections\":{},",
                    "\"in_msgs\":{},",
                    "\"out_msgs\":{},",
                    "\"in_bytes\":{},",
                    "\"out_bytes\":{},",
                    "\"slow_consumers\":{},",
                    "\"subscriptions\":{},",
                    "\"max_payload\":{},",
                    "\"max_connections\":{}",
                    "}}"
                ),
                state.info.server_id,
                state.info.server_name,
                state.info.version,
                state.info.host,
                state.info.port,
                uptime.as_secs(),
                state.active_connections.load(Ordering::Relaxed),
                state.stats.total_connections.load(Ordering::Relaxed),
                state.stats.in_msgs.load(Ordering::Relaxed),
                state.stats.out_msgs.load(Ordering::Relaxed),
                state.stats.in_bytes.load(Ordering::Relaxed),
                state.stats.out_bytes.load(Ordering::Relaxed),
                state.stats.slow_consumers.load(Ordering::Relaxed),
                subs_count,
                state.max_payload.load(Ordering::Relaxed),
                state.max_connections.load(Ordering::Relaxed),
            );
            ("200 OK", "application/json", body)
        }
        _ => ("404 Not Found", "text/plain", "404 Not Found\n".to_string()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

/// Build a rustls `ServerConfig` from PEM cert and key files.
///
/// When `ca_cert_path` is provided and `verify` is `true`, client certificates
/// are required and verified against the given CA (mutual TLS).
pub(crate) fn build_tls_server_config(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
    ca_cert_path: Option<&std::path::Path>,
    verify: bool,
) -> io::Result<Arc<rustls::ServerConfig>> {
    use rustls::server::WebPkiClientVerifier;
    use rustls_pemfile::{certs, private_key};

    let cert_file = &mut io::BufReader::new(std::fs::File::open(cert_path)?);
    let key_file = &mut io::BufReader::new(std::fs::File::open(key_path)?);

    let cert_chain: Vec<rustls_pki_types::CertificateDer<'static>> =
        certs(cert_file).collect::<Result<Vec<_>, _>>()?;
    let key = private_key(key_file)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no private key found"))?;

    let config = if verify {
        let ca_path = ca_cert_path.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "tls_verify requires tls_ca_cert",
            )
        })?;
        let ca_file = &mut io::BufReader::new(std::fs::File::open(ca_path)?);
        let mut root_store = rustls::RootCertStore::empty();
        for cert in certs(ca_file) {
            let cert = cert?;
            root_store
                .add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("CA cert: {e}")))?;
        }

        let verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("client verifier: {e}"))
            })?;

        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS config: {e}")))?
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS config: {e}")))?
    };

    Ok(Arc::new(config))
}

/// Build a rustls `ClientConfig` using system root certificates (webpki-roots).
/// Used for TLS connections to upstream hub servers.
pub(crate) fn build_tls_client_config() -> Arc<rustls::ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Arc::new(config)
}

/// Shared server state accessible by all client connections.
/// Aggregated server statistics for /varz monitoring.
pub(crate) struct ServerStats {
    pub in_msgs: AtomicU64,
    pub out_msgs: AtomicU64,
    pub in_bytes: AtomicU64,
    pub out_bytes: AtomicU64,
    pub total_connections: AtomicU64,
    pub slow_consumers: AtomicU64,
    pub start_time: Instant,
}

impl Default for ServerStats {
    fn default() -> Self {
        Self {
            in_msgs: AtomicU64::new(0),
            out_msgs: AtomicU64::new(0),
            in_bytes: AtomicU64::new(0),
            out_bytes: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            slow_consumers: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
}

/// Registry of connected route peers and known route URLs for gossip discovery.
#[cfg(feature = "cluster")]
pub(crate) struct RoutePeerRegistry {
    /// server_id → route address for all connected peers.
    pub connected: HashMap<String, String>,
    /// All known route URLs (own endpoint + all peers). Normalized via parse_route_url().
    pub known_urls: HashSet<String>,
}

/// Registry of connected gateway peers and known gateway URLs for gossip discovery.
#[cfg(feature = "gateway")]
pub(crate) struct GatewayPeerRegistry {
    /// cluster_name → set of conn_ids for that cluster.
    pub connected: HashMap<String, HashSet<u64>>,
    /// All known gateway URLs (own endpoint + discovered).
    pub known_urls: HashSet<String>,
}

/// Interest mode for an outbound gateway connection.
///
/// Gateways start in **Optimistic** mode (forward everything unless the remote
/// has signaled negative interest via RS-). After accumulating enough RS- signals,
/// the gateway transitions through **Transitioning** (send all current local RS+)
/// to **InterestOnly** (only forward when a matching RS+ sub exists in SubList).
#[cfg(feature = "gateway")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GatewayInterestMode {
    /// Forward messages to this gateway unless the subject is in the negative interest set.
    Optimistic,
    /// Sending all current local RS+ subs before switching to InterestOnly.
    Transitioning,
    /// Only forward messages when a matching gateway RS+ subscription exists in SubList.
    InterestOnly,
}

/// Per-outbound-gateway interest tracking state.
#[cfg(feature = "gateway")]
pub(crate) struct GatewayInterestState {
    /// Current interest mode for this outbound gateway.
    pub mode: GatewayInterestMode,
    /// Subjects the remote has signaled "no interest" for (Optimistic mode only).
    pub ni: HashSet<String>,
    /// Total RS- signals received from this gateway peer.
    pub ni_count: u64,
    /// DirectWriter for this outbound gateway (for optimistic forwarding).
    pub writer: DirectWriter,
}

/// Number of RS- signals before switching from Optimistic to InterestOnly mode.
#[cfg(feature = "gateway")]
pub(crate) const GATEWAY_MAX_NI_BEFORE_SWITCH: u64 = 1000;

pub(crate) struct ServerState {
    pub info: ServerInfo,
    pub auth: ClientAuth,
    pub ping_interval_ms: AtomicU64,
    pub auth_timeout_ms: AtomicU64,
    pub max_pings_outstanding: AtomicU32,
    pub subs: std::sync::RwLock<SubList>,
    #[cfg(feature = "leaf")]
    pub upstream: std::sync::RwLock<Option<Upstream>>,
    /// Lock-free sender for forwarding publishes to the upstream hub.
    /// Set once after upstream connects; read without locking on every publish.
    #[cfg(feature = "leaf")]
    pub upstream_tx: std::sync::RwLock<Option<mpsc::Sender<UpstreamCmd>>>,
    /// Lock-free flag: true when at least one subscription exists.
    /// Updated on subscribe/unsubscribe. Avoids taking subs lock on every publish
    /// just to check emptiness.
    pub has_subs: AtomicBool,
    pub buf_config: BufConfig,
    next_cid: AtomicU64,
    /// TLS configuration for client connections. `None` = plaintext.
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
    /// Global count of active client connections (across all workers).
    pub active_connections: AtomicU64,
    /// Maximum simultaneous client connections. 0 = unlimited.
    pub max_connections: AtomicUsize,
    /// Maximum message payload size in bytes.
    pub max_payload: AtomicUsize,
    /// Maximum control line length in bytes.
    pub max_control_line: AtomicUsize,
    /// Maximum subscriptions per client connection. 0 = unlimited.
    pub max_subscriptions: AtomicUsize,
    /// Aggregated server statistics.
    pub stats: ServerStats,
    /// Port advertised in INFO to inbound leaf connections (`leafnodes.listen` port).
    /// `None` when hub mode is not enabled.
    #[cfg(feature = "hub")]
    pub leafnode_port: Option<u16>,
    /// Registry of DirectWriters for inbound leaf connections.
    /// Used to propagate LS+/LS- when local clients subscribe/unsubscribe.
    #[cfg(feature = "hub")]
    pub leaf_writers: std::sync::RwLock<HashMap<u64, DirectWriter>>,
    /// Registry of DirectWriters for inbound route connections.
    /// Used to propagate RS+/RS- when local clients subscribe/unsubscribe.
    #[cfg(feature = "cluster")]
    pub route_writers: std::sync::RwLock<HashMap<u64, DirectWriter>>,
    /// Port for inbound route connections. `None` when cluster mode is not enabled.
    #[cfg(feature = "cluster")]
    pub cluster_port: Option<u16>,
    /// Cluster name. Must match between peers.
    #[cfg(feature = "cluster")]
    pub cluster_name: Option<String>,
    /// Seed route URLs for outbound connections.
    #[cfg(feature = "cluster")]
    pub cluster_seeds: Vec<String>,
    /// Registry of connected route peers and known route URLs.
    #[cfg(feature = "cluster")]
    pub route_peers: Mutex<RoutePeerRegistry>,
    /// Channel sender for the route coordinator thread. New gossip-discovered URLs
    /// are sent here to trigger outbound connections.
    #[cfg(feature = "cluster")]
    pub route_connect_tx: Mutex<Option<std::sync::mpsc::Sender<String>>>,
    /// Registry of DirectWriters for inbound gateway connections.
    #[cfg(feature = "gateway")]
    pub gateway_writers: std::sync::RwLock<HashMap<u64, DirectWriter>>,
    /// Port for inbound gateway connections.
    #[cfg(feature = "gateway")]
    pub gateway_port: Option<u16>,
    /// This cluster's gateway name.
    #[cfg(feature = "gateway")]
    pub gateway_name: Option<String>,
    /// Remote clusters to connect to via gateways.
    #[cfg(feature = "gateway")]
    pub gateway_remotes: Vec<GatewayRemote>,
    /// Registry of connected gateway peers and known gateway URLs.
    #[cfg(feature = "gateway")]
    pub gateway_peers: Mutex<GatewayPeerRegistry>,
    /// Channel sender for the gateway coordinator thread.
    #[cfg(feature = "gateway")]
    pub gateway_connect_tx: Mutex<Option<std::sync::mpsc::Sender<String>>>,
    /// Pre-computed `_GR_.<cluster_hash>.<server_hash>.` prefix for reply rewriting.
    #[cfg(feature = "gateway")]
    pub gateway_reply_prefix: Vec<u8>,
    /// Cached INFO JSON line for gateway handshake and gossip.
    /// Rebuilt when gateway URLs change.
    #[cfg(feature = "gateway")]
    pub cached_gateway_info: Mutex<String>,
    /// Per-outbound-gateway interest mode state (optimistic → interest-only transition).
    /// Keyed by outbound gateway conn_id. Used for optimistic forwarding and
    /// negative interest tracking.
    #[cfg(feature = "gateway")]
    pub gateway_interest: std::sync::RwLock<HashMap<u64, GatewayInterestState>>,
    /// Fast flag: true when any outbound gateway is in Optimistic mode.
    /// Prevents the can_skip PUB optimization from discarding messages
    /// that need to be forwarded across optimistic gateways.
    #[cfg(feature = "gateway")]
    pub has_gateway_interest: AtomicBool,
}

impl ServerState {
    #[allow(clippy::too_many_arguments)]
    fn new(
        info: ServerInfo,
        auth: ClientAuth,
        ping_interval: std::time::Duration,
        auth_timeout: std::time::Duration,
        max_pings_outstanding: u32,
        buf_config: BufConfig,
        tls_config: Option<Arc<rustls::ServerConfig>>,
        max_connections: usize,
        max_payload: usize,
        max_control_line: usize,
        max_subscriptions: usize,
        #[cfg(feature = "hub")] leafnode_port: Option<u16>,
        #[cfg(feature = "cluster")] cluster_port: Option<u16>,
        #[cfg(feature = "cluster")] cluster_name: Option<String>,
        #[cfg(feature = "cluster")] cluster_seeds: Vec<String>,
        #[cfg(feature = "gateway")] gateway_port: Option<u16>,
        #[cfg(feature = "gateway")] gateway_name: Option<String>,
        #[cfg(feature = "gateway")] gateway_remotes: Vec<GatewayRemote>,
    ) -> Self {
        #[cfg(feature = "cluster")]
        let cluster_self_host = if info.host.is_empty() || info.host == "0.0.0.0" {
            "127.0.0.1".to_string()
        } else {
            info.host.clone()
        };

        #[cfg(feature = "gateway")]
        let gateway_cluster_hash = {
            let name = gateway_name.as_deref().unwrap_or("default");
            fnv_hash_base36(name)
        };
        #[cfg(feature = "gateway")]
        let gateway_server_hash = fnv_hash_base36(&info.server_id);
        #[cfg(feature = "gateway")]
        let gateway_reply_prefix = {
            let mut prefix = Vec::with_capacity(
                5 + gateway_cluster_hash.len() + 1 + gateway_server_hash.len() + 1,
            );
            prefix.extend_from_slice(b"_GR_.");
            prefix.extend_from_slice(gateway_cluster_hash.as_bytes());
            prefix.push(b'.');
            prefix.extend_from_slice(gateway_server_hash.as_bytes());
            prefix.push(b'.');
            prefix
        };

        Self {
            info,
            auth,
            ping_interval_ms: AtomicU64::new(ping_interval.as_millis() as u64),
            auth_timeout_ms: AtomicU64::new(auth_timeout.as_millis() as u64),
            max_pings_outstanding: AtomicU32::new(max_pings_outstanding),
            subs: std::sync::RwLock::new(SubList::new()),
            #[cfg(feature = "leaf")]
            upstream: std::sync::RwLock::new(None),
            #[cfg(feature = "leaf")]
            upstream_tx: std::sync::RwLock::new(None),
            has_subs: AtomicBool::new(false),
            buf_config,
            next_cid: AtomicU64::new(1),
            tls_config,
            active_connections: AtomicU64::new(0),
            max_connections: AtomicUsize::new(max_connections),
            max_payload: AtomicUsize::new(max_payload),
            max_control_line: AtomicUsize::new(max_control_line),
            max_subscriptions: AtomicUsize::new(max_subscriptions),
            stats: ServerStats::default(),
            #[cfg(feature = "hub")]
            leafnode_port,
            #[cfg(feature = "hub")]
            leaf_writers: std::sync::RwLock::new(HashMap::new()),
            #[cfg(feature = "cluster")]
            route_writers: std::sync::RwLock::new(HashMap::new()),
            #[cfg(feature = "cluster")]
            cluster_port,
            #[cfg(feature = "cluster")]
            cluster_name,
            #[cfg(feature = "cluster")]
            route_peers: {
                let mut known_urls = HashSet::new();
                // Add own cluster endpoint (use configured host or 127.0.0.1;
                // never 0.0.0.0 which isn't a routable address for peers).
                if let Some(cp) = cluster_port {
                    known_urls.insert(format!("{}:{cp}", cluster_self_host));
                }
                // Add all configured seeds
                for seed in &cluster_seeds {
                    known_urls.insert(crate::route_conn::normalize_route_url(seed));
                }
                Mutex::new(RoutePeerRegistry {
                    connected: HashMap::new(),
                    known_urls,
                })
            },
            #[cfg(feature = "cluster")]
            route_connect_tx: Mutex::new(None),
            #[cfg(feature = "cluster")]
            cluster_seeds,
            #[cfg(feature = "gateway")]
            gateway_writers: std::sync::RwLock::new(HashMap::new()),
            #[cfg(feature = "gateway")]
            gateway_port,
            #[cfg(feature = "gateway")]
            gateway_name: gateway_name.clone(),
            #[cfg(feature = "gateway")]
            gateway_remotes,
            #[cfg(feature = "gateway")]
            gateway_peers: Mutex::new(GatewayPeerRegistry {
                connected: HashMap::new(),
                known_urls: HashSet::new(),
            }),
            #[cfg(feature = "gateway")]
            gateway_connect_tx: Mutex::new(None),
            #[cfg(feature = "gateway")]
            gateway_reply_prefix,
            #[cfg(feature = "gateway")]
            cached_gateway_info: Mutex::new(String::new()),
            #[cfg(feature = "gateway")]
            gateway_interest: std::sync::RwLock::new(HashMap::new()),
            #[cfg(feature = "gateway")]
            has_gateway_interest: AtomicBool::new(false),
        }
    }

    pub(crate) fn next_client_id(&self) -> u64 {
        self.next_cid.fetch_add(1, Ordering::Relaxed)
    }
}

/// An open-wire NATS-compatible message relay server.
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

        let tls_config = match (&config.tls_cert, &config.tls_key) {
            (Some(cert), Some(key)) => Some(
                build_tls_server_config(
                    cert,
                    key,
                    config.tls_ca_cert.as_deref(),
                    config.tls_verify,
                )
                .expect("failed to load TLS cert/key"),
            ),
            _ => None,
        };

        let info = ServerInfo {
            server_id: format!("LEAF_{}", rand::random::<u32>()),
            server_name: config.server_name.clone(),
            version: "0.5.0".to_string(),
            proto: 1,
            max_payload: config.max_payload,
            headers: true,
            host: config.host.clone(),
            port: config.port,
            auth_required: config.client_auth.is_required(),
            tls_required: tls_config.is_some(),
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
            config: config.clone(),
            state: Arc::new(ServerState::new(
                info,
                auth,
                config.ping_interval,
                config.auth_timeout,
                config.max_pings_outstanding,
                buf_config,
                tls_config,
                config.max_connections,
                config.max_payload,
                config.max_control_line,
                config.max_subscriptions,
                #[cfg(feature = "hub")]
                config.leafnode_port,
                #[cfg(feature = "cluster")]
                config.cluster_port,
                #[cfg(feature = "cluster")]
                config.cluster_name.clone(),
                #[cfg(feature = "cluster")]
                config.cluster_seeds.clone(),
                #[cfg(feature = "gateway")]
                config.gateway_port,
                #[cfg(feature = "gateway")]
                config.gateway_name.clone(),
                #[cfg(feature = "gateway")]
                config.gateway_remotes.clone(),
            )),
        }
    }

    /// Connect to the upstream hub if configured, using the leaf node protocol.
    /// Uses a supervisor pattern: initial failure is non-fatal, the supervisor
    /// will keep retrying with exponential backoff.
    #[cfg(feature = "leaf")]
    fn connect_upstream(&self) {
        if let Some(ref hub_url) = self.config.hub_url {
            info!(url = %hub_url, "connecting to upstream hub (leaf protocol)");
            #[cfg(feature = "interest-collapse")]
            let collapse_templates = self.config.interest_collapse.clone();
            #[cfg(feature = "subject-mapping")]
            let subject_mappings = self.config.subject_mappings.clone();
            let build_pipeline = move || {
                crate::interest::InterestPipeline::new(
                    #[cfg(feature = "interest-collapse")]
                    collapse_templates.clone(),
                    #[cfg(feature = "subject-mapping")]
                    subject_mappings.clone(),
                )
            };
            // Try initial connect synchronously for fast startup
            match Upstream::connect(
                hub_url,
                self.config.hub_credentials.as_ref(),
                Arc::clone(&self.state),
                build_pipeline(),
            ) {
                Ok(upstream) => {
                    let sender = upstream.sender();
                    *self.state.upstream.write().unwrap() = Some(upstream);
                    *self.state.upstream_tx.write().unwrap() = Some(sender);
                    info!("connected to upstream hub");
                }
                Err(e) => {
                    // Initial connect failed — spawn supervisor to keep retrying
                    warn!(error = %e, "initial hub connection failed, will retry in background");
                    let upstream = Upstream::spawn_supervisor(
                        hub_url.clone(),
                        self.config.hub_credentials.clone(),
                        Arc::clone(&self.state),
                        build_pipeline,
                    );
                    let sender = upstream.sender();
                    *self.state.upstream.write().unwrap() = Some(upstream);
                    *self.state.upstream_tx.write().unwrap() = Some(sender);
                }
            }
        }
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
        // Enforce max_connections limit.
        let max = self.state.max_connections.load(Ordering::Relaxed);
        if max > 0 {
            let current = self.state.active_connections.load(Ordering::Relaxed);
            if current >= max as u64 {
                warn!(addr = %addr, max_connections = max, "maximum connections exceeded, rejecting");
                counter!("connections_rejected_total").increment(1);
                drop(tcp_stream);
                return;
            }
        }
        self.state
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .stats
            .total_connections
            .fetch_add(1, Ordering::Relaxed);

        let cid = self.state.next_client_id();
        let idx = *next_worker % workers.len();
        *next_worker = idx + 1;
        workers[idx].send_conn(cid, tcp_stream, addr, is_websocket);
    }

    /// Distribute a new inbound leaf TCP connection to the next worker (round-robin).
    #[cfg(feature = "hub")]
    fn accept_leaf_tcp(
        &self,
        tcp_stream: TcpStream,
        addr: std::net::SocketAddr,
        workers: &[WorkerHandle],
        next_worker: &mut usize,
    ) {
        self.state
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        let cid = self.state.next_client_id();
        let idx = *next_worker % workers.len();
        *next_worker = idx + 1;
        workers[idx].send_leaf_conn(cid, tcp_stream, addr);
    }

    /// Distribute a new inbound route TCP connection to the next worker (round-robin).
    #[cfg(feature = "cluster")]
    fn accept_route_tcp(
        &self,
        tcp_stream: TcpStream,
        addr: std::net::SocketAddr,
        workers: &[WorkerHandle],
        next_worker: &mut usize,
    ) {
        self.state
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        let cid = self.state.next_client_id();
        let idx = *next_worker % workers.len();
        *next_worker = idx + 1;
        workers[idx].send_route_conn(cid, tcp_stream, addr);
    }

    #[cfg(feature = "gateway")]
    fn accept_gateway_tcp(
        &self,
        tcp_stream: TcpStream,
        addr: std::net::SocketAddr,
        workers: &[WorkerHandle],
        next_worker: &mut usize,
    ) {
        self.state
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        let cid = self.state.next_client_id();
        let idx = *next_worker % workers.len();
        *next_worker = idx + 1;
        workers[idx].send_gateway_conn(cid, tcp_stream, addr);
    }

    /// Run the leaf server. Listens for connections and optionally
    /// connects to the upstream hub. Blocks forever.
    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(port) = self.config.metrics_port {
            install_metrics_exporter(port)?;
            info!(port, "prometheus metrics endpoint listening");
        }

        if let Some(port) = self.config.monitoring_port {
            spawn_monitoring_server(port, Arc::clone(&self.state));
        }

        #[cfg(feature = "leaf")]
        self.connect_upstream();

        // Spawn outbound route connections to seed peers
        #[cfg(feature = "cluster")]
        let _route_mgr = if !self.state.cluster_seeds.is_empty() {
            info!(seeds = ?self.state.cluster_seeds, "connecting to route peers");
            Some(crate::route_conn::RouteConnManager::spawn(Arc::clone(
                &self.state,
            )))
        } else {
            None
        };

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

        #[cfg(feature = "hub")]
        let leaf_listener = if let Some(leaf_port) = self.config.leafnode_port {
            let leaf_addr = format!("{}:{}", self.config.host, leaf_port);
            let ll = TcpListener::bind(&leaf_addr)?;
            info!(addr = %leaf_addr, "leaf server listening (leafnode)");
            Some(ll)
        } else {
            None
        };
        #[cfg(not(feature = "hub"))]
        let leaf_listener: Option<TcpListener> = None;

        #[cfg(feature = "cluster")]
        let cluster_listener = if let Some(cluster_port) = self.config.cluster_port {
            let cluster_addr = format!("{}:{}", self.config.host, cluster_port);
            let cl = TcpListener::bind(&cluster_addr)?;
            info!(addr = %cluster_addr, "leaf server listening (cluster route)");
            Some(cl)
        } else {
            None
        };
        #[cfg(not(feature = "cluster"))]
        let cluster_listener: Option<TcpListener> = None;

        #[cfg(feature = "gateway")]
        let gateway_listener = if let Some(gw_port) = self.config.gateway_port {
            let gw_addr = format!("{}:{}", self.config.host, gw_port);
            let gl = TcpListener::bind(&gw_addr)?;
            info!(addr = %gw_addr, "leaf server listening (gateway)");
            Some(gl)
        } else {
            None
        };
        #[cfg(not(feature = "gateway"))]
        let gateway_listener: Option<TcpListener> = None;

        // Spawn outbound gateway connections to remote clusters
        #[cfg(feature = "gateway")]
        let _gateway_mgr = if !self.state.gateway_remotes.is_empty() {
            info!(remotes = ?self.state.gateway_remotes.iter().map(|r| &r.name).collect::<Vec<_>>(), "connecting to gateway peers");
            Some(crate::gateway_conn::GatewayConnManager::spawn(Arc::clone(
                &self.state,
            )))
        } else {
            None
        };

        let has_extra_listeners = ws_listener.is_some()
            || leaf_listener.is_some()
            || cluster_listener.is_some()
            || gateway_listener.is_some();
        if has_extra_listeners {
            // Poll multiple listeners
            listener.set_nonblocking(true)?;
            if let Some(ref wl) = ws_listener {
                wl.set_nonblocking(true)?;
            }
            if let Some(ref ll) = leaf_listener {
                ll.set_nonblocking(true)?;
            }
            if let Some(ref cl) = cluster_listener {
                cl.set_nonblocking(true)?;
            }
            if let Some(ref gl) = gateway_listener {
                gl.set_nonblocking(true)?;
            }

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
                libc::pollfd {
                    fd: leaf_listener.as_ref().map(|l| l.as_raw_fd()).unwrap_or(-1),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: cluster_listener
                        .as_ref()
                        .map(|l| l.as_raw_fd())
                        .unwrap_or(-1),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: gateway_listener
                        .as_ref()
                        .map(|l| l.as_raw_fd())
                        .unwrap_or(-1),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            loop {
                pfds[0].revents = 0;
                pfds[1].revents = 0;
                pfds[2].revents = 0;
                pfds[3].revents = 0;
                pfds[4].revents = 0;
                let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 5, -1) };
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
                    if let Some(ref wl) = ws_listener {
                        while let Ok((stream, addr)) = wl.accept() {
                            self.accept_tcp(stream, addr, &workers, &mut next_worker, true);
                        }
                    }
                }
                #[cfg(feature = "hub")]
                if pfds[2].revents & libc::POLLIN != 0 {
                    if let Some(ref ll) = leaf_listener {
                        while let Ok((stream, addr)) = ll.accept() {
                            self.accept_leaf_tcp(stream, addr, &workers, &mut next_worker);
                        }
                    }
                }
                #[cfg(feature = "cluster")]
                if pfds[3].revents & libc::POLLIN != 0 {
                    if let Some(ref cl) = cluster_listener {
                        while let Ok((stream, addr)) = cl.accept() {
                            self.accept_route_tcp(stream, addr, &workers, &mut next_worker);
                        }
                    }
                }
                #[cfg(feature = "gateway")]
                if pfds[4].revents & libc::POLLIN != 0 {
                    if let Some(ref gl) = gateway_listener {
                        while let Ok((stream, addr)) = gl.accept() {
                            self.accept_gateway_tcp(stream, addr, &workers, &mut next_worker);
                        }
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
    ///
    /// If `reload` is set (via SIGHUP), the server re-reads the config file and
    /// updates hot-reloadable values (max_payload, max_connections, etc.).
    pub fn run_until_shutdown(
        &self,
        shutdown: Arc<AtomicBool>,
        reload: Arc<AtomicBool>,
        config_path: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(port) = self.config.metrics_port {
            install_metrics_exporter(port)?;
            info!(port, "prometheus metrics endpoint listening");
        }

        if let Some(port) = self.config.monitoring_port {
            spawn_monitoring_server(port, Arc::clone(&self.state));
        }

        #[cfg(feature = "leaf")]
        self.connect_upstream();

        // Spawn outbound route connections to seed peers
        #[cfg(feature = "cluster")]
        let _route_mgr = if !self.state.cluster_seeds.is_empty() {
            info!(seeds = ?self.state.cluster_seeds, "connecting to route peers");
            Some(crate::route_conn::RouteConnManager::spawn(Arc::clone(
                &self.state,
            )))
        } else {
            None
        };

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

        #[cfg(feature = "hub")]
        let leaf_listener = if let Some(leaf_port) = self.config.leafnode_port {
            let leaf_addr = format!("{}:{}", self.config.host, leaf_port);
            let ll = TcpListener::bind(&leaf_addr)?;
            ll.set_nonblocking(true)?;
            info!(addr = %leaf_addr, "leaf server listening (leafnode)");
            Some(ll)
        } else {
            None
        };
        #[cfg(not(feature = "hub"))]
        let leaf_listener: Option<TcpListener> = None;

        #[cfg(feature = "cluster")]
        let cluster_listener = if let Some(cluster_port) = self.config.cluster_port {
            let cluster_addr = format!("{}:{}", self.config.host, cluster_port);
            let cl = TcpListener::bind(&cluster_addr)?;
            cl.set_nonblocking(true)?;
            info!(addr = %cluster_addr, "leaf server listening (cluster route)");
            Some(cl)
        } else {
            None
        };
        #[cfg(not(feature = "cluster"))]
        let cluster_listener: Option<TcpListener> = None;

        #[cfg(feature = "gateway")]
        let gateway_listener = if let Some(gw_port) = self.config.gateway_port {
            let gw_addr = format!("{}:{}", self.config.host, gw_port);
            let gl = TcpListener::bind(&gw_addr)?;
            gl.set_nonblocking(true)?;
            info!(addr = %gw_addr, "leaf server listening (gateway)");
            Some(gl)
        } else {
            None
        };
        #[cfg(not(feature = "gateway"))]
        let gateway_listener: Option<TcpListener> = None;

        // Spawn outbound gateway connections to remote clusters
        #[cfg(feature = "gateway")]
        let _gateway_mgr = if !self.state.gateway_remotes.is_empty() {
            info!(remotes = ?self.state.gateway_remotes.iter().map(|r| &r.name).collect::<Vec<_>>(), "connecting to gateway peers");
            Some(crate::gateway_conn::GatewayConnManager::spawn(Arc::clone(
                &self.state,
            )))
        } else {
            None
        };

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
            libc::pollfd {
                fd: leaf_listener.as_ref().map(|l| l.as_raw_fd()).unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: cluster_listener
                    .as_ref()
                    .map(|l| l.as_raw_fd())
                    .unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: gateway_listener
                    .as_ref()
                    .map(|l| l.as_raw_fd())
                    .unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];

        loop {
            if shutdown.load(Ordering::Relaxed) {
                info!("shutting down leaf server");
                break;
            }

            // Check for config reload (SIGHUP)
            if reload.load(Ordering::Relaxed) {
                reload.store(false, Ordering::Relaxed);
                if let Some(path) = config_path {
                    self.reload_config(path);
                } else {
                    warn!("SIGHUP received but no config file specified");
                }
            }

            pfds[0].revents = 0;
            pfds[1].revents = 0;
            pfds[2].revents = 0;
            pfds[3].revents = 0;
            pfds[4].revents = 0;
            let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 5, 1000) };

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

            #[cfg(feature = "hub")]
            if ret > 0 && pfds[2].revents & libc::POLLIN != 0 {
                if let Some(ref ll) = leaf_listener {
                    while let Ok((tcp_stream, addr)) = ll.accept() {
                        self.accept_leaf_tcp(tcp_stream, addr, &workers, &mut next_worker);
                    }
                }
            }

            #[cfg(feature = "cluster")]
            if ret > 0 && pfds[3].revents & libc::POLLIN != 0 {
                if let Some(ref cl) = cluster_listener {
                    while let Ok((stream, addr)) = cl.accept() {
                        self.accept_route_tcp(stream, addr, &workers, &mut next_worker);
                    }
                }
            }

            #[cfg(feature = "gateway")]
            if ret > 0 && pfds[4].revents & libc::POLLIN != 0 {
                if let Some(ref gl) = gateway_listener {
                    while let Ok((stream, addr)) = gl.accept() {
                        self.accept_gateway_tcp(stream, addr, &workers, &mut next_worker);
                    }
                }
            }
        }

        // --- Lame duck mode shutdown ---
        // 1. Build INFO with ldm: true and send to all workers
        let mut ldm_info = self.state.info.clone();
        ldm_info.lame_duck_mode = true;
        let ldm_json = serde_json::to_string(&ldm_info).unwrap_or_default();
        let ldm_line = format!("INFO {ldm_json}\r\n").into_bytes();

        info!(
            duration_secs = self.config.lame_duck_duration.as_secs(),
            "entering lame duck mode"
        );
        for w in &workers {
            w.send_lame_duck(ldm_line.clone());
        }

        // 2. Sleep for lame_duck_duration (stop accepting)
        let end = std::time::Instant::now() + self.config.lame_duck_duration;
        while std::time::Instant::now() < end {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // 3. Drain all connections (flush pending, remove subs, disconnect)
        info!("draining connections");
        for w in &workers {
            w.send_drain();
        }
        // Give workers time to flush
        std::thread::sleep(std::time::Duration::from_secs(1));

        // 4. Shutdown workers
        for w in &workers {
            w.shutdown();
        }
        for w in workers {
            w.join();
        }

        // Cleanup: clear all subscriptions.
        {
            let mut subs = self.state.subs.write().unwrap();
            *subs = SubList::new();
        }

        // Drop upstream
        #[cfg(feature = "leaf")]
        {
            *self.state.upstream_tx.write().unwrap() = None;
            let mut upstream = self.state.upstream.write().unwrap();
            *upstream = None;
        }

        Ok(())
    }

    /// Reload configuration from file. Updates hot-reloadable values.
    fn reload_config(&self, path: &str) {
        info!(path, "reloading configuration");
        match crate::config::load_config(std::path::Path::new(path)) {
            Ok(new_config) => {
                // Hot-reload numeric limits
                self.state
                    .max_payload
                    .store(new_config.max_payload, Ordering::Relaxed);
                self.state
                    .max_connections
                    .store(new_config.max_connections, Ordering::Relaxed);
                self.state
                    .max_control_line
                    .store(new_config.max_control_line, Ordering::Relaxed);
                self.state
                    .max_subscriptions
                    .store(new_config.max_subscriptions, Ordering::Relaxed);
                self.state
                    .max_pings_outstanding
                    .store(new_config.max_pings_outstanding, Ordering::Relaxed);
                self.state.ping_interval_ms.store(
                    new_config.ping_interval.as_millis() as u64,
                    Ordering::Relaxed,
                );
                self.state.auth_timeout_ms.store(
                    new_config.auth_timeout.as_millis() as u64,
                    Ordering::Relaxed,
                );

                // Warn about non-reloadable fields
                if new_config.host != self.config.host {
                    warn!("host change requires restart (ignored)");
                }
                if new_config.port != self.config.port {
                    warn!("port change requires restart (ignored)");
                }
                if new_config.workers != self.config.workers {
                    warn!("workers change requires restart (ignored)");
                }
                if new_config.tls_cert != self.config.tls_cert
                    || new_config.tls_key != self.config.tls_key
                    || new_config.tls_ca_cert != self.config.tls_ca_cert
                    || new_config.tls_verify != self.config.tls_verify
                {
                    warn!("TLS config change requires restart (ignored)");
                }

                info!("configuration reloaded successfully");
            }
            Err(e) => {
                error!(error = %e, "failed to reload configuration");
            }
        }
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

    // --- install_metrics_exporter ---

    #[test]
    fn install_metrics_exporter_does_not_panic() {
        // Port 0 lets the OS pick an available port. install() sets a global
        // recorder, so this can only succeed once per process. If another test
        // has already installed a recorder, install() returns Err — that's fine.
        let _ = install_metrics_exporter(0);
    }

    // --- metrics_port config ---

    #[test]
    fn default_metrics_port_is_none() {
        let config = LeafServerConfig::default();
        assert!(config.metrics_port.is_none());
    }

    // --- ClientAuth::Users ---

    #[test]
    fn users_match() {
        let auth = ClientAuth::Users(vec![
            UserConfig {
                user: "alice".into(),
                pass: "pass1".into(),
                permissions: None,
            },
            UserConfig {
                user: "bob".into(),
                pass: "pass2".into(),
                permissions: None,
            },
        ]);
        let info = ConnectInfo {
            user: Some("bob".into()),
            pass: Some("pass2".into()),
            ..Default::default()
        };
        assert!(auth.validate(&info, ""));
    }

    #[test]
    fn users_wrong_pass() {
        let auth = ClientAuth::Users(vec![UserConfig {
            user: "alice".into(),
            pass: "pass1".into(),
            permissions: None,
        }]);
        let info = ConnectInfo {
            user: Some("alice".into()),
            pass: Some("wrong".into()),
            ..Default::default()
        };
        assert!(!auth.validate(&info, ""));
    }

    #[test]
    fn users_unknown_user() {
        let auth = ClientAuth::Users(vec![UserConfig {
            user: "alice".into(),
            pass: "pass1".into(),
            permissions: None,
        }]);
        let info = ConnectInfo {
            user: Some("unknown".into()),
            pass: Some("pass1".into()),
            ..Default::default()
        };
        assert!(!auth.validate(&info, ""));
    }

    #[test]
    fn users_is_required() {
        let auth = ClientAuth::Users(vec![]);
        assert!(auth.is_required());
        assert!(!auth.needs_nonce());
    }

    // --- Permission ---

    #[test]
    fn permission_allow_all_by_default() {
        let perm = Permission::default();
        assert!(perm.is_allowed("foo.bar"));
    }

    #[test]
    fn permission_allow_list() {
        let perm = Permission {
            allow: vec!["foo.>".into()],
            deny: Vec::new(),
        };
        assert!(perm.is_allowed("foo.bar"));
        assert!(!perm.is_allowed("bar.baz"));
    }

    #[test]
    fn permission_deny_takes_precedence() {
        let perm = Permission {
            allow: vec![">".into()],
            deny: vec!["secret.>".into()],
        };
        assert!(perm.is_allowed("foo.bar"));
        assert!(!perm.is_allowed("secret.data"));
    }

    #[test]
    fn permission_deny_with_empty_allow() {
        let perm = Permission {
            allow: Vec::new(),
            deny: vec!["_SYS.>".into()],
        };
        assert!(perm.is_allowed("foo.bar"));
        assert!(!perm.is_allowed("_SYS.monitor"));
    }

    // --- lookup_permissions ---

    #[test]
    fn lookup_permissions_found() {
        let auth = ClientAuth::Users(vec![UserConfig {
            user: "alice".into(),
            pass: "pass".into(),
            permissions: Some(Permissions {
                publish: Permission {
                    allow: vec!["pub.>".into()],
                    deny: Vec::new(),
                },
                subscribe: Permission::default(),
            }),
        }]);
        let info = ConnectInfo {
            user: Some("alice".into()),
            pass: Some("pass".into()),
            ..Default::default()
        };
        let perms = auth.lookup_permissions(&info);
        assert!(perms.is_some());
        assert_eq!(perms.unwrap().publish.allow, vec!["pub.>"]);
    }

    #[test]
    fn lookup_permissions_none_for_non_users_auth() {
        let auth = ClientAuth::Token("t".into());
        let info = ConnectInfo::default();
        assert!(auth.lookup_permissions(&info).is_none());
    }

    // --- auth_timeout default ---

    #[test]
    fn default_auth_timeout() {
        let config = LeafServerConfig::default();
        assert_eq!(config.auth_timeout, std::time::Duration::from_secs(2));
    }
}
