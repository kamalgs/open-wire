// open-wire — NATS-compatible message relay
//
// Usage:
//   cargo run -- --port 4222
//   cargo run -- --port 4222 --hub nats://localhost:7422
//   cargo run -- --workers 8
//   cargo run -- --read-buf-max 32768 --write-buf-size 32768
//   cargo run -- --ws-port 8222

use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;

use tracing_subscriber::EnvFilter;

use open_wire::config;
#[cfg(feature = "leaf")]
use open_wire::HubCredentials;
use open_wire::{ClientAuth, LeafServer, LeafServerConfig};

/// Global pointer to the reload flag, accessible from the SIGHUP handler.
static RELOAD_PTR: AtomicPtr<AtomicBool> = AtomicPtr::new(ptr::null_mut());

/// Global pointer to the shutdown flag, accessible from the signal handler.
static SHUTDOWN_PTR: AtomicPtr<AtomicBool> = AtomicPtr::new(ptr::null_mut());

extern "C" fn handle_shutdown(_sig: libc::c_int) {
    let ptr = SHUTDOWN_PTR.load(Ordering::Relaxed);
    if !ptr.is_null() {
        unsafe { &*ptr }.store(true, Ordering::Release);
    }
}

extern "C" fn handle_reload(_sig: libc::c_int) {
    let ptr = RELOAD_PTR.load(Ordering::Relaxed);
    if !ptr.is_null() {
        unsafe { &*ptr }.store(true, Ordering::Release);
    }
}

/// Install a signal handler for the given signal using `sigaction` with `SA_RESTART`.
fn install_signal_handler(sig: libc::c_int, handler: extern "C" fn(libc::c_int)) {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handler as *const () as usize;
        sa.sa_flags = libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask);
        libc::sigaction(sig, &sa, ptr::null_mut());
    }
}

/// Set up graceful shutdown and hot-reload via signals.
fn setup_signals() -> (Arc<AtomicBool>, Arc<AtomicBool>) {
    let shutdown = Arc::new(AtomicBool::new(false));
    SHUTDOWN_PTR.store(Arc::as_ptr(&shutdown) as *mut AtomicBool, Ordering::Release);

    let reload = Arc::new(AtomicBool::new(false));
    RELOAD_PTR.store(Arc::as_ptr(&reload) as *mut AtomicBool, Ordering::Release);

    install_signal_handler(libc::SIGTERM, handle_shutdown);
    install_signal_handler(libc::SIGINT, handle_shutdown);
    install_signal_handler(libc::SIGHUP, handle_reload);

    (shutdown, reload)
}

const USAGE: &str = "\
Usage: open-wire [--config FILE] [--port PORT] [--host HOST] \
[--hub URL] [--name NAME] \
[--read-buf-max BYTES] [--write-buf-size BYTES] [--workers N] \
[--ws-port PORT] [--token TOKEN] [--user USER] [--pass PASS] \
[--nkey PUBKEY] [--hub-user USER] [--hub-pass PASS] \
[--hub-token TOKEN] [--hub-creds PATH] \
[--metrics-port PORT] [--tls-cert PATH] [--tls-key PATH] \
[--tls-ca-cert PATH] [--tls-verify] \
[--max-payload BYTES] [--max-connections N] \
[--max-control-line BYTES] [--max-subscriptions N] \
[--pid-file PATH] [--log-file PATH] [--monitoring-port PORT] \
[--auth-timeout SECS]";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = pico_args::Arguments::from_env();

    // Load config file as base (if provided)
    let cfg_path: Option<String> = args.opt_value_from_str(["-c", "--config"])?;
    let mut config = if let Some(ref path) = cfg_path {
        config::load_config(std::path::Path::new(path))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?
    } else {
        LeafServerConfig::default()
    };

    // Override config with CLI flags
    if let Some(v) = args.opt_value_from_str(["-p", "--port"])? {
        config.port = v;
    }
    if let Some(v) = args.opt_value_from_str(["-h", "--host"])? {
        config.host = v;
    }
    if let Some(v) = args.opt_value_from_str(["-w", "--workers"])? {
        config.workers = v;
    }
    if let Some(v) = args.opt_value_from_str("--name")? {
        config.server_name = v;
    }
    if let Some(v) = args.opt_value_from_str("--ws-port")? {
        config.ws_port = Some(v);
    }
    if let Some(v) = args.opt_value_from_str("--read-buf-max")? {
        config.max_read_buf_capacity = v;
    }
    if let Some(v) = args.opt_value_from_str("--write-buf-size")? {
        config.write_buf_capacity = v;
    }
    if let Some(v) = args.opt_value_from_str("--metrics-port")? {
        config.metrics_port = Some(v);
    }
    if let Some(v) = args.opt_value_from_str("--max-payload")? {
        config.max_payload = v;
    }
    if let Some(v) = args.opt_value_from_str("--max-connections")? {
        config.max_connections = v;
    }
    if let Some(v) = args.opt_value_from_str("--max-control-line")? {
        config.max_control_line = v;
    }
    if let Some(v) = args.opt_value_from_str("--max-subscriptions")? {
        config.max_subscriptions = v;
    }
    if let Some(v) = args.opt_value_from_str::<_, String>("--tls-cert")? {
        config.tls_cert = Some(std::path::PathBuf::from(v));
    }
    if let Some(v) = args.opt_value_from_str::<_, String>("--tls-key")? {
        config.tls_key = Some(std::path::PathBuf::from(v));
    }
    if let Some(v) = args.opt_value_from_str::<_, String>("--tls-ca-cert")? {
        config.tls_ca_cert = Some(std::path::PathBuf::from(v));
    }
    if args.contains("--tls-verify") {
        config.tls_verify = true;
    }
    if let Some(v) = args.opt_value_from_str::<_, String>("--pid-file")? {
        config.pid_file = Some(std::path::PathBuf::from(v));
    }
    if let Some(v) = args.opt_value_from_str::<_, String>("--log-file")? {
        config.log_file = Some(std::path::PathBuf::from(v));
    }
    if let Some(v) = args.opt_value_from_str(["--monitoring-port", "--http-port"])? {
        config.monitoring_port = Some(v);
    }
    if let Some(secs) = args.opt_value_from_str::<_, u64>("--auth-timeout")? {
        config.auth_timeout = std::time::Duration::from_secs(secs);
    }

    // Auth flags
    let auth_token: Option<String> = args.opt_value_from_str("--token")?;
    let auth_user: Option<String> = args.opt_value_from_str("--user")?;
    let auth_pass: Option<String> = args.opt_value_from_str("--pass")?;
    let mut auth_nkeys: Vec<String> = Vec::new();
    while let Some(v) = args.opt_value_from_str::<_, String>("--nkey")? {
        auth_nkeys.push(v);
    }

    // Hub credential flags
    #[cfg(feature = "leaf")]
    let mut hub_creds = HubCredentials::default();
    #[cfg(feature = "leaf")]
    let mut has_hub_creds = false;

    #[cfg(feature = "leaf")]
    if let Some(v) = args.opt_value_from_str("--hub")? {
        config.hub_url = Some(v);
    }
    #[cfg(feature = "leaf")]
    if let Some(v) = args.opt_value_from_str::<_, String>("--hub-user")? {
        hub_creds.user = Some(v);
        has_hub_creds = true;
    }
    #[cfg(feature = "leaf")]
    if let Some(v) = args.opt_value_from_str::<_, String>("--hub-pass")? {
        hub_creds.pass = Some(v);
        has_hub_creds = true;
    }
    #[cfg(feature = "leaf")]
    if let Some(v) = args.opt_value_from_str::<_, String>("--hub-token")? {
        hub_creds.token = Some(v);
        has_hub_creds = true;
    }
    #[cfg(feature = "leaf")]
    if let Some(v) = args.opt_value_from_str::<_, String>("--hub-creds")? {
        hub_creds.creds_file = Some(v);
        has_hub_creds = true;
    }

    // Cluster flags
    #[cfg(feature = "mesh")]
    if let Some(v) = args.opt_value_from_str("--cluster-port")? {
        config.cluster_port = Some(v);
    }
    #[cfg(feature = "mesh")]
    if let Some(v) = args.opt_value_from_str::<_, String>("--cluster-seeds")? {
        config
            .cluster_seeds
            .extend(v.split(',').map(|s| s.trim().to_string()));
    }
    #[cfg(feature = "mesh")]
    if let Some(v) = args.opt_value_from_str("--cluster-name")? {
        config.cluster_name = Some(v);
    }

    // Gateway flags
    #[cfg(feature = "gateway")]
    if let Some(v) = args.opt_value_from_str("--gateway-port")? {
        config.gateway_port = Some(v);
    }
    #[cfg(feature = "gateway")]
    if let Some(v) = args.opt_value_from_str("--gateway-name")? {
        config.gateway_name = Some(v);
    }
    #[cfg(feature = "gateway")]
    if let Some(v) = args.opt_value_from_str::<_, String>("--gateway-remotes")? {
        // Format: "cluster-b=host1:7222,host2:7222;cluster-c=host3:7222"
        for cluster_spec in v.split(';') {
            let cluster_spec = cluster_spec.trim();
            if cluster_spec.is_empty() {
                continue;
            }
            if let Some((name, urls_str)) = cluster_spec.split_once('=') {
                let urls: Vec<String> = urls_str.split(',').map(|s| s.trim().to_string()).collect();
                config
                    .gateway_remotes
                    .push(open_wire::server::GatewayRemote {
                        name: name.trim().to_string(),
                        urls,
                    });
            }
        }
    }

    // Reject unknown arguments
    let remaining = args.finish();
    if !remaining.is_empty() {
        for arg in &remaining {
            eprintln!("Unknown argument: {}", arg.to_string_lossy());
        }
        eprintln!("{USAGE}");
        std::process::exit(1);
    }

    // Build client auth from CLI flags
    if !auth_nkeys.is_empty() {
        config.client_auth = ClientAuth::NKey(auth_nkeys);
    } else if let Some(token) = auth_token {
        config.client_auth = ClientAuth::Token(token);
    } else if let (Some(user), Some(pass)) = (auth_user, auth_pass) {
        config.client_auth = ClientAuth::UserPass { user, pass };
    }

    #[cfg(feature = "leaf")]
    if has_hub_creds {
        config.hub_credentials = Some(hub_creds);
    }

    // Initialize tracing — optionally to a log file
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if let Some(ref log_path) = config.log_file {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .expect("failed to open log file");
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::sync::Mutex::new(file))
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }

    // Write PID file
    if let Some(ref pid_path) = config.pid_file {
        std::fs::write(pid_path, format!("{}", std::process::id()))
            .expect("failed to write pid file");
    }

    println!(
        "Starting leaf node server on {}:{} ({} workers)",
        config.host, config.port, config.workers
    );
    #[cfg(feature = "leaf")]
    if let Some(ref hub) = config.hub_url {
        println!("Upstream hub: {hub}");
    }
    if let Some(ws_port) = config.ws_port {
        println!("WebSocket port: {ws_port}");
    }
    if let Some(metrics_port) = config.metrics_port {
        println!("Metrics port: {metrics_port}");
    }
    if let Some(monitoring_port) = config.monitoring_port {
        println!("Monitoring port: {monitoring_port}");
    }

    let (shutdown, reload) = setup_signals();

    let pid_file = config.pid_file.clone();
    let server = LeafServer::new(config);
    let result = server.run_until_shutdown(shutdown, reload, cfg_path.as_deref());

    // Clear the global pointers (the Arcs are about to drop).
    SHUTDOWN_PTR.store(ptr::null_mut(), Ordering::Release);
    RELOAD_PTR.store(ptr::null_mut(), Ordering::Release);

    // Remove PID file
    if let Some(ref pid_path) = pid_file {
        let _ = std::fs::remove_file(pid_path);
    }

    println!("Server shut down gracefully");
    result
}
