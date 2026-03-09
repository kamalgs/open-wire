// NATS Leaf Node Gateway Server
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
use open_wire::{ClientAuth, HubCredentials, LeafServer, LeafServerConfig};

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Defer tracing init until after config is parsed (may need log_file).
    // First pass: look for --config / -c to load config file as base
    let args: Vec<String> = std::env::args().collect();
    let mut config = {
        let mut cfg_path: Option<String> = None;
        let mut i = 1;
        while i < args.len() {
            if matches!(args[i].as_str(), "--config" | "-c") {
                i += 1;
                if i < args.len() {
                    cfg_path = Some(args[i].clone());
                }
            }
            i += 1;
        }
        if let Some(path) = cfg_path {
            config::load_config(std::path::Path::new(&path))
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?
        } else {
            LeafServerConfig::default()
        }
    };

    // Accumulate auth-related CLI values before building config
    let mut auth_token: Option<String> = None;
    let mut auth_user: Option<String> = None;
    let mut auth_pass: Option<String> = None;
    let mut auth_nkeys: Vec<String> = Vec::new();
    let mut hub_creds = HubCredentials::default();
    let mut has_hub_creds = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" | "-c" => {
                i += 1; // already handled above, skip value
            }
            "--port" | "-p" => {
                i += 1;
                config.port = args[i].parse().expect("invalid port");
            }
            "--host" | "-h" => {
                i += 1;
                config.host = args[i].clone();
            }
            "--hub" => {
                i += 1;
                config.hub_url = Some(args[i].clone());
            }
            "--name" => {
                i += 1;
                config.server_name = args[i].clone();
            }
            "--read-buf-max" => {
                i += 1;
                config.max_read_buf_capacity = args[i].parse().expect("invalid read-buf-max");
            }
            "--write-buf-size" => {
                i += 1;
                config.write_buf_capacity = args[i].parse().expect("invalid write-buf-size");
            }
            "--workers" | "-w" => {
                i += 1;
                config.workers = args[i].parse().expect("invalid workers count");
            }
            "--ws-port" => {
                i += 1;
                config.ws_port = Some(args[i].parse().expect("invalid ws-port"));
            }
            "--token" => {
                i += 1;
                auth_token = Some(args[i].clone());
            }
            "--user" => {
                i += 1;
                auth_user = Some(args[i].clone());
            }
            "--pass" => {
                i += 1;
                auth_pass = Some(args[i].clone());
            }
            "--nkey" => {
                i += 1;
                auth_nkeys.push(args[i].clone());
            }
            "--hub-user" => {
                i += 1;
                hub_creds.user = Some(args[i].clone());
                has_hub_creds = true;
            }
            "--hub-pass" => {
                i += 1;
                hub_creds.pass = Some(args[i].clone());
                has_hub_creds = true;
            }
            "--hub-token" => {
                i += 1;
                hub_creds.token = Some(args[i].clone());
                has_hub_creds = true;
            }
            "--hub-creds" => {
                i += 1;
                hub_creds.creds_file = Some(args[i].clone());
                has_hub_creds = true;
            }
            "--metrics-port" => {
                i += 1;
                config.metrics_port = Some(args[i].parse().expect("invalid metrics-port"));
            }
            "--tls-cert" => {
                i += 1;
                config.tls_cert = Some(std::path::PathBuf::from(&args[i]));
            }
            "--tls-key" => {
                i += 1;
                config.tls_key = Some(std::path::PathBuf::from(&args[i]));
            }
            "--tls-ca-cert" => {
                i += 1;
                config.tls_ca_cert = Some(std::path::PathBuf::from(&args[i]));
            }
            "--tls-verify" => {
                config.tls_verify = true;
            }
            "--max-payload" => {
                i += 1;
                config.max_payload = args[i].parse().expect("invalid max-payload");
            }
            "--max-connections" => {
                i += 1;
                config.max_connections = args[i].parse().expect("invalid max-connections");
            }
            "--max-control-line" => {
                i += 1;
                config.max_control_line = args[i].parse().expect("invalid max-control-line");
            }
            "--max-subscriptions" => {
                i += 1;
                config.max_subscriptions = args[i].parse().expect("invalid max-subscriptions");
            }
            "--pid-file" => {
                i += 1;
                config.pid_file = Some(std::path::PathBuf::from(&args[i]));
            }
            "--log-file" => {
                i += 1;
                config.log_file = Some(std::path::PathBuf::from(&args[i]));
            }
            "--monitoring-port" | "--http-port" => {
                i += 1;
                config.monitoring_port = Some(args[i].parse().expect("invalid monitoring-port"));
            }
            "--auth-timeout" => {
                i += 1;
                let secs: u64 = args[i].parse().expect("invalid auth-timeout");
                config.auth_timeout = std::time::Duration::from_secs(secs);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                eprintln!(
                    "Usage: open-wire [--config FILE] [--port PORT] [--host HOST] \
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
                     [--auth-timeout SECS]"
                );
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Build client auth from CLI flags
    if !auth_nkeys.is_empty() {
        config.client_auth = ClientAuth::NKey(auth_nkeys);
    } else if let Some(token) = auth_token {
        config.client_auth = ClientAuth::Token(token);
    } else if let (Some(user), Some(pass)) = (auth_user, auth_pass) {
        config.client_auth = ClientAuth::UserPass { user, pass };
    }

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

    // Set up graceful shutdown via signals
    let shutdown = Arc::new(AtomicBool::new(false));
    SHUTDOWN_PTR.store(Arc::as_ptr(&shutdown) as *mut AtomicBool, Ordering::Release);

    let reload = Arc::new(AtomicBool::new(false));
    RELOAD_PTR.store(Arc::as_ptr(&reload) as *mut AtomicBool, Ordering::Release);

    install_signal_handler(libc::SIGTERM, handle_shutdown);
    install_signal_handler(libc::SIGINT, handle_shutdown);
    install_signal_handler(libc::SIGHUP, handle_reload);

    // Determine config path for hot-reload
    let config_path: Option<String> = {
        let mut cp = None;
        let mut j = 1;
        while j < args.len() {
            if matches!(args[j].as_str(), "--config" | "-c") {
                j += 1;
                if j < args.len() {
                    cp = Some(args[j].clone());
                }
            }
            j += 1;
        }
        cp
    };

    let pid_file = config.pid_file.clone();
    let server = LeafServer::new(config);
    let result = server.run_until_shutdown(shutdown, reload, config_path.as_deref());

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
