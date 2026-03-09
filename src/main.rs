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

use open_wire::{ClientAuth, HubCredentials, LeafServer, LeafServerConfig};

/// Global pointer to the shutdown flag, accessible from the signal handler.
static SHUTDOWN_PTR: AtomicPtr<AtomicBool> = AtomicPtr::new(ptr::null_mut());

extern "C" fn handle_signal(_sig: libc::c_int) {
    let ptr = SHUTDOWN_PTR.load(Ordering::Relaxed);
    if !ptr.is_null() {
        unsafe { &*ptr }.store(true, Ordering::Release);
    }
}

/// Install a signal handler for the given signal using `sigaction` with `SA_RESTART`.
fn install_signal_handler(sig: libc::c_int) {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handle_signal as *const () as usize;
        sa.sa_flags = libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask);
        libc::sigaction(sig, &sa, ptr::null_mut());
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut config = LeafServerConfig::default();

    // Accumulate auth-related CLI values before building config
    let mut auth_token: Option<String> = None;
    let mut auth_user: Option<String> = None;
    let mut auth_pass: Option<String> = None;
    let mut auth_nkeys: Vec<String> = Vec::new();
    let mut hub_creds = HubCredentials::default();
    let mut has_hub_creds = false;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
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
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                eprintln!(
                    "Usage: open-wire [--port PORT] [--host HOST] [--hub URL] [--name NAME] \
                     [--read-buf-max BYTES] [--write-buf-size BYTES] [--workers N] \
                     [--ws-port PORT] [--token TOKEN] [--user USER] [--pass PASS] \
                     [--nkey PUBKEY] [--hub-user USER] [--hub-pass PASS] \
                     [--hub-token TOKEN] [--hub-creds PATH] \
                     [--metrics-port PORT] [--tls-cert PATH] [--tls-key PATH] \
                     [--max-payload BYTES] [--max-connections N] \
                     [--max-control-line BYTES] [--max-subscriptions N]"
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

    // Set up graceful shutdown via signals
    let shutdown = Arc::new(AtomicBool::new(false));
    SHUTDOWN_PTR.store(Arc::as_ptr(&shutdown) as *mut AtomicBool, Ordering::Release);

    install_signal_handler(libc::SIGTERM);
    install_signal_handler(libc::SIGINT);
    install_signal_handler(libc::SIGHUP);

    let server = LeafServer::new(config);
    let result = server.run_until_shutdown(shutdown);

    // Clear the global pointer (the Arc is about to drop).
    SHUTDOWN_PTR.store(ptr::null_mut(), Ordering::Release);

    println!("Server shut down gracefully");
    result
}
