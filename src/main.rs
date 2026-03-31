// open-wire — NATS-compatible message relay
//
// Usage:
//   cargo run -- --port 4222
//   cargo run -- --port 4222 --hub nats://localhost:7422
//   cargo run -- --workers 8
//   cargo run -- --read-buf-max 32768 --write-buf-size 32768
//   cargo run -- --ws-port 8222

mod signals;

use tracing_subscriber::EnvFilter;

use open_wire::{config, LeafServer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (config, cfg_path) = config::from_args()?;

    // Initialize tracing — optionally to a log file.
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

    // Write PID file.
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

    let (shutdown, reload) = signals::setup();
    let pid_file = config.pid_file.clone();
    let server = LeafServer::new(config);
    let result = server.run_until_shutdown(shutdown, reload, cfg_path.as_deref());

    signals::clear();

    if let Some(ref pid_path) = pid_file {
        let _ = std::fs::remove_file(pid_path);
    }

    println!("Server shut down gracefully");
    result
}
