// NATS Leaf Node Gateway Server
//
// Usage:
//   cargo run -- --port 4222
//   cargo run -- --port 4222 --hub nats://localhost:7422
//   cargo run -- --workers 8
//   cargo run -- --read-buf-max 32768 --write-buf-size 32768
//   cargo run -- --ws-port 8222

use open_wire::{LeafServer, LeafServerConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing if tracing-subscriber is available
    #[cfg(feature = "_example_tracing")]
    tracing_subscriber::fmt::init();

    let mut config = LeafServerConfig::default();

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
                config.max_read_buf_capacity =
                    args[i].parse().expect("invalid read-buf-max");
            }
            "--write-buf-size" => {
                i += 1;
                config.write_buf_capacity =
                    args[i].parse().expect("invalid write-buf-size");
            }
            "--workers" | "-w" => {
                i += 1;
                config.workers = args[i].parse().expect("invalid workers count");
            }
            "--ws-port" => {
                i += 1;
                config.ws_port =
                    Some(args[i].parse().expect("invalid ws-port"));
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                eprintln!(
                    "Usage: leaf_server [--port PORT] [--host HOST] [--hub URL] [--name NAME] \
                     [--read-buf-max BYTES] [--write-buf-size BYTES] [--workers N] \
                     [--ws-port PORT]"
                );
                std::process::exit(1);
            }
        }
        i += 1;
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

    let server = LeafServer::new(config);
    server.run()
}
