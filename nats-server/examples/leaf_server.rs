// Example: NATS Leaf Node Gateway Server
//
// Usage:
//   cargo run --example leaf_server -- --port 4222
//   cargo run --example leaf_server -- --port 4222 --hub nats://localhost:7422
//   cargo run --example leaf_server -- --port 4222 --ws-port 4223

use nats_server::{LeafServer, LeafServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
            #[cfg(feature = "websockets")]
            "--ws-port" => {
                i += 1;
                config.ws_port = Some(args[i].parse().expect("invalid ws-port"));
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                eprintln!(
                    "Usage: leaf_server [--port PORT] [--host HOST] [--hub URL] [--name NAME] [--ws-port PORT]"
                );
                std::process::exit(1);
            }
        }
        i += 1;
    }

    println!(
        "Starting leaf node server on {}:{}",
        config.host, config.port
    );
    if let Some(ref hub) = config.hub_url {
        println!("Upstream hub: {hub}");
    }
    #[cfg(feature = "websockets")]
    if let Some(ws_port) = config.ws_port {
        println!("WebSocket listener on port {ws_port}");
    }

    let server = LeafServer::new(config);
    server.run().await
}
