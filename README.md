# open-wire

A high-performance NATS-compatible message relay written in Rust. It speaks the standard NATS client, leaf node, and route protocols, routes messages between local clients, optionally bridges traffic to an upstream NATS hub server, and can form full-mesh clusters with peer nodes.

Built with raw epoll, zero-copy parsing, and no async runtime — to see how close bare-metal Rust can get to the Go nats-server.

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

## Performance

Benchmarked against Go nats-server v2.14.0-dev leaf node, 128B messages, 3-run average:

| Scenario | Rust / Go |
|---|---|
| Pub only | 100% |
| Local pub/sub | 109% |
| Fan-out x5 | 210% |
| Leaf → Hub | 128% |
| Hub → Leaf | 104% |
| Cluster A→B | 192% |
| Cluster fan-out x3 | 162% |
| WS fan-out x5 | 238% |
| WS fan-out x10 | 295% |

Memory: 3.3 MB idle vs Go's 12.6 MB. At 10K idle connections: 1.3 KB/conn vs Go's 41.4 KB/conn.

See [BENCHMARKS.md](BENCHMARKS.md) for the full results log.

## Features

- **Local pub/sub** — clients connect and exchange messages through the relay
- **Upstream hub forwarding** — connects to a NATS server via leaf node protocol, bridges traffic bidirectionally
- **NATS wildcard matching** — `*` (single token) and `>` (tail match)
- **Standard NATS protocol** — works with any NATS client (nats CLI, async-nats, nats.go, nats.ws, etc.)
- **Headers** — HMSG/HPUB protocol support
- **WebSocket** — accepts browser and WS-capable NATS clients on a separate port
- **N-worker epoll** — multi-threaded event loop with batched cross-worker notifications
- **Zero-copy parsing** — protocol parsed directly from read buffers, no intermediate allocations
- **Full-mesh clustering** — route connections between peers with one-hop message forwarding (`cluster` feature)
- **Minimal dependencies** — no async runtime, no TLS library, ~886 KB binary

## Quick Start

```bash
# Build
cargo build --release

# Run standalone
./target/release/open-wire --port 4222

# Run with upstream hub
./target/release/open-wire --port 4222 --hub nats://hub:4111

# Run with WebSocket support
./target/release/open-wire --port 4222 --ws-port 4223

# Build with cluster support
cargo build --release --features cluster

# Run a 3-node cluster
./target/release/open-wire -c node-a.conf
./target/release/open-wire -c node-b.conf
./target/release/open-wire -c node-c.conf
```

### Docker

```bash
docker build -t open-wire .
docker run -p 4222:4222 open-wire
docker run -p 4222:4222 open-wire --hub nats://hub:4111
```

## CLI Options

| Flag | Default | Description |
|---|---|---|
| `--port`, `-p` | `4222` | TCP listen port |
| `--host` | `0.0.0.0` | Bind address |
| `--hub` | *(none)* | Upstream NATS server URL |
| `--name` | `open-wire` | Server name |
| `--workers`, `-w` | CPU count | Number of worker threads |
| `--ws-port` | *(none)* | WebSocket listen port |
| `--cluster-port` | *(none)* | Cluster route listen port (requires `cluster` feature) |
| `--cluster-seeds` | *(none)* | Comma-separated route seed URLs |
| `--cluster-name` | *(none)* | Cluster name |
| `-c`, `--config` | *(none)* | Config file path (Go nats-server `.conf` format) |

## Usage Examples

### Local pub/sub

```bash
# Terminal 1
./target/release/open-wire --port 4222

# Terminal 2
nats sub test.subject -s nats://localhost:4222

# Terminal 3
nats pub test.subject "hello" -s nats://localhost:4222
```

### Leaf with upstream hub

```bash
# Terminal 1 — start a standard NATS server as the hub
nats-server -p 4111

# Terminal 2 — start open-wire pointing at the hub
./target/release/open-wire --port 4222 --hub nats://localhost:4111

# Terminal 3 — subscribe via the leaf
nats sub "test.>" -s nats://localhost:4222

# Terminal 4 — publish to the hub; message arrives at the leaf subscriber
nats pub test.hello "from hub" -s nats://localhost:4111
```

### 3-Node Cluster

```bash
# node-a.conf
# listen: 127.0.0.1:4222
# server_name: node-a
# cluster {
#   name: my-cluster
#   listen: 0.0.0.0:4248
#   routes = [
#     "nats-route://node-b:4248"
#     "nats-route://node-c:4248"
#   ]
# }

# Start all three nodes (requires --features cluster)
./target/release/open-wire -c node-a.conf
./target/release/open-wire -c node-b.conf
./target/release/open-wire -c node-c.conf

# Subscribe on node A, publish on node B — message flows across the cluster
nats sub test.hello -s nats://node-a:4222
nats pub test.hello "from node B" -s nats://node-b:4222
```

### As a library

```rust
use open_wire::{LeafServer, LeafServerConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = LeafServerConfig {
        host: "0.0.0.0".into(),
        port: 4222,
        hub_url: Some("nats://hub:4111".into()),
        server_name: "my-leaf".into(),
        ..Default::default()
    };

    let server = LeafServer::new(config);
    server.run()
}
```

## Architecture

```
                        ┌────────────────────────┐
                        │     Upstream Hub        │
                        │  (standard nats-server) │
                        └───────────┬────────────┘
                                    │ leaf node protocol
                        ┌───────────┴────────────┐
                        │    Upstream Module      │
                        │  reader + writer thread │
                        └───────────┬────────────┘
                                    │
    ┌───────────────────────────────┼───────────────────────────────┐
    │  ┌──────────┐  round  ┌──────┴─────┐  round  ┌──────────┐   │
    │  │ Worker 0 │◄─robin─►│  Acceptor  │◄─robin─►│ Worker N │   │
    │  └────┬─────┘         └────────────┘         └────┬─────┘   │
    │       │ epoll                                      │ epoll   │
    │  ┌────┴──────────┐                          ┌─────┴───────┐ │
    │  │ C0  C1  C2 .. │                          │ Cm .. Cn    │ │
    │  └───────────────┘                          └─────────────┘ │
    └─────────────────────────────────────────────────────────────┘
```

| Module | Purpose |
|---|---|
| `server.rs` | Accept loop, worker spawning, shutdown |
| `worker.rs` | Per-thread epoll event loop, connection state machine |
| `nats_proto.rs` | Zero-copy protocol parser and message builder |
| `sub_list.rs` | Subscription storage, wildcard matching, DirectWriter fan-out |
| `upstream.rs` | Hub connection via leaf node protocol |
| `route_handler.rs` | Route protocol dispatch (RS+/RS-/RMSG) — `cluster` feature |
| `route_conn.rs` | Outbound route connection manager — `cluster` feature |
| `protocol.rs` | Connection I/O wrappers, adaptive buffers |
| `websocket.rs` | HTTP upgrade handshake, WS frame codec |
| `types.rs` | ServerInfo, ConnectInfo, HeaderMap |

See [architecture.md](docs/architecture.md) for detailed message flow diagrams.

## Tests

```bash
cargo test --lib                    # 260 unit tests (with --features cluster)
cargo test                          # unit + integration (requires nats-server in PATH)
cargo test --test e2e --features cluster -- cluster   # cluster integration tests
```

## Acknowledgments

open-wire began as a fork of [nats-io/nats.rs](https://github.com/nats-io/nats.rs) and owes
a debt of gratitude to the NATS Authors and the broader [NATS](https://nats.io) community.
The protocol design, wire format, and many architectural ideas — particularly the parser
structure and adaptive buffering — were directly informed by studying the Go
[nats-server](https://github.com/nats-io/nats-server). Thank you for building such an
excellent foundation in the open.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE) for attribution details.
