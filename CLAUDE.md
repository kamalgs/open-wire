# CLAUDE.md — AI Agent Instructions for open-wire

## Project Overview

**open-wire** is a high-performance NATS-compatible message relay written in Rust.
It speaks the standard NATS client, leaf node, and route protocols, routes messages between
local clients, optionally bridges traffic to an upstream NATS hub server, and can form
full-mesh clusters with peer nodes.

Built with raw epoll, zero-copy parsing, and no async runtime.

## Repository Structure

```
src/
├── main.rs              # CLI binary (--port, --hub, --ws-port, --workers, --cluster-*)
├── lib.rs               # Module declarations + public re-exports
├── infra/               # Shared infrastructure
│   ├── mod.rs           # Facade re-exports
│   ├── server.rs        # LeafServer, ServerState, accept loop
│   ├── worker.rs        # N-worker epoll reactor
│   ├── config.rs        # Go nats-server .conf file parser
│   ├── nats_proto.rs    # Zero-copy protocol parser + MsgBuilder
│   ├── sub_list.rs      # SubscriptionManager (exact + wildcard matching)
│   ├── msg_writer.rs    # MsgWriter: cross-worker delivery via eventfd
│   ├── buf.rs           # AdaptiveBuf, BufConfig, Backoff
│   ├── types.rs         # ServerInfo, ConnectInfo, HeaderMap
│   └── websocket.rs     # HTTP upgrade handshake, WS frame codec
├── handler/             # Handler framework + client protocol + propagation
│   ├── mod.rs           # Facade re-exports
│   ├── conn.rs          # ConnectionHandler trait, ConnCtx, ConnExt
│   ├── delivery.rs      # Msg, deliver_to_subs, publish
│   ├── client.rs        # Client protocol dispatch (PUB/SUB/UNSUB/PING/PONG)
│   └── propagation.rs   # Interest propagation (LS+/LS-, RS+/RS-) + gateway reply rewriting
├── cluster/             # Full-mesh clustering [feature = "cluster"]
│   ├── mod.rs           # Facade re-exports
│   ├── conn.rs          # Outbound route connection manager
│   └── handler.rs       # Route protocol dispatch (RS+/RS-/RMSG)
├── gateway/             # Gateway inter-cluster traffic [feature = "gateway"]
│   ├── mod.rs           # Facade re-exports
│   ├── conn.rs          # Outbound gateway connection manager
│   └── handler.rs       # Gateway protocol dispatch (RS+/RS-/RMSG)
└── leaf/                # Leaf node + hub connection [features "leaf"/"hub"]
    ├── mod.rs           # Facade re-exports (per-submodule feature gates)
    ├── conn.rs          # LeafConn, LeafReader, LeafWriter, HubStream
    ├── handler.rs       # Inbound leaf protocol dispatch (LS+/LS-/LMSG)
    ├── upstream.rs      # Hub connection via leaf node protocol
    └── interest.rs      # InterestPipeline: subject mapping + interest collapse
examples/
└── chat/            # Sample chat app (HTML + README)
tests/
├── e2e.rs           # Integration tests (requires nats-server + async-nats)
├── throughput.rs    # Criterion benchmarks
├── throughput.sh    # Main Rust vs Go leaf benchmark
├── smoke_test.sh    # Quick functional smoke test
├── profile.sh       # Perf profiling (pub-only, pubsub, fanout)
├── profile_run.sh   # Ad-hoc perf: pub-only + pubsub
├── profile_pubsub.sh   # Ad-hoc perf: pubsub with frame pointers
├── profile_pubonly.sh   # Ad-hoc perf: pub-only with frame pointers
├── profile_hubleaf.sh   # Ad-hoc perf: hub→leaf with frame pointers
├── memory.sh        # Idle-connection memory comparison (Go vs Rust)
├── clients/         # Go helper binary for memory bench
└── configs/         # nats-server configs for benchmarks
docs/
├── architecture.md  # Detailed message flow diagrams
├── goals.md         # Project goals
├── backlog.md       # Feature backlog
├── portability.md   # Portability notes
└── adr/             # Architecture decision records
Cargo.toml               # Package manifest
Cargo.lock
BENCHMARKS.md            # Full benchmark results log
Dockerfile  .dockerignore
.gitignore  .rustfmt.toml
CLAUDE.md  LICENSE  NOTICE  README.md
.cargo/  .claude/  .github/
```

## Build & Test Commands

```bash
# Check
cargo check

# Check with cluster feature
cargo check --features cluster

# Test (unit — no external deps)
cargo test --lib

# Test with cluster feature
cargo test --lib --features cluster

# Test (all — requires nats-server in PATH)
cargo test

# Test cluster integration tests
cargo test --test e2e --features cluster -- cluster

# Format (required: nightly toolchain)
cargo +nightly fmt

# Lint
cargo clippy --all-targets -- --deny clippy::all
cargo clippy --all-targets --features cluster -- --deny clippy::all

# Build release
cargo build --release

# Build release with cluster
cargo build --release --features cluster

# Build release with frame pointers (for perf profiling)
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release

# Benchmarks (quick — 5 core scenarios)
cd tests && ./throughput.sh

# Benchmarks (full — all 16 scenarios including cluster)
cd tests && ./throughput.sh --full

cd tests && ./smoke_test.sh
```

**nats-server**: Integration tests and benchmarks require the `nats-server` binary:
```bash
go install github.com/nats-io/nats-server/v2@main
```

## Formatting Rules

Defined in `.rustfmt.toml`:
- `max_width = 100`
- `reorder_imports = true`
- `format_code_in_doc_comments = true`
- Edition: 2018 (rustfmt setting — the crate itself is edition 2021)

Always run `cargo +nightly fmt` before committing.

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

### Key Design Points

- **N-worker epoll model**: N worker threads, each with one epoll instance multiplexing many connections.
- **MsgWriter**: Cross-worker message delivery via shared buffers + eventfd notifications.
  Fan-out to N conns on same worker = 1 eventfd write. Batched notifications reduce syscalls.
- **Zero-copy parsing**: Protocol parsed directly from read buffers via `nats_proto.rs`.
- **No async runtime**: Pure `std::thread` + `epoll` + `std::sync::mpsc`.
- **Connection state machine**: `SendInfo → WaitConnect → Active` phases in worker.
- **AdaptiveBuf**: Go-style dynamic buffer sizing (512B → 64KB).
- **Full-mesh clustering** (`cluster` feature): Route connections between peers using RS+/RS-/RMSG
  protocol. One-hop message forwarding — messages from routes are never re-forwarded to other routes.

### Key Types

| Type | Location | Purpose |
|------|----------|---------|
| `LeafServer` | `infra/server.rs` | Public API entry point |
| `LeafServerConfig` | `infra/server.rs` | Server configuration |
| `load_config` | `infra/config.rs` | Go nats-server `.conf` file parser |
| `Worker` | `infra/worker.rs` | Per-thread epoll event loop |
| `NatsProto` / `MsgBuilder` | `infra/nats_proto.rs` | Protocol parser + message builder |
| `SubscriptionManager` | `infra/sub_list.rs` | Subscription storage + wildcard matching |
| `MsgWriter` | `infra/msg_writer.rs` | Shared buffer + eventfd cross-worker delivery |
| `ServerConn` | `infra/buf.rs` | Connection I/O wrapper (test-only) |
| `Backoff` | `infra/buf.rs` | Exponential backoff with jitter |
| `AdaptiveBuf` | `infra/buf.rs` | Dynamic read buffer |
| `LeafConn` | `leaf/conn.rs` | Leaf connection I/O wrapper |
| `Upstream` | `leaf/upstream.rs` | Hub connection management |
| `InterestPipeline` | `leaf/interest.rs` | Subject mapping + interest collapse |
| `RouteHandler` | `cluster/handler.rs` | Route protocol dispatch (`cluster`) |
| `RouteConnManager` | `cluster/conn.rs` | Outbound route connections (`cluster`) |
| `GatewayHandler` | `gateway/handler.rs` | Gateway protocol dispatch (`gateway`) |
| `GatewayConnManager` | `gateway/conn.rs` | Outbound gateway connections (`gateway`) |

## Feature Flags

| Feature | Default | Purpose |
|---------|---------|---------|
| `leaf` | yes | Upstream hub connection support |
| `hub` | yes | Inbound leaf node connection support |
| `interest-collapse` | yes | N:1 wildcard aggregation for upstream subs |
| `subject-mapping` | yes | Stateless subject rewriting before upstream |
| `cluster` | no | Full mesh route clustering (RS+/RS-/RMSG) |
| `gateway` | no | Gateway inter-cluster traffic |
| `accounts` | no | Multi-tenant per-account subject isolation |
| `worker-affinity` | no | Subject-based worker affinity tracking |

## Dependencies

```toml
[dependencies]
bytes = "1.4.0"
libc = "0.2"
memchr = "2.4"
rand = "0.8"
serde = { version = "1", features = ["derive"] }
serde_json = "1.0.104"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
metrics = "0.24"
metrics-exporter-prometheus = "0.16"
nkeys = { version = "0.4", default-features = false }
data-encoding = "2"
rustls = "0.23"
rustls-pemfile = "2"
rustls-pki-types = "1"
webpki-roots = "0.26"
```

Minimal dependency footprint. No async runtime.

New dependencies should use permissive licenses: MIT, Apache-2.0, ISC, BSD-2-Clause, BSD-3-Clause.

## Build Environment

- Using zig as C compiler/linker (no system gcc). Config in `.cargo/config.toml`.
- `[profile.release] debug = 1, strip = false` for perf symbol resolution.

## Code Conventions

- **No `unwrap()` or `expect()`** in library code.
- **Imports**: Group std → external crates → crate-internal.
- All public items should have doc comments.
- Tests go in `#[cfg(test)] mod tests` within each source file (295 unit tests with `--all-features`).
- Integration tests in `tests/e2e.rs` use `async-nats` client against a real `nats-server`.

## Commit & PR Standards

- Linear history — rebase, no merge commits.
- Atomic, reasonably-sized commits.
- Well-formed commit messages.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE) for attribution.
This project is a fork of [nats-io/nats.rs](https://github.com/nats-io/nats.rs).
