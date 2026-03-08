# CLAUDE.md — AI Agent Instructions for open-wire

## Project Overview

**open-wire** is a high-performance NATS-compatible message relay (leaf node server) written in Rust.
It speaks the standard NATS client and leaf node protocols, routes messages between local clients,
and optionally bridges traffic to an upstream NATS hub server.

Built with raw epoll, zero-copy parsing, and no async runtime.

## Repository Structure

```
src/
├── main.rs          # CLI binary (--port, --hub, --ws-port, --workers)
├── lib.rs           # Public API: LeafServer, LeafServerConfig
├── server.rs        # Accept loop, worker spawning, shutdown
├── worker.rs        # Per-thread epoll event loop, connection state machine
├── nats_proto.rs    # Zero-copy protocol parser + MsgBuilder
├── sub_list.rs      # SubList (exact + wildcard), DirectWriter fan-out
├── upstream.rs      # Hub connection via leaf node protocol
├── protocol.rs      # Connection I/O wrappers, AdaptiveBuf
├── websocket.rs     # HTTP upgrade handshake, WS frame codec
└── types.rs         # ServerInfo, ConnectInfo, HeaderMap
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

# Test (unit — no external deps)
cargo test --lib

# Test (all — requires nats-server in PATH)
cargo test

# Format (required: nightly toolchain)
cargo +nightly fmt

# Lint
cargo clippy --all-targets -- --deny clippy::all

# Build release
cargo build --release

# Build release with frame pointers (for perf profiling)
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release

# Benchmarks
cd tests && ./throughput.sh
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
- **DirectWriter**: Cross-worker message delivery via shared buffers + eventfd notifications.
  Fan-out to N conns on same worker = 1 eventfd write. Batched notifications reduce syscalls.
- **Zero-copy parsing**: Protocol parsed directly from read buffers via `nats_proto.rs`.
- **No async runtime**: Pure `std::thread` + `epoll` + `std::sync::mpsc`.
- **Connection state machine**: `SendInfo → WaitConnect → Active` phases in worker.
- **AdaptiveBuf**: Go-style dynamic buffer sizing (512B → 64KB).

### Key Types

| Type | Location | Purpose |
|------|----------|---------|
| `LeafServer` | `lib.rs` | Public API entry point |
| `LeafServerConfig` | `lib.rs` | Server configuration |
| `Worker` | `worker.rs` | Per-thread epoll event loop |
| `NatsProto` / `MsgBuilder` | `nats_proto.rs` | Protocol parser + message builder |
| `SubList` / `DirectWriter` | `sub_list.rs` | Subscription storage + fan-out delivery |
| `ServerConn` / `LeafConn` | `protocol.rs` | Connection I/O wrappers |
| `AdaptiveBuf` | `protocol.rs` | Dynamic read buffer |
| `Upstream` | `upstream.rs` | Hub connection management |

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
```

Minimal dependency footprint. No async runtime, no TLS library.

New dependencies should use permissive licenses: MIT, Apache-2.0, ISC, BSD-2-Clause, BSD-3-Clause.

## Build Environment

- Using zig as C compiler/linker (no system gcc). Config in `.cargo/config.toml`.
- `[profile.release] debug = 1, strip = false` for perf symbol resolution.

## Code Conventions

- **No `unwrap()` or `expect()`** in library code.
- **Imports**: Group std → external crates → crate-internal.
- All public items should have doc comments.
- Tests go in `#[cfg(test)] mod tests` within each source file (106 unit tests currently).
- Integration tests in `tests/e2e.rs` use `async-nats` client against a real `nats-server`.

## Commit & PR Standards

- Linear history — rebase, no merge commits.
- Atomic, reasonably-sized commits.
- Well-formed commit messages.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE) for attribution.
This project is a fork of [nats-io/nats.rs](https://github.com/nats-io/nats.rs).
