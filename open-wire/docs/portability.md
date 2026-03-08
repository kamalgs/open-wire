# Future Direction: Cross-Platform Portability

## Goal

Run the leaf server on any OS, any architecture, any scale — from a
Raspberry Pi to a CDN edge node to a bare-metal server. One codebase,
maximum reach.

## Why This Matters

NATS is a connectivity fabric. The leaf server's value comes from running
*everywhere* — close to the data, close to the devices, close to the users.
A leaf that only runs on Linux x86_64 misses most of the deployment surface:

- **Mobile** — Android (sidecar in apps, local mesh), iOS (background services)
- **IoT / embedded** — ARM boards, routers, industrial controllers
- **Edge / CDN** — Cloudflare Workers, Fastly Compute, Fermyon Spin
- **Desktop / developer** — macOS (Apple Silicon), Windows
- **Legacy / constrained** — FreeBSD, older Linux kernels, MIPS
- **Containers** — Alpine/musl, scratch images, WASM containers

## Current State

The server is Linux-only due to three OS-specific APIs:

| API | Used in | Purpose |
|---|---|---|
| `epoll` | `worker.rs` | I/O multiplexing (event loop) |
| `eventfd` | `sub_list.rs`, `worker.rs` | Cross-thread wake notification |
| `poll()` (POSIX) | `server.rs` | Multi-listener accept |

Everything else — protocol parsing, subscription routing, message building,
WebSocket codec, type definitions — is pure Rust with no OS dependencies.

```
┌──────────────────────────────────────────────────┐
│           Platform-independent (~60%)            │
│                                                  │
│  nats_proto.rs  sub_list.rs  types.rs            │
│  websocket.rs   (routing logic in worker.rs)     │
├──────────────────────────────────────────────────┤
│           Platform-specific (~40%)               │
│                                                  │
│  I/O reactor    Listener     Wake mechanism      │
│  (epoll)        (TcpListener) (eventfd)          │
└──────────────────────────────────────────────────┘
```

## Strategy: Two Tiers

### Tier 1 — Native cross-platform (high priority)

Compile natively on each OS using its own I/O multiplexer.
Same performance characteristics as today, just different syscalls.

| OS | I/O multiplexer | Wake mechanism | Listener | Cross-compile target |
|---|---|---|---|---|
| Linux | `epoll` | `eventfd` | `TcpListener` | `x86_64-unknown-linux-gnu` |
| Android | `epoll` | `eventfd` | `TcpListener` | `aarch64-linux-android` |
| macOS | `kqueue` | `kevent` (user event) | `TcpListener` | `aarch64-apple-darwin` |
| iOS | `kqueue` | `kevent` (user event) | `TcpListener` | `aarch64-apple-ios` |
| FreeBSD / BSD | `kqueue` | `kevent` (user event) | `TcpListener` | `x86_64-unknown-freebsd` |
| Windows | IOCP | `PostQueuedCompletionStatus` | `TcpListener` | `x86_64-pc-windows-msvc` |
| POSIX fallback | `poll()` | pipe | `TcpListener` | any POSIX target |

All of these share `std::net::TcpListener` and `std::net::TcpStream`.
The only divergence is the event loop and wake mechanism.

**Android** is Linux — the current epoll + eventfd code works unmodified.
Just cross-compile with the Android NDK target.

**iOS** is Darwin (BSD kernel) — same as macOS. A single `reactor_kqueue`
module covers macOS, iOS, tvOS, watchOS, and all BSDs.

#### Abstraction

```rust
/// Platform I/O reactor — one impl per OS.
trait Reactor {
    type Poller;   // epoll fd, kqueue fd, IOCP handle
    type Waker;    // eventfd, kevent, IOCP packet

    fn create() -> io::Result<Self::Poller>;
    fn register(poller: &Self::Poller, fd: RawFd, token: usize) -> io::Result<()>;
    fn deregister(poller: &Self::Poller, fd: RawFd) -> io::Result<()>;
    fn poll(poller: &Self::Poller, events: &mut Events, timeout_ms: i32) -> io::Result<usize>;
    fn create_waker(poller: &Self::Poller) -> io::Result<Self::Waker>;
    fn wake(waker: &Self::Waker) -> io::Result<()>;
}

// Linux + Android — same kernel, same syscalls
#[cfg(any(target_os = "linux", target_os = "android"))]
mod reactor_epoll;

// macOS + iOS + FreeBSD + other BSDs — all have kqueue
#[cfg(any(target_os = "macos", target_os = "ios",
          target_os = "freebsd", target_os = "netbsd",
          target_os = "openbsd", target_os = "dragonfly"))]
mod reactor_kqueue;

#[cfg(target_os = "windows")]
mod reactor_iocp;

// Everything else — POSIX poll() works on any Unix-like system
#[cfg(not(any(target_os = "linux", target_os = "android",
              target_os = "macos", target_os = "ios",
              target_os = "freebsd", target_os = "netbsd",
              target_os = "openbsd", target_os = "dragonfly",
              target_os = "windows")))]
mod reactor_poll;
```

The worker event loop, connection management, and protocol handling stay
identical across platforms. Only the ~100-line reactor module changes.

#### POSIX `poll()` fallback

A `poll()`-based reactor works on virtually any POSIX system. It's O(n) in
file descriptors (vs epoll's O(1)), but perfectly adequate for small
deployments (IoT, dev, embedded) where connection counts are low. This
gives us a "runs anywhere" baseline with zero porting effort per platform.

### Tier 2 — WASM (medium priority)

WASM extends reach to platforms that have a JavaScript or WASM runtime
but no native compilation toolchain. This is the path to edge CDNs,
browsers (for testing/dev tools), and containerized WASM runtimes.

| Target | Runtime | I/O model |
|---|---|---|
| `wasm32-wasip2` | Wasmtime, WasmEdge | `wasi:sockets` + `wasi:io/poll` |
| Cloudflare Workers | V8 + workerd | WebSocket inbound, `connect()` outbound |
| Fastly Compute | Wasmtime | HTTP/WS triggers, outbound TCP |
| Fermyon Spin | Wasmtime | HTTP/WS triggers |

#### WASI Preview 2 (standalone runtimes)

The closest to native. `wasi:sockets` provides TCP listen/accept/connect
and `wasi:io/poll` provides non-blocking I/O multiplexing. The reactor
trait maps cleanly:

| Reactor method | WASI equivalent |
|---|---|
| `poll()` | `wasi:io/poll::poll()` |
| `Listener` | `wasi:sockets/tcp::TcpSocket` |
| `Stream` | `wasi:sockets/tcp::TcpSocket` |
| `Waker` | `wasi:io/poll::Pollable` |

Constraint: WASI has no threads, so the N-worker model collapses to a
single-threaded event loop. Fine for edge workloads.

#### Edge CDN platforms (Cloudflare, Fastly, Spin)

These are request-driven, not connection-driven. The leaf server would
need a different integration layer:

- Platform handles inbound WebSocket upgrade
- Hub connection via outbound TCP `connect()` or WebSocket
- State persistence via Durable Objects (Cloudflare) or similar
- No accept loop — runtime delivers connections as events

This is a separate adaptation, not just a reactor swap.

## Dependency Audit for Portability

Current runtime dependencies and their portability:

| Crate | WASM-compatible | Notes |
|---|---|---|
| `bytes` | Yes | Pure Rust |
| `memchr` | Yes | Pure Rust (SIMD optional) |
| `serde` | Yes | Pure Rust |
| `serde_json` | Yes | Pure Rust |
| `rand` | Yes | Has WASM support via `getrandom` |
| `tracing` | Yes | Pure Rust |
| `libc` | No | Only needed by reactor, behind `#[cfg]` |

No dependency blocks WASM. `libc` is only used in the platform-specific
reactor code and would be `#[cfg(not(target_arch = "wasm32"))]`.

## Implementation Order

1. **Extract reactor trait** from current epoll code. No behavior change,
   just move ~100 lines behind a trait boundary. All tests keep passing.

2. **Add `poll()` fallback reactor.** Enables macOS, FreeBSD, and any POSIX
   system immediately. Simple to implement (~80 lines), easy to test.

3. **Add kqueue reactor** for macOS/FreeBSD. Better performance than
   `poll()` on those platforms, similar complexity to epoll.

4. **Add WASI reactor.** Compile with `--target wasm32-wasip2`, test on
   Wasmtime. Single-threaded, but functionally complete.

5. **(Optional) Edge integrations.** Cloudflare Workers adapter using
   WebSocket + Durable Objects. Separate crate/module.

6. **(Optional) IOCP reactor** for native Windows. Lower priority since
   Windows users can use WSL or the WASM binary.

Steps 1-2 unlock most of the deployment surface with minimal effort.

## Binary Size

| Target | Size (stripped) |
|---|---|
| Linux x86_64 (current) | ~886 KB |
| WASM (estimated) | Comparable or smaller |

The minimal dependency set (no TLS, no async runtime, no HTTP framework)
keeps the binary small across all targets.

## References

- [WASI Sockets Proposal](https://github.com/WebAssembly/wasi-sockets)
- [wasi:sockets API](https://wa.dev/wasi:sockets)
- [Cloudflare Workers TCP Sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)
- [Cloudflare Workers WebSockets](https://developers.cloudflare.com/workers/runtime-apis/websockets/)
- [Bytecode Alliance WASM Roadmap](https://bytecodealliance.org/articles/webassembly-the-updated-roadmap-for-developers)
