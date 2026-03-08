# Architecture

A high-performance NATS leaf node server written in Rust. It accepts client
connections over TCP, routes messages between local subscribers, and optionally
forwards traffic to an upstream NATS hub via the leaf node protocol. The design
prioritises throughput and low latency over feature completeness — there is no
JetStream, clustering, auth, or TLS.

## System Diagram

```
                          ┌─────────────────────────────┐
                          │      Upstream Hub            │
                          │   (standard nats-server)     │
                          └──────────┬──────────────────┘
                                     │ leaf node protocol
                                     │ (LS+/LS-/LMSG)
                          ┌──────────┴──────────────────┐
                          │       Upstream Module        │
                          │  reader thread  writer thread│
                          └──────────┬──────────────────┘
                                     │ Arc<ServerState>
    ┌────────────────────────────────┼────────────────────────────────┐
    │                                │                                │
    │  ┌──────────┐  round   ┌──────┴─────┐  round   ┌──────────┐   │
    │  │ Worker 0 │◄─robin──►│  Acceptor  │◄─robin──►│ Worker N │   │
    │  └────┬─────┘          └────────────┘          └────┬─────┘   │
    │       │ epoll                                        │ epoll   │
    │  ┌────┴────────────┐                         ┌──────┴──────┐  │
    │  │ C0  C1  C2  ... │                         │ Cm ... Cn   │  │
    │  │ (client sockets) │                         │             │  │
    │  └─────────────────┘                         └─────────────┘  │
    └───────────────────────────────────────────────────────────────┘
```

## Module Map

| File | Purpose | Key Types |
|------|---------|-----------|
| `lib.rs` | Public API re-exports | `LeafServer`, `LeafServerConfig` |
| `server.rs` | Accept loop, worker spawning, shutdown | `LeafServer`, `ServerState` |
| `worker.rs` | Per-thread epoll event loop | `Worker`, `ClientState`, `ConnPhase`, `WorkerHandle` |
| `protocol.rs` | Connection I/O wrappers, adaptive buffers | `ServerConn`, `LeafConn`, `AdaptiveBuf`, `BufConfig` |
| `nats_proto.rs` | Zero-copy protocol parser and message builder | `ClientOp`, `LeafOp`, `MsgBuilder` |
| `sub_list.rs` | Subscription storage and fan-out | `SubList`, `Subscription`, `DirectWriter` |
| `upstream.rs` | Hub connection (reader + writer threads) | `Upstream`, `UpstreamCmd` |

## Connection Lifecycle

```
  accept()
     │
     ▼
  SendInfo ──write INFO──► client
     │
     ▼
  WaitConnect ◄──CONNECT── client
     │
     ▼
  Active ◄──── PUB / SUB / UNSUB / PING / PONG
     │
     ▼
  close (EOF / error / shutdown)
```

States are tracked per-connection in `ConnPhase`. The worker adds each new
socket to its epoll instance and manages state transitions inline.

## Message Flow: Local Pub/Sub (Same Worker)

```
  Publisher conn           SubList              Subscriber conn
       │                     │                       │
  PUB "foo" ──EPOLLIN──►    │                       │
       │            for_each_match("foo")            │
       │                     │                       │
       │              DirectWriter::write_msg()      │
       │                     │──► shared buf ────────┤
       │                     │                       │
       │         flush_pending() (no eventfd)        │
       │                     │         socket write ◄┘
```

When publisher and subscriber are on the same worker, `flush_pending()` drains
the subscriber's `DirectWriter` buffer directly — no eventfd wake is needed.

## Message Flow: Cross-Worker Pub/Sub

```
  Worker A                                    Worker B
  ────────                                    ────────
  PUB "foo"                                       │
       │                                          │
  for_each_match()                                │
       │                                          │
  DirectWriter::write_msg()                       │
  (into subscriber's shared buf)                  │
       │                                          │
  accumulate eventfd in pending_notify[]          │
       │                                          │
  flush_notifications() ── eventfd write(1) ──►   │
                                            epoll wakes
                                            scan has_pending
                                            drain buf → socket
```

Eventfd notifications are batched: all PUBs in a single read buffer are
processed first, then one deduplicated eventfd write per remote worker.
See [ADR-005](adr/005-batched-notifications.md).

## Message Flow: Leaf ↔ Hub

```
  Local client        Upstream module           Hub server
       │                    │                       │
  SUB "foo" ──────► add_interest("foo")             │
       │              refcount++ ──► LS+ foo ──────►│
       │                    │                       │
       │                    │◄── LMSG foo payload ──│
       │                    │                       │
       │   for_each_match("foo")                    │
       │◄── DirectWriter ──┘                        │
       │                                            │
  PUB "bar" ────────────────► LMSG bar payload ────►│
```

The upstream module reference-counts local subscriptions. The first SUB sends
`LS+` to the hub; the last UNSUB sends `LS-`. The reader and writer each run
on their own OS thread with blocking I/O.

## Subscription Model

`SubList` splits subscriptions into two collections:

- **Exact subjects** — `HashMap<String, Vec<Subscription>>`, O(1) lookup.
- **Wildcard patterns** (`*`, `>`) — `Vec<Subscription>`, linear scan.

`for_each_match(subject, callback)` avoids allocation by invoking a closure
on each match rather than collecting into a `Vec`.

Each `Subscription` holds a `DirectWriter` that points to the subscriber's
shared buffer. Fan-out is lock → memcpy → unlock per subscriber, with a
single eventfd notification per remote worker.

## See Also

- [ADR-001: epoll over Tokio](adr/001-epoll-over-tokio.md)
- [ADR-002: DirectWriter](adr/002-direct-writer.md)
- [ADR-003: Zero-copy parsing](adr/003-zero-copy-parsing.md)
- [ADR-004: Adaptive buffers](adr/004-adaptive-buffers.md)
- [ADR-005: Batched notifications](adr/005-batched-notifications.md)
