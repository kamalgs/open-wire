# Backlog

Ideas to explore, roughly categorised. Nothing here is committed.

## Performance

- **io_uring** — replace epoll with io_uring for batched syscalls. Could reduce
  syscall overhead on high-connection-count workloads.
- **writev / vectored writes** — coalesce multiple small messages into a single
  syscall instead of memcpy into a contiguous buffer.
- **Connection affinity** — pin publisher and its subscribers to the same worker
  to avoid cross-worker eventfd notifications.
- **Trie-based subject matching** — replace the wildcard `Vec` linear scan with
  a trie or radix tree for faster matching at scale.
- **NUMA-aware allocation** — bind workers to CPU cores and allocate buffers
  from local NUMA memory.
- **Lock-free DirectWriter** — replace `Mutex<BytesMut>` with a lock-free SPSC
  or MPSC ring buffer.

## Features

- **TLS** — accept TLS connections (rustls). Required for any non-loopback use.
- **Queue groups** — distribute messages across subscribers in a group.
- **Request-reply** — inbox-based request-reply with timeout.
- **UNSUB max** — auto-unsubscribe after N messages.
- **Multiple hub connections** — connect to more than one upstream hub.
- **Leaf node solicited** — accept inbound leaf connections from other servers.

## Operational

- **Metrics endpoint** — HTTP `/varz` or Prometheus endpoint for connections,
  messages, bytes, subscriptions.
- **Config file** — load host/port/hub/workers from a TOML or JSON file instead
  of CLI flags.
- **Graceful drain** — stop accepting new connections, drain in-flight messages,
  then shut down.
- **Logging** — structured logging with configurable levels.
- **Signal handling** — SIGHUP reload, SIGTERM graceful shutdown.

## Platform

- **Cross-platform portability** — run on any OS/arch, from Raspberry Pi to
  CDN edge. Extract a reactor trait, add poll()/kqueue/IOCP/WASI backends.
  See [portability.md](portability.md) for the full design direction.
