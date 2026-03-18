# ADR-001: Raw epoll over Tokio

**Status:** Accepted

## Context

The initial implementation used Tokio for async I/O. Profiling showed 7–10%
of CPU time spent in the Tokio runtime: task scheduling, waker management,
and multi-layer polling. On a pub-only benchmark the Rust server achieved
~95% of Go nats-server throughput, with the async runtime as the primary gap.

open-wire has a simple concurrency model — N workers each
multiplexing many connections — that maps directly to epoll. There is no need
for Tokio's general-purpose task scheduler, timers, or spawn model.

## Decision

Replace Tokio with raw Linux epoll via libc. Each worker thread owns one epoll
instance and runs a blocking `epoll_wait` loop. Client sockets are set
non-blocking and registered with `EPOLLIN | EPOLLET`-style flags (actually
level-triggered). An eventfd per worker handles cross-thread wakeups.

## Consequences

- **Positive:** Pub-only throughput went from 95% to 100% of Go. Removed the
  largest single source of overhead.
- **Positive:** Simpler mental model — no futures, no pinning, no Send bounds.
  Each worker is a straightforward event loop.
- **Positive:** Eliminated Tokio, mio, and socket2 from the dependency tree.
- **Negative:** Linux-only. macOS and Windows require separate backends
  (kqueue, IOCP) — see [backlog](../backlog.md).
- **Negative:** Must manually handle partial reads/writes, buffer management,
  and connection lifecycle that Tokio previously abstracted.
