# ADR-002: DirectWriter for Fan-Out

**Status:** Accepted

## Context

The initial cross-worker message delivery used an mpsc channel per subscriber.
Each PUB with N matching subscribers required N channel sends, each involving
an allocation and a memory fence. On hub→leaf workloads this was the bottleneck,
achieving only ~50% of Go throughput.

The Go nats-server writes directly into each subscriber's output buffer under
a lock. This avoids per-message allocation and amortises synchronisation cost
across the entire fan-out.

## Decision

Replace per-subscriber channels with `DirectWriter`: a `Mutex<BytesMut>` shared
between the writer (any thread doing fan-out) and the reader (the owning worker
thread). Each worker has one `eventfd`; all connections on that worker share it.
Fields:

- `buf: Arc<Mutex<BytesMut>>` — shared write buffer
- `has_pending: Arc<AtomicBool>` — set by writer, cleared by worker
- `event_fd: Arc<OwnedFd>` — worker-level eventfd (not per-connection)

Fan-out: lock → `MsgBuilder::build_msg()` into buf → unlock → set `has_pending`.
The owning worker scans `has_pending` on eventfd wake and drains buffers to
sockets.

## Consequences

- **Positive:** Hub→leaf throughput went from 50% to 117% of Go. The lock is
  held only for a memcpy, so contention is low.
- **Positive:** One eventfd per worker instead of one channel per subscriber
  reduces file descriptor count and syscall volume.
- **Positive:** Same-worker delivery skips the eventfd entirely —
  `flush_pending()` drains buffers inline.
- **Negative:** Mutex contention is possible under extreme fan-out from many
  concurrent publishers to the same subscriber. Not yet observed in benchmarks.
- **Negative:** Tighter coupling between the subscription layer and the worker
  loop compared to channel-based decoupling.
