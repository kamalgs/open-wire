# ADR-007: Batched Metrics Flush per Epoll Iteration

**Status:** Accepted

## Context

After adding per-message `counter!()` calls for `messages_received_total`,
`messages_received_bytes`, `messages_delivered_total`, and
`messages_delivered_bytes` (ADR-006), benchmarks showed a severe regression
in the pub-only scenario: Rust throughput dropped from 100% to 62% of Go.

Each `counter!().increment()` call performs an atomic fetch-add (~2ns). In the
pub-only path (no subscribers, no I/O), the tight parse loop processes millions
of PUBs per second. Two atomic increments per PUB (received count + received
bytes) added measurable overhead. In the fan-out path, the cost was worse:
2 additional atomics per subscriber per message.

The key insight is that each worker thread is single-threaded — there is no
concurrent access to the worker's own counters within an epoll iteration. Only
the global recorder needs atomics, and it only needs to be updated once per
batch, not once per message.

## Decision

Replace per-message `counter!()` calls on the hot path with plain `u64` fields
on the `Worker` struct:

```rust
struct Worker {
    // ... existing fields ...
    msgs_received: u64,
    msgs_received_bytes: u64,
    msgs_delivered: u64,
    msgs_delivered_bytes: u64,
}
```

In the PUB handler and `for_each_match` fan-out closure, increment these with
plain `+=` (zero cost — single `add` instruction, no atomic, no fence).

After each epoll iteration, call `flush_metrics()` which conditionally flushes
non-zero accumulators to the global `metrics` recorder:

```rust
fn flush_metrics(&mut self) {
    if self.msgs_received > 0 {
        counter!("messages_received_total", ...).increment(self.msgs_received);
        counter!("messages_received_bytes", ...).increment(self.msgs_received_bytes);
        self.msgs_received = 0;
        self.msgs_received_bytes = 0;
    }
    // ... same for delivered ...
}
```

This amortises N per-message atomic writes down to at most 4 per epoll batch.

Infrequent metrics (connections, subscriptions, slow consumers, auth failures)
remain direct `counter!()` / `gauge!()` calls since they fire on connect/
disconnect events, not per-message.

## Consequences

- **Positive:** Pub-only throughput recovered from 62% to 92% of Go (within
  noise of pre-observability baseline).
- **Positive:** Fan-out scenarios improved significantly beyond the previous
  baseline — x5 went from 124% to 213%, WS x10 from 175% to 324%.
- **Positive:** Zero per-message overhead in the hot path. The `+=` on a local
  `u64` compiles to a single register add.
- **Negative:** Metric values are approximate — they lag behind reality by up
  to one epoll batch (typically sub-millisecond). For monitoring and alerting
  this is negligible.
- **Negative:** If a worker crashes mid-batch, unflushed counts are lost. This
  is acceptable since a worker crash implies connection loss anyway.
