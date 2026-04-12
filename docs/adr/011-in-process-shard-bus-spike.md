# ADR-011: In-Process ShardBus (Spike — Not Adopted)

**Status:** Spike — not adopted
**Date:** 2026-04-12
**Branch:** `feat/shard-spike`

## Context

open-wire's N-worker epoll model scales by giving each worker thread its own epoll
instance that multiplexes many connections. Within a single worker, message delivery
is synchronous: the worker calls `write_msg` for each matching subscriber.

The hypothesis was: what if multiple server instances ran in the same process, each
owning a disjoint set of clients, and exchanged messages via a lock-free in-process
bus instead of TCP? This could, in theory, give better latency than loopback TCP and
better CPU locality than the current cross-worker eventfd delivery path.

This spike built the mechanism and benchmarked it against a single-server baseline.

## What Was Built

A `ShardBus` type connecting N in-process `Server` instances via bounded crossbeam
channels (depth 65536) and eventfd wake-up:

```
  Server-0 (worker)          Server-1 (worker)
  ┌──────────────┐           ┌──────────────┐
  │  clients     │  channel  │  clients     │
  │  SubList     │──────────►│  SubList     │
  │              │◄──────────│              │
  └──────────────┘  eventfd  └──────────────┘
```

- `broadcast()` is non-blocking: if the peer channel is full, the message is dropped.
- `handle_shard_event()` in the worker epoll loop drains the inbound channel and
  delivers to local subscribers. Eventfd is read before clearing `has_pending` to
  prevent a race where a concurrent write lands between the clear and the read.
- `build_shard_buses(n)` wires N buses into a full mesh (N-1 outbound senders per bus).
- `Server::attach_shard_bus()` wires a bus before `run()`.

The spike also uncovered and fixed an unrelated bug: `backpressure_check()` in
`write_msg` used a hardcoded 32 MB HWM independent of the configured `max_pending`.
This caused bimodal throughput in the benchmark (0.04 vs 1.2 Mmsg/s) when
`max_pending` was raised to 512 MB. Fixed by scaling HWM from `max_pending / 2`.

## Benchmark Results

3-shard in-process benchmark, 128B messages, 8s, 6-core host:

```
local        pub=1.09 Mmsg/s  sub=1.09 Mmsg/s  — pub+sub on same shard, no bus
cross-shard  pub=0.64 Mmsg/s  sub=1.21 Mmsg/s  — pub→shard-0, 2 subs on shards 1+2
no-bus       pub=3.71 Mmsg/s  sub=0.00 Mmsg/s  — pub ceiling with no delivery
```

Cross-shard delivery: **0.60 Mmsg/s per subscriber**.

Diagnostic from best run:
```
broadcast ok=9973742  drop=342946  (97% delivery)
drained=9973742  delivered=9973742  (all queued messages delivered)
```

Diagnostic from a bad run (channel backlogged):
```
broadcast ok=1048990  drop=13596642  (7% delivery — 93% silent drop)
```

## Problems Found

### 1. Silent drops with no backpressure propagation

The bounded channel drops messages silently when the receiving worker can't drain
fast enough. The TCP publisher has already received its ACK; it has no way to know
the message was discarded. Drop rates of 93% were observed in contended runs.

This is the inverse of the route backpressure design (`read_budget` + TCP flow
control), which guarantees delivery or explicit throttling of the publisher before
any message is lost. A bus that drops messages without signalling the producer is
not suitable for production use.

Fixing this would require propagating the congestion signal back across shard
boundaries to the publishing worker and into that connection's `read_budget`. This
is non-trivial: the publishing worker is on a different Server instance, and the
congestion path would need to cross shard boundaries synchronously.

### 2. The use case is already covered

The existing architecture already handles the problems the ShardBus was meant to
solve:

| Goal | ShardBus | Existing solution |
|------|----------|-------------------|
| Scale CPU utilisation across cores | N separate server instances | N worker threads, each owns its epoll loop |
| Cross-worker message delivery | Crossbeam channel + eventfd | `MsgWriter` + shared eventfd (same mechanism, less complexity) |
| Cross-machine message routing | Out of scope | Full-mesh TCP routes with `read_budget` backpressure |

The single-process N-worker model already distributes work across CPUs via OS
scheduling. The `MsgWriter + eventfd` path already delivers messages between workers
in-process without an extra bus tier.

### 3. Throughput is bounded by TCP, not by routing overhead

The benchmark publisher was bottlenecked at 0.64–0.90 Mmsg/s by the TCP connection
from the benchmark client to shard-0 — not by the bus itself. In production, clients
connect via TCP regardless; removing the bus would not meaningfully increase the
publisher rate.

## Decision

**Not adopted.** The in-process ShardBus mechanism is correct (eventfd ordering,
channel delivery) but the silent-drop problem makes it unsuitable for a reliable
message broker without significant additional engineering. The use case it targets
is already handled adequately by the existing N-worker architecture and TCP mesh
clustering.

The `backpressure_check` HWM fix (scaling from `max_pending` rather than hardcoded
32 MB) was retained as it improves production correctness.

## What Would Change the Decision

- If a workload emerged where loopback TCP route latency was a measurable bottleneck
  and in-process delivery latency mattered (sub-microsecond delivery SLA).
- If a reliable backpressure path could be established: when the bus channel fills,
  propagate congestion back to the publishing worker's `read_budget` in the same
  way route congestion works today.
- If a use case required strict CPU/NUMA affinity per subscriber group — isolating
  each shard to a NUMA domain and using the bus to cross domains.

None of these apply to current workloads.

## Related

- [ADR-007: Sharded Cluster Mode](007-sharded-cluster-mode.md) — distributed prefix
  sharding with Raft; a separate and more complex design for large clusters.
- [ADR-002: Direct Writer](002-direct-writer.md) — the `MsgWriter + eventfd` design
  that already solves in-process cross-worker delivery.
- [ADR-010: Full-Mesh Clustering](010-full-mesh-clustering.md) — TCP route mesh with
  per-connection backpressure; the production cross-node delivery mechanism.
