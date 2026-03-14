# ADR-007: Sharded Cluster Mode (Spike)

**Status:** Spike / Exploration
**Date:** 2026-03-14

## Context

open-wire currently supports two modes: standalone (single node) and leaf node (connects
upstream to a hub). To operate as a full cluster, we need multi-node coordination.

NATS uses **full mesh clustering** — every node connects to every other node, every node
knows every subscription. This works well for small clusters (3-5 nodes) but has known
scaling limits with high subscription counts.

This ADR explores an alternative: **subject-range sharding** inspired by CockroachDB's
range-based partitioning, where each node is authoritative for a subset of the subject
namespace.

## How NATS Full Mesh Clustering Works

```
        ┌──────┐  RS+/RS-  ┌──────┐
        │Node A├───────────►│Node B│
        └──┬───┘            └──┬───┘
           │     RS+/RS-       │
           │    ┌──────┐       │
           └───►│Node C│◄──────┘
                └──────┘
```

- **Full mesh topology**: N nodes = N*(N-1)/2 TCP connections
- **Subscription propagation**: RS+/RS- flooded to ALL routes on every unique subject
  gaining/losing its first/last subscriber
- **Message routing**: RMSG sent only to routes with matching interest (not flooded)
- **One-hop limit**: Messages from routes are never re-forwarded to other routes
- **Subscription collapse**: 1000 local subs on "foo" = 1 RS+ per route (ref-counted)

### NATS Overhead Model

| Operation | Cost |
|-----------|------|
| New unique subscription | O(N-1) RS+ messages |
| Publish (fan-out F across K nodes) | O(K) RMSG sends |
| Memory per node | O(S) — full interest graph, S = total unique subjects |
| Route connections | O(N^2) |
| Route initial sync | O(S) RS+ messages per new route |

### NATS Super-cluster (Gateways)

For scale-out, NATS uses **gateways** connecting separate clusters:

- **Optimistic mode** (default): forward everything, learn what's NOT wanted
- **Interest-only mode**: triggered when too many no-interest responses accumulate;
  switches to explicit positive interest (like a route, but per-account)
- Mode is tracked per-account per-gateway — automatic transition

This is the closest NATS gets to demand-based routing.

## The Wildcard Problem

The central challenge for any sharded pub/sub system. NATS supports:

- `*` — matches exactly one token: `orders.*` matches `orders.us` not `orders.us.east`
- `>` — matches one or more tokens: `orders.>` matches `orders.us`, `orders.us.east.123`

**Why wildcards break hash-based sharding:**

```
Subject              Hash   Shard
─────────────────────────────────
orders.us           0x3A    Shard 1
orders.eu           0x7F    Shard 2
orders.us.east      0xB2    Shard 3
```

A subscription to `orders.>` must receive messages from shards 1, 2, AND 3. But the
subscriber connects to only one node.

**Redis 7 proved this is fundamental**: they added sharded pub/sub (SPUBLISH/SSUBSCRIBE)
but it **does not support pattern subscriptions**. Classic PSUBSCRIBE still broadcasts to
all nodes. Two completely separate systems.

## Proposed Design: Subject-Prefix Sharding

### Core Idea

Shard by subject prefix (first N tokens, default N=1). The first token determines the
**owning shard**. The shard owner is authoritative for ALL subjects under that prefix.

```
Prefix "orders"    → Shard A  (owns orders.us, orders.eu, orders.*.fulfilled, etc.)
Prefix "inventory" → Shard B
Prefix "payments"  → Shard A  (multiple prefixes per shard)
```

Unlike hash-based sharding, this preserves wildcard locality:
- `orders.>` → routes to Shard A only (the owner of "orders")
- `orders.*` → routes to Shard A only
- `>` → must broadcast to ALL shards (but this is rare in practice)

### Architecture

```
                    ┌─────────────────┐
                    │  Metadata Raft   │
                    │  Group (3 nodes) │
                    │                  │
                    │ Prefix → Shard   │
                    │ mapping + leader │
                    │ election         │
                    └────────┬────────┘
                             │ prefix assignments
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌──────────┐   ┌──────────┐   ┌──────────┐
        │ Shard A  │   │ Shard B  │   │ Shard C  │
        │ (leader) │   │ (leader) │   │ (leader) │
        │          │   │          │   │          │
        │ orders.* │   │ invent.* │   │ user.*   │
        │ payment.*│   │ ship.*   │   │ auth.*   │
        │          │   │          │   │          │
        │ SubList  │   │ SubList  │   │ SubList  │
        │ (local)  │   │ (local)  │   │ (local)  │
        └──────────┘   └──────────┘   └──────────┘
             │              │              │
         clients        clients        clients
```

### Component Breakdown

#### 1. Metadata Raft Group

A small Raft group (3 or 5 nodes) managing cluster-wide metadata:

```
Raft Log:
  1: ASSIGN_PREFIX { prefix: "orders", shard: A }
  2: ASSIGN_PREFIX { prefix: "inventory", shard: B }
  3: SPLIT_PREFIX  { prefix: "orders", into: ["orders.us", "orders.eu"] }
  4: MOVE_PREFIX   { prefix: "orders.us", from: A, to: C }
```

This is low-churn state — prefix assignments change only on rebalancing or node
failure, not on every SUB/UNSUB. Similar to CockroachDB's Meta Range or Kafka's
controller.

#### 2. Shard Leaders (Leaseholders)

Each shard leader owns a set of subject prefixes and is authoritative for:
- Maintaining the SubList for its prefix range
- Routing published messages to matching subscribers
- Forwarding messages to other shards when cross-shard subscribers exist

Like CockroachDB's leaseholder, the shard leader handles all operations without
consensus for each message. Raft is only for metadata and leader election.

#### 3. Client Connection Routing

Clients connect to any node. The node acts as a **gateway**:

- **PUB "orders.us.123"**: Gateway looks up "orders" prefix → Shard A. Forward to A.
  If the gateway IS shard A, route locally (zero-hop fast path).
- **SUB "orders.>"**: Gateway looks up "orders" prefix → Shard A. Register sub on A.
- **SUB ">"**: Gateway registers sub on ALL shards (broadcast).

### The Publish Path

```
Client → PUB orders.us.123 payload
  │
  ▼
Gateway Node
  │ lookup prefix "orders" → Shard A
  │
  ├─── Am I Shard A? ──► YES: local SubList match → deliver to local subs
  │                              + forward to remote subs (DirectWriter)
  │
  └─── NO: forward to Shard A leader
              │
              ▼
        Shard A leader: SubList match → deliver
```

Best case (client connected to owning shard): **zero network hops**, same as
standalone. Worst case (client on wrong shard): **one hop** to shard leader.

### The Subscribe Path

```
Client → SUB orders.> 1
  │
  ▼
Gateway Node
  │ parse subject → prefix "orders"
  │ lookup prefix → Shard A
  │
  ├─── Am I Shard A? ──► YES: add to local SubList
  │
  └─── NO: register cross-shard subscription on Shard A
              (Shard A adds a remote subscriber entry)
```

For wildcard `>`:
```
Client → SUB > 1
  │
  ▼
Gateway Node
  │ parse subject → global wildcard
  │ register on ALL shards (broadcast)
```

### Cross-Shard Wildcard Subscriptions

When a subscription spans multiple shards, it is registered as a **remote subscriber**
on each relevant shard. The shard leader treats it like any other subscriber but
delivers via a cross-shard forwarding channel (similar to NATS route RMSG).

```
Shard A SubList:
  "orders.us"  → [local_conn_1, local_conn_2]
  "orders.>"   → [local_conn_3, remote(Shard_B, conn_7)]  ← cross-shard wildcard
```

## Comparison: Full Mesh vs Sharded

### Subscription Propagation

| Aspect | NATS Full Mesh | Sharded |
|--------|---------------|---------|
| Exact sub "orders.us" | RS+ to N-1 nodes | Register on 1 shard only |
| Wildcard "orders.>" | RS+ to N-1 nodes | Register on 1 shard only |
| Global wildcard ">" | RS+ to N-1 nodes | Register on ALL shards |
| Memory per node | O(S) — all subs | O(S/K) — only owned subs + cross-shard wildcards |
| SUB/UNSUB overhead | O(N) messages | O(1) for exact, O(1) for prefix-scoped wildcard |

### Message Routing

| Aspect | NATS Full Mesh | Sharded |
|--------|---------------|---------|
| Publish to exact subject | Forward to nodes with interest | Route to shard owner, deliver locally |
| Fan-out to F subscribers on K nodes | K RMSG sends | If all on same shard: local. If cross-shard: 1 hop to owner + deliver |
| Client on wrong node | Always 0-1 hops (any node has full interest) | 0-1 hops (forward to shard owner) |
| Routing decision | Local (full interest graph) | Local if on owning shard; 1 lookup + forward otherwise |

### Scalability

| Aspect | NATS Full Mesh | Sharded |
|--------|---------------|---------|
| Max nodes | ~5-7 practical | 10s-100s (prefix-based, not N^2 connections) |
| Connections between nodes | N*(N-1)/2 full mesh | K shard groups, each 3-5 nodes + gateway mesh |
| 100K unique subscriptions | 100K RS+ per new route join | Distributed across shards, each sees ~100K/S |
| 1M unique subscriptions | Severe route propagation delay | Manageable if well-distributed across prefixes |
| Adding a node | Full interest sync (O(S) RS+ messages) | Only shard reassignment + prefix-local sync |

### The High-Subscription Cross-Talk Scenario

**Scenario**: 100K unique subscriptions spread across 10 nodes.

**NATS Full Mesh (10 nodes):**
- Each node stores all 100K subscriptions
- Total subscription state: 100K × 10 = 1M entries cluster-wide
- Each new unique sub generates 9 RS+ messages
- Route join: 100K RS+ messages to new node (burst)
- Every node can route every message locally (no forwarding needed for matching)

**Sharded (10 nodes, ~1000 prefixes, ~100 subs per prefix):**
- Each shard stores ~10K subscriptions (its own) + cross-shard wildcards
- If 10% of subs are wildcards spanning multiple shards: ~10K local + ~1K remote entries
- Total subscription state: ~11K × 10 = ~110K entries cluster-wide (vs 1M)
- New unique sub: 1 message to shard owner
- Node join: only reassigned prefix subs need syncing

**Where sharding loses**: If most subscriptions are global wildcards (`>`), every sub
must register on all shards — degrading to full mesh behavior plus the overhead of
cross-shard forwarding. But `>` subscriptions are rare in real-world NATS deployments.

**Where sharding wins**: When subscriptions cluster around subject prefixes (the
common case — `orders.*`, `inventory.*`, `users.*`), each shard sees only its subset,
and subscription propagation is O(1) instead of O(N).

### Cross-Talk Quantification

Define **cross-talk ratio** = fraction of publish operations requiring cross-shard forwarding.

| Workload Pattern | Full Mesh Cross-Talk | Sharded Cross-Talk |
|------------------|---------------------|-------------------|
| All clients on same node | 0% | 0% (if on owning shard) |
| Clients spread evenly, exact subs | ~(N-1)/N per pub | ~(N-1)/N per pub (similar — client may not be on owning shard) |
| Clients spread, prefix-local wildcards | ~(N-1)/N per pub | ~0% (wildcard resolved locally on shard) |
| Global wildcards (">") | Same | Same (broadcast either way) |
| Fan-out x100, all on one node | 0% forwarding, local delivery | 0% if on owning shard |
| Fan-out x100, spread across nodes | Forward to K nodes with subs | Forward to K nodes with subs |

**Key insight**: Sharding's advantage is NOT in reducing message forwarding (that's
similar for both). It's in reducing **subscription state propagation** — O(1) per sub
instead of O(N), and O(S/K) memory per node instead of O(S).

## Implementation Sketch

### Phase 1: Metadata Raft

Minimal Raft implementation for prefix → shard assignment:

```rust
/// Raft log entry for cluster metadata
enum MetaOp {
    AssignPrefix { prefix: String, shard_id: u64 },
    RemovePrefix { prefix: String },
    SplitPrefix { prefix: String, into: Vec<String> },
    AddNode { node_id: u64, addr: SocketAddr },
    RemoveNode { node_id: u64 },
}

/// Applied state machine
struct PrefixMap {
    /// prefix → (shard_id, leader_node)
    assignments: BTreeMap<String, ShardAssignment>,
    nodes: HashMap<u64, NodeInfo>,
}

impl PrefixMap {
    /// Find the owning shard for a subject
    fn lookup(&self, subject: &str) -> Option<&ShardAssignment> {
        let prefix = first_token(subject);
        self.assignments.get(prefix)
            .or_else(|| self.assignments.get("*"))  // default shard
    }
}
```

### Phase 2: Shard-Aware Routing

Extend the worker to route publishes to the correct shard:

```rust
// In worker publish handling:
fn handle_pub(&mut self, subject: &[u8], payload: &[u8]) {
    let prefix = first_token(subject);
    if self.owns_prefix(prefix) {
        // Fast path: local SubList match + deliver
        self.sub_list.for_each_match(subject, |sub| { ... });
    } else {
        // Forward to shard owner
        let shard = self.prefix_map.lookup(prefix);
        self.forward_to_shard(shard, subject, payload);
    }
}
```

### Phase 3: Cross-Shard Subscriptions

Handle wildcard subscriptions that span multiple shards:

```rust
fn handle_sub(&mut self, subject: &[u8], sid: &[u8]) {
    if is_global_wildcard(subject) {
        // Register on ALL shards
        for shard in self.all_shards() {
            shard.register_remote_sub(subject, self.node_id, conn_id);
        }
    } else {
        let prefix = wildcard_prefix(subject);  // "orders" from "orders.>"
        let shard = self.prefix_map.lookup(prefix);
        if self.owns_prefix(prefix) {
            self.sub_list.add(subject, subscriber);
        } else {
            shard.register_remote_sub(subject, self.node_id, conn_id);
        }
    }
}
```

### Raft Library Options

| Library | Pros | Cons |
|---------|------|------|
| `openraft` (Rust) | Pure Rust, well-maintained, async | Async (we're sync) |
| `raft-rs` (TiKV) | Battle-tested (TiKV), sync-friendly | Heavy, C++ heritage |
| Custom minimal | No dependencies, fits our sync model | Significant effort |
| Use NATS subjects (like JetStream) | Piggyback on existing connections | Requires cluster connection infra |

Recommendation: Start with `openraft` behind a sync wrapper, or implement a minimal
Raft (metadata-only, low-churn log) given our no-async-runtime constraint.

## Trade-off Summary

| Dimension | Full Mesh (NATS) | Sharded (Proposed) |
|-----------|-----------------|-------------------|
| Simplicity | Simple: every node identical | Complex: shard assignment, routing, rebalancing |
| Subscription overhead | O(N) per unique sub | O(1) per sub (O(K) for global wildcards) |
| Memory per node | O(S) — full interest | O(S/K) + cross-shard wildcards |
| Publish latency | Always local match | Local if on owning shard; +1 hop otherwise |
| Cluster size | 3-5 nodes | 10s-100s of nodes |
| Wildcard support | Native, free | Native within prefix; global wildcards costly |
| Node failure impact | Any node can serve any client | Shard reassignment needed; prefix unavailable during election |
| Client reconnection | Connect to any node | Connect to any node (gateway routing) |
| Operational complexity | Low | Higher: shard balancing, prefix splitting |

## Recommendation

**For open-wire's current scale**: Full mesh (NATS-style) clustering is the right first
step. It's simpler, well-understood, and sufficient for 3-5 node clusters.

**When to consider sharding**: When targeting 10+ node clusters OR when subscription
counts exceed 100K unique subjects with high churn rate. The subscription propagation
savings become significant at this scale.

**Spike priority**: Implement full mesh first (it's simpler and validates the routing
infrastructure), then layer sharding on top as an alternative routing strategy using the
same inter-node connection infrastructure.

## References

- [NATS Clustering](https://docs.nats.io/running-a-nats-service/configuration/clustering)
- [NATS Super-cluster Gateways](https://docs.nats.io/running-a-nats-service/configuration/gateways)
- [NATS Cluster Protocol](https://docs.nats.io/reference/reference-protocols/nats-server-protocol)
- [CockroachDB Architecture](https://www.cockroachlabs.com/docs/stable/architecture/overview.html)
- [Redis Cluster Pub/Sub Limitations](https://github.com/redis/redis/issues/2672)
- [Redis 7 Sharded Pub/Sub](https://redis.io/docs/latest/develop/pubsub/)
- [Kafka Consumer Group Protocol](https://developer.confluent.io/courses/architecture/consumer-group-protocol/)
- [Google Cloud Pub/Sub Architecture](https://docs.google.com/pubsub/architecture)
- [NATS JetStream Subject Partitioning](https://docs.nats.io/nats-concepts/subject_mapping)
