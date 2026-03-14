# ADR-010: Full-Mesh Clustering

**Status:** Accepted
**Date:** 2026-03-14

## Context

open-wire supports standalone mode and leaf-node mode (outbound to a hub,
inbound from leafs). To form a self-contained cluster without depending on
an external hub server, we need **route connections** between peers — the
NATS "full mesh" clustering model.

In full mesh, every node connects to every other node, every node knows
every subscription across the cluster, and messages are forwarded only to
peers with matching interest (one-hop limit).

## Decision

Implement full-mesh clustering behind a `cluster` Cargo feature (opt-in,
not in default features). The implementation follows the NATS route protocol
wire format and mirrors the existing modular structure: just as
`leaf_handler.rs` handles inbound leaf protocol, `route_handler.rs` handles
route protocol.

### Wire Protocol

```
RS+ <account> <subject>\r\n                    # route subscribe
RS+ <account> <subject> <queue> <weight>\r\n   # queue subscribe
RS- <account> <subject>\r\n                    # route unsubscribe
RMSG <account> <subject> [reply] <size>\r\n    # route message
     <payload>\r\n
INFO {...}\r\n   CONNECT {...}\r\n   PING\r\n   PONG\r\n
```

`$G` is the default global account (single-account mode).

### Architecture

```
                    ┌──────────┐
          RS+/RS-   │  Node B  │   RS+/RS-
        ┌──────────►│          │◄──────────┐
        │    RMSG   │  :4248   │   RMSG    │
        │           └──────────┘           │
   ┌────┴─────┐                      ┌────┴─────┐
   │  Node A  │       RS+/RS-        │  Node C  │
   │          │◄─────────────────────►│          │
   │          │       RMSG           │          │
   └──────────┘                      └──────────┘
```

Each node:
- Listens on a **cluster port** for inbound route connections
- Makes outbound connections to **seed nodes** (routes config)
- Propagates RS+/RS- for local subscription changes to all route peers
- Forwards messages (RMSG) only to routes with matching interest
- **One-hop rule**: messages received from a route are never re-forwarded
  to other routes

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| `RouteOp` | `nats_proto.rs` | Route protocol parser (RS+/RS-/RMSG/INFO/CONNECT/PING/PONG) |
| `RouteHandler` | `route_handler.rs` | Dispatch RS+/RS-/RMSG for inbound route connections |
| `RouteConnManager` | `route_conn.rs` | Outbound route connections (supervisor + reader/writer threads) |
| `ConnExt::Route` | `handler.rs` | Route connection state (SID counter, SID map) |
| `Subscription.is_route` | `sub_list.rs` | Route subscriptions delivered via RMSG |
| `DirectWriter::write_rmsg` | `sub_list.rs` | Format RMSG wire bytes for route delivery |

### Connection Types

**Inbound routes** are accepted on the cluster port and managed by the
worker epoll loop (multiplexed like client connections). The handshake:
1. Server sends INFO
2. Peer sends INFO + CONNECT + PING
3. Server sends PONG, registers DirectWriter, exchanges RS+ for existing subs
4. Enter Active phase — route ops dispatched via `RouteHandler`

**Outbound routes** are managed by `RouteConnManager` which spawns a
supervisor thread per seed URL. Each supervisor maintains a persistent
connection with reconnect + exponential backoff, spawning reader + writer
thread pairs per connection.

### One-Hop Enforcement

The one-hop rule is enforced via `skip_routes` parameter in
`deliver_to_subs()`. When called from `RouteHandler::handle_rmsg()`,
`skip_routes=true` ensures route subscriptions are skipped — messages
from routes are delivered only to local client and leaf subscribers.

### Subscription Propagation

When a client or leaf subscribes/unsubscribes, the change is propagated
to all route peers via `propagate_route_sub()` / `propagate_route_unsub()`
in `handler.rs`. These write RS+/RS- to all registered `route_writers`.

### Configuration

```toml
# Cargo.toml
[features]
cluster = []   # opt-in
```

```
# nats-server compatible config file
cluster {
  name: my-cluster
  listen: 0.0.0.0:4248
  routes = [
    "nats-route://host2:4248"
    "nats-route://host3:4248"
  ]
}
```

CLI flags: `--cluster-port`, `--cluster-seeds`, `--cluster-name`.

## Consequences

**Positive:**
- Self-contained clustering without external hub server dependency
- Compatible with NATS route protocol (RS+/RS-/RMSG)
- Feature-gated — zero overhead when not compiled in
- Mirrors existing leaf pattern — consistent code structure
- 162-192% of Go nats-server throughput on cluster benchmarks

**Negative:**
- Full mesh scales to ~5-7 nodes (O(N^2) connections)
- No route dedup by server_id yet (duplicate connections possible)
- No gossip-based peer discovery (explicit seeds required)
- `//` in config file URLs requires quoting (parser treats `//` as comment)

## Related

- [ADR-007: Sharded cluster mode (spike)](007-sharded-cluster-mode.md) —
  future alternative for larger clusters
- [ADR-009: Leaf and hub feature flags](009-leaf-hub-feature-flags.md) —
  the feature-gating pattern this follows
