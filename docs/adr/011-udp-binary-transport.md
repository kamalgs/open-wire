# ADR-011: UDP Binary Transport for Inter-Cluster Communication

**Status:** Proposed
**Date:** 2026-03-20

## Context

open-wire's inter-cluster communication uses TCP with the NATS text protocol
(RMSG/RS+/RS-). Every forwarded message traverses:

```
source socket → read() into userspace → parse text header → format text header
→ write() to each dest socket → kernel copies to TCP send buffer
```

For N destination peers, the payload is copied N+1 times through userspace. The
text protocol requires byte-scanning for `\r\n` delimiters and ASCII integer
parsing. TCP's stream semantics mean messages can span segment boundaries,
making kernel-level forwarding (splice/tee) impractical without complex framing.

### Key bottlenecks

1. **Payload copies**: N userspace round-trips for N-peer fan-out
2. **Text parsing**: ~20ns per header scan vs ~2ns for fixed-offset binary read
3. **TCP stream framing**: no message boundaries, can't splice without parsing
4. **Head-of-line blocking**: one lost TCP segment stalls all messages behind it
5. **Per-message syscalls**: individual read()/write() per message

### Opportunity

For inter-cluster links (which we fully control), we can use a binary protocol
over UDP. UDP datagrams are message-framed by the kernel — each `recvmsg()`
returns exactly one complete datagram. This makes zero-copy forwarding via
`sendmsg(MSG_ZEROCOPY)` trivial: peek at a fixed-size binary header for routing,
forward the entire datagram without interpreting the payload.

The [`rusty_enet`](https://github.com/jabuwu/rusty_enet) crate provides a pure
Rust port of the ENet reliable UDP library — battle-tested game networking
(reliable + unreliable channels, fragmentation, congestion control) in ~40 lines
of integration code. Using it from the start eliminates the need for a separate
reliability phase and solves MTU fragmentation out of the box.

## Decision

Implement a **UDP binary transport** as an opt-in feature (`udp-transport`)
in a **self-contained module directory** (`src/udp/`). The existing TCP text
cluster transport remains the default and is unaffected. The UDP transport is
negotiated during route INFO exchange and runs as a parallel data channel
alongside the TCP control channel. Reliability is provided by `rusty_enet`
from day one.

### Design principles

1. **Separate module**: `src/udp/` directory with its own types, no coupling to
   existing cluster code except at well-defined hook points
2. **Feature-gated**: `#[cfg(feature = "udp-transport")]` — zero cost when disabled
3. **Hybrid TCP+UDP**: TCP handles control plane (SUB/UNSUB, PING/PONG, handshake);
   UDP handles data plane (message forwarding)
4. **enet from day one**: no raw UDP phase — `rusty_enet` provides reliability,
   fragmentation, and congestion control with minimal integration effort

## Binary wire protocol

### Subject tokenization

Instead of sending raw subject strings on every message, subjects are tokenized
using a **dynamically built vocabulary** negotiated per connection pair. This
compresses repeated subjects (common in NATS workloads where thousands of messages
flow through the same subject patterns) from variable-length UTF-8 strings to
fixed 2-byte token IDs.

#### Token table

Each peer maintains a bidirectional token table:

```
Token ID (u16)  →  Subject string
0x0001          →  "orders.new"
0x0002          →  "orders.shipped"
0x0003          →  "events.>"
...
0x0000          →  (reserved: inline subject, no tokenization)
```

#### Assignment protocol

Tokens are assigned **by the sender** and communicated inline:

```
First occurrence of "orders.new":
  Message uses token_id=0, subject sent inline (full bytes)
  + DEFINE_TOKEN entry in datagram header: {id=1, subject="orders.new"}

Subsequent occurrences:
  Message uses token_id=1, no subject bytes (2 bytes instead of 10)
```

The DEFINE_TOKEN entries are piggybacked on data datagrams — no extra round-trip.
The receiver builds its lookup table incrementally. Token definitions are sent on
the **reliable channel** (once enet is integrated) to guarantee delivery.

#### Compression ratio

Typical NATS subject: 15-30 bytes (e.g., `svc.orders.created`).
Tokenized: 2 bytes (u16 token ID).
For a steady-state workload where most subjects are already in the table,
this is an **8-15x reduction** in subject bytes per message.

#### Overflow and eviction

- Token space: 65,534 usable IDs (0x0001-0xFFFF). Sufficient for most workloads.
- If the table fills, least-recently-used eviction with a REVOKE_TOKEN message.
- Fallback: token_id=0 means "subject inline" — always works, just no compression.

### Datagram format

Each enet packet carries one batch. Reliability, sequencing, and fragmentation
are handled by enet's transport layer — our datagram header only contains
application-level framing.

```
Batch header (4 bytes, fixed):

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├───────────────────────────────────┼───────────────────────────────┤
│          magic (0xCA 0xFE)        │  msg_count(1B)│ tok_defs(1B) │
├───────────────────────────────────────────────────────────────────┤
│  token definitions (variable, tok_defs entries)...               │
│  messages (variable, msg_count entries)...                       │
└───────────────────────────────────────────────────────────────────┘
```

**4-byte header** — enet handles seq/ack/retransmit, so we don't duplicate that.
The batch header just frames the application payload: how many token definitions
and how many messages follow.

#### Token definition entry (variable length)

```
├───────────────────────────────────┤
│  token_id (2B)  │ subject_len(2B)│
├───────────────────────────────────┤
│  subject bytes (subject_len)...  │
└───────────────────────────────────┘
```

#### Message entry (variable length)

```
├───────────────────────────────────┤
│  type (1B)      │ token_id (2B)  │
├───────────────────────────────────────────────────────────────────┤
│                     payload_len (4B)                             │
├───────────────────────────────────┤
│  reply_len (2B) │  hdr_len (2B)  │
├───────────────────────────────────┤
│  [subject bytes, only if token_id == 0: subject_len(2B) + data] │
│  [reply bytes (reply_len), omitted if 0]                        │
│  [header bytes (hdr_len), omitted if 0]                         │
│  [payload bytes (payload_len)]                                  │
└───────────────────────────────────────────────────────────────────┘

Message header: 11 bytes fixed.

Type byte:
  0x01 = MSG             (subject + reply + payload)
  0x02 = MSG_NOREPLY     (subject + payload, no reply — skip reply_len)
  0x03 = MSG_HEADERS     (subject + reply + headers + payload)

When token_id > 0:
  Subject bytes are OMITTED entirely — receiver looks up from token table.
  Saves subject_len + 2 bytes per message.

When token_id == 0:
  subject_len (2B) + subject bytes are present inline after the fixed header.
```

### Reliability: rusty_enet

[`rusty_enet`](https://github.com/jabuwu/rusty_enet) is a pure Rust port of
[ENet](http://enet.bespin.org/) — the game networking library used in production
since 2002. It provides reliable and unreliable channels over UDP with congestion
control, automatic fragmentation/reassembly, and peer management. MIT licensed,
no C FFI, no async runtime dependency.

**Integration is ~40 lines:**

```rust
// Create host (server or client side)
let socket = UdpSocket::bind("0.0.0.0:9222")?;
let mut host = enet::Host::new(socket, enet::HostSettings {
    peer_limit: 32,
    channel_limit: 2,
    compressor: Some(Box::new(enet::RangeCoder::new())),
    checksum: Some(Box::new(enet::crc32)),
    ..Default::default()
});

// Connect to peer (after TCP negotiation provides peer's UDP address)
let peer = host.connect(peer_addr, 2, 0)?;

// Send — one-liner difference between reliable and unreliable
peer.send(0, &enet::Packet::unreliable_sequenced(data, 0));  // ch 0: messages
peer.send(1, &enet::Packet::reliable(token_def));              // ch 1: token defs

// Receive — poll in a loop (fits into a dedicated thread)
while let Some(event) = host.service()? {
    match event {
        Event::Receive { peer, channel_id, packet } => { /* packet.data() */ }
        Event::Connect { peer, .. } => { /* peer connected */ }
        Event::Disconnect { peer, .. } => { /* peer gone */ }
    }
}
```

**Channel mapping:**

| ENet channel | Reliability | NATS traffic |
|---|---|---|
| 0 | Unreliable sequenced | Message forwarding (MSG type) |
| 1 | Reliable ordered | Token definitions (DEFINE_TOKEN, REVOKE_TOKEN) |

Core NATS PUB is at-most-once — transport-level loss is tolerable. JetStream
adds its own application-level reliability via stream acknowledgments. The
unreliable sequenced channel provides ordering (discards out-of-order packets)
which is sufficient for most workloads.

Token definitions use the reliable channel to guarantee the receiver builds
a consistent vocabulary.

**What enet gives us for free** (things we'd otherwise build ourselves):
- Reliable + unreliable + sequenced delivery modes
- Automatic fragmentation and reassembly for messages > MTU
- Congestion control (won't flood the link)
- Connection keepalives and timeout detection
- Built-in compression (RangeCoder)
- Peer management (connect/disconnect events)

### Batching

Multiple messages are accumulated into a single datagram (up to MTU):

```
Batch accumulation (configurable, default 200µs or MTU-full):

┌─────────────────────────────────────────────────┐
│ datagram header (20B)                           │
│ msg1: token_id=3 (11B hdr + 64B payload)        │
│ msg2: token_id=3 (11B hdr + 128B payload)       │
│ msg3: token_id=7 (11B hdr + 32B payload)        │
│ ...until MTU (~8900B jumbo, ~1400B standard)    │
└─────────────────────────────────────────────────┘
```

With jumbo frames (9000 MTU) and 128B payloads: ~60 messages per datagram.
With standard MTU (1500) and 128B payloads: ~9 messages per datagram.

### Large messages (> MTU)

For messages exceeding one datagram, enet handles fragmentation and reassembly
transparently. No MTU fallback to TCP needed — enet splits large packets into
fragments, sends them individually, and reassembles at the receiver with
automatic retransmit for lost fragments.

## Module structure

```
src/udp/
├── mod.rs              — Public API: UdpTransport, UdpTransportConfig, UdpCmd
├── codec.rs            — Binary encode/decode, zero-copy message parsing
├── token_table.rs      — Subject tokenization: assign, lookup, evict
└── transport.rs        — enet Host wrapper, reader/writer threads, batch accumulation
```

All types are `pub(crate)`. The module exposes:

```rust
/// Configuration for the UDP transport layer.
pub(crate) struct UdpTransportConfig {
    pub udp_port: u16,
    pub batch_interval_us: u64,     // default: 200
    pub enable_reliability: bool,   // false in phase 1
}

/// A UDP data channel to a single route peer.
/// Spawns reader + writer threads, mirrors upstream.rs pattern.
pub(crate) struct UdpTransport {
    /// Send messages to the writer thread for batching + transmission.
    cmd_tx: mpsc::Sender<UdpCmd>,
    /// Shutdown flag (shared with threads).
    shutdown: Arc<AtomicBool>,
}

pub(crate) enum UdpCmd {
    /// Forward a message to this peer.
    Send {
        subject: Bytes,
        reply: Option<Bytes>,
        headers: Option<HeaderMap>,
        payload: Bytes,
    },
    Shutdown,
}
```

### Thread architecture

Mirrors the existing `upstream.rs` and `route_conn.rs` patterns:

```
UdpTransport::new()
  → bind UdpSocket (non-blocking)
  → spawn writer thread: udp_writer_loop()
      receives UdpCmd from mpsc channel
      accumulates messages into batch buffer
      flushes on batch_interval or buffer full
      encodes binary, sendmsg() to peer
  → spawn reader thread: udp_reader_loop()
      recvmmsg() from UdpSocket
      decodes binary header
      for each message: resolve token → subject
      delivers to local subs via deliver_to_subs_upstream_inner()
      accumulates dirty_writers for batch eventfd notification
```

## Touch points in main source tree

Minimal hooks — 5 files modified, all behind `#[cfg(feature = "udp-transport")]`:

### 1. `Cargo.toml` — feature flag + dependency

```toml
[features]
udp-transport = ["cluster"]  # depends on cluster feature

[dependencies]
rusty_enet = { version = "0.4", optional = true }
```

`rusty_enet` is pure Rust (no C FFI), MIT licensed, and compiles with zig
like the rest of the project.

### 2. `src/lib.rs` — module declaration

```rust
#[cfg(feature = "udp-transport")]
pub(crate) mod udp;
```

### 3. `src/server.rs` — config field

Add to `LeafServerConfig`:

```rust
#[cfg(feature = "udp-transport")]
pub cluster_udp_port: Option<u16>,
```

### 4. `src/route_conn.rs` — negotiate + spawn UDP channel

In `build_route_info()`: include `"udp_port"` field when configured.
In `connect_route()`: after TCP handshake, if peer's INFO includes `udp_port`,
spawn `UdpTransport` and store alongside the TCP `DirectWriter`.

```rust
// In connect_route(), after TCP handshake succeeds:
#[cfg(feature = "udp-transport")]
let udp_tx = if let Some(peer_udp_port) = peer_info.udp_port {
    let transport = UdpTransport::new(peer_addr, peer_udp_port, state.clone());
    Some(transport.cmd_tx())
} else {
    None
};
```

### 5. `src/route_handler.rs` — forward via UDP when available

In the RMSG forwarding path, check if a UDP channel exists for the destination
peer. If so, send via `UdpCmd::Send` instead of `DirectWriter::write_rmsg()`.

```rust
// In propagation path:
#[cfg(feature = "udp-transport")]
if let Some(udp_tx) = route_udp_tx {
    udp_tx.send(UdpCmd::Send { subject, reply, headers, payload });
} else {
    writer.write_rmsg(subject, reply, headers, payload);
}
```

## Implementation phases

### Phase 1 — Binary UDP with enet (tracer bullet, end-to-end)

**Goal**: Working cluster transport over binary UDP with reliability.
Side-by-side benchmark vs TCP text protocol.

**Scope**:
- `src/udp/mod.rs` — module root, UdpTransport, UdpCmd
- `src/udp/codec.rs` — binary encode/decode
- `src/udp/token_table.rs` — subject tokenization (assign + lookup)
- `src/udp/transport.rs` — enet Host wrapper, reader/writer threads, batching
- Touch points: Cargo.toml, lib.rs, server.rs, route_conn.rs, route_handler.rs
- Benchmark script: `tests/throughput.sh` — add "Cluster UDP" scenarios

**enet from the start** — adds ~40 lines over raw UdpSocket but provides
reliability, fragmentation, congestion control. No separate reliability phase
needed. Token definitions use enet's reliable channel; message forwarding uses
unreliable sequenced channel.

**Deliverable**: Side-by-side benchmark of cluster pub/sub and fan-out over
TCP text vs UDP binary+enet. Measures both throughput and latency.

### Phase 2 — Zero-copy fan-out

**Goal**: Eliminate payload copies for multi-peer forwarding.

**Scope**:
- `sendmsg(MSG_ZEROCOPY)` for payload forwarding
- `recvmmsg()` / `sendmmsg()` for batch syscall reduction
- Rewrite only the datagram header per destination; payload bytes shared
  via kernel page reference counting

**Deliverable**: Benchmark fan-out scenarios (pub on A, sub on B+C+D).

## Benchmark plan

Add to `tests/throughput.sh` (full mode):

```
Scenario 20: Cluster UDP pub/sub     — pub on A, sub on B, UDP binary transport
Scenario 21: Cluster UDP fan-out x3  — pub on A, sub on B+C (UDP binary)
Scenario 22: Cluster UDP vs TCP      — same workload, side-by-side comparison
```

Standalone micro-benchmarks in `tests/throughput.rs` (Criterion):

```
bench_binary_codec_encode    — encode 128B message to binary
bench_binary_codec_decode    — decode binary datagram to messages
bench_token_table_lookup     — token table lookup by subject
bench_token_table_assign     — token table assign new subject
bench_binary_vs_text_encode  — binary encode vs RMSG text format
bench_binary_vs_text_decode  — binary decode vs text parse
```

## Verification

```bash
cargo check --features udp-transport
cargo test --lib --features udp-transport
cargo clippy --all-targets --features udp-transport -- --deny clippy::all
cargo +nightly fmt

# Existing features unaffected:
cargo check
cargo check --features cluster
cargo test --lib
cargo test --lib --features cluster
```

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| UDP packet loss degrades throughput | enet provides reliability + congestion control from day one |
| MTU limits batch size on non-jumbo networks | enet handles fragmentation/reassembly transparently |
| Token table consistency (lost DEFINE_TOKEN) | Token defs sent on enet reliable channel; inline fallback (token_id=0) always works |
| rusty_enet maturity | Pure Rust transpile of battle-tested C library; well-maintained; MIT licensed |
| Feature flag explosion | udp-transport implies cluster; no cross-product with other features |

## References

- [rusty_enet — pure Rust ENet port](https://github.com/jabuwu/rusty_enet)
- [ENet reliable UDP library (original C)](http://enet.bespin.org/)
- [Gaffer on Games: Reliable UDP](https://gafferongames.com/post/reliability_ordering_and_congestion_avoidance_over_udp/)
- [Linux MSG_ZEROCOPY](https://www.kernel.org/doc/html/latest/networking/msg_zerocopy.html)
- [sendmmsg/recvmmsg batching](https://man7.org/linux/man-pages/man2/sendmmsg.2.html)
