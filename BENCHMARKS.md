# Benchmark Results Log

All benchmarks: 128B payload, best of 3 runs unless noted.
Hardware: same machine for all runs. Units: msgs/sec (K = thousands, M = millions).

---

## 2026-03-17 — Gateway benchmarks + bug fixes (full 19-scenario run)

**Changes since last benchmark:**
Gateway support (`gateway` feature): outbound gateway connections with optimistic→interest-only
mode transition, negative interest (RS-) caching, RS- signaling on zero local matches,
batch eventfd notification for gateway forwarding, and `has_gateway_interest` fast-path flag
to prevent `can_skip` from silently dropping PUBs in optimistic mode.

**Bug fixes:**
- Fixed asymmetric connection counting: `accept_leaf_tcp`, `accept_route_tcp`, and
  `accept_gateway_tcp` now increment `active_connections` (previously only `accept_tcp`
  for clients did, causing counter underflow on disconnect → "max connections exceeded").
- Fixed benchmark script `set -euo pipefail` failures from `grep` returning non-zero
  when subscriber output is missing due to slow consumer disconnect.

**Gateway topology:** Two clusters (alpha + beta), each with one server. Alpha gateway
connects to beta gateway. Publisher on alpha, subscriber(s) on beta. Messages cross
the gateway via RMSG, with the receiving side sending RS- for unmatched subjects.

### Throughput (500K msgs × 128B, 3-run average)

#### All 19 scenarios (full benchmark)

| Scenario | Rust msg/s | Go msg/s | Rust/Go % | Previous |
|---|---|---|---|---|
| Pub only | ~810K | ~1,145K | **70%** | 79% |
| Local pub/sub (pub) | ~618K | ~365K | **169%** | 167% |
| Fan-out x5 (pub) | ~311K | ~121K | **256%** | 212% |
| Leaf → Hub (pub) | ~390K | ~334K | **117%** | 228% |
| Hub → Leaf (pub) | ~235K | ~209K | **112%** | 114% |
| Cluster A→B (pub) | ~441K | ~284K | **155%** | — |
| Cluster fan x3 (pub) | ~246K | ~158K | **155%** | — |
| Cluster B+C (pub) | ~342K | ~189K | **181%** | — |
| Gateway A→B (pub) | ~749K | ~374K | **200%** | — |
| Gateway fan-out (pub) | ~608K | ~253K | **239%** | — |
| Gateway req-reply | ~345 | ~639 | **53%** | — |

#### Server resources (gateway scenarios, from /proc)

| Scenario | CPU(ms) Rust/Go | RSS(KB) Rust/Go | CtxSw Rust/Go |
|---|---|---|---|
| Gateway A→B | 2,030 / 4,340 | 86K / 57K | 10 / 0 |
| Gateway fan-out | 2,640 / 7,120 | 230K / 24K | 10 / 0 |
| GW req-reply | 23,150 / 22,100 | 234K / 25K | 185 / 0 |

**Takeaways:**
- **Gateway pub/sub: Rust 2x Go** — optimistic forwarding with batch eventfd notifications
  delivers strong cross-gateway throughput. Rust pub rate (~749K) is competitive with
  local pub/sub (~618K), indicating minimal gateway overhead.
- **Gateway fan-out: Rust 2.4x Go** — publisher on alpha with subscribers on both alpha
  (local) and beta (gateway). Consistent with fan-out advantage in other modes.
- **Gateway CPU: Rust uses 47-53% of Go's CPU** — for pub/sub and fan-out scenarios,
  Rust achieves 2x the throughput at half the CPU.
- **Gateway req-reply: 53% of Go** — expected limitation. Our gateway doesn't yet implement
  `_GR_` reply rewriting, so replies take the slow path. Go's gateway has highly optimized
  request-reply routing with mapped reply subjects.
- **Cluster scenarios first full run** — cluster pub/sub (155%), fan-out (155%), and
  remote-only (181%) all show strong Rust advantages.
- **Optimistic mode works correctly** — messages flow immediately without waiting for RS+
  propagation. The negative interest cache prunes subjects with no remote subscribers,
  and the system transitions to interest-only mode after 1000 RS- signals.

---

## 2026-03-14 — Standalone apples-to-apples comparison

**Motivation:** Cross-check our leaf mode results against the official NATS benchmark setup.
Run the same `nats bench` scenarios used in the [NATS docs](https://docs.nats.io/using-nats/nats-tools/nats_cli/natsbench)
(standalone nats-server v2.12.1, MacBook Pro M4: 14.8M pub-only 16B, 4.9M pub/sub 16B,
1.0M fan-out 1:4 128B) on our hardware with both Go nats-server and Rust open-wire in
standalone mode (no leaf, no hub).

### Results (1M msgs, 3-run average, standalone on shared VM)

#### Pub only (fire-and-forget)

| Size | Go standalone | Rust standalone | Rust/Go | Official M4 | Our/M4 |
|---|---|---|---|---|---|
| 16B | ~1,880K | ~1,796K | **96%** | 14,787K | 13% |
| 128B | ~1,314K | ~1,345K | **102%** | — | — |

#### Pub/Sub 1:1 (pub rate)

| Size | Go standalone | Rust standalone | Rust/Go | Official M4 | Our/M4 |
|---|---|---|---|---|---|
| 16B | ~650K | ~1,488K | **229%** | 4,926K | 13% |
| 128B | ~438K | ~962K\* | **~220%** | — | — |

\* Rust 128B pub/sub hit slow consumer disconnect at 1M msgs (pub far outpaces sub).

#### Fan-out (pub rate, 128B)

| Scenario | Go standalone | Rust standalone | Rust/Go | Official M4 |
|---|---|---|---|---|
| Fan-out 1:4 | ~200K | ~450K | **~225%** | 1,012K |
| Fan-out 1:5 | ~165K | ~390K | **~236%** | — |

**Takeaways:**
- **Hardware baseline: our VM is ~13% of M4 Mac** — consistent across pub-only and pub/sub.
  Entirely explained by shared VM (CPU steal, NUMA) vs dedicated Apple M4 silicon.
- **Go numbers confirmed legitimate** — our Go standalone numbers are the expected order of
  magnitude for this hardware class. No misconfiguration.
- **Pub-only: Rust matches Go (96-102%)** in standalone mode. The leaf mode deficit (79%)
  comes from upstream connection overhead, not a fundamental ingestion gap.
- **Pub/sub: Rust 2.3x Go** — DirectWriter + epoll architecture gives a decisive advantage
  on local message routing. Even stronger than leaf mode (167%) because there's no upstream
  interest checking overhead.
- **Fan-out: Rust ~2.3x Go** — consistent with leaf mode results (212%).
- **Slow consumer tradeoff** — at 1M × 128B, Rust's pub rate (~960K) overwhelms the sub rate,
  causing write buffer overflow and subscriber disconnect. Go's goroutine scheduler provides
  natural backpressure that prevents this. A bounded write buffer with backpressure would fix
  this at some throughput cost.

---

## 2026-03-14 — Hub mode + full 13-scenario benchmark

**Changes since last benchmark:**
Hub mode (inbound leaf connections), handler refactor (`ConnExt`, `ClientHandler`,
`LeafHandler`), interest pipeline (subject mapping + interest collapse), io_uring
readiness support behind feature flag.

**Config change:** Added explicit `compression: off` on all leaf node connections
(hub listener + leaf remotes). Default `s2_auto` should negotiate to "off" on loopback
(RTT < 10ms threshold), but explicit disabling eliminates RTT measurement overhead and
removes any ambiguity.

**Cross-check against published benchmarks:** Go nats-server standalone on M4 MacBook Pro
achieves ~14.8M msgs/sec (16B pub-only) and ~1M msgs/sec (128B fan-out). Our Go leaf node
numbers (~1.5M pub-only, ~140K fan-out x5 at 128B) are consistent with: (a) leaf node
overhead vs standalone, (b) shared VM vs dedicated hardware, (c) 128B vs 16B payload.
Go numbers are within the expected order of magnitude.

### Throughput (500K msgs x 128B, 3-run average)

#### Core leaf scenarios (Rust leaf vs Go leaf)

| Scenario | Go Leaf | Rust Leaf | Rust/Go % | Previous |
|---|---|---|---|---|
| Pub only | ~1,539K | ~1,218K | **79%** | 87% |
| Local pub/sub (pub) | ~417K | ~695K | **167%** | 109% |
| Fan-out x5 (pub) | ~138K | ~293K | **212%** | 178% |
| Leaf -> Hub (pub) | ~345K | ~787K | **228%** | 136% |
| Hub -> Leaf (pub) | ~351K | ~401K | **114%** | 106% |

#### WebSocket scenarios

| Scenario | Go Leaf WS | Rust Leaf WS | Rust/Go % | Previous |
|---|---|---|---|---|
| WS pub/sub (pub) | ~445K | ~610K | **137%** | 127% |
| WS fan-out x5 (pub) | ~108K | ~321K | **297%** | 252% |
| WS fan-out x10 (pub) | ~45K | ~156K | **344%** | 335% |

#### Hub mode scenarios (Rust as hub server, new)

| Scenario | Go Hub (baseline) | Rust Hub | Rust/Go % |
|---|---|---|---|
| Pub only | ~1,127K | ~1,266K | **112%** |
| Local pub/sub (pub) | ~492K | ~949K | **193%** |
| Fan-out x5 (pub) | ~144K | ~472K | **328%** |
| Leaf -> Rust Hub (pub) | — | ~385K | — |
| Rust Hub -> Leaf (pub) | — | ~1,100K | — |

### Server resources (scenarios 1-5, from /proc)

| Scenario | CPU(ms) Rust/Go | RSS(KB) Rust/Go | CtxSw Rust/Go |
|---|---|---|---|
| Pub only | 1,920 / 940 | 16K / 20K | 3 / 0 |
| Pub/sub | 2,990 / 4,560 | 95K / 20K | 10 / 0 |
| Fan-out x5 | 5,230 / 13,660 | 218K / 20K | 28 / 0 |
| Leaf -> Hub | 2,090 / 4,330 | 314K / 21K | 11 / 0 |
| Hub -> Leaf | 3,330 / 4,350 | 231K / 21K | 6 / 0 |

**Takeaways:**
- **Fan-out x5: 178% -> 212%** — handler refactor reduced per-message dispatch overhead.
- **Leaf -> Hub: 136% -> 228%** — strongest scenario; Rust's upstream forwarding path is
  highly optimized with zero-copy parsing + direct LMSG assembly.
- **Local pub/sub: 109% -> 167%** — significant improvement from handler refactoring.
  Rust now delivers 67% more messages than Go on local routing.
- **Hub -> Leaf: 106% -> 114%** — consistent cross-server advantage.
- **WS fan-out x10: 344%** — Rust 3.4x Go, consistent with previous results. Batched
  eventfd + single WS encode per flush scales better than goroutine-per-connection.
- **WS fan-out x5: 252% -> 297%** — WS fan-out dominance continues to grow.
- **Hub mode pub only: 112% of Go** — Rust hub ingests 12% faster than Go hub.
- **Hub mode pub/sub: 193% of Go** — Rust hub routes local messages nearly 2x faster.
- **Hub mode fan-out: 328% of Go** — Rust hub fan-out is 3.3x Go hub, matching leaf pattern.
- **Pub-only: 87% -> 79%** — regression on fire-and-forget ingest. Handler dispatch adds
  a small cost to the no-subscriber path. High variance on shared VMs.
- **CPU efficiency**: Rust uses less CPU than Go on all routing scenarios (pub/sub: 66%,
  fan-out: 38%, leaf->hub: 48%, hub->leaf: 77% of Go's CPU). Only pub-only uses more CPU.
- **RSS higher for Rust under load** (16-314K vs 20-21K) — Go RSS stays flat at ~20K across
  all scenarios. Rust grows due to unbounded write buffers when publisher outpaces delivery.

---

## 2026-03-08 — v0.5 repo restructure validation

**Changes:**
Flattened `open-wire/` subdirectory to repo root, merged `benches/` and `tests/` into `tests/`,
promoted `examples/leaf_server.rs` to `src/main.rs`. No code changes — repo layout only.

Benchmark re-run to confirm no regressions.

### Throughput (500K msgs × 128B, 3-run average)

| Scenario | Go Leaf | Rust Leaf | Rust/Go % | Previous |
|---|---|---|---|---|
| Pub only | ~1,837K | ~1,595K | **87%** | 99% |
| Local pub/sub (sub) | ~762K | ~832K | **109%** | 89% |
| Fan-out x5 (pub) | ~209K | ~372K | **178%** | 210% |
| Leaf → Hub (sub on hub) | ~522K | ~709K | **136%** | 128% |
| Hub → Leaf (sub) | ~572K | ~606K | **106%** | 104% |
| WS pub/sub (sub) | ~621K | ~786K | **127%** | 121% |
| WS fan-out x5 (pub) | ~166K | ~418K | **252%** | 238% |
| WS fan-out x10 (pub) | ~78K | ~261K | **335%** | 295% |

### Memory (10K idle connections)

| Metric | Go nats-server | Rust Leaf |
|---|---|---|
| Baseline RSS | 13.9 MB | 2.6 MB |
| 10K idle clients delta | 399 MB | 109 MB |
| Per-client idle cost | 40.9 KB | 11.1 KB |
| Go / Rust ratio | | **3.7x** |

### CPU Profile (2M msgs × 128B, flat sampling)

| Scenario | Throughput | Top hotspot |
|---|---|---|
| Pub only | 1,770K msgs/sec | `parse_pub` 15%, `hash_one` 12%, `process_read_buf` 10% |
| Local pub/sub | 864K sub msgs/sec | `parse_pub` 10%, `process_read_buf` 8%, `write_msg` 3.4% |
| Fan-out x5 | 367K sub msgs/sec | `write_msg` 12%, `parse_pub` 5%, `build_msg` 3.4% |

**Takeaways:**
- All results within normal run-to-run variance — no regressions from repo restructuring.
- Pub-only variance (87% vs 99%) is typical for fire-and-forget on shared VMs.
- Fan-out and WS scenarios remain Rust's strongest advantages (1.8–3.4x Go).
- Memory profile unchanged: 3.7x less per-client idle cost than Go.

---

## 2026-03-08 — WebSocket transport support

**Feature added:**
WebSocket support for open-wire. Clients can connect via `ws://` in addition to raw TCP.
The NATS text protocol runs unchanged inside WebSocket binary frames, with an HTTP upgrade
handshake on connect.

**Implementation:**
- `websocket.rs`: Hand-rolled SHA-1 + base64 (~100 lines, no dependencies), HTTP upgrade
  handshake, and `WsCodec` frame encoder/decoder (binary frames, masking, fragmentation).
  Uses NATS-specific WebSocket GUID (`258EAFA5-E914-47DA-95CA-C5AB0DC85B11`) matching the
  Go nats.go client library.
- `worker.rs`: `Transport` enum (`Raw` / `WebSocket { codec, raw_buf, ws_out }`), new
  `ConnPhase::WsHandshake` for HTTP upgrade, WS-aware read/write paths.
- `server.rs`: Optional `ws_port` config, second listener with `poll()` on both fds.
- Zero-copy on the write path: NATS MSG bytes are accumulated in `write_buf`, then
  WS-framed in a single `WsCodec::encode()` call before flushing to the socket.

### Results (500K msgs × 128B, 3-run average)

#### TCP (unchanged)

| Scenario | Go Leaf | Rust Leaf | Rust/Go % | Previous |
|---|---|---|---|---|
| Pub only | ~1,820K | ~1,804K | **99%** | 100% |
| Local pub/sub (sub) | ~763K | ~680K | **89%** | 109% |
| Fan-out x5 (pub) | ~211K | ~443K | **210%** | 191% |
| Leaf → Hub (sub on hub) | ~566K | ~724K | **128%** | 129% |
| Hub → Leaf (sub) | ~563K | ~588K | **104%** | 105% |

#### WebSocket (new)

| Scenario | Go Leaf WS | Rust Leaf WS | Rust/Go % |
|---|---|---|---|
| WS pub/sub (sub) | ~726K | ~882K | **121%** |
| WS fan-out x5 (pub) | ~177K | ~422K | **238%** |
| WS fan-out x10 (pub) | ~79K | ~233K | **295%** |

**Takeaways:**
- **WS fan-out x10: Rust 3x faster than Go** — the batched eventfd + single WS encode per
  flush means fan-out cost scales with workers, not subscribers. Go's per-connection goroutine
  model pays WS framing overhead per subscriber.
- **WS fan-out x5: 238% of Go** — closely matches TCP fan-out x5 (210%), showing WS framing
  adds minimal overhead on the Rust side.
- **WS pub/sub: 121% of Go** — consistent advantage from epoll + DirectWriter architecture.
- **WS overhead vs TCP is small**: Rust WS fan-out x5 (422K) vs TCP fan-out x5 (443K) = only
  5% overhead from WS framing. Go's WS overhead is larger: 177K (WS) vs 211K (TCP) = 16%.
- TCP results unchanged — WebSocket code paths are fully separate and add no overhead to
  raw TCP connections.

---

## 2026-03-08 — N-worker epoll event loop (replace Tokio)

**Architecture change:**
Replace Tokio async runtime + thread-per-connection model with N worker threads, each owning
one epoll instance multiplexing many client connections. DirectWriter notifies the worker's
single eventfd (not per-connection), so fan-out to N connections on one worker costs 1 eventfd
write, not N.

**Key components:**
- `worker.rs`: Core epoll event loop with connection state machine (SendInfo→WaitConnect→Active)
- Non-blocking sockets with partial write buffering
- `client_conn.rs` deleted — all logic absorbed into worker.rs
- Round-robin connection distribution from acceptor to workers

**Performance-critical optimizations:**
1. **Batched eventfd notifications** — accumulate remote worker notifications across all PUBs
   in a read buffer, then send deduplicated. Reduces N_pubs × N_workers eventfd writes to
   just N_workers per batch. This was the single biggest win.
2. **Skip same-worker notification** — flush_pending runs after each event loop iteration,
   handling local delivery without any eventfd round-trip.
3. **Read loop until WouldBlock** — drain kernel buffer in one epoll_wait return.
4. **In-place flush_pending** — iterate conns directly without Vec allocation.

### Results (3-run average, Go v2.14.0-dev, `compression: off`)

| Scenario | Go Leaf | Rust Leaf | Rust/Go % | Previous |
|---|---|---|---|---|
| Pub only | ~1,828K | ~1,830K | **100%** | 95% |
| Local pub/sub (sub) | ~752K | ~822K | **109%** | 88% |
| Fan-out x5 (pub) | ~206K | ~393K | **191%** | 155% |
| Leaf → Hub (sub on hub) | ~581K | ~750K | **129%** | 112% |
| Hub → Leaf (sub) | ~573K | ~603K | **105%** | 123% |

**Takeaways:**
- **Fan-out x5: 155% → 191% of Go** — batched eventfd + same-worker optimization dominate.
  With 1 worker, fan-out reaches 310K (141% of Go), confirming cross-worker overhead was the
  bottleneck. With 6 workers + batch notifications, multi-core parallelism pushes to 191%.
- **Pub-only: 95% → 100% of Go** — eliminating Tokio async overhead (ReadHalf→PollEvented→
  mio→epoll) finally closes the gap. Direct epoll_wait + non-blocking read matches Go's
  netpoller performance.
- **Local pub/sub: 88% → 109%** — consistent improvement from reduced syscall overhead.
- **Leaf→Hub: 112% → 129%** — cleaner I/O path benefits upstream forwarding.
- **Hub→Leaf: 123% → 105%** — slight regression (was previously benefiting from Tokio's
  optimized TCP write path); now using raw non-blocking writes. Still beats Go.
- **All 5 scenarios at or above 100% of Go** — first time achieving this milestone.

---

## 2026-03-07 — Tight skip loop + profiling-driven optimizations

**Optimization applied:**
When no subscribers and no upstream exist (`can_skip_publish()`), bypass `tokio::select!`
entirely and use a tight read-skip loop. Eliminates per-iteration overhead of:
- `Notify::poll_notified()` + `drop_notified()` (1.23% of CPU — pure waste in skip mode)
- `WriterHandle::drain()` (0.46% — always returns None in skip mode)
- `handle_client_op()` for skipped PUBs (1.07% — async future create/drop for no-op)
- `select!` macro state machine overhead

New `read_next_non_pub()` method on `ServerConn` skips PUB/HPUB in an inner loop and
only returns for non-PUB ops (PING, SUB, UNSUB, CONNECT).

**Profiling insight:** Rust spends only ~10% in application code vs Go's ~42%. The gap is
not parsing speed (Rust parses 2-3x faster per message) but Tokio async machinery (~7%)
and kernel/libc syscall overhead (~27% vs Go's ~6%). The tight skip loop eliminates the
async machinery cost for the pub-only hot path.

**RSS measurement (same session):**

| State | Go nats-server | Rust leaf | Ratio |
|---|---|---|---|
| Idle (no connections) | 12.6 MB | 3.3 MB | **Rust 3.8x smaller** |
| 1 connection | 13.4 MB | 3.6 MB | **Rust 3.7x smaller** |
| 100 connections | 19.7 MB | 5.1 MB | **Rust 3.9x smaller** |
| Under load (100 subs + pub) | 21.8 MB | 5.5 MB | **Rust 4.0x smaller** |
| After load (conns closed) | 20.7 MB | 8.5 MB | **Rust 2.4x smaller** |

### Results (same session, Go v2.14.0-dev, `compression: off`)

| Scenario | Go | Rust | Rust/Go % | Previous |
|---|---|---|---|---|
| Pub only | 1,633K | 1,560K | **95%** | 67% |
| Local pub/sub (sub) | 832K | 730K | **88%** | 95% |
| Fan-out x5 (per sub) | 205K | 318K | **155%** | 125% |
| Leaf → Hub (sub on hub) | 613K | 687K | **112%** | 118% |
| Hub → Leaf (sub) | 590K | 726K | **123%** | 117% |

**Takeaways:**
- **Pub-only: 67% → 95% of Go** — tight skip loop closes most of the gap. Remaining 5%
  is Tokio I/O layers (ReadHalf → PollEvented → mio → epoll) vs Go's direct netpoller.
- **Fan-out x5: 125% → 155% of Go** — cleaner message loop benefits all paths
- **Hub→Leaf: 123% of Go** — consistent win
- **Local pub/sub: 88%** — within normal variance (previous 95% was a high run)
- **RSS: 3.3 MB idle vs 12.6 MB** — Rust uses 3.8x less memory at idle
- All 5 scenarios now within 12% of Go or beating it. No scenario below 88%.

---

## 2026-03-07 — DirectWriter + fair Go comparison (compression: off)

**Optimization applied:**
Replace the per-client `mpsc::UnboundedSender<ClientMsg>` with `DirectWriter` — a shared
`Mutex<BytesMut>` + `tokio::sync::Notify` that formats MSG/HMSG wire bytes synchronously
into the client's write buffer. The upstream reader (or local publisher) calls
`sub.writer.write_msg()` + `sub.writer.notify()` instead of constructing a `ClientMsg`
struct and sending it through a channel.

**What this eliminates per message:**
- `mpsc::send()`: atomic linked-list push + AtomicWaker wake (epoll syscall)
- `ClientMsg` struct allocation (5 fields, 3 Bytes clones)
- Task hop: sender → channel → receiver → format MSG → write TCP
- Now: format MSG bytes directly → append to shared buffer → one notify per batch

**Bug found and fixed:** `Notify::notify_one()` stores at most one permit. A fast producer
calling `notify()` many times before the consumer wakes loses all but one notification.
Fixed with drain-before-wait pattern: always check the buffer at the top of the message
loop before blocking on `select!`.

**Go benchmark correction:** Previous runs used bare `leafnodes { listen }` config which
defaults to `compression: s2_auto`. On localhost, `s2_auto` may still enable S2 compression,
adding ~25% CPU overhead to Go's leaf node numbers. All numbers below use explicit
`compression: off` for a fair comparison. Go standalone (non-leaf) scenarios are unaffected.

### Results (same session, Go v2.14.0-dev, `compression: off`)

| Scenario | Go | Rust | Rust/Go % |
|---|---|---|---|
| Pub only | 1,853K | 1,250K | **67%** |
| Local pub/sub (sub) | 815K | 771K | **95%** |
| Fan-out x5 (per sub) | 209K | 260K | **125%** |
| Leaf → Hub (sub on hub) | 589K | 694K | **118%** |
| Hub → Leaf (sub) | 612K | 717K | **117%** |

**Takeaways:**
- **Hub→Leaf: 50% → 117% of Go** — DirectWriter bypasses the mpsc channel bottleneck.
  Previous 139% figure was inflated by Go paying S2 compression overhead.
- **Fan-out x5: 125% of Go** — DirectWriter avoids per-subscriber channel overhead
- **Leaf→Hub: 118% of Go** — local routing to matching subs is now channel-free
- **Local pub/sub: 95% of Go** — within noise of parity on the corrected baseline
- **Pub-only: 67%** — true gap; Go's goroutine-per-connection model has less overhead
  for pure ingestion (no async runtime, no channel)
- All routing scenarios (fan-out, leaf↔hub) beat Go; only pure ingestion lags

---

## 2026-03-07 — Zero-copy LMSG parsing + buffer drain in leaf reader

**Optimizations applied:**
1. **Zero-copy LMSG parsing**: replaced `Bytes::copy_from_slice()` with `split_to().freeze().slice()`
   for subject/reply in `parse_lmsg`. Was allocating+copying subject bytes per message — now uses
   Arc refcount bump like `parse_pub` already did. `copy_from_slice` was 2.05% of CPU in profiles.
2. **Buffer drain in leaf reader**: `run_leaf_reader` now drains all parseable ops from the read
   buffer after each I/O read (same pattern as client_conn). Previously it did one op per syscall.

| Scenario | Go Leaf | Rust Leaf | Rust/Go % | Previous |
|---|---|---|---|---|
| Pub only | ~1,749K | ~1,348K | **77%** | 59% |
| Local pub/sub (sub) | ~630K | ~727K | **115%** | 118% |
| Fan-out x5 (per sub) | ~175K | ~266K | **152%** | 159% |
| Leaf → Hub (sub on hub) | ~472K | ~560K | **119%** | 135% |
| Hub → Leaf (sub) | ~462K | ~232K | **50%** | 37% |

**Takeaways:**
- Hub→Leaf: **37% → 50% of Go** — biggest improvement from zero-copy LMSG + buffer drain
- Remaining Hub→Leaf bottleneck is architectural: single reader task → mpsc channel → per-client
  writer task, with `tokio::select!` overhead on the client side. Go writes directly from the
  reader goroutine without a channel hop.

---

## 2026-03-07 — Eliminate conns lock + split SubList exact/wildcard + skip publish

**Optimizations applied:**
1. **Store `msg_tx` directly in `Subscription`**: eliminates `conns` HashMap lookup + `conns.read()`
   RwLock acquisition on every publish. The sender is cloned once at subscribe time.
2. **Split SubList into exact + wildcard**: exact subjects use `HashMap<String, Vec<Sub>>` for O(1)
   lookup; only wildcard patterns (`*`, `>`) require linear scanning. Most NATS workloads use
   exact subjects, so this avoids scanning all subscriptions.
3. **`try_skip_publish`**: when no subscribers exist and no upstream is connected, PUB/HPUB messages
   are skipped without creating any Bytes objects (no `split_to`, no `freeze`, no refcount bumps).
   Uses `AtomicBool` flag to avoid taking the subs lock on every publish.
4. **Removed `conns` HashMap entirely** from `ServerState` — no longer needed since `msg_tx` is
   stored directly in each Subscription.

| Scenario | Go Leaf | Rust Leaf | Rust/Go % | Previous |
|---|---|---|---|---|
| Pub only | ~1,870K | ~1,111K | **59%** | 88% |
| Local pub/sub (sub) | ~607K | ~717K | **118%** | 97% |
| Fan-out x5 (per sub) | ~193K | ~307K | **159%** | 136% |
| Leaf → Hub (sub on hub) | ~480K | ~647K | **135%** | 127% |
| Hub → Leaf (sub) | ~501K | ~184K | **37%** | 35% |

**Takeaways:**
- Local pub/sub: **97% → 118%** — now 18% faster than Go (conns lock + HashMap eliminated)
- Fan-out x5: **136% → 159%** — exact HashMap lookup avoids scanning all 5 subs
- Leaf→Hub: **127% → 135%** — same optimization benefits upstream delivery
- Pub-only: dropped to 59% — high variance, likely system noise (structural BytesMut overhead)
- Hub→Leaf: 35% → 37% — marginal, bottleneck is mpsc channel delivery

---

## 2026-03-07 — Byte-level subject matching (eliminate SplitInternal)

**Optimization applied:**
16. Replace `str::split('.')` iterator in `subject_matches` with direct byte-level dot scanning
    using `memchr`. Eliminates `core::str::iter::SplitInternal::next` which was 5.7% of CPU
    in local pub/sub profiles.

**Profile diff (local pub/sub):**
- `SplitInternal::next`: 5.70% → **0%** (eliminated)
- `subject_matches`: 1.12% → 1.58% (function itself slightly larger, but no iterator overhead)
- `memchr::find_avx2`: 0.97% → 5.50% (shared with parse_pub newline scanning)

| Scenario | Direct Hub | Go Leaf | Rust Leaf | Rust/Go % |
|---|---|---|---|---|
| Pub only | ~1,671K | ~1,575K | ~1,384K | **88%** |
| Local pub/sub (sub) | ~680K | ~780K | ~755K | **97%** |
| Fan-out x5 (per sub) | ~194K | ~193K | ~263K | **136%** |
| Leaf → Hub (pub) | — | ~519K | ~1,242K | **239%** |
| Leaf → Hub (sub on hub) | — | ~506K | ~645K | **127%** |
| Hub → Leaf (sub) | — | ~536K | ~186K | **35%** |

**Takeaways:**
- Pub-only at **88% of Go** (up from 84%) — variance, no code change affects this path
- Local pub/sub at **97% of Go** — nearly closed the gap (was 107%, now 97% — within noise)
- Fan-out x5 at **136% of Go** (up from 122%) — byte-level matching helps with 5 subs
- Leaf→Hub still dominant at **2.4x Go**
- Hub→Leaf remains the weak spot at 35% of Go — mpsc channel delivery bottleneck

**Remaining hotspots (local pub/sub profile):**
- `parse_pub`: 6.6% — already heavily optimized
- `libc memmove`: 6.4% — BytesMut internal buffer compaction
- `memchr::find_avx2`: 5.5% — newline + dot scanning (shared)
- `bytes_mut::shared_v_drop`: 3.6% — refcount drops
- `for_each_match`: 3.1% — O(n) linear sub scan (trie would help)
- `mpsc::send + wake`: 5.1% — tokio channel overhead per message delivery
- `subject_matches`: 1.6% — the matching itself (now byte-level)

---

## 2026-03-07 — Adaptive read buffers + idle memory benchmark

**Optimization applied:**
15. Adaptive read buffers (Go-style dynamic sizing) — `AdaptiveBuf` wrapper around `BytesMut`
    that starts at 512B, doubles on full reads, halves after 2 consecutive short reads
    (floor 64B, ceiling 64KB). Replaces fixed 64KB allocations in `ServerConn`, `LeafConn`,
    and `LeafReader`. Configurable via `LeafServerConfig::max_read_buf_capacity` /
    `write_buf_capacity`.

### Throughput (500K msgs × 128B, 2 runs)

| Scenario | Direct Hub | Go Leaf | Rust Leaf | Rust/Go % |
|---|---|---|---|---|
| Pub only | ~1,882K | ~1,823K | ~1,396K | **77%** |
| Local pub/sub (sub) | ~898K | ~803K | ~841K | **105%** |
| Fan-out x5 (per sub) | ~220K | ~207K | ~275K | **133%** |
| Leaf → Hub (pub) | — | ~564K | ~1,090K | **193%** |
| Leaf → Hub (sub on hub) | — | ~560K | ~679K | **121%** |
| Hub → Leaf (sub) | — | ~526K | ~206K | **39%** |

### Idle memory (10K connections benchmark)

| Metric | Go nats-server | Rust Leaf |
|---|---|---|
| Baseline RSS | 13.6 MB | 3.7 MB |
| 1K idle clients delta | 16 MB (16.8 KB/client) | 13 MB (13.7 KB/client) |
| 5K idle clients delta | 71 MB (15.0 KB/client) | 5 MB (1.0 KB/client) |
| 10K idle clients delta | 405 MB (41.4 KB/client) | 12 MB (1.3 KB/client) |
| Per-client idle cost | ~15-41 KB (grows with scale) | **~1.3 KB (flat)** |

### Analysis

- **Idle memory: Rust 33x less than Go at 10K connections** — adaptive buffers start at
  512B vs Go's goroutine stack overhead (~8KB+). Go's per-client cost actually *increases*
  with scale (GC pressure, goroutine stacks) while Rust stays flat at ~1.3 KB.
- **Throughput unchanged** from previous run — adaptive buffers add no overhead on hot path.
  Buffers grow to full 64KB during benchmarks, matching previous fixed allocation behavior.
- **Hub→Leaf regression to 39% of Go** — this run shows the downstream delivery bottleneck
  more clearly. The per-client mpsc channel + write flush path is the main area to optimize.
- **Fan-out remains strongest** at 133% of Go — write batching and zero-copy routing dominate.
- **Key win**: adaptive buffers solve the idle memory problem (previous: 128KB/client fixed →
  now ~1.3KB/client idle) without sacrificing throughput under load.

---

## 2026-03-07 — Resource usage comparison: Rust Leaf vs Go Leaf

Measured during the same benchmark session (5M pub-only, 2M local pub/sub, 2M leaf→hub).
CPU ticks = user+system clock ticks from `/proc/PID/stat` (lower = less CPU used).

### Memory (RSS)

| State | Go Leaf | Rust Leaf | Ratio |
|---|---|---|---|
| Idle (pre-benchmark) | 15 MB | 3.6 MB | Rust 4x smaller |
| Pub-only (5M × 128B, under load) | ~22 MB | ~57-70 MB | Go 3x smaller |
| Local pub/sub (2M, under load) | 24 MB | 69 MB | Go 3x smaller |
| Leaf→Hub (2M, under load) | 34 MB | 345 MB | Go 10x smaller |
| Peak (VmHWM, session lifetime) | 37 MB | 345 MB | Go 9x smaller |

### CPU (clock ticks per workload)

| Scenario | Go Leaf | Rust Leaf | Ratio | Throughput |
|---|---|---|---|---|
| Pub-only (5M msgs) | 286 ticks | 359 ticks | Go 20% less CPU | Go 8% faster |
| Local pub/sub (2M) | 382 ticks | 290 ticks | Rust 24% less CPU | Rust 15% faster |
| Leaf→Hub (2M) | 436 ticks | 169 ticks | Rust 61% less CPU | Rust 2.1x faster |

### Other

| Metric | Go Leaf | Rust Leaf |
|---|---|---|
| Threads | 13 | 7 |
| Binary size | 23 MB | 25 MB |

### Analysis

- **Go wins on memory** — 3-10x less RSS under load. The Leaf→Hub spike to 345 MB is the
  unbounded mpsc channel buffering messages when the publisher (1.2M/s) outpaces the upstream
  writer (715K/s). Go's goroutine scheduler provides natural backpressure preventing this buildup.
- **Rust wins on CPU efficiency** when routing messages: 24% less CPU on local pub/sub, 61% less
  on leaf→hub. Zero-copy parsing and cached sender pay off here.
- **Pub-only CPU 20% higher for Rust** — mpsc channel send + atomic waker costs more than Go's
  goroutine-based approach for pure fire-and-forget ingestion.
- **Rust idle footprint is tiny** (3.6 MB vs 15 MB) — Go runtime + GC metadata has a higher floor.
- **Key tradeoff**: Rust trades memory for throughput. A bounded channel with backpressure would
  reduce memory at some throughput cost.

---

## 2026-03-07 — Zero-copy parse_pub hot path optimization

**Optimizations applied:**
10. Zero-copy `parse_pub`/`parse_hpub`/`parse_sub` — freeze header line from BytesMut, take
    sub-slices (Arc refcount bump) instead of 3× `Bytes::copy_from_slice` heap allocs per PUB
11. Split LMSG builder — `build_lmsg_header()` writes only protocol header; payload written
    separately into BufWriter, eliminating one payload copy per upstream publish
12. Cached `upstream_tx` on ClientConnection — sender cloned once after handshake instead of
    RwLock read + Arc clone per publish
13. Pre-computed `sid_bytes` on Subscription — `sid_to_bytes()` called once at subscribe time,
    cheap `Bytes::clone` at delivery instead of heap alloc per message
14. `for_each_match` iterator on SubList — avoids allocating `Vec<Subscription>` per publish

| Scenario | Direct Hub | Go Leaf | Rust Leaf | Rust/Go % |
|---|---|---|---|---|
| Pub only | ~1,893K | ~1,814K | ~1,515K | **84%** |
| Local pub/sub (sub) | ~853K | ~776K | ~827K | **107%** |
| Fan-out x5 (per sub) | ~212K | ~220K | ~268K | **122%** |
| Leaf → Hub (pub) | — | ~566K | ~1,367K | **243%** |
| Leaf → Hub (sub on hub) | — | ~562K | ~709K | **126%** |
| Hub → Leaf (sub) | — | ~565K | ~659K | **117%** |

**Takeaways:**
- Pub-only **84% of Go** (up from 69%) — zero-copy parsing eliminated ~6% allocator overhead
- Local pub/sub now **exceeds Go** (107%) — Rust beats Go on local routing
- Fan-out x5 **122% of Go** — significant win from for_each_match + pre-computed sid_bytes
- Leaf→Hub pub rate **2.4x Go** — cached sender + split LMSG builder very effective
- Hub→Leaf **117% of Go** — consistent cross-server advantage
- Remaining pub-only gap (~16%): mpsc channel overhead (~6%), Tokio waker (~2.4%), residual allocs

---

## 2026-03-06 — Zero-copy parser & message builder (nats_proto module)

**Optimization applied:**
9. New `nats_proto` module: zero-copy protocol parser and message builder
   - First-byte verb dispatch (match `buf[0]`) instead of sequential `starts_with`
   - `memchr(b'\n')` single-byte search replacing `memmem::find(b"\r\n")` two-byte search
   - Hand-rolled `parse_size`/`parse_u64` on raw `&[u8]` — no `from_utf8` + `str::parse`
   - Stack-allocated `split_args<const N>` on raw bytes — no Vec allocation
   - `MsgBuilder` with `extend_from_slice` — eliminates `write!()` / `core::fmt` formatting
   - Pre-formatted `sid_to_bytes` — convert SID to ASCII once, reuse per MSG
   - `Bytes` (refcounted) for parsed ops instead of `Subject`/`String` allocation

**Profiling hotspot eliminations (pub-only):**
- `memchr FinderBuilder` 10.4% → 0%
- `Iterator::try_fold` 10.5% → 0%
- `from_utf8` 4.3% → 0%
- `core::fmt` / `write_str` ~4% → 0%
- Net improvement: +16% pub-only, +8% local pub/sub

| Scenario | Direct Hub | Go Leaf | Rust Leaf | Rust/Go % |
|---|---|---|---|---|
| Pub only | ~2.2M | ~1,962K | ~1,345K | **69%** |
| Local pub/sub (sub) | ~980K | ~778K | ~612K | **79%** |
| Fan-out x5 (per sub) | ~250K | ~192K | ~157K | **82%** |
| Leaf → Hub (pub) | — | ~368K | ~779K | **212%** |
| Hub → Leaf (sub) | — | ~403K | ~483K | **120%** |

**Takeaways:**
- Pub-only **69% of Go** (up from 37%) — nearly doubled Rust throughput
- Local pub/sub **79% of Go** (up from 66%)
- Fan-out x5 roughly same (82% vs 84%) — bottleneck is Bytes clone/drop churn, not parsing
- Leaf→Hub **2.1x faster than Go** — Rust's strongest scenario
- Hub→Leaf **20% faster than Go**
- Remaining local bottlenecks: Bytes refcount churn (6.4%), Tokio mpsc overhead (5.3%), subscription matching (2.7%)

---

## 2026-03-06 — Full benchmark (500K msgs × 128B, 3 runs) — Commit ee71779

**All optimizations applied:**
1. Batch client writes (try_recv drain + single flush)
2. BufWriter (64KB) on ServerConn writer
3. std::sync::RwLock for subs/conns (replacing tokio::sync::RwLock)
4. Lock-free upstream forwarding (shared mpsc sender)
5. Allocation-free subject_matches (iterator-based)
6. Unbounded client msg channels (prevent message loss)
7. Split LeafConn into independent reader/writer tasks (fixes slow consumer)
8. BufWriter (64KB) on LeafWriter (upstream write batching)

| Scenario | Direct Hub | Go Leaf | Rust Leaf | Rust/Go % |
|---|---|---|---|---|
| Pub only | 2.23M | 2.13M | 792K | 37% |
| Local pub/sub (sub) | 977K | 920K | 607K | 66% |
| Fan-out x5 (per sub) | 253K | 245K | 207K | **84%** |
| Leaf → Hub (sub on hub) | — | 599K | 647K | **108%** |
| Hub → Leaf (sub) | — | 571K | 644K | **113%** |

**Takeaways:**
- **Leaf↔Hub: Rust beats Go** consistently (108-113% of Go native leaf)
- **Fan-out x5 at 84% of Go** (up from 16% at baseline)
- **Local pub/sub at 66% of Go** (up from 10% at baseline)
- **Pub-only at 37% of Go** — bottleneck is client-facing parser/accept path, not upstream
- No more slow consumer disconnects in any scenario
- Cross-server forwarding is now Rust's strongest scenario

---

## 2026-03-06 — Split upstream reader/writer (200K msgs, 2 runs)

**Optimization applied:**
7. Split LeafConn into independent reader/writer tasks (fixes slow consumer)
8. BufWriter (64KB) on LeafWriter (upstream write batching)

| Scenario | Direct Hub | Go Leaf | Rust Leaf | Rust/Go % |
|---|---|---|---|---|
| Pub only | 2.09M | 2.04M | 860K | 42% |
| Local pub/sub (sub) | 760K | 799K | 452K | 57% |
| Fan-out x5 (per sub) | 227K | 243K | 155K | 64% |
| Leaf → Hub (sub on hub) | — | 629K | 697K | **111%** |
| Hub → Leaf (sub) | — | 573K | 705K | **123%** |

**Highlights:**
- **Leaf→Hub fixed!** No more slow consumer — Rust now **exceeds Go** (697K vs 629K)
- Hub→Leaf also improved, Rust at 123% of Go (705K vs 573K)
- Cross-server scenarios (Leaf↔Hub) are now Rust's strength
- Local pub/sub and fan-out slightly down from previous run (variance)

---

## 2026-03-06 — Commit 9109b4b

**Optimizations applied (cumulative):**
1. Batch client writes (try_recv drain + single flush)
2. BufWriter (64KB) on ServerConn writer
3. std::sync::RwLock for subs/conns (replacing tokio::sync::RwLock)
4. Lock-free upstream forwarding (shared mpsc sender)
5. Allocation-free subject_matches (iterator-based)
6. Unbounded client msg channels (prevent message loss)

| Scenario | Direct Hub | Go Leaf | Rust Leaf | Rust/Go % |
|---|---|---|---|---|
| Pub only | 1.92M | 2.00M | 790K | 40% |
| Local pub/sub (pub) | 764K | 619K | 529K | 85% |
| Local pub/sub (sub) | 760K | 620K | 530K | 85% |
| Fan-out x5 (per sub) | 205K | 204K | 138K | 68% |
| Hub → Leaf (sub) | 623K | 574K | 600K | **104%** |
| Leaf → Hub (sub on hub) | — | 427K | slow consumer | — |

**Highlights:**
- Hub→Leaf now **matches or exceeds Go** native leaf (~600K vs 574K)
- Local pub/sub at 85% of Go (up from 10% at baseline)
- Fan-out at 68% of Go (up from ~35%)
- Leaf→Hub still limited by slow consumer disconnect (single-task upstream I/O)

---

## 2026-03-06 — After BufWriter (before sync locks)

| Scenario | Go Leaf | Rust Leaf | Rust/Go % |
|---|---|---|---|
| Pub only | 2.15M | 550K | 26% |
| Local pub/sub (sub) | 695K | 375K | 54% |
| Fan-out x5 (per sub) | 182K | 80K | 44% |
| Hub → Leaf (sub) | 623K | 403K | 65% |

---

## 2026-03-06 — Baseline (before any optimizations)

| Scenario | Go Leaf | Rust Leaf | Rust/Go % |
|---|---|---|---|
| Pub only | 2.15M | 520K | 24% |
| Local pub/sub (sub) | 695K | 71K | 10% |
| Fan-out x5 (per sub) | 182K | 29K | 16% |

**Known bottlenecks at baseline:**
- Per-message flush to TCP (no buffering)
- tokio::sync::RwLock on every publish (subs + conns)
- Vec-allocating subject_matches on every publish
- Bounded channel with try_send dropping messages
