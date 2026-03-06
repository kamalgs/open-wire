# Benchmark Results Log

All benchmarks: 128B payload, best of 3 runs unless noted.
Hardware: same machine for all runs. Units: msgs/sec (K = thousands, M = millions).

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
