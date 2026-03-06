# Benchmark Results Log

All benchmarks: 200,000 msgs × 128B payload, best of 2-3 runs.
Hardware: same machine for all runs. Units: msgs/sec (K = thousands, M = millions).

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
