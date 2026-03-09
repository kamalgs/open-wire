# Compatibility Gap Analysis

Tracking the gap between open-wire and Go nats-server for leaf node workloads.
Items marked ~~strikethrough~~ are already implemented.

## Protocol Gaps

- ~~**UNSUB max enforcement**~~ — auto-unsubscribe after N messages (`UNSUB <sid> <max>`).
- **Drain protocol** — graceful client drain (`-ERR` + disconnect after flush).
- **Subject deny import/export** — per-connection subject allow/deny lists on
  leaf connections.
- **HMSG header forwarding fidelity** — verify all header edge cases match Go
  behavior (multi-value, empty headers, etc.).
- **Request-reply timeout** — server-side request timeout support.
- **Multiple hub connections** — connect to more than one upstream hub.
- **Inbound leaf connections** — accept solicited leaf connections from other servers.

## Config / Operational Gaps

- ~~**Config file**~~ — Go nats-server `.conf` format parser (`--config` flag).
- ~~**Config reload**~~ — `SIGHUP` triggers config file reload of hot-reloadable values.
- ~~**pid_file**~~ — write PID to a file on startup, remove on shutdown.
- ~~**log_file**~~ — write logs to a file instead of stderr.
- ~~**Lame duck mode**~~ — stop accepting new connections, send lame-duck INFO to
  existing clients, wait for lame_duck_duration, then shut down.
- ~~**Varz / Healthz endpoints**~~ — HTTP `/varz`, `/healthz` monitoring endpoints
  on configurable `monitoring_port`.
- ~~**Metrics endpoint**~~ — Prometheus metrics on a configurable port.
- ~~**Signal handling**~~ — SIGTERM/SIGINT graceful shutdown.
- ~~**Structured logging**~~ — `tracing` with env-filter.

## Security Gaps

- **mTLS** — mutual TLS with client certificate verification.
- **TLS to upstream hub** — TLS on the leaf→hub connection (currently plaintext only).
- ~~**TLS for clients**~~ — accept TLS client connections (rustls).
- ~~**Token / user-pass auth**~~ — single token or user/password authentication.
- ~~**NKey auth**~~ — NKey challenge-response authentication.
- **Per-user permissions** — publish/subscribe allow/deny per user.
- **Auth timeout** — disconnect clients that don't authenticate within a deadline.
- **Accounts** — multi-tenant account isolation (non-goal for now, but noted for
  completeness).

## Performance Ideas

- **io_uring** — replace epoll with io_uring for batched syscalls.
- **writev / vectored writes** — coalesce messages into a single syscall.
- **Connection affinity** — pin publisher and subscribers to the same worker.
- **Trie-based subject matching** — replace wildcard `Vec` linear scan with a
  trie or radix tree.
- **NUMA-aware allocation** — bind workers to CPU cores, allocate from local memory.
- **Lock-free DirectWriter** — replace `Mutex<BytesMut>` with a lock-free ring buffer.

## Overload Safeguards

- ~~**Slow consumer detection**~~ — disconnect clients exceeding `max_pending` write buffer.
- ~~**Max payload enforcement**~~ — reject PUB/HPUB exceeding `max_payload`.
- ~~**Max connections**~~ — reject new connections beyond `max_connections`.
- ~~**Max subscriptions**~~ — reject SUB beyond per-connection limit.
- ~~**Queue groups**~~ — load-balanced message delivery across subscriber groups.

## Platform

- **Cross-platform portability** — extract a reactor trait, add poll()/kqueue/IOCP/WASI
  backends. See [portability.md](portability.md).
