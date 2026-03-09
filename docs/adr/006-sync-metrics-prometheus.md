# ADR-006: Sync-Compatible Metrics with Prometheus Exporter

**Status:** Accepted

## Context

open-wire needs production observability — counters for messages, connections,
subscriptions, and slow consumers, plus a scrapable endpoint for Prometheus.

The obvious choice in the Rust ecosystem is OpenTelemetry (opentelemetry-otlp),
but its OTLP exporters require a Tokio runtime. open-wire deliberately avoids
async runtimes (see ADR-001) and runs on raw epoll + `std::thread`.

We need a metrics solution that:
1. Works without Tokio or any async runtime.
2. Provides a Prometheus-compatible HTTP endpoint for scraping.
3. Has negligible hot-path cost (lock-free atomic operations).
4. Uses permissive licenses (MIT/Apache-2.0).

## Decision

Use the `metrics` crate (v0.24) as the recording API and
`metrics-exporter-prometheus` (v0.16) as the exporter:

- **`metrics`** provides `counter!`, `gauge!`, and `histogram!` macros that
  resolve to lock-free atomic operations via a global recorder. No allocation,
  no locking, no syscall per call.
- **`metrics-exporter-prometheus`** spawns its own background listener thread
  (using `hyper` internally) to serve `/metrics` in Prometheus text format. It
  does not require the caller to have a Tokio runtime — it creates one
  internally for just the HTTP listener.

The exporter is optional: it is only installed when the user passes
`--metrics-port <PORT>`. When not configured, no listener is spawned and
the `metrics` macros still work (they become no-ops if no recorder is
installed).

Additionally, `tracing-subscriber` (v0.3) with the `env-filter` feature
replaces the previous conditional `#[cfg(feature)]` tracing init. Structured
logging is always on, defaulting to `info` level, overridable via `RUST_LOG`.

## Consequences

- **Positive:** Full Prometheus observability with 9 metrics (connections,
  messages, subscriptions, slow consumers, auth failures) and per-worker labels.
- **Positive:** Zero runtime dependency on Tokio in the main server path. The
  exporter's internal Tokio is isolated to the metrics listener thread.
- **Positive:** Structured logging always available, no feature flag needed.
- **Negative:** Three new dependencies (`metrics`, `metrics-exporter-prometheus`,
  `tracing-subscriber`) add to compile time and binary size.
- **Negative:** The `metrics-exporter-prometheus` crate pulls in `hyper` and
  `tokio` as transitive dependencies (used only for the listener thread). This
  increases the dependency tree but does not affect the server's hot path.
