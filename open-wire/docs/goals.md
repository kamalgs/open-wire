# Goals and Non-Goals

**Project name: open-wire** — a NATS-compatible message relay, written in Rust.
(Crate: `open-wire`)

## Goals

- **Match or beat Go nats-server throughput** on leaf node workloads (pub/sub,
  fan-out, leaf↔hub forwarding). The Go server is the reference benchmark.
- **Minimal idle memory** — a server with 10K mostly-idle connections should not
  consume hundreds of megabytes of buffer space.
- **Minimal dependencies** — prefer std and libc over large frameworks. Keep the
  dependency tree small and auditable.
- **Single binary** — the server compiles to one static binary with no runtime
  dependencies beyond libc.
- **Cross-platform** — run on any OS, any architecture, any scale. From a
  Raspberry Pi to a CDN edge node. See [portability.md](portability.md).
- **Learning project** — explore what raw-syscall, zero-copy Rust can achieve
  against a mature Go implementation. Document decisions and tradeoffs.

## Non-Goals

- **Full NATS server** — no JetStream, clustering, accounts, authorization,
  or multi-tenancy. This is a leaf node only.
- **TLS / authentication** — not implemented. The server trusts its network.
- **Production use** — this is an experiment, not a hardened production system.
  Error handling, observability, and operational tooling are minimal.
- **Protocol completeness** — only the subset of NATS client and leaf protocols
  needed for pub/sub benchmarking is implemented (no queue groups, no request
  headers, no UNSUB max-messages).
