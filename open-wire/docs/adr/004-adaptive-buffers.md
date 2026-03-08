# ADR-004: Adaptive Buffer Sizing

**Status:** Accepted

## Context

The initial implementation used fixed 64 KB read buffers per connection. With
10,000 mostly-idle connections this consumed 640 MB of memory just for read
buffers. Most connections send small or infrequent messages and never need
64 KB.

The Go nats-server uses a dynamic buffer strategy: start small, grow on large
reads, shrink back after a period of small reads.

## Decision

Implement `AdaptiveBuf` in `protocol.rs` with Go-inspired dynamic sizing:

- **Start** at 512 bytes (`DEFAULT_START_BUF`).
- **Grow** to double the current capacity when a read fills ≥ half the buffer,
  up to a configurable ceiling (default 64 KB, `DEFAULT_MAX_BUF`).
- **Shrink** back toward the minimum (64 bytes, `DEFAULT_MIN_BUF`) after 2
  consecutive short reads (`SHORTS_TO_SHRINK`) and when the buffer is empty.
- `read_from_fd(fd)` reads directly via `libc::read` into the buffer's spare
  capacity, avoiding a double-copy through `std::io::Read`.

## Consequences

- **Positive:** Idle memory per connection dropped from 64 KB to ~512 bytes —
  roughly 33× reduction. 10K idle connections use ~5 MB instead of 640 MB.
- **Positive:** Active connections still scale up to 64 KB for throughput.
  The grow/shrink cycle adds negligible overhead.
- **Positive:** The `read_from_fd` method avoids the `Read` trait's requirement
  to initialise the buffer, saving a memset on each grow.
- **Negative:** Added complexity in buffer lifecycle. Incorrect shrink logic
  could cause excessive reallocation under bursty workloads. Mitigated by
  the `SHORTS_TO_SHRINK` hysteresis counter.
