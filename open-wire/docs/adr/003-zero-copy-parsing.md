# ADR-003: Zero-Copy Protocol Parsing

**Status:** Accepted

## Context

The initial protocol parser converted incoming bytes to `&str` (via
`from_utf8`) and then used `str::split`, `str::parse::<usize>`, and string
allocation to extract PUB/SUB fields. Profiling showed 15–20% of CPU time
in parsing, with `from_utf8` validation and `parse::<usize>` as the main
costs. Pub-only throughput was ~37% of Go.

The NATS protocol is ASCII. Subjects, SIDs, and sizes are all ASCII byte
sequences that can be processed without UTF-8 validation.

## Decision

Write a custom byte-level parser in `nats_proto.rs` that operates directly on
`&[u8]` / `BytesMut`:

- `parse_size()` — hand-rolled ASCII-to-usize, no `from_utf8` or `str::parse`.
- `split_args::<N>()` — splits a line into N space-delimited `&[u8]` segments
  without allocation.
- `find_newline()` — scans for `\r\n` delimiters.
- `try_parse_client_op()` — returns `ClientOp` with `Bytes` fields (zero-copy
  slices from the read buffer via `BytesMut::split_to`).
- `try_skip_or_parse_client_op()` — fast path that skips over PUB/HPUB
  messages without extracting fields, used when the server has no subscribers.
- `MsgBuilder` — assembles outgoing MSG/HMSG/LMSG directly as byte sequences
  using `usize_to_buf()` instead of `write!()` formatting.
- `sid_to_bytes()` — pre-formats subscription IDs as ASCII `Bytes` at
  subscribe time, avoiding per-message conversion.

## Consequences

- **Positive:** Pub-only throughput went from 37% to 84% of Go (before other
  optimisations pushed it further). Parsing dropped from 15–20% to < 3% of CPU.
- **Positive:** Zero heap allocation in the PUB hot path — subjects and
  payloads are `Bytes` slices into the existing read buffer.
- **Positive:** The skip path (`try_skip_or_parse_client_op`) avoids even
  slicing when there are no matching subscribers.
- **Negative:** The parser is hand-written and harder to audit for correctness
  than a parser-combinator or `str`-based approach. Mitigated by 80+ unit tests.
- **Negative:** Assumes ASCII protocol — would need rework for any future
  binary framing.
