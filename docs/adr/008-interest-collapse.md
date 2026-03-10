# ADR-008: Interest Collapse Templates for Leaf Node Subscriptions

**Status:** Accepted

## Context

When a leaf node manages thousands of clients subscribing to session-specific
subjects (e.g., `app.node1.session1`, `app.node1.session2`, ...), each unique
subject generates an individual `LS+` to the hub. With 10K sessions this means
10K `LS+` messages, 10K entries in the hub's interest map, and 10K `RS+`
propagations across hub cluster routes.

This creates significant overhead on the hub side — both in memory and in route
propagation — while the leaf node itself only needs precise local routing.

Two approaches were considered:

1. **Threshold-based**: Automatically collapse after N unique subjects match a
   wildcard pattern. Requires runtime heuristics and can surprise operators.
2. **Template-based**: Operators configure explicit patterns to collapse.
   Predictable, zero runtime overhead for pattern detection.

## Decision

Use operator-configured **collapse templates**. Each template is a NATS subject
pattern (e.g., `app.*.sessions.>`). When a client subscribes to a subject
matching a template, the leaf sends a single collapsed wildcard `LS+` to the hub
instead of a per-subject `LS+`.

The leaf still tracks exact subscriptions locally for precise `for_each_match`
delivery. The hub may forward messages that don't match any local subscriber;
these get zero matches in `for_each_match` and are silently dropped — negligible
cost compared to the savings in upstream interest propagation.

Configuration lives in `leafnodes.remotes[0].interest_collapse` as an array of
subject patterns. Queue group subscriptions always use exact interest (no
collapsing) because queue semantics require precise routing.

## Consequences

- **Positive:** Upstream interest scales O(templates) instead of O(subjects).
  10K session subs collapse to 1 wildcard `LS+`.
- **Positive:** Hub-transparent — no protocol changes, hub sees standard
  wildcard subscriptions.
- **Positive:** Local routing precision preserved — `SubList.for_each_match`
  still uses exact subjects.
- **Negative:** Hub may over-deliver messages matching the wildcard but not any
  local subscriber. Filtered at leaf delivery time at negligible cost.
- **Negative:** Operators must explicitly configure templates. Misconfigured
  templates could cause excessive over-delivery.

## Update: Interest Transform Pipeline (v2)

Interest collapse was refactored from inline code in `upstream.rs` into a generic
two-stage `InterestPipeline` in `src/interest.rs`:

1. **Subject mapping** (`subject-mapping` feature) — stateless prefix/exact rewrite
2. **Interest collapse** (`interest-collapse` feature) — stateful N:1 aggregation

Both stages are Cargo feature-gated (`default = ["interest-collapse", "subject-mapping"]`)
and compile to no-ops when disabled. The pipeline composes naturally: mapping runs
first, then collapse operates on the mapped subjects.

Configuration added: `leafnodes.remotes[0].subject_mappings` as an array of
`{ from, to }` blocks supporting prefix mappings (`local.>` → `prod.>`) and
exact mappings.
