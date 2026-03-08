#!/usr/bin/env bash
# Profile the Rust leaf server under two workloads:
#   1. Publish only (fire-and-forget)
#   2. Local pub/sub (1 pub + 1 sub)
#
# Produces perf report text files in open-wire/profile_results/
set -euo pipefail

MSGS=2000000
SIZE=128
RUST_PORT=15223
OUTDIR="open-wire/profile_results"
BIN="./target/release/examples/leaf_server"

mkdir -p "$OUTDIR"

wait_for_port() {
    for _ in $(seq 1 50); do
        nc -z 127.0.0.1 "$1" 2>/dev/null && return 0
        sleep 0.1
    done
    echo "port $1 never ready" >&2; return 1
}

# ═══════════════════════════════════════════
# Scenario 1: Publish Only
# ═══════════════════════════════════════════
echo "=== Profiling: PUBLISH ONLY (${MSGS} msgs × ${SIZE}B) ==="

$BIN --port $RUST_PORT &>/dev/null &
SRV_PID=$!
wait_for_port $RUST_PORT
sleep 0.5

# Start perf recording on the server process
perf record -g --call-graph dwarf,32768 -F 997 -p $SRV_PID -o "$OUTDIR/pub_only_new.perf.data" &
PERF_PID=$!
sleep 0.5

# Run the workload
nats bench pub bench.test --msgs $MSGS --size $SIZE --no-progress \
    -s "nats://127.0.0.1:$RUST_PORT" 2>&1 | grep -E "stats:"

sleep 1
kill $PERF_PID 2>/dev/null; wait $PERF_PID 2>/dev/null || true
kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null || true
sleep 1

# Generate report
perf report -i "$OUTDIR/pub_only_new.perf.data" --stdio --no-children \
    --percent-limit 0.5 2>&1 > "$OUTDIR/pub_only_new.perf.txt"
echo "  Saved: $OUTDIR/pub_only_new.perf.txt"
echo ""

# ═══════════════════════════════════════════
# Scenario 2: Local Pub/Sub (1:1)
# ═══════════════════════════════════════════
echo "=== Profiling: LOCAL PUB/SUB (${MSGS} msgs × ${SIZE}B) ==="

$BIN --port $RUST_PORT &>/dev/null &
SRV_PID=$!
wait_for_port $RUST_PORT
sleep 0.5

# Start subscriber first
nats bench sub bench.ps.test --msgs $MSGS --size $SIZE --no-progress \
    -s "nats://127.0.0.1:$RUST_PORT" > /tmp/profile_sub.out 2>&1 &
SUB_PID=$!
sleep 1

# Start perf recording
perf record -g --call-graph dwarf,32768 -F 997 -p $SRV_PID -o "$OUTDIR/local_pubsub_new.perf.data" &
PERF_PID=$!
sleep 0.5

# Run publisher
nats bench pub bench.ps.test --msgs $MSGS --size $SIZE --no-progress \
    -s "nats://127.0.0.1:$RUST_PORT" 2>&1 | grep -E "stats:"

# Wait for sub
timeout 30 tail --pid=$SUB_PID -f /dev/null 2>/dev/null || true
wait $SUB_PID 2>/dev/null || true
grep -E "stats:" /tmp/profile_sub.out 2>/dev/null | sed 's/^/  sub /' || true

sleep 1
kill $PERF_PID 2>/dev/null; wait $PERF_PID 2>/dev/null || true
kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null || true

# Generate report
perf report -i "$OUTDIR/local_pubsub_new.perf.data" --stdio --no-children \
    --percent-limit 0.5 2>&1 > "$OUTDIR/local_pubsub_new.perf.txt"
echo "  Saved: $OUTDIR/local_pubsub_new.perf.txt"
echo ""

echo "=== Done ==="
