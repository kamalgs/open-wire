#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BIN="$REPO_ROOT/target/release/open-wire"
OUTDIR="$SCRIPT_DIR/profile_results"
RUST_PORT=15229
HUB_PORT=14333
LEAF_PORT=17422
MSGS=3000000

mkdir -p "$OUTDIR"

# Start hub
nats-server -a 127.0.0.1 -p $HUB_PORT \
  --leafnodes "127.0.0.1:${LEAF_PORT}" &>/dev/null &
HUB_PID=$!
for i in $(seq 1 50); do nc -z 127.0.0.1 $HUB_PORT 2>/dev/null && break; sleep 0.1; done
sleep 0.5
echo "Hub alive (PID $HUB_PID)"

# Start Rust leaf connected to hub
$BIN --port $RUST_PORT --hub "nats://127.0.0.1:$LEAF_PORT" &>/dev/null &
SRV_PID=$!
for i in $(seq 1 50); do nc -z 127.0.0.1 $RUST_PORT 2>/dev/null && break; sleep 0.1; done
sleep 1
echo "Rust leaf alive (PID $SRV_PID)"

# Start subscriber on Rust leaf
nats bench sub bench.hl.test --msgs $MSGS --size 128 --no-progress \
    -s "nats://127.0.0.1:$RUST_PORT" > /tmp/profile_hl_sub.out 2>&1 &
SUB_PID=$!
echo "Sub PID: $SUB_PID"
sleep 1

# Start perf on Rust leaf
perf record -g --call-graph fp -F 997 -p $SRV_PID -o "$OUTDIR/hub_leaf_fp.perf.data" &
PERF_PID=$!
sleep 0.5

# Publish on the hub
echo "Running publisher on hub..."
nats bench pub bench.hl.test --msgs $MSGS --size 128 --no-progress \
    -s "nats://127.0.0.1:$HUB_PORT" 2>&1 | grep stats:

# Wait for subscriber
for i in $(seq 1 60); do
    kill -0 $SUB_PID 2>/dev/null || break
    sleep 0.5
done
wait $SUB_PID 2>/dev/null || true
grep stats: /tmp/profile_hl_sub.out 2>/dev/null | sed 's/^/  sub /' || true

sleep 0.5
kill $PERF_PID 2>/dev/null; wait $PERF_PID 2>/dev/null || true
kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null || true
kill $HUB_PID 2>/dev/null; wait $HUB_PID 2>/dev/null || true

echo ""
echo "=== HUB→LEAF FLAT PROFILE (self%) ==="
perf report -i "$OUTDIR/hub_leaf_fp.perf.data" --stdio --no-children \
    --percent-limit 0.3 2>&1 | grep -E "^\s+[0-9].*open.wire|^\s+[0-9].*libc" | head -40
