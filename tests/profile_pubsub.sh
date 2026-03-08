#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BIN="$REPO_ROOT/target/release/open-wire"
OUTDIR="$SCRIPT_DIR/profile_results"
PORT=15225
MSGS=3000000

mkdir -p "$OUTDIR"

# Start server
$BIN --port $PORT &>/dev/null &
SRV_PID=$!
echo "Server PID: $SRV_PID"
for i in $(seq 1 50); do nc -z 127.0.0.1 $PORT 2>/dev/null && break; sleep 0.1; done
sleep 0.5

# Start subscriber
nats bench sub bench.ps.test --msgs $MSGS --size 128 --no-progress \
    -s "nats://127.0.0.1:$PORT" > /tmp/profile_sub.out 2>&1 &
SUB_PID=$!
echo "Sub PID: $SUB_PID"
sleep 1

# Verify
kill -0 $SRV_PID && echo "Server alive" || { echo "Server dead!"; exit 1; }

# Start perf
perf record -g --call-graph fp -F 997 -p $SRV_PID -o "$OUTDIR/local_pubsub_fp.perf.data" &
PERF_PID=$!
sleep 0.5

# Run publisher
echo "Running publisher..."
nats bench pub bench.ps.test --msgs $MSGS --size 128 --no-progress \
    -s "nats://127.0.0.1:$PORT" 2>&1 | grep stats:

# Wait for subscriber
for i in $(seq 1 60); do
    kill -0 $SUB_PID 2>/dev/null || break
    sleep 0.5
done
wait $SUB_PID 2>/dev/null || true
grep stats: /tmp/profile_sub.out 2>/dev/null | sed 's/^/  sub /' || true

sleep 0.5
kill $PERF_PID 2>/dev/null; wait $PERF_PID 2>/dev/null || true
kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null || true

echo ""
echo "=== LOCAL PUB/SUB FLAT PROFILE (self%) ==="
perf report -i "$OUTDIR/local_pubsub_fp.perf.data" --stdio --no-children \
    --percent-limit 0.3 2>&1 | grep -E "^\s+[0-9].*open.wire|^\s+[0-9].*libc" | head -40
