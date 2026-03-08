#!/usr/bin/env bash
set -euo pipefail

BIN="./target/release/examples/leaf_server"
OUTDIR="open-wire/profile_results"
PORT=15226
MSGS=3000000

mkdir -p "$OUTDIR"

$BIN --port $PORT &>/dev/null &
SRV_PID=$!
for i in $(seq 1 50); do nc -z 127.0.0.1 $PORT 2>/dev/null && break; sleep 0.1; done
sleep 0.5

kill -0 $SRV_PID && echo "Server alive (PID $SRV_PID)" || { echo "Server dead!"; exit 1; }

perf record -g --call-graph fp -F 997 -p $SRV_PID -o "$OUTDIR/pub_only_fp3.perf.data" &
PERF_PID=$!
sleep 0.5

echo "Running publisher..."
nats bench pub bench.test --msgs $MSGS --size 128 --no-progress \
    -s "nats://127.0.0.1:$PORT" 2>&1 | grep stats:

sleep 0.5
kill $PERF_PID 2>/dev/null; wait $PERF_PID 2>/dev/null || true
kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null || true

echo ""
echo "=== PUB-ONLY FLAT PROFILE (self%) ==="
perf report -i "$OUTDIR/pub_only_fp3.perf.data" --stdio --no-children \
    --percent-limit 0.3 2>&1 | grep -E "^\s+[0-9].*leaf_server|^\s+[0-9].*libc" | head -40
