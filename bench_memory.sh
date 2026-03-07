#!/usr/bin/env bash
# Benchmark: compare idle-connection memory between Go nats-server and Rust leaf server.
set -euo pipefail

NUM_CLIENTS=${1:-1000}
GO_PORT=24222
RUST_PORT=24223
CLIENT_BIN="./bench_clients_bin"

rss_kb() {
    ps -o rss= -p "$1" 2>/dev/null | tr -d ' '
}

wait_for_port() {
    for _ in $(seq 1 50); do
        if nc -z 127.0.0.1 "$1" 2>/dev/null; then return 0; fi
        sleep 0.1
    done
    echo "ERROR: port $1 never ready" >&2; return 1
}

GO_PID="" ; RUST_PID="" ; CLIENT_PID=""
cleanup() {
    [ -n "$CLIENT_PID" ] && kill "$CLIENT_PID" 2>/dev/null || true
    [ -n "$GO_PID" ] && kill "$GO_PID" 2>/dev/null || true
    [ -n "$RUST_PID" ] && kill "$RUST_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

echo "========================================"
echo "Memory Benchmark: Go vs Rust Leaf Server"
echo "Idle clients: $NUM_CLIENTS"
echo "========================================"
echo

# ===================== GO NATS-SERVER =====================
echo "--- Go nats-server ---"
nats-server -p $GO_PORT &>/dev/null &
GO_PID=$!
wait_for_port $GO_PORT
sleep 1

GO_BASELINE=$(rss_kb $GO_PID)
echo "  Baseline RSS:  ${GO_BASELINE} KB"

$CLIENT_BIN "127.0.0.1:$GO_PORT" "$NUM_CLIENTS" &
CLIENT_PID=$!
sleep 4

GO_LOADED=$(rss_kb $GO_PID)
echo "  Loaded RSS:    ${GO_LOADED} KB  ($NUM_CLIENTS clients)"

GO_DELTA=$((GO_LOADED - GO_BASELINE))
GO_PER_CLIENT=$((GO_DELTA * 1024 / NUM_CLIENTS))
echo "  Delta:         ${GO_DELTA} KB"
echo "  Per-client:    ${GO_PER_CLIENT} bytes"

kill "$CLIENT_PID" 2>/dev/null || true; wait "$CLIENT_PID" 2>/dev/null || true; CLIENT_PID=""
kill "$GO_PID" 2>/dev/null || true; wait "$GO_PID" 2>/dev/null || true; GO_PID=""
sleep 2

# ===================== RUST LEAF SERVER =====================
echo
echo "--- Rust leaf server (adaptive bufs) ---"
./target/release/examples/leaf_server --port $RUST_PORT &>/dev/null &
RUST_PID=$!
wait_for_port $RUST_PORT
sleep 1

RUST_BASELINE=$(rss_kb $RUST_PID)
echo "  Baseline RSS:  ${RUST_BASELINE} KB"

$CLIENT_BIN "127.0.0.1:$RUST_PORT" "$NUM_CLIENTS" &
CLIENT_PID=$!
sleep 4

RUST_LOADED=$(rss_kb $RUST_PID)
echo "  Loaded RSS:    ${RUST_LOADED} KB  ($NUM_CLIENTS clients)"

RUST_DELTA=$((RUST_LOADED - RUST_BASELINE))
RUST_PER_CLIENT=$((RUST_DELTA * 1024 / NUM_CLIENTS))
echo "  Delta:         ${RUST_DELTA} KB"
echo "  Per-client:    ${RUST_PER_CLIENT} bytes"

kill "$CLIENT_PID" 2>/dev/null || true; wait "$CLIENT_PID" 2>/dev/null || true; CLIENT_PID=""
kill "$RUST_PID" 2>/dev/null || true; wait "$RUST_PID" 2>/dev/null || true; RUST_PID=""

# ===================== SUMMARY =====================
echo
echo "========================================"
echo "Summary (per-client idle memory cost)"
echo "========================================"
printf "  %-25s %6d KB total delta  |  %6d bytes/client\n" "Go nats-server" "$GO_DELTA" "$GO_PER_CLIENT"
printf "  %-25s %6d KB total delta  |  %6d bytes/client\n" "Rust leaf (adaptive)" "$RUST_DELTA" "$RUST_PER_CLIENT"
if [ "$RUST_PER_CLIENT" -gt 0 ] && [ "$GO_PER_CLIENT" -gt 0 ]; then
    RATIO=$(awk "BEGIN{printf \"%.1f\", $GO_PER_CLIENT / $RUST_PER_CLIENT}")
    echo
    echo "  Go / Rust ratio: ${RATIO}x"
fi
echo
