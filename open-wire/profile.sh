#!/usr/bin/env bash
# Profile the Rust leaf server under nats bench workloads using perf.
# Requires: nats-server, nats CLI, perf, and cargo (with release build).
#
# Note: uses flat sampling (no DWARF call-graph) because addr2line/binutils
# are not installed. Build requires debug=1 in [profile.release] for symbols.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/profile_results"
LEAF_PORT=14222
HUB_PORT=14223
MSGS=2000000
MSG_SIZE=128

mkdir -p "$RESULTS_DIR"

# Track PIDs for cleanup
PERF_PID=""
LEAF_PID=""
HUB_PID=""
SUB_PIDS=()

cleanup() {
    echo "Cleaning up..."
    for pid in "$PERF_PID" "${SUB_PIDS[@]}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    [ -n "$LEAF_PID" ] && kill "$LEAF_PID" 2>/dev/null || true
    [ -n "$HUB_PID" ] && kill "$HUB_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

# Build release binary with debug symbols (touch to force rebuild)
echo "=== Building release binary ==="
cd "$ROOT_DIR"
touch open-wire/src/lib.rs
cargo build --release -p open-wire --example leaf_server 2>&1 | tail -5
LEAF_BIN="$ROOT_DIR/target/release/examples/leaf_server"

# Verify binary has symbols (match "not stripped" explicitly)
if ! file "$LEAF_BIN" | grep -q "not stripped"; then
    echo "ERROR: binary is stripped, perf won't resolve symbols"
    echo "Ensure [profile.release] debug = 1 in Cargo.toml"
    exit 1
fi
echo "Binary OK: $(file "$LEAF_BIN" | grep -o 'not stripped')"

start_servers() {
    # Start hub nats-server
    nats-server -p "$HUB_PORT" -a 127.0.0.1 &
    HUB_PID=$!
    sleep 0.5

    # Start Rust leaf server connected to hub
    "$LEAF_BIN" --port "$LEAF_PORT" --host 127.0.0.1 --hub "nats://127.0.0.1:${HUB_PORT}" &
    LEAF_PID=$!
    sleep 0.5

    # Verify leaf is up
    if ! kill -0 "$LEAF_PID" 2>/dev/null; then
        echo "ERROR: leaf server failed to start"
        exit 1
    fi
    echo "Hub PID=$HUB_PID  Leaf PID=$LEAF_PID"
}

stop_servers() {
    kill "$LEAF_PID" 2>/dev/null || true
    kill "$HUB_PID" 2>/dev/null || true
    wait "$LEAF_PID" 2>/dev/null || true
    wait "$HUB_PID" 2>/dev/null || true
    LEAF_PID=""
    HUB_PID=""
}

generate_report() {
    local PERF_DATA="$1"
    local PERF_TXT="$2"
    local NAME="$3"

    {
        echo "=== Flat profile: top functions by self% ==="
        echo ""
        perf report -i "$PERF_DATA" --stdio --no-children --percent-limit 0.5 2>&1
    } > "$PERF_TXT"

    # Check if report has actual function data
    if ! grep -qE '^\s+[0-9]+\.[0-9]+%' "$PERF_TXT"; then
        echo "WARNING: perf report empty for $NAME, trying profile-bpfcc fallback..."
        if command -v profile-bpfcc &>/dev/null; then
            echo "(eBPF fallback — sampling leaf PID=$LEAF_PID for 10s)"
            sudo profile-bpfcc -p "$LEAF_PID" -F 997 10 > "$PERF_TXT" 2>&1 || true
        else
            echo "No profile-bpfcc available either; skipping $NAME"
        fi
    fi

    # Show summary to stdout
    echo ""
    echo "Top 20 hotspots for $NAME:"
    grep -E '^\s+[0-9]+\.[0-9]+%' "$PERF_TXT" | head -20 || true

    # Clean up perf.data (large file)
    rm -f "$PERF_DATA"
}

LEAF_URL="nats://127.0.0.1:${LEAF_PORT}"

# ============================================================
# Scenario 1: Pub-only (publish to leaf, no subscribers)
# ============================================================
echo ""
echo "=== Profiling: pub_only ==="
start_servers

PERF_DATA="$RESULTS_DIR/pub_only.perf.data"
perf record -F 997 -p "$LEAF_PID" -o "$PERF_DATA" &
PERF_PID=$!
sleep 0.3

echo "Running: nats bench pub (${MSGS} msgs, ${MSG_SIZE}B)"
nats bench pub test.bench -s "$LEAF_URL" --msgs "$MSGS" --size "$MSG_SIZE" --no-progress || true

kill -INT "$PERF_PID" 2>/dev/null || true
wait "$PERF_PID" 2>/dev/null || true
PERF_PID=""

generate_report "$PERF_DATA" "$RESULTS_DIR/pub_only.perf.txt" "pub_only"
stop_servers
sleep 0.3

# ============================================================
# Scenario 2: Local pub/sub (1 subscriber + 1 publisher on leaf)
# ============================================================
echo ""
echo "=== Profiling: local_pubsub ==="
start_servers

PERF_DATA="$RESULTS_DIR/local_pubsub.perf.data"
perf record -F 997 -p "$LEAF_PID" -o "$PERF_DATA" &
PERF_PID=$!
sleep 0.3

# Start subscriber in background
nats bench sub test.bench -s "$LEAF_URL" --msgs "$MSGS" --no-progress &
SUB_PIDS=($!)
sleep 0.3

# Run publisher
echo "Running: nats bench pub+sub (${MSGS} msgs, ${MSG_SIZE}B)"
nats bench pub test.bench -s "$LEAF_URL" --msgs "$MSGS" --size "$MSG_SIZE" --no-progress || true

# Wait for subscriber to finish
for pid in "${SUB_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
SUB_PIDS=()

kill -INT "$PERF_PID" 2>/dev/null || true
wait "$PERF_PID" 2>/dev/null || true
PERF_PID=""

generate_report "$PERF_DATA" "$RESULTS_DIR/local_pubsub.perf.txt" "local_pubsub"
stop_servers
sleep 0.3

# ============================================================
# Scenario 3: Fan-out x5 (5 subscribers + 1 publisher on leaf)
# ============================================================
echo ""
echo "=== Profiling: fanout ==="
start_servers

PERF_DATA="$RESULTS_DIR/fanout.perf.data"
perf record -F 997 -p "$LEAF_PID" -o "$PERF_DATA" &
PERF_PID=$!
sleep 0.3

# Start 5 subscribers in background
SUB_PIDS=()
for i in 1 2 3 4 5; do
    nats bench sub test.fanout -s "$LEAF_URL" --msgs "$MSGS" --no-progress &
    SUB_PIDS+=($!)
done
sleep 0.5

# Run publisher
echo "Running: nats bench pub + 5 subs (${MSGS} msgs, ${MSG_SIZE}B)"
nats bench pub test.fanout -s "$LEAF_URL" --msgs "$MSGS" --size "$MSG_SIZE" --no-progress || true

# Wait for subscribers
for pid in "${SUB_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
SUB_PIDS=()

kill -INT "$PERF_PID" 2>/dev/null || true
wait "$PERF_PID" 2>/dev/null || true
PERF_PID=""

generate_report "$PERF_DATA" "$RESULTS_DIR/fanout.perf.txt" "fanout"
stop_servers

echo ""
echo "=== Profiling complete ==="
echo "Results in: $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"*.perf.txt 2>/dev/null
