#!/usr/bin/env bash
# Leaf node benchmark: Rust leaf vs Go native leaf vs direct hub.
#
# Scenarios:
#   1. Publish only        — raw ingest rate (fire-and-forget)
#   2. Local pub/sub       — 1 pub + 1 sub on same server (message routing)
#   3. Local fan-out       — 1 pub + 5 subs on same server (fan-out delivery)
#   4. Leaf→Hub pub/sub    — pub on leaf, sub on hub (upstream forwarding)
#   5. Hub→Leaf pub/sub    — pub on hub, sub on leaf (downstream delivery)
#   6. WS pub/sub          — 1 pub + 1 sub over WebSocket
#   7. WS fan-out x5       — 1 pub + 5 subs over WebSocket
#   8. WS fan-out x10      — 1 pub + 10 subs over WebSocket (high fan-out)
#
# Prerequisites:
#   - nats-server in PATH  (go install github.com/nats-io/nats-server/v2@main)
#   - nats CLI in PATH     (go install github.com/nats-io/natscli/nats@latest)
#   - cargo (Rust toolchain)
#
# Usage:
#   cd open-wire && ./bench.sh
#   ./bench.sh --msgs 500000 --size 256 --runs 2

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults (overridable via CLI args)
MSGS=500000
SIZE=128
RUNS=3

# Parse optional overrides
while [[ $# -gt 0 ]]; do
  case "$1" in
    --msgs)  MSGS="$2";  shift 2 ;;
    --size)  SIZE="$2";  shift 2 ;;
    --runs)  RUNS="$2";  shift 2 ;;
    *)       echo "Unknown arg: $1"; echo "Usage: $0 [--msgs N] [--size N] [--runs N]"; exit 1 ;;
  esac
done

# Ports
HUB_CLIENT_PORT=4333
HUB_LEAF_PORT=7422
GO_LEAF_PORT=4225
GO_LEAF_WS_PORT=4226
RUST_LEAF_PORT=5223
RUST_LEAF_WS_PORT=5224

# PID tracking for cleanup
PIDS=()
BG_PIDS=()
cleanup() {
  echo ""
  echo "Cleaning up..."
  for pid in "${BG_PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

# Check prerequisites
for cmd in nats-server nats cargo; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: $cmd not found in PATH"
    exit 1
  fi
done

# Check ports are free
for port in $HUB_CLIENT_PORT $HUB_LEAF_PORT $GO_LEAF_PORT $GO_LEAF_WS_PORT \
            $RUST_LEAF_PORT $RUST_LEAF_WS_PORT; do
  if ss -tln 2>/dev/null | grep -q ":${port} "; then
    echo "ERROR: port $port already in use"
    exit 1
  fi
done

echo "================================================================"
echo "  Leaf Node Benchmark"
echo "  msgs=$MSGS  size=${SIZE}B  runs=$RUNS"
echo "================================================================"
echo ""

# --- Build Rust leaf server ---
echo "Building Rust leaf server (release)..."
cargo build --manifest-path "$REPO_ROOT/Cargo.toml" \
  -p open-wire --release --example leaf_server 2>&1 | tail -1
RUST_BIN="$REPO_ROOT/target/release/examples/leaf_server"
echo ""

# --- Start hub ---
echo "Starting hub (client=$HUB_CLIENT_PORT, leafnode=$HUB_LEAF_PORT)..."
nats-server -c "$SCRIPT_DIR/configs/bench_hub.conf" &
PIDS+=($!)
sleep 1

# --- Start Go native leaf (with WebSocket) ---
echo "Starting Go native leaf (tcp=$GO_LEAF_PORT, ws=$GO_LEAF_WS_PORT)..."
nats-server -c "$SCRIPT_DIR/configs/bench_go_leaf_ws.conf" &
PIDS+=($!)
sleep 1

# --- Start Rust leaf (with WebSocket) ---
echo "Starting Rust leaf (tcp=$RUST_LEAF_PORT, ws=$RUST_LEAF_WS_PORT)..."
"$RUST_BIN" --port "$RUST_LEAF_PORT" --ws-port "$RUST_LEAF_WS_PORT" \
  --hub "nats://127.0.0.1:$HUB_LEAF_PORT" &
PIDS+=($!)
sleep 2

# Verify connections
echo ""
echo "Verifying connectivity..."
nats pub _bench.ping pong -s "nats://127.0.0.1:$HUB_CLIENT_PORT" >/dev/null 2>&1 || { echo "FAIL: hub"; exit 1; }
nats pub _bench.ping pong -s "nats://127.0.0.1:$GO_LEAF_PORT"    >/dev/null 2>&1 || { echo "FAIL: go leaf"; exit 1; }
nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_LEAF_PORT"  >/dev/null 2>&1 || { echo "FAIL: rust leaf"; exit 1; }
nats pub _bench.ping pong -s "ws://127.0.0.1:$GO_LEAF_WS_PORT"   >/dev/null 2>&1 || { echo "FAIL: go leaf ws"; exit 1; }
nats pub _bench.ping pong -s "ws://127.0.0.1:$RUST_LEAF_WS_PORT" >/dev/null 2>&1 || { echo "FAIL: rust leaf ws"; exit 1; }
echo "All servers responding (TCP + WebSocket)."
echo ""

# Kill any lingering background bench processes
kill_bg() {
  for pid in "${BG_PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
  BG_PIDS=()
}

# ──────────────────────────────────────────────────────────────────────
# Scenario 1: Publish Only (fire-and-forget ingest)
# ──────────────────────────────────────────────────────────────────────
run_pub_only() {
  local label="$1" url="$2"
  echo "--- $label ---"
  for i in $(seq 1 "$RUNS"); do
    nats bench pub bench.test \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" 2>&1 | grep -E "stats:"
  done
  echo ""
}

echo "================================================================"
echo "  1. PUBLISH ONLY (fire-and-forget, no subscribers)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_pub_only "Direct Hub"       "nats://127.0.0.1:$HUB_CLIENT_PORT"
run_pub_only "Go Native Leaf"   "nats://127.0.0.1:$GO_LEAF_PORT"
run_pub_only "Rust Leaf"        "nats://127.0.0.1:$RUST_LEAF_PORT"

# ──────────────────────────────────────────────────────────────────────
# Scenario 2: Local Pub/Sub (1 publisher + 1 subscriber, same server)
# ──────────────────────────────────────────────────────────────────────
wait_or_kill() {
  local pid="$1" max_wait="${2:-30}"
  if timeout "$max_wait" tail --pid="$pid" -f /dev/null 2>/dev/null; then
    wait "$pid" 2>/dev/null || true
  else
    echo "  (subscriber $pid timed out after ${max_wait}s, killing)"
    kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true
  fi
}

# Generic pub/sub benchmark using full URLs.
# Pub and sub use the same URL. Supports nats:// and ws:// schemes.
run_url_pubsub() {
  local label="$1" url="$2" subs="$3" subject="${4:-bench.ps.test}"
  echo "--- $label ---"
  for i in $(seq 1 "$RUNS"); do
    # Start subscriber(s) in background
    for s in $(seq 1 "$subs"); do
      nats bench sub "$subject" \
        --msgs "$MSGS" --size "$SIZE" --no-progress \
        -s "$url" >"/tmp/bench_sub_${s}.out" 2>&1 &
      BG_PIDS+=($!)
    done
    sleep 0.5  # let subscribers connect and register

    # Run publisher (foreground) — its output has the pub stats
    nats bench pub "$subject" \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" 2>&1 | grep -E "stats:"

    # Wait for subscribers (with timeout) and print their stats
    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    for s in $(seq 1 "$subs"); do
      grep -E "stats:" "/tmp/bench_sub_${s}.out" 2>/dev/null | sed "s/^/  sub[$s] /"
      rm -f "/tmp/bench_sub_${s}.out"
    done
    BG_PIDS=()
  done
  echo ""
}

echo "================================================================"
echo "  2. LOCAL PUB/SUB (1 pub + 1 sub, same server)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_url_pubsub "Direct Hub"       "nats://127.0.0.1:$HUB_CLIENT_PORT" 1
run_url_pubsub "Go Native Leaf"   "nats://127.0.0.1:$GO_LEAF_PORT"    1
run_url_pubsub "Rust Leaf"        "nats://127.0.0.1:$RUST_LEAF_PORT"  1

# ──────────────────────────────────────────────────────────────────────
# Scenario 3: Fan-out (1 pub + 5 subs, same server)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  3. FAN-OUT (1 pub + 5 subs, same server)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_url_pubsub "Direct Hub"       "nats://127.0.0.1:$HUB_CLIENT_PORT" 5
run_url_pubsub "Go Native Leaf"   "nats://127.0.0.1:$GO_LEAF_PORT"    5
run_url_pubsub "Rust Leaf"        "nats://127.0.0.1:$RUST_LEAF_PORT"  5

# ──────────────────────────────────────────────────────────────────────
# Scenario 4: Leaf→Hub (pub on leaf, sub on hub)
# ──────────────────────────────────────────────────────────────────────
run_cross_pubsub() {
  local label="$1" pub_url="$2" sub_url="$3"
  echo "--- $label ---"
  for i in $(seq 1 "$RUNS"); do
    # Subscriber on destination server
    nats bench sub "bench.cross.test" \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$sub_url" >"/tmp/bench_cross_sub.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    # Publisher on source server
    nats bench pub "bench.cross.test" \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$pub_url" 2>&1 | grep -E "stats:"

    # Wait for subscriber (with timeout)
    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    grep -E "stats:" /tmp/bench_cross_sub.out 2>/dev/null | sed 's/^/  sub  /'
    rm -f /tmp/bench_cross_sub.out
    BG_PIDS=()
  done
  echo ""
}

echo "================================================================"
echo "  4. LEAF → HUB (pub on leaf, sub on hub)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_cross_pubsub "Go Leaf → Hub"    "nats://127.0.0.1:$GO_LEAF_PORT"   "nats://127.0.0.1:$HUB_CLIENT_PORT"
run_cross_pubsub "Rust Leaf → Hub"  "nats://127.0.0.1:$RUST_LEAF_PORT" "nats://127.0.0.1:$HUB_CLIENT_PORT"

# ──────────────────────────────────────────────────────────────────────
# Scenario 5: Hub→Leaf (pub on hub, sub on leaf)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  5. HUB → LEAF (pub on hub, sub on leaf)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_cross_pubsub "Hub → Go Leaf"    "nats://127.0.0.1:$HUB_CLIENT_PORT" "nats://127.0.0.1:$GO_LEAF_PORT"
run_cross_pubsub "Hub → Rust Leaf"  "nats://127.0.0.1:$HUB_CLIENT_PORT" "nats://127.0.0.1:$RUST_LEAF_PORT"

# ──────────────────────────────────────────────────────────────────────
# Scenario 6: WebSocket Pub/Sub (1 pub + 1 sub over WS)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  6. WEBSOCKET PUB/SUB (1 pub + 1 sub, same server, ws://)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_url_pubsub "Go Leaf WS"    "ws://127.0.0.1:$GO_LEAF_WS_PORT"   1 "bench.ws.test"
run_url_pubsub "Rust Leaf WS"  "ws://127.0.0.1:$RUST_LEAF_WS_PORT" 1 "bench.ws.test"

# ──────────────────────────────────────────────────────────────────────
# Scenario 7: WebSocket Fan-out x5 (1 pub + 5 subs over WS)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  7. WEBSOCKET FAN-OUT x5 (1 pub + 5 subs, same server, ws://)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_url_pubsub "Go Leaf WS"    "ws://127.0.0.1:$GO_LEAF_WS_PORT"   5 "bench.ws.fan5"
run_url_pubsub "Rust Leaf WS"  "ws://127.0.0.1:$RUST_LEAF_WS_PORT" 5 "bench.ws.fan5"

# ──────────────────────────────────────────────────────────────────────
# Scenario 8: WebSocket Fan-out x10 (1 pub + 10 subs over WS)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  8. WEBSOCKET FAN-OUT x10 (1 pub + 10 subs, same server, ws://)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_url_pubsub "Go Leaf WS"    "ws://127.0.0.1:$GO_LEAF_WS_PORT"   10 "bench.ws.fan10"
run_url_pubsub "Rust Leaf WS"  "ws://127.0.0.1:$RUST_LEAF_WS_PORT" 10 "bench.ws.fan10"

echo "================================================================"
echo "  BENCHMARK COMPLETE"
echo "================================================================"
