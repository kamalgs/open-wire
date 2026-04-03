#!/usr/bin/env bash
# Leaf node benchmark: Rust+Rust (Rust leaf + Rust hub) vs Go+Go (Go leaf + Go hub).
#
# Modes:
#   --quick (default) — 5 core scenarios, 1 run, 100K msgs (~1-2 min)
#   --full            — all 19 scenarios, 3 runs, 500K msgs (~5-10 min)
#
# Scenarios (quick mode runs 1-5 only, without "Direct Hub" baseline):
#   1. Publish only        — raw ingest rate (fire-and-forget)
#   2. Local pub/sub       — 1 pub + 1 sub on same server (message routing)
#   3. Local fan-out       — 1 pub + 5 subs on same server (fan-out delivery)
#   4. Leaf→Hub pub/sub    — pub on leaf, sub on hub (upstream forwarding)
#   5. Hub→Leaf pub/sub    — pub on hub, sub on leaf (downstream delivery)
#   6. WS pub/sub          — 1 pub + 1 sub over WebSocket
#   7. WS fan-out x5       — 1 pub + 5 subs over WebSocket
#   8. WS fan-out x10      — 1 pub + 10 subs over WebSocket (high fan-out)
#   9. Hub mode: pub only  — fire-and-forget on Rust hub
#  10. Hub mode: pub/sub   — 1 pub + 1 sub on Rust hub (local routing)
#  11. Hub mode: fan-out   — 1 pub + 5 subs on Rust hub
#  12. Hub mode: leaf→hub  — pub on Go leaf, sub on Rust hub
#  13. Hub mode: hub→leaf  — pub on Rust hub, sub on Go leaf
#  14. Cluster: pub/sub    — pub on A, sub on B (cross-node 1:1)
#  15. Cluster: fan-out x3 — pub on A, sub on A+B+C (3-node fan-out)
#  16. Cluster: remote B+C — pub on A, sub on B + sub on C (no local sub)
#  17. Gateway: pub/sub    — pub on alpha, sub on beta (cross-gateway 1:1)
#  18. Gateway: fan-out    — pub on alpha, sub on alpha + beta (gateway fan-out)
#  19. Gateway: req-reply  — request-reply across the gateway
#
# Prerequisites:
#   - nats-server in PATH  (go install github.com/nats-io/nats-server/v2@main)
#   - nats CLI in PATH     (go install github.com/nats-io/natscli/nats@latest)
#   - cargo (Rust toolchain)
#
# Usage:
#   cd tests && ./throughput.sh              # quick mode (default)
#   cd tests && ./throughput.sh --full       # full mode (all 13 scenarios)
#   ./throughput.sh --quick --msgs 50000     # quick with custom msg count
#   ./throughput.sh --full --runs 1          # full with fewer runs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse CLI args — mode flag + optional overrides
MODE=quick
MSGS=""
SIZE=""
RUNS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick) MODE=quick; shift ;;
    --full)  MODE=full;  shift ;;
    --msgs)  MSGS="$2";  shift 2 ;;
    --size)  SIZE="$2";  shift 2 ;;
    --runs)  RUNS="$2";  shift 2 ;;
    *)       echo "Unknown arg: $1"
             echo "Usage: $0 [--quick|--full] [--msgs N] [--size N] [--runs N]"
             exit 1 ;;
  esac
done

# Apply mode defaults for unset values
if [[ "$MODE" == "quick" ]]; then
  MSGS="${MSGS:-100000}"; SIZE="${SIZE:-128}"; RUNS="${RUNS:-1}"
else
  MSGS="${MSGS:-500000}"; SIZE="${SIZE:-128}"; RUNS="${RUNS:-3}"
fi

# Ports
HUB_CLIENT_PORT=4333
HUB_LEAF_PORT=7422
GO_LEAF_PORT=4225
GO_LEAF_WS_PORT=4226
RUST_LEAF_PORT=5223
RUST_LEAF_WS_PORT=5224
RUST_HUB_CLIENT_PORT=6333
RUST_HUB_LEAF_PORT=6422
RUST_LEAF2_PORT=5225      # Rust leaf connected to Rust hub (for Rust+Rust pair)
GO_LEAF_TO_RUST_PORT=6225

# 3-node cluster ports (full mode only)
GO_CLUSTER_A_PORT=7001;   GO_CLUSTER_A_ROUTE=7101
GO_CLUSTER_B_PORT=7002;   GO_CLUSTER_B_ROUTE=7102
GO_CLUSTER_C_PORT=7003;   GO_CLUSTER_C_ROUTE=7103
RUST_CLUSTER_A_PORT=8001; RUST_CLUSTER_A_ROUTE=8101
RUST_CLUSTER_B_PORT=8002; RUST_CLUSTER_B_ROUTE=8102
RUST_CLUSTER_C_PORT=8003; RUST_CLUSTER_C_ROUTE=8103

# 2-node gateway ports (full mode only)
GO_GW_A_PORT=9001; GO_GW_A_GW=9101
GO_GW_B_PORT=9002; GO_GW_B_GW=9102
RUST_GW_A_PORT=9201; RUST_GW_A_GW=9301
RUST_GW_B_PORT=9202; RUST_GW_B_GW=9302

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
PORTS_TO_CHECK="$HUB_CLIENT_PORT $HUB_LEAF_PORT $GO_LEAF_PORT $GO_LEAF_WS_PORT \
  $RUST_LEAF_PORT $RUST_LEAF_WS_PORT $RUST_HUB_CLIENT_PORT $RUST_HUB_LEAF_PORT $RUST_LEAF2_PORT"
if [[ "$MODE" == "full" ]]; then
  PORTS_TO_CHECK="$PORTS_TO_CHECK $GO_LEAF_TO_RUST_PORT \
    $GO_CLUSTER_A_PORT $GO_CLUSTER_A_ROUTE $GO_CLUSTER_B_PORT $GO_CLUSTER_B_ROUTE \
    $GO_CLUSTER_C_PORT $GO_CLUSTER_C_ROUTE \
    $RUST_CLUSTER_A_PORT $RUST_CLUSTER_A_ROUTE $RUST_CLUSTER_B_PORT $RUST_CLUSTER_B_ROUTE \
    $RUST_CLUSTER_C_PORT $RUST_CLUSTER_C_ROUTE \
    $GO_GW_A_PORT $GO_GW_A_GW $GO_GW_B_PORT $GO_GW_B_GW \
    $RUST_GW_A_PORT $RUST_GW_A_GW $RUST_GW_B_PORT $RUST_GW_B_GW"
fi
for port in $PORTS_TO_CHECK; do
  if ss -tln 2>/dev/null | grep -q ":${port} "; then
    echo "ERROR: port $port already in use"
    exit 1
  fi
done

echo "================================================================"
echo "  Leaf Node Benchmark ($MODE mode)"
echo "  msgs=$MSGS  size=${SIZE}B  runs=$RUNS"
echo "================================================================"
echo ""

# --- Build Rust leaf server ---
echo "Building Rust leaf server (release)..."
if [[ "$MODE" == "full" ]]; then
  cargo build --manifest-path "$REPO_ROOT/Cargo.toml" \
    --release --features mesh,gateway 2>&1 | tail -1
else
  cargo build --manifest-path "$REPO_ROOT/Cargo.toml" \
    --release 2>&1 | tail -1
fi
RUST_BIN="$REPO_ROOT/target/release/open-wire"
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
GO_LEAF_PID=$!
sleep 1

# --- Start Rust leaf (with WebSocket) — connects to Go hub ---
echo "Starting Rust leaf (tcp=$RUST_LEAF_PORT, ws=$RUST_LEAF_WS_PORT)..."
RUST_LOG=warn "$RUST_BIN" --port "$RUST_LEAF_PORT" --ws-port "$RUST_LEAF_WS_PORT" \
  --hub "nats://127.0.0.1:$HUB_LEAF_PORT" &
PIDS+=($!)
RUST_LEAF_PID=$!
sleep 2

# --- Start Rust hub (hub mode — accepts inbound leaf connections) ---
echo "Starting Rust hub (client=$RUST_HUB_CLIENT_PORT, leafnode=$RUST_HUB_LEAF_PORT)..."
RUST_LOG=warn "$RUST_BIN" -c "$SCRIPT_DIR/configs/bench_rust_hub.conf" &
PIDS+=($!)
RUST_HUB_PID=$!
sleep 1

# --- Start Rust leaf → Rust hub (for Rust+Rust comparison) ---
echo "Starting Rust leaf → Rust hub (tcp=$RUST_LEAF2_PORT)..."
RUST_LOG=warn "$RUST_BIN" --port "$RUST_LEAF2_PORT" \
  --hub "nats://127.0.0.1:$RUST_HUB_LEAF_PORT" &
PIDS+=($!)
RUST_LEAF2_PID=$!
sleep 2

if [[ "$MODE" == "full" ]]; then
  # --- Start Go leaf connecting to Rust hub (for hub-mode scenarios 12-13) ---
  echo "Starting Go leaf → Rust hub (tcp=$GO_LEAF_TO_RUST_PORT)..."
  nats-server -c "$SCRIPT_DIR/configs/bench_go_leaf_to_rust.conf" &
  PIDS+=($!)
  sleep 1
fi

# Verify connections
echo ""
echo "Verifying connectivity..."
nats pub _bench.ping pong -s "nats://127.0.0.1:$HUB_CLIENT_PORT"      >/dev/null 2>&1 || { echo "FAIL: hub"; exit 1; }
nats pub _bench.ping pong -s "nats://127.0.0.1:$GO_LEAF_PORT"         >/dev/null 2>&1 || { echo "FAIL: go leaf"; exit 1; }
nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_LEAF_PORT"       >/dev/null 2>&1 || { echo "FAIL: rust leaf"; exit 1; }
nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_HUB_CLIENT_PORT" >/dev/null 2>&1 || { echo "FAIL: rust hub"; exit 1; }
nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_LEAF2_PORT"      >/dev/null 2>&1 || { echo "FAIL: rust leaf2"; exit 1; }
if [[ "$MODE" == "full" ]]; then
  nats pub _bench.ping pong -s "ws://127.0.0.1:$GO_LEAF_WS_PORT"      >/dev/null 2>&1 || { echo "FAIL: go leaf ws"; exit 1; }
  nats pub _bench.ping pong -s "ws://127.0.0.1:$RUST_LEAF_WS_PORT"    >/dev/null 2>&1 || { echo "FAIL: rust leaf ws"; exit 1; }
  nats pub _bench.ping pong -s "nats://127.0.0.1:$GO_LEAF_TO_RUST_PORT" >/dev/null 2>&1 || { echo "FAIL: go leaf→rust hub"; exit 1; }
  echo "All servers responding (TCP + WebSocket + Rust+Rust pair)."
else
  echo "All servers responding (TCP + Rust+Rust pair)."
fi
echo ""

# Kill any lingering background bench processes
kill_bg() {
  for pid in "${BG_PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
  BG_PIDS=()
}

# ──────────────────────────────────────────────────────────────────────
# Resource stats helpers (Linux /proc)
# ──────────────────────────────────────────────────────────────────────
CLK_TCK=$(getconf CLK_TCK 2>/dev/null || echo 100)

# CPU ticks (utime + stime) for a PID
cpu_ticks() {
  local pid="$1"
  awk '{print $14 + $15}' "/proc/$pid/stat" 2>/dev/null || echo 0
}

# RSS in KB
rss_kb() {
  local pid="$1"
  awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0
}

# Context switches (voluntary + involuntary)
ctx_switches() {
  local pid="$1"
  awk '/ctxt_switches/{sum+=$2} END{print sum+0}' "/proc/$pid/status" 2>/dev/null || echo 0
}

# Format number with thousands separators
fmt_num() {
  printf "%'d" "$1" 2>/dev/null || echo "$1"
}

# Format context switches (compact: K suffix for >= 1000)
fmt_ctx() {
  local n="$1"
  if [[ "$n" -ge 1000 ]]; then
    # Print as X.YK
    local whole=$(( n / 1000 ))
    local frac=$(( (n % 1000) / 100 ))
    echo "${whole}.${frac}K"
  else
    echo "$n"
  fi
}

# ──────────────────────────────────────────────────────────────────────
# Summary table storage
# ──────────────────────────────────────────────────────────────────────
# Indexed arrays for summary results
SUMMARY_LABELS=()
SUMMARY_RUST_RATES=()
SUMMARY_GO_RATES=()
SUMMARY_RUST_CPU=()
SUMMARY_GO_CPU=()
SUMMARY_RUST_RSS=()
SUMMARY_GO_RSS=()
SUMMARY_RUST_CTX=()
SUMMARY_GO_CTX=()

# Extract msgs/sec from nats bench output (pub stats line)
extract_rate() {
  grep -oP '[\d,]+(?= msgs/sec)' | head -1 | tr -d ',' || true
}

# ──────────────────────────────────────────────────────────────────────
# Scenario runners (original behavior preserved, plus stats capture)
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

run_pub_only() {
  local label="$1" url="$2"
  echo "--- $label ---"
  for i in $(seq 1 "$RUNS"); do
    nats bench pub bench.test \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" 2>&1 | grep -E "stats:" || true
  done
  echo ""
}

# Pub-only with stats capture. Returns rate via global CAPTURED_RATE.
run_pub_only_capture() {
  local label="$1" url="$2" server_pid="$3"
  local cpu_before ctx_before output rate_sum=0

  cpu_before=$(cpu_ticks "$server_pid")
  ctx_before=$(ctx_switches "$server_pid")

  echo "--- $label ---"
  for i in $(seq 1 "$RUNS"); do
    output=$(nats bench pub bench.test \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" 2>&1)
    echo "$output" | grep -E "stats:" || true
    local rate
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))
  done
  echo ""

  CAPTURED_RATE=$(( rate_sum / RUNS ))
  CAPTURED_CPU=$(( ($(cpu_ticks "$server_pid") - cpu_before) * 1000 / CLK_TCK ))
  CAPTURED_RSS=$(rss_kb "$server_pid")
  CAPTURED_CTX=$(( $(ctx_switches "$server_pid") - ctx_before ))
}

# Generic pub/sub benchmark using full URLs.
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
      -s "$url" 2>&1 | grep -E "stats:" || true

    # Wait for subscribers (with timeout) and print their stats
    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    for s in $(seq 1 "$subs"); do
      grep -E "stats:" "/tmp/bench_sub_${s}.out" 2>/dev/null | sed "s/^/  sub[$s] //" || true
      rm -f "/tmp/bench_sub_${s}.out"
    done
    BG_PIDS=()
  done
  echo ""
}

# Pub/sub with stats capture. Returns rate via global CAPTURED_RATE.
run_url_pubsub_capture() {
  local label="$1" url="$2" subs="$3" server_pid="$4" subject="${5:-bench.ps.test}"
  local cpu_before ctx_before output rate_sum=0

  cpu_before=$(cpu_ticks "$server_pid")
  ctx_before=$(ctx_switches "$server_pid")

  echo "--- $label ---"
  for i in $(seq 1 "$RUNS"); do
    for s in $(seq 1 "$subs"); do
      nats bench sub "$subject" \
        --msgs "$MSGS" --size "$SIZE" --no-progress \
        -s "$url" >"/tmp/bench_sub_${s}.out" 2>&1 &
      BG_PIDS+=($!)
    done
    sleep 0.5

    output=$(nats bench pub "$subject" \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" 2>&1)
    echo "$output" | grep -E "stats:" || true
    local rate
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    for s in $(seq 1 "$subs"); do
      grep -E "stats:" "/tmp/bench_sub_${s}.out" 2>/dev/null | sed "s/^/  sub[$s] //" || true
      rm -f "/tmp/bench_sub_${s}.out"
    done
    BG_PIDS=()
  done
  echo ""

  CAPTURED_RATE=$(( rate_sum / RUNS ))
  CAPTURED_CPU=$(( ($(cpu_ticks "$server_pid") - cpu_before) * 1000 / CLK_TCK ))
  CAPTURED_RSS=$(rss_kb "$server_pid")
  CAPTURED_CTX=$(( $(ctx_switches "$server_pid") - ctx_before ))
}

# Cross-server pub/sub (pub on one server, sub on another)
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
      -s "$pub_url" 2>&1 | grep -E "stats:" || true

    # Wait for subscriber (with timeout)
    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    grep -E "stats:" /tmp/bench_cross_sub.out 2>/dev/null | sed 's/^/  sub  /' || true
    rm -f /tmp/bench_cross_sub.out
    BG_PIDS=()
  done
  echo ""
}

# Cross-server pub/sub with stats capture
run_cross_pubsub_capture() {
  local label="$1" pub_url="$2" sub_url="$3" server_pid="$4"
  local cpu_before ctx_before output rate_sum=0

  cpu_before=$(cpu_ticks "$server_pid")
  ctx_before=$(ctx_switches "$server_pid")

  echo "--- $label ---"
  for i in $(seq 1 "$RUNS"); do
    nats bench sub "bench.cross.test" \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$sub_url" >"/tmp/bench_cross_sub.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    output=$(nats bench pub "bench.cross.test" \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$pub_url" 2>&1)
    echo "$output" | grep -E "stats:" || true
    local rate
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    grep -E "stats:" /tmp/bench_cross_sub.out 2>/dev/null | sed 's/^/  sub  /' || true
    rm -f /tmp/bench_cross_sub.out
    BG_PIDS=()
  done
  echo ""

  CAPTURED_RATE=$(( rate_sum / RUNS ))
  CAPTURED_CPU=$(( ($(cpu_ticks "$server_pid") - cpu_before) * 1000 / CLK_TCK ))
  CAPTURED_RSS=$(rss_kb "$server_pid")
  CAPTURED_CTX=$(( $(ctx_switches "$server_pid") - ctx_before ))
}

# Helper to record a summary row after running both Rust and Go variants
record_summary() {
  local label="$1"
  local rust_rate="$2" go_rate="$3"
  local rust_cpu="$4" go_cpu="$5"
  local rust_rss="$6" go_rss="$7"
  local rust_ctx="$8" go_ctx="$9"
  SUMMARY_LABELS+=("$label")
  SUMMARY_RUST_RATES+=("$rust_rate")
  SUMMARY_GO_RATES+=("$go_rate")
  SUMMARY_RUST_CPU+=("$rust_cpu")
  SUMMARY_GO_CPU+=("$go_cpu")
  SUMMARY_RUST_RSS+=("$rust_rss")
  SUMMARY_GO_RSS+=("$go_rss")
  SUMMARY_RUST_CTX+=("$rust_ctx")
  SUMMARY_GO_CTX+=("$go_ctx")
}

# ──────────────────────────────────────────────────────────────────────
# Scenario 1: Publish Only (fire-and-forget ingest)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  1. PUBLISH ONLY (fire-and-forget, no subscribers)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
if [[ "$MODE" == "full" ]]; then
  run_pub_only "Direct Hub" "nats://127.0.0.1:$HUB_CLIENT_PORT"
fi
run_pub_only_capture "Go+Go Leaf" "nats://127.0.0.1:$GO_LEAF_PORT" "$GO_LEAF_PID"
go_pub_rate=$CAPTURED_RATE go_pub_cpu=$CAPTURED_CPU go_pub_rss=$CAPTURED_RSS go_pub_ctx=$CAPTURED_CTX

run_pub_only_capture "Rust+Rust Leaf" "nats://127.0.0.1:$RUST_LEAF2_PORT" "$RUST_LEAF2_PID"
rust_pub_rate=$CAPTURED_RATE rust_pub_cpu=$CAPTURED_CPU rust_pub_rss=$CAPTURED_RSS rust_pub_ctx=$CAPTURED_CTX

record_summary "Pub only" \
  "$rust_pub_rate" "$go_pub_rate" \
  "$rust_pub_cpu" "$go_pub_cpu" \
  "$rust_pub_rss" "$go_pub_rss" \
  "$rust_pub_ctx" "$go_pub_ctx"

# ──────────────────────────────────────────────────────────────────────
# Scenario 2: Local Pub/Sub (1 publisher + 1 subscriber, same server)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  2. LOCAL PUB/SUB (1 pub + 1 sub, same server)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
if [[ "$MODE" == "full" ]]; then
  run_url_pubsub "Direct Hub" "nats://127.0.0.1:$HUB_CLIENT_PORT" 1
fi
run_url_pubsub_capture "Go+Go Leaf" "nats://127.0.0.1:$GO_LEAF_PORT" 1 "$GO_LEAF_PID"
go_ps_rate=$CAPTURED_RATE go_ps_cpu=$CAPTURED_CPU go_ps_rss=$CAPTURED_RSS go_ps_ctx=$CAPTURED_CTX

run_url_pubsub_capture "Rust+Rust Leaf" "nats://127.0.0.1:$RUST_LEAF2_PORT" 1 "$RUST_LEAF2_PID"
rust_ps_rate=$CAPTURED_RATE rust_ps_cpu=$CAPTURED_CPU rust_ps_rss=$CAPTURED_RSS rust_ps_ctx=$CAPTURED_CTX

record_summary "Pub/sub" \
  "$rust_ps_rate" "$go_ps_rate" \
  "$rust_ps_cpu" "$go_ps_cpu" \
  "$rust_ps_rss" "$go_ps_rss" \
  "$rust_ps_ctx" "$go_ps_ctx"

# ──────────────────────────────────────────────────────────────────────
# Scenario 3: Fan-out (1 pub + 5 subs, same server)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  3. FAN-OUT (1 pub + 5 subs, same server)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
if [[ "$MODE" == "full" ]]; then
  run_url_pubsub "Direct Hub" "nats://127.0.0.1:$HUB_CLIENT_PORT" 5
fi
run_url_pubsub_capture "Go+Go Leaf" "nats://127.0.0.1:$GO_LEAF_PORT" 5 "$GO_LEAF_PID"
go_fan_rate=$CAPTURED_RATE go_fan_cpu=$CAPTURED_CPU go_fan_rss=$CAPTURED_RSS go_fan_ctx=$CAPTURED_CTX

run_url_pubsub_capture "Rust+Rust Leaf" "nats://127.0.0.1:$RUST_LEAF2_PORT" 5 "$RUST_LEAF2_PID"
rust_fan_rate=$CAPTURED_RATE rust_fan_cpu=$CAPTURED_CPU rust_fan_rss=$CAPTURED_RSS rust_fan_ctx=$CAPTURED_CTX

record_summary "Fan-out x5" \
  "$rust_fan_rate" "$go_fan_rate" \
  "$rust_fan_cpu" "$go_fan_cpu" \
  "$rust_fan_rss" "$go_fan_rss" \
  "$rust_fan_ctx" "$go_fan_ctx"

# ──────────────────────────────────────────────────────────────────────
# Scenario 4: Leaf→Hub (pub on leaf, sub on hub)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  4. LEAF → HUB (pub on leaf, sub on hub)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_cross_pubsub_capture "Go+Go Leaf → Hub" \
  "nats://127.0.0.1:$GO_LEAF_PORT" "nats://127.0.0.1:$HUB_CLIENT_PORT" "$GO_LEAF_PID"
go_l2h_rate=$CAPTURED_RATE go_l2h_cpu=$CAPTURED_CPU go_l2h_rss=$CAPTURED_RSS go_l2h_ctx=$CAPTURED_CTX

run_cross_pubsub_capture "Rust+Rust Leaf → Hub" \
  "nats://127.0.0.1:$RUST_LEAF2_PORT" "nats://127.0.0.1:$RUST_HUB_CLIENT_PORT" "$RUST_LEAF2_PID"
rust_l2h_rate=$CAPTURED_RATE rust_l2h_cpu=$CAPTURED_CPU rust_l2h_rss=$CAPTURED_RSS rust_l2h_ctx=$CAPTURED_CTX

record_summary "Leaf→Hub" \
  "$rust_l2h_rate" "$go_l2h_rate" \
  "$rust_l2h_cpu" "$go_l2h_cpu" \
  "$rust_l2h_rss" "$go_l2h_rss" \
  "$rust_l2h_ctx" "$go_l2h_ctx"

# ──────────────────────────────────────────────────────────────────────
# Scenario 5: Hub→Leaf (pub on hub, sub on leaf)
# ──────────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  5. HUB → LEAF (pub on hub, sub on leaf)"
echo "     ${MSGS} msgs × ${SIZE}B"
echo "================================================================"
echo ""
run_cross_pubsub_capture "Go Hub → Go Leaf" \
  "nats://127.0.0.1:$HUB_CLIENT_PORT" "nats://127.0.0.1:$GO_LEAF_PORT" "$GO_LEAF_PID"
go_h2l_rate=$CAPTURED_RATE go_h2l_cpu=$CAPTURED_CPU go_h2l_rss=$CAPTURED_RSS go_h2l_ctx=$CAPTURED_CTX

run_cross_pubsub_capture "Rust Hub → Rust Leaf" \
  "nats://127.0.0.1:$RUST_HUB_CLIENT_PORT" "nats://127.0.0.1:$RUST_LEAF2_PORT" "$RUST_LEAF2_PID"
rust_h2l_rate=$CAPTURED_RATE rust_h2l_cpu=$CAPTURED_CPU rust_h2l_rss=$CAPTURED_RSS rust_h2l_ctx=$CAPTURED_CTX

record_summary "Hub→Leaf" \
  "$rust_h2l_rate" "$go_h2l_rate" \
  "$rust_h2l_cpu" "$go_h2l_cpu" \
  "$rust_h2l_rss" "$go_h2l_rss" \
  "$rust_h2l_ctx" "$go_h2l_ctx"

# ──────────────────────────────────────────────────────────────────────
# Scenarios 6-13: Full mode only
# ──────────────────────────────────────────────────────────────────────
if [[ "$MODE" == "full" ]]; then
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

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 9: Hub Mode — Publish Only (Rust as hub)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  9. HUB MODE: PUBLISH ONLY (fire-and-forget, Rust as hub)"
  echo "     ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""
  run_pub_only "Go Hub (baseline)"  "nats://127.0.0.1:$HUB_CLIENT_PORT"
  run_pub_only "Rust Hub"           "nats://127.0.0.1:$RUST_HUB_CLIENT_PORT"

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 10: Hub Mode — Local Pub/Sub (1 pub + 1 sub on Rust hub)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  10. HUB MODE: LOCAL PUB/SUB (1 pub + 1 sub, Rust as hub)"
  echo "      ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""
  run_url_pubsub "Go Hub (baseline)"  "nats://127.0.0.1:$HUB_CLIENT_PORT"      1
  run_url_pubsub "Rust Hub"           "nats://127.0.0.1:$RUST_HUB_CLIENT_PORT"  1

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 11: Hub Mode — Fan-out (1 pub + 5 subs on Rust hub)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  11. HUB MODE: FAN-OUT (1 pub + 5 subs, Rust as hub)"
  echo "      ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""
  run_url_pubsub "Go Hub (baseline)"  "nats://127.0.0.1:$HUB_CLIENT_PORT"      5
  run_url_pubsub "Rust Hub"           "nats://127.0.0.1:$RUST_HUB_CLIENT_PORT"  5

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 12: Hub Mode — Leaf→Hub (pub on Go leaf, sub on Rust hub)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  12. HUB MODE: LEAF → RUST HUB (pub on Go leaf, sub on Rust hub)"
  echo "      ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""
  run_cross_pubsub "Go Leaf → Rust Hub"  "nats://127.0.0.1:$GO_LEAF_TO_RUST_PORT" "nats://127.0.0.1:$RUST_HUB_CLIENT_PORT"

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 13: Hub Mode — Hub→Leaf (pub on Rust hub, sub on Go leaf)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  13. HUB MODE: RUST HUB → LEAF (pub on Rust hub, sub on Go leaf)"
  echo "      ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""
  run_cross_pubsub "Rust Hub → Go Leaf"  "nats://127.0.0.1:$RUST_HUB_CLIENT_PORT" "nats://127.0.0.1:$GO_LEAF_TO_RUST_PORT"

  # ──────────────────────────────────────────────────────────────────────
  # Cluster mode: start 3-node Go cluster + 3-node Rust cluster
  # ──────────────────────────────────────────────────────────────────────
  echo ""
  echo "Starting 3-node Go cluster..."
  nats-server -c "$SCRIPT_DIR/configs/bench_go_cluster_a.conf" &
  PIDS+=($!); GO_CLUSTER_A_PID=$!
  nats-server -c "$SCRIPT_DIR/configs/bench_go_cluster_b.conf" &
  PIDS+=($!)
  nats-server -c "$SCRIPT_DIR/configs/bench_go_cluster_c.conf" &
  PIDS+=($!)
  sleep 2

  echo "Starting 3-node Rust cluster..."
  RUST_LOG=warn "$RUST_BIN" -c "$SCRIPT_DIR/configs/bench_rust_cluster_a.conf" &
  PIDS+=($!); RUST_CLUSTER_A_PID=$!
  sleep 0.5
  RUST_LOG=warn "$RUST_BIN" -c "$SCRIPT_DIR/configs/bench_rust_cluster_b.conf" &
  PIDS+=($!)
  RUST_LOG=warn "$RUST_BIN" -c "$SCRIPT_DIR/configs/bench_rust_cluster_c.conf" &
  PIDS+=($!)
  sleep 2

  # Verify cluster connectivity
  nats pub _bench.ping pong -s "nats://127.0.0.1:$GO_CLUSTER_A_PORT"   >/dev/null 2>&1 || { echo "FAIL: go cluster A"; exit 1; }
  nats pub _bench.ping pong -s "nats://127.0.0.1:$GO_CLUSTER_B_PORT"   >/dev/null 2>&1 || { echo "FAIL: go cluster B"; exit 1; }
  nats pub _bench.ping pong -s "nats://127.0.0.1:$GO_CLUSTER_C_PORT"   >/dev/null 2>&1 || { echo "FAIL: go cluster C"; exit 1; }
  nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_CLUSTER_A_PORT" >/dev/null 2>&1 || { echo "FAIL: rust cluster A"; exit 1; }
  nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_CLUSTER_B_PORT" >/dev/null 2>&1 || { echo "FAIL: rust cluster B"; exit 1; }
  nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_CLUSTER_C_PORT" >/dev/null 2>&1 || { echo "FAIL: rust cluster C"; exit 1; }
  echo "All cluster nodes responding."
  echo ""

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 14: Cluster pub/sub (pub on A, sub on B)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  14. CLUSTER PUB/SUB (pub on A, sub on B — cross-node)"
  echo "      ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""
  run_cross_pubsub_capture "Go Cluster A→B" \
    "nats://127.0.0.1:$GO_CLUSTER_A_PORT" "nats://127.0.0.1:$GO_CLUSTER_B_PORT" "$GO_CLUSTER_A_PID"
  go_cl_ps_rate=$CAPTURED_RATE go_cl_ps_cpu=$CAPTURED_CPU go_cl_ps_rss=$CAPTURED_RSS go_cl_ps_ctx=$CAPTURED_CTX

  run_cross_pubsub_capture "Rust Cluster A→B" \
    "nats://127.0.0.1:$RUST_CLUSTER_A_PORT" "nats://127.0.0.1:$RUST_CLUSTER_B_PORT" "$RUST_CLUSTER_A_PID"
  rust_cl_ps_rate=$CAPTURED_RATE rust_cl_ps_cpu=$CAPTURED_CPU rust_cl_ps_rss=$CAPTURED_RSS rust_cl_ps_ctx=$CAPTURED_CTX

  record_summary "Cluster A→B" \
    "$rust_cl_ps_rate" "$go_cl_ps_rate" \
    "$rust_cl_ps_cpu" "$go_cl_ps_cpu" \
    "$rust_cl_ps_rss" "$go_cl_ps_rss" \
    "$rust_cl_ps_ctx" "$go_cl_ps_ctx"

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 15: Cluster fan-out (pub on A, 1 sub on each of A, B, C)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  15. CLUSTER FAN-OUT x3 (pub on A, sub on A+B+C)"
  echo "      ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""

  # Go cluster fan-out x3
  echo "--- Go Cluster fan-out x3 ---"
  cpu_before=$(cpu_ticks "$GO_CLUSTER_A_PID")
  ctx_before=$(ctx_switches "$GO_CLUSTER_A_PID")
  rate_sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench sub "bench.cl.fan3" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_CLUSTER_A_PORT" >"/tmp/bench_cl_sub_1.out" 2>&1 &
    BG_PIDS+=($!)
    nats bench sub "bench.cl.fan3" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_CLUSTER_B_PORT" >"/tmp/bench_cl_sub_2.out" 2>&1 &
    BG_PIDS+=($!)
    nats bench sub "bench.cl.fan3" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_CLUSTER_C_PORT" >"/tmp/bench_cl_sub_3.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    output=$(nats bench pub "bench.cl.fan3" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_CLUSTER_A_PORT" 2>&1)
    echo "$output" | grep -E "stats:" || true
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do wait_or_kill "$pid" 30; done
    for s in 1 2 3; do
      grep -E "stats:" "/tmp/bench_cl_sub_${s}.out" 2>/dev/null | sed "s/^/  sub[$s] /" || true
      rm -f "/tmp/bench_cl_sub_${s}.out"
    done
    BG_PIDS=()
  done
  echo ""
  go_cl_fan_rate=$(( rate_sum / RUNS ))
  go_cl_fan_cpu=$(( ($(cpu_ticks "$GO_CLUSTER_A_PID") - cpu_before) * 1000 / CLK_TCK ))
  go_cl_fan_rss=$(rss_kb "$GO_CLUSTER_A_PID")
  go_cl_fan_ctx=$(( $(ctx_switches "$GO_CLUSTER_A_PID") - ctx_before ))

  # Rust cluster fan-out x3
  echo "--- Rust Cluster fan-out x3 ---"
  cpu_before=$(cpu_ticks "$RUST_CLUSTER_A_PID")
  ctx_before=$(ctx_switches "$RUST_CLUSTER_A_PID")
  rate_sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench sub "bench.cl.fan3" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_CLUSTER_A_PORT" >"/tmp/bench_cl_sub_1.out" 2>&1 &
    BG_PIDS+=($!)
    nats bench sub "bench.cl.fan3" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_CLUSTER_B_PORT" >"/tmp/bench_cl_sub_2.out" 2>&1 &
    BG_PIDS+=($!)
    nats bench sub "bench.cl.fan3" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_CLUSTER_C_PORT" >"/tmp/bench_cl_sub_3.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    output=$(nats bench pub "bench.cl.fan3" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_CLUSTER_A_PORT" 2>&1)
    echo "$output" | grep -E "stats:" || true
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do wait_or_kill "$pid" 30; done
    for s in 1 2 3; do
      grep -E "stats:" "/tmp/bench_cl_sub_${s}.out" 2>/dev/null | sed "s/^/  sub[$s] /" || true
      rm -f "/tmp/bench_cl_sub_${s}.out"
    done
    BG_PIDS=()
  done
  echo ""
  rust_cl_fan_rate=$(( rate_sum / RUNS ))
  rust_cl_fan_cpu=$(( ($(cpu_ticks "$RUST_CLUSTER_A_PID") - cpu_before) * 1000 / CLK_TCK ))
  rust_cl_fan_rss=$(rss_kb "$RUST_CLUSTER_A_PID")
  rust_cl_fan_ctx=$(( $(ctx_switches "$RUST_CLUSTER_A_PID") - ctx_before ))

  record_summary "Cluster fan x3" \
    "$rust_cl_fan_rate" "$go_cl_fan_rate" \
    "$rust_cl_fan_cpu" "$go_cl_fan_cpu" \
    "$rust_cl_fan_rss" "$go_cl_fan_rss" \
    "$rust_cl_fan_ctx" "$go_cl_fan_ctx"

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 16: Cluster pub on A, sub on B and C (no local sub)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  16. CLUSTER REMOTE-ONLY (pub on A, sub on B + sub on C)"
  echo "      ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""

  # Go cluster remote-only
  echo "--- Go Cluster remote B+C ---"
  cpu_before=$(cpu_ticks "$GO_CLUSTER_A_PID")
  ctx_before=$(ctx_switches "$GO_CLUSTER_A_PID")
  rate_sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench sub "bench.cl.remote" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_CLUSTER_B_PORT" >"/tmp/bench_cl_sub_1.out" 2>&1 &
    BG_PIDS+=($!)
    nats bench sub "bench.cl.remote" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_CLUSTER_C_PORT" >"/tmp/bench_cl_sub_2.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    output=$(nats bench pub "bench.cl.remote" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_CLUSTER_A_PORT" 2>&1)
    echo "$output" | grep -E "stats:" || true
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do wait_or_kill "$pid" 30; done
    for s in 1 2; do
      grep -E "stats:" "/tmp/bench_cl_sub_${s}.out" 2>/dev/null | sed "s/^/  sub[$s] /" || true
      rm -f "/tmp/bench_cl_sub_${s}.out"
    done
    BG_PIDS=()
  done
  echo ""
  go_cl_rem_rate=$(( rate_sum / RUNS ))
  go_cl_rem_cpu=$(( ($(cpu_ticks "$GO_CLUSTER_A_PID") - cpu_before) * 1000 / CLK_TCK ))
  go_cl_rem_rss=$(rss_kb "$GO_CLUSTER_A_PID")
  go_cl_rem_ctx=$(( $(ctx_switches "$GO_CLUSTER_A_PID") - ctx_before ))

  # Rust cluster remote-only
  echo "--- Rust Cluster remote B+C ---"
  cpu_before=$(cpu_ticks "$RUST_CLUSTER_A_PID")
  ctx_before=$(ctx_switches "$RUST_CLUSTER_A_PID")
  rate_sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench sub "bench.cl.remote" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_CLUSTER_B_PORT" >"/tmp/bench_cl_sub_1.out" 2>&1 &
    BG_PIDS+=($!)
    nats bench sub "bench.cl.remote" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_CLUSTER_C_PORT" >"/tmp/bench_cl_sub_2.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    output=$(nats bench pub "bench.cl.remote" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_CLUSTER_A_PORT" 2>&1)
    echo "$output" | grep -E "stats:" || true
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do wait_or_kill "$pid" 30; done
    for s in 1 2; do
      grep -E "stats:" "/tmp/bench_cl_sub_${s}.out" 2>/dev/null | sed "s/^/  sub[$s] /" || true
      rm -f "/tmp/bench_cl_sub_${s}.out"
    done
    BG_PIDS=()
  done
  echo ""
  rust_cl_rem_rate=$(( rate_sum / RUNS ))
  rust_cl_rem_cpu=$(( ($(cpu_ticks "$RUST_CLUSTER_A_PID") - cpu_before) * 1000 / CLK_TCK ))
  rust_cl_rem_rss=$(rss_kb "$RUST_CLUSTER_A_PID")
  rust_cl_rem_ctx=$(( $(ctx_switches "$RUST_CLUSTER_A_PID") - ctx_before ))

  record_summary "Cluster B+C" \
    "$rust_cl_rem_rate" "$go_cl_rem_rate" \
    "$rust_cl_rem_cpu" "$go_cl_rem_cpu" \
    "$rust_cl_rem_rss" "$go_cl_rem_rss" \
    "$rust_cl_rem_ctx" "$go_cl_rem_ctx"

  # ──────────────────────────────────────────────────────────────────────
  # Gateway mode: start 2 Go gateway nodes + 2 Rust gateway nodes
  # ──────────────────────────────────────────────────────────────────────
  echo ""
  echo "Starting 2-node Go gateway (alpha + beta)..."
  nats-server -c "$SCRIPT_DIR/configs/bench_go_gateway_a.conf" &
  PIDS+=($!); GO_GW_A_PID=$!
  nats-server -c "$SCRIPT_DIR/configs/bench_go_gateway_b.conf" &
  PIDS+=($!)
  sleep 2

  echo "Starting 2-node Rust gateway (alpha + beta)..."
  RUST_LOG=warn "$RUST_BIN" -c "$SCRIPT_DIR/configs/bench_rust_gateway_a.conf" &
  PIDS+=($!); RUST_GW_A_PID=$!
  sleep 0.5
  RUST_LOG=warn "$RUST_BIN" -c "$SCRIPT_DIR/configs/bench_rust_gateway_b.conf" &
  PIDS+=($!)
  sleep 2

  # Verify gateway connectivity
  nats pub _bench.ping pong -s "nats://127.0.0.1:$GO_GW_A_PORT"   >/dev/null 2>&1 || { echo "FAIL: go gateway A"; exit 1; }
  nats pub _bench.ping pong -s "nats://127.0.0.1:$GO_GW_B_PORT"   >/dev/null 2>&1 || { echo "FAIL: go gateway B"; exit 1; }
  nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_GW_A_PORT" >/dev/null 2>&1 || { echo "FAIL: rust gateway A"; exit 1; }
  nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_GW_B_PORT" >/dev/null 2>&1 || { echo "FAIL: rust gateway B"; exit 1; }
  echo "All gateway nodes responding."
  echo ""

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 17: Gateway pub/sub (pub on alpha, sub on beta)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  17. GATEWAY PUB/SUB (pub on alpha, sub on beta — cross-gateway)"
  echo "      ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""
  run_cross_pubsub_capture "Go Gateway alpha→beta" \
    "nats://127.0.0.1:$GO_GW_A_PORT" "nats://127.0.0.1:$GO_GW_B_PORT" "$GO_GW_A_PID"
  go_gw_ps_rate=$CAPTURED_RATE go_gw_ps_cpu=$CAPTURED_CPU go_gw_ps_rss=$CAPTURED_RSS go_gw_ps_ctx=$CAPTURED_CTX

  run_cross_pubsub_capture "Rust Gateway alpha→beta" \
    "nats://127.0.0.1:$RUST_GW_A_PORT" "nats://127.0.0.1:$RUST_GW_B_PORT" "$RUST_GW_A_PID"
  rust_gw_ps_rate=$CAPTURED_RATE rust_gw_ps_cpu=$CAPTURED_CPU rust_gw_ps_rss=$CAPTURED_RSS rust_gw_ps_ctx=$CAPTURED_CTX

  record_summary "Gateway A→B" \
    "$rust_gw_ps_rate" "$go_gw_ps_rate" \
    "$rust_gw_ps_cpu" "$go_gw_ps_cpu" \
    "$rust_gw_ps_rss" "$go_gw_ps_rss" \
    "$rust_gw_ps_ctx" "$go_gw_ps_ctx"

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 18: Gateway fan-out (pub on alpha, sub on alpha + sub on beta)
  # ──────────────────────────────────────────────────────────────────────
  echo "================================================================"
  echo "  18. GATEWAY FAN-OUT (pub on alpha, sub on alpha + beta)"
  echo "      ${MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""

  # Go gateway fan-out
  echo "--- Go Gateway fan-out ---"
  cpu_before=$(cpu_ticks "$GO_GW_A_PID")
  ctx_before=$(ctx_switches "$GO_GW_A_PID")
  rate_sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench sub "bench.gw.fan" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_GW_A_PORT" >"/tmp/bench_gw_sub_1.out" 2>&1 &
    BG_PIDS+=($!)
    nats bench sub "bench.gw.fan" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_GW_B_PORT" >"/tmp/bench_gw_sub_2.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    output=$(nats bench pub "bench.gw.fan" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_GW_A_PORT" 2>&1)
    echo "$output" | grep -E "stats:" || true
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do wait_or_kill "$pid" 30; done
    for s in 1 2; do
      grep -E "stats:" "/tmp/bench_gw_sub_${s}.out" 2>/dev/null | sed "s/^/  sub[$s] /" || true
      rm -f "/tmp/bench_gw_sub_${s}.out"
    done
    BG_PIDS=()
  done
  echo ""
  go_gw_fan_rate=$(( rate_sum / RUNS ))
  go_gw_fan_cpu=$(( ($(cpu_ticks "$GO_GW_A_PID") - cpu_before) * 1000 / CLK_TCK ))
  go_gw_fan_rss=$(rss_kb "$GO_GW_A_PID")
  go_gw_fan_ctx=$(( $(ctx_switches "$GO_GW_A_PID") - ctx_before ))

  # Rust gateway fan-out
  echo "--- Rust Gateway fan-out ---"
  cpu_before=$(cpu_ticks "$RUST_GW_A_PID")
  ctx_before=$(ctx_switches "$RUST_GW_A_PID")
  rate_sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench sub "bench.gw.fan" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_GW_A_PORT" >"/tmp/bench_gw_sub_1.out" 2>&1 &
    BG_PIDS+=($!)
    nats bench sub "bench.gw.fan" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_GW_B_PORT" >"/tmp/bench_gw_sub_2.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    output=$(nats bench pub "bench.gw.fan" --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_GW_A_PORT" 2>&1)
    echo "$output" | grep -E "stats:" || true
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do wait_or_kill "$pid" 30; done
    for s in 1 2; do
      grep -E "stats:" "/tmp/bench_gw_sub_${s}.out" 2>/dev/null | sed "s/^/  sub[$s] /" || true
      rm -f "/tmp/bench_gw_sub_${s}.out"
    done
    BG_PIDS=()
  done
  echo ""
  rust_gw_fan_rate=$(( rate_sum / RUNS ))
  rust_gw_fan_cpu=$(( ($(cpu_ticks "$RUST_GW_A_PID") - cpu_before) * 1000 / CLK_TCK ))
  rust_gw_fan_rss=$(rss_kb "$RUST_GW_A_PID")
  rust_gw_fan_ctx=$(( $(ctx_switches "$RUST_GW_A_PID") - ctx_before ))

  record_summary "Gateway fan" \
    "$rust_gw_fan_rate" "$go_gw_fan_rate" \
    "$rust_gw_fan_cpu" "$go_gw_fan_cpu" \
    "$rust_gw_fan_rss" "$go_gw_fan_rss" \
    "$rust_gw_fan_ctx" "$go_gw_fan_ctx"

  # ──────────────────────────────────────────────────────────────────────
  # Scenario 19: Gateway request-reply (service mode across the gateway)
  # ──────────────────────────────────────────────────────────────────────
  RR_MSGS=10000  # req-reply is sequential round-trip, ~400 msgs/sec across gateway
  echo "================================================================"
  echo "  19. GATEWAY REQUEST-REPLY (req on alpha, reply on beta)"
  echo "      ${RR_MSGS} msgs × ${SIZE}B"
  echo "================================================================"
  echo ""

  # Go gateway request-reply
  echo "--- Go Gateway req-reply ---"
  cpu_before=$(cpu_ticks "$GO_GW_A_PID")
  ctx_before=$(ctx_switches "$GO_GW_A_PID")
  rate_sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench service serve "bench.reqrep.svc" --msgs "$RR_MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_GW_B_PORT" >"/tmp/bench_gw_reply.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    output=$(timeout 60 nats bench service request "bench.reqrep.svc" --msgs "$RR_MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$GO_GW_A_PORT" 2>&1) || true
    echo "$output" | grep -E "stats:" || true
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do wait_or_kill "$pid" 30; done
    BG_PIDS=()
  done
  echo ""
  go_gw_rr_rate=$(( rate_sum / RUNS ))
  go_gw_rr_cpu=$(( ($(cpu_ticks "$GO_GW_A_PID") - cpu_before) * 1000 / CLK_TCK ))
  go_gw_rr_rss=$(rss_kb "$GO_GW_A_PID")
  go_gw_rr_ctx=$(( $(ctx_switches "$GO_GW_A_PID") - ctx_before ))

  # Rust gateway request-reply
  echo "--- Rust Gateway req-reply ---"
  cpu_before=$(cpu_ticks "$RUST_GW_A_PID")
  ctx_before=$(ctx_switches "$RUST_GW_A_PID")
  rate_sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench service serve "bench.reqrep.svc" --msgs "$RR_MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_GW_B_PORT" >"/tmp/bench_gw_reply.out" 2>&1 &
    BG_PIDS+=($!)
    sleep 0.5

    output=$(timeout 60 nats bench service request "bench.reqrep.svc" --msgs "$RR_MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$RUST_GW_A_PORT" 2>&1) || true
    echo "$output" | grep -E "stats:" || true
    rate=$(echo "$output" | extract_rate)
    rate_sum=$(( rate_sum + ${rate:-0} ))

    for pid in "${BG_PIDS[@]}"; do wait_or_kill "$pid" 30; done
    BG_PIDS=()
  done
  echo ""
  rust_gw_rr_rate=$(( rate_sum / RUNS ))
  rust_gw_rr_cpu=$(( ($(cpu_ticks "$RUST_GW_A_PID") - cpu_before) * 1000 / CLK_TCK ))
  rust_gw_rr_rss=$(rss_kb "$RUST_GW_A_PID")
  rust_gw_rr_ctx=$(( $(ctx_switches "$RUST_GW_A_PID") - ctx_before ))

  record_summary "GW req-reply" \
    "$rust_gw_rr_rate" "$go_gw_rr_rate" \
    "$rust_gw_rr_cpu" "$go_gw_rr_cpu" \
    "$rust_gw_rr_rss" "$go_gw_rr_rss" \
    "$rust_gw_rr_ctx" "$go_gw_rr_ctx"
fi

# ──────────────────────────────────────────────────────────────────────
# Summary table
# ──────────────────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo "  SUMMARY ($MODE mode)"
echo "  msgs=$MSGS  size=${SIZE}B  runs=$RUNS"
echo "================================================================"
echo ""

# Throughput table
printf "  %-16s %14s %14s %8s\n" "Scenario" "Rust+Rust" "Go+Go" "Ratio"
echo "  ─────────────────────────────────────────────────────────────"
for i in "${!SUMMARY_LABELS[@]}"; do
  local_rust="${SUMMARY_RUST_RATES[$i]}"
  local_go="${SUMMARY_GO_RATES[$i]}"
  if [[ -n "$local_rust" && -n "$local_go" && "$local_go" -gt 0 ]]; then
    ratio=$(( local_rust * 100 / local_go ))
    printf "  %-16s %14s %14s %7d%%\n" \
      "${SUMMARY_LABELS[$i]}" "$(fmt_num "$local_rust")" "$(fmt_num "$local_go")" "$ratio"
  fi
done
echo ""

# Resource stats table
printf "  %-16s %16s %16s %16s\n" "Scenario" "CPU(ms)" "RSS(KB)" "CtxSw"
printf "  %-16s %16s %16s %16s\n" "" "Rust / Go" "Rust / Go" "Rust / Go"
echo "  ─────────────────────────────────────────────────────────────"
for i in "${!SUMMARY_LABELS[@]}"; do
  printf "  %-16s %7s / %-7s %7s / %-7s %7s / %-7s\n" \
    "${SUMMARY_LABELS[$i]}" \
    "${SUMMARY_RUST_CPU[$i]}" "${SUMMARY_GO_CPU[$i]}" \
    "$(fmt_num "${SUMMARY_RUST_RSS[$i]}")" "$(fmt_num "${SUMMARY_GO_RSS[$i]}")" \
    "$(fmt_ctx "${SUMMARY_RUST_CTX[$i]}")" "$(fmt_ctx "${SUMMARY_GO_CTX[$i]}")"
done

echo ""
echo "================================================================"
echo "  BENCHMARK COMPLETE"
echo "================================================================"
