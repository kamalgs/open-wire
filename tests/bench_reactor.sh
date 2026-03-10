#!/usr/bin/env bash
# Benchmark: epoll vs io_uring reactor
#
# Runs key scenarios with both reactor backends and prints a comparison table.
# Usage: cd tests && ./bench_reactor.sh [--msgs N] [--size N] [--runs N]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MSGS=500000
SIZE=128
RUNS=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --msgs)  MSGS="$2";  shift 2 ;;
    --size)  SIZE="$2";  shift 2 ;;
    --runs)  RUNS="$2";  shift 2 ;;
    *)       echo "Unknown arg: $1"; exit 1 ;;
  esac
done

HUB_CLIENT_PORT=4333
HUB_LEAF_PORT=7422
RUST_PORT=5223

PIDS=()
BG_PIDS=()
cleanup() {
  for pid in "${BG_PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

for cmd in nats-server nats cargo; do
  command -v "$cmd" &>/dev/null || { echo "ERROR: $cmd not found"; exit 1; }
done

echo "================================================================"
echo "  Reactor Benchmark: epoll vs io_uring"
echo "  msgs=$MSGS  size=${SIZE}B  runs=$RUNS"
echo "================================================================"
echo ""

# --- Build both binaries ---
echo "Building epoll binary (default)..."
cargo build --manifest-path "$REPO_ROOT/Cargo.toml" --release 2>&1 | tail -1
cp "$REPO_ROOT/target/release/open-wire" /tmp/open-wire-epoll

echo "Building io_uring binary..."
cargo build --manifest-path "$REPO_ROOT/Cargo.toml" --release --features io-uring 2>&1 | tail -1
cp "$REPO_ROOT/target/release/open-wire" /tmp/open-wire-uring

echo ""

# --- Helper: extract msg/sec from nats bench output ---
extract_rate() {
  grep -oP '[\d,]+ msgs/sec' | head -1 | tr -d ','
}

wait_or_kill() {
  local pid="$1" max_wait="${2:-30}"
  if timeout "$max_wait" tail --pid="$pid" -f /dev/null 2>/dev/null; then
    wait "$pid" 2>/dev/null || true
  else
    kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true
  fi
}

kill_bg() {
  for pid in "${BG_PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
  BG_PIDS=()
}

# --- Run benchmarks for a given binary ---
run_benchmarks() {
  local label="$1" bin="$2"
  local results=()

  # Start hub
  nats-server -c "$SCRIPT_DIR/configs/bench_hub.conf" >/dev/null 2>&1 &
  PIDS+=($!)
  sleep 0.5

  # Start leaf
  RUST_LOG=warn "$bin" --port $RUST_PORT --hub "nats://127.0.0.1:$HUB_LEAF_PORT" >/dev/null 2>&1 &
  PIDS+=($!)
  sleep 1

  # Verify
  nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_PORT" >/dev/null 2>&1 || { echo "FAIL: $label not responding"; return 1; }

  local url="nats://127.0.0.1:$RUST_PORT"
  local hub_url="nats://127.0.0.1:$HUB_CLIENT_PORT"

  # --- Scenario 1: Pub only ---
  local sum=0
  for i in $(seq 1 "$RUNS"); do
    rate=$(nats bench pub bench.test --msgs "$MSGS" --size "$SIZE" --no-progress -s "$url" 2>&1 | extract_rate)
    rate_num=${rate%% *}
    sum=$((sum + rate_num))
  done
  local avg_pub=$((sum / RUNS))
  results+=("$avg_pub")
  echo "  $label  pub-only:     $avg_pub msgs/sec"

  # --- Scenario 2: Local pub/sub ---
  sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench sub bench.ps.test --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" >/tmp/bench_sub.out 2>&1 &
    BG_PIDS+=($!)
    sleep 0.3

    nats bench pub bench.ps.test --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" >/dev/null 2>&1

    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    rate=$(cat /tmp/bench_sub.out | extract_rate)
    rate_num=${rate%% *}
    sum=$((sum + rate_num))
    BG_PIDS=()
  done
  local avg_pubsub=$((sum / RUNS))
  results+=("$avg_pubsub")
  echo "  $label  pub/sub:      $avg_pubsub msgs/sec"

  # --- Scenario 3: Fan-out x5 ---
  sum=0
  for i in $(seq 1 "$RUNS"); do
    for s in $(seq 1 5); do
      nats bench sub bench.fan.test --msgs "$MSGS" --size "$SIZE" --no-progress \
        -s "$url" >"/tmp/bench_sub_${s}.out" 2>&1 &
      BG_PIDS+=($!)
    done
    sleep 0.3

    nats bench pub bench.fan.test --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" >/dev/null 2>&1

    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    # Average sub rate across all 5 subs
    local sub_sum=0
    for s in $(seq 1 5); do
      rate=$(cat "/tmp/bench_sub_${s}.out" | extract_rate)
      rate_num=${rate%% *}
      sub_sum=$((sub_sum + rate_num))
    done
    sum=$((sum + sub_sum / 5))
    BG_PIDS=()
  done
  local avg_fanout=$((sum / RUNS))
  results+=("$avg_fanout")
  echo "  $label  fan-out x5:   $avg_fanout msgs/sec"

  # --- Scenario 4: Leaf→Hub ---
  sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench sub bench.cross.test --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$hub_url" >/tmp/bench_cross_sub.out 2>&1 &
    BG_PIDS+=($!)
    sleep 0.3

    nats bench pub bench.cross.test --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" >/dev/null 2>&1

    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    rate=$(cat /tmp/bench_cross_sub.out | extract_rate)
    rate_num=${rate%% *}
    sum=$((sum + rate_num))
    BG_PIDS=()
  done
  local avg_leafhub=$((sum / RUNS))
  results+=("$avg_leafhub")
  echo "  $label  leaf→hub:     $avg_leafhub msgs/sec"

  # --- Scenario 5: Hub→Leaf ---
  sum=0
  for i in $(seq 1 "$RUNS"); do
    nats bench sub bench.down.test --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$url" >/tmp/bench_down_sub.out 2>&1 &
    BG_PIDS+=($!)
    sleep 0.3

    nats bench pub bench.down.test --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "$hub_url" >/dev/null 2>&1

    for pid in "${BG_PIDS[@]}"; do
      wait_or_kill "$pid" 30
    done
    rate=$(cat /tmp/bench_down_sub.out | extract_rate)
    rate_num=${rate%% *}
    sum=$((sum + rate_num))
    BG_PIDS=()
  done
  local avg_hubleaf=$((sum / RUNS))
  results+=("$avg_hubleaf")
  echo "  $label  hub→leaf:     $avg_hubleaf msgs/sec"

  # Stop servers
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
  PIDS=()
  sleep 0.5

  # Export results
  echo "${results[*]}" > "/tmp/bench_${label}.results"
}

# --- Run both ---
echo "--- Running epoll benchmarks ---"
run_benchmarks "epoll" /tmp/open-wire-epoll
echo ""
echo "--- Running io_uring benchmarks ---"
run_benchmarks "uring" /tmp/open-wire-uring
echo ""

# --- Comparison table ---
read -ra EPOLL < /tmp/bench_epoll.results
read -ra URING < /tmp/bench_uring.results

SCENARIOS=("Pub only" "Local pub/sub" "Fan-out x5" "Leaf→Hub" "Hub→Leaf")

echo "================================================================"
echo "  RESULTS: epoll vs io_uring  ($RUNS-run avg)"
echo "  msgs=$MSGS  size=${SIZE}B"
echo "================================================================"
printf "%-16s %12s %12s %8s\n" "Scenario" "epoll" "io_uring" "Δ%"
printf "%-16s %12s %12s %8s\n" "--------" "-----" "--------" "--"
for i in "${!SCENARIOS[@]}"; do
  e="${EPOLL[$i]}"
  u="${URING[$i]}"
  if [ "$e" -gt 0 ]; then
    delta=$(echo "scale=1; (($u - $e) * 100) / $e" | bc)
    sign=""
    [[ "$delta" != -* ]] && sign="+"
    printf "%-16s %10s/s %10s/s %7s%%\n" "${SCENARIOS[$i]}" "$e" "$u" "${sign}${delta}"
  else
    printf "%-16s %10s/s %10s/s %8s\n" "${SCENARIOS[$i]}" "$e" "$u" "N/A"
  fi
done
echo ""
echo "Positive Δ% = io_uring faster. Negative = epoll faster."
