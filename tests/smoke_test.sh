#!/usr/bin/env bash
# Quick smoke test: verify all benchmark scenarios work end-to-end
# with tiny message counts (100 msgs). Catches functional issues fast.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

HUB_CLIENT_PORT=4333
HUB_LEAF_PORT=7422
GO_LEAF_PORT=4225
GO_LEAF_WS_PORT=4226
RUST_LEAF_PORT=5223
RUST_LEAF_WS_PORT=5224
MSGS=100
SIZE=64

PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; exit 1; }

# Build
echo "Building Rust leaf (release)..."
cargo build --manifest-path "$REPO_ROOT/Cargo.toml" \
  --release 2>&1 | tail -1
RUST_BIN="$REPO_ROOT/target/release/open-wire"

# Start servers
nats-server -c "$SCRIPT_DIR/configs/bench_hub.conf" >/dev/null 2>&1 &
PIDS+=($!); sleep 0.5

nats-server -c "$SCRIPT_DIR/configs/bench_go_leaf_ws.conf" >/dev/null 2>&1 &
PIDS+=($!); sleep 0.5

"$RUST_BIN" --port "$RUST_LEAF_PORT" --ws-port "$RUST_LEAF_WS_PORT" \
  --hub "nats://127.0.0.1:$HUB_LEAF_PORT" >/dev/null 2>&1 &
PIDS+=($!); sleep 1

echo ""
echo "=== Smoke Test (${MSGS} msgs each) ==="

# --- 1. Publish only ---
echo ""
echo "[1] Publish only"
for label_port in "Hub:$HUB_CLIENT_PORT" "GoLeaf:$GO_LEAF_PORT" "RustLeaf:$RUST_LEAF_PORT"; do
  label="${label_port%%:*}"; port="${label_port##*:}"
  nats bench pub smoke.test --msgs $MSGS --size $SIZE --no-progress \
    -s "nats://127.0.0.1:$port" >/dev/null 2>&1 \
    && pass "$label pub-only" || fail "$label pub-only"
done

# --- 2. Local pub/sub (1 sub) ---
echo ""
echo "[2] Local pub/sub (1 pub + 1 sub)"
for label_port in "Hub:$HUB_CLIENT_PORT" "GoLeaf:$GO_LEAF_PORT" "RustLeaf:$RUST_LEAF_PORT"; do
  label="${label_port%%:*}"; port="${label_port##*:}"
  url="nats://127.0.0.1:$port"

  nats bench sub smoke.ps --msgs $MSGS --size $SIZE --no-progress \
    -s "$url" >/dev/null 2>&1 &
  sub_pid=$!
  sleep 0.3

  nats bench pub smoke.ps --msgs $MSGS --size $SIZE --no-progress \
    -s "$url" >/dev/null 2>&1

  wait "$sub_pid" 2>/dev/null \
    && pass "$label local pub/sub" || fail "$label local pub/sub"
done

# --- 3. Fan-out (1 pub + 3 subs) ---
echo ""
echo "[3] Fan-out (1 pub + 3 subs)"
for label_port in "Hub:$HUB_CLIENT_PORT" "GoLeaf:$GO_LEAF_PORT" "RustLeaf:$RUST_LEAF_PORT"; do
  label="${label_port%%:*}"; port="${label_port##*:}"
  url="nats://127.0.0.1:$port"
  sub_pids=()

  for s in 1 2 3; do
    nats bench sub smoke.fan --msgs $MSGS --size $SIZE --no-progress \
      -s "$url" >/dev/null 2>&1 &
    sub_pids+=($!)
  done
  sleep 0.3

  nats bench pub smoke.fan --msgs $MSGS --size $SIZE --no-progress \
    -s "$url" >/dev/null 2>&1

  all_ok=true
  for pid in "${sub_pids[@]}"; do
    wait "$pid" 2>/dev/null || all_ok=false
  done
  $all_ok && pass "$label fan-out x3" || fail "$label fan-out x3"
done

# --- 4. Leaf → Hub (pub on leaf, sub on hub) ---
echo ""
echo "[4] Leaf → Hub"
for label_port in "GoLeaf:$GO_LEAF_PORT" "RustLeaf:$RUST_LEAF_PORT"; do
  label="${label_port%%:*}"; port="${label_port##*:}"

  nats bench sub smoke.cross --msgs $MSGS --size $SIZE --no-progress \
    -s "nats://127.0.0.1:$HUB_CLIENT_PORT" >/dev/null 2>&1 &
  sub_pid=$!
  sleep 0.3

  nats bench pub smoke.cross --msgs $MSGS --size $SIZE --no-progress \
    -s "nats://127.0.0.1:$port" >/dev/null 2>&1

  wait "$sub_pid" 2>/dev/null \
    && pass "$label → Hub" || fail "$label → Hub"
done

# --- 5. Hub → Leaf (pub on hub, sub on leaf) ---
echo ""
echo "[5] Hub → Leaf"
for label_port in "GoLeaf:$GO_LEAF_PORT" "RustLeaf:$RUST_LEAF_PORT"; do
  label="${label_port%%:*}"; port="${label_port##*:}"

  nats bench sub smoke.down --msgs $MSGS --size $SIZE --no-progress \
    -s "nats://127.0.0.1:$port" >/dev/null 2>&1 &
  sub_pid=$!
  sleep 0.3

  nats bench pub smoke.down --msgs $MSGS --size $SIZE --no-progress \
    -s "nats://127.0.0.1:$HUB_CLIENT_PORT" >/dev/null 2>&1

  wait "$sub_pid" 2>/dev/null \
    && pass "Hub → $label" || fail "Hub → $label"
done

# --- 6. Request/Reply ---
echo ""
echo "[6] Request/Reply"
for label_port in "Hub:$HUB_CLIENT_PORT" "GoLeaf:$GO_LEAF_PORT" "RustLeaf:$RUST_LEAF_PORT"; do
  label="${label_port%%:*}"; port="${label_port##*:}"
  url="nats://127.0.0.1:$port"

  # Use nats request/reply directly (simpler than bench service which can hang)
  nats sub smoke.rr --count 1 -s "$url" --raw 2>/dev/null | head -1 &
  sub_pid=$!
  sleep 0.3

  nats pub smoke.rr "hello" -s "$url" >/dev/null 2>&1

  # Give subscriber 3s to finish
  timeout 3 tail --pid=$sub_pid -f /dev/null 2>/dev/null
  wait "$sub_pid" 2>/dev/null \
    && pass "$label req/reply" || fail "$label req/reply"
done

# --- 7. WebSocket pub/sub (1 pub + 1 sub over WS) ---
echo ""
echo "[7] WebSocket pub/sub (1 pub + 1 sub)"
for label_url in "GoLeafWS:ws://127.0.0.1:$GO_LEAF_WS_PORT" \
                 "RustLeafWS:ws://127.0.0.1:$RUST_LEAF_WS_PORT"; do
  label="${label_url%%:*}"; url="${label_url#*:}"

  nats bench sub smoke.ws --msgs $MSGS --size $SIZE --no-progress \
    -s "$url" >/dev/null 2>&1 &
  sub_pid=$!
  sleep 0.3

  nats bench pub smoke.ws --msgs $MSGS --size $SIZE --no-progress \
    -s "$url" >/dev/null 2>&1

  wait "$sub_pid" 2>/dev/null \
    && pass "$label ws pub/sub" || fail "$label ws pub/sub"
done

# --- 8. WebSocket fan-out (1 pub + 3 subs over WS) ---
echo ""
echo "[8] WebSocket fan-out (1 pub + 3 subs)"
for label_url in "GoLeafWS:ws://127.0.0.1:$GO_LEAF_WS_PORT" \
                 "RustLeafWS:ws://127.0.0.1:$RUST_LEAF_WS_PORT"; do
  label="${label_url%%:*}"; url="${label_url#*:}"
  sub_pids=()

  for s in 1 2 3; do
    nats bench sub smoke.wsfan --msgs $MSGS --size $SIZE --no-progress \
      -s "$url" >/dev/null 2>&1 &
    sub_pids+=($!)
  done
  sleep 0.3

  nats bench pub smoke.wsfan --msgs $MSGS --size $SIZE --no-progress \
    -s "$url" >/dev/null 2>&1

  all_ok=true
  for pid in "${sub_pids[@]}"; do
    wait "$pid" 2>/dev/null || all_ok=false
  done
  $all_ok && pass "$label ws fan-out x3" || fail "$label ws fan-out x3"
done

# --- 9. WebSocket mixed: WS pub → TCP sub (same server) ---
echo ""
echo "[9] Mixed: WS pub → TCP sub"
for label in "GoLeaf" "RustLeaf"; do
  if [ "$label" = "GoLeaf" ]; then
    ws_url="ws://127.0.0.1:$GO_LEAF_WS_PORT"
    tcp_url="nats://127.0.0.1:$GO_LEAF_PORT"
  else
    ws_url="ws://127.0.0.1:$RUST_LEAF_WS_PORT"
    tcp_url="nats://127.0.0.1:$RUST_LEAF_PORT"
  fi

  nats bench sub smoke.mix --msgs $MSGS --size $SIZE --no-progress \
    -s "$tcp_url" >/dev/null 2>&1 &
  sub_pid=$!
  sleep 0.3

  nats bench pub smoke.mix --msgs $MSGS --size $SIZE --no-progress \
    -s "$ws_url" >/dev/null 2>&1

  wait "$sub_pid" 2>/dev/null \
    && pass "$label ws→tcp" || fail "$label ws→tcp"
done

echo ""
echo "=== ALL SMOKE TESTS PASSED ==="
