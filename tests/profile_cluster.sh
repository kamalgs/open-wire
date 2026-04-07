#!/usr/bin/env bash
# Profile Rust-Rust cluster: pub on node A, sub on node B.
# Both nodes use binary inter-node protocol (open_wire=1).
# Builds with frame pointers for accurate call graphs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTDIR="$SCRIPT_DIR/profile_results"

PORT_A=19001
PORT_B=19002
CPORT_A=19101
CPORT_B=19102
MSGS=500000
MSG_SIZE=128

mkdir -p "$OUTDIR"

# Build with frame pointers + mesh feature
echo "Building with frame pointers + mesh..."
RUSTFLAGS="-C force-frame-pointers=yes" \
    cargo build --release --features mesh -q \
    --manifest-path "$REPO_ROOT/Cargo.toml"
BIN="$REPO_ROOT/target/release/open-wire"

# Write cluster configs
cat > /tmp/profile_cluster_a.conf << EOF
listen: 127.0.0.1:${PORT_A}
server_name: prof-a
cluster {
  name: prof-cluster
  listen: 127.0.0.1:${CPORT_A}
  routes = ["nats-route://127.0.0.1:${CPORT_B}"]
}
EOF

cat > /tmp/profile_cluster_b.conf << EOF
listen: 127.0.0.1:${PORT_B}
server_name: prof-b
cluster {
  name: prof-cluster
  listen: 127.0.0.1:${CPORT_B}
  routes = ["nats-route://127.0.0.1:${CPORT_A}"]
}
EOF

cleanup() {
    kill $SRV_A $SRV_B 2>/dev/null || true
    wait $SRV_A $SRV_B 2>/dev/null || true
}
trap cleanup EXIT

# Start node A
RUST_LOG=warn $BIN -c /tmp/profile_cluster_a.conf &>/dev/null &
SRV_A=$!
for i in $(seq 1 50); do nc -z 127.0.0.1 $PORT_A 2>/dev/null && break; sleep 0.1; done
echo "Node A alive (PID $SRV_A, client=:${PORT_A}, route=:${CPORT_A})"

# Start node B
RUST_LOG=warn $BIN -c /tmp/profile_cluster_b.conf &>/dev/null &
SRV_B=$!
for i in $(seq 1 50); do nc -z 127.0.0.1 $PORT_B 2>/dev/null && break; sleep 0.1; done
sleep 0.5  # let route connection establish
echo "Node B alive (PID $SRV_B, client=:${PORT_B}, route=:${CPORT_B})"

# Subscribe on node B
nats bench sub bench.cluster.test --msgs $MSGS --size $MSG_SIZE --no-progress \
    -s "nats://127.0.0.1:${PORT_B}" > /tmp/profile_cluster_sub.out 2>&1 &
SUB_PID=$!
echo "Subscriber on B (PID $SUB_PID)"
sleep 0.5

# Profile both nodes: A handles pub parsing + route delivery, B handles route receive + client deliver
echo "Starting perf on node A (pub side)..."
perf record -g --call-graph fp -F 997 -p $SRV_A \
    -o "$OUTDIR/cluster_node_a.perf.data" &>/dev/null &
PERF_A=$!

PERF_B=
sleep 0.3

# Publish on node A — messages cross the route to B
echo "Publishing $MSGS x ${MSG_SIZE}B messages on node A..."
nats bench pub bench.cluster.test --msgs $MSGS --size $MSG_SIZE --no-progress \
    -s "nats://127.0.0.1:${PORT_A}" 2>&1 | grep -E 'stats:|msg/s'

# Wait for subscriber
for i in $(seq 1 90); do kill -0 $SUB_PID 2>/dev/null || break; sleep 0.5; done
wait $SUB_PID 2>/dev/null || true
grep -E 'stats:|msg/s' /tmp/profile_cluster_sub.out 2>/dev/null | sed 's/^/  sub: /' || true

sleep 0.5
kill $PERF_A 2>/dev/null; wait $PERF_A 2>/dev/null || true

echo ""
echo "======================================================"
echo "  NODE A — pub side (client receive + route delivery)"
echo "======================================================"
perf report -i "$OUTDIR/cluster_node_a.perf.data" --stdio --no-children \
    --percent-limit 0.5 2>&1 \
    | grep -E "^\s+[0-9]" | head -30



echo ""
echo "=== flamegraph: perf script -i $OUTDIR/cluster_node_a.perf.data | flamegraph > cluster_a.svg ==="
