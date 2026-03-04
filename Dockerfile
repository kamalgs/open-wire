# Stage 1: Build
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Copy manifests first for dependency caching
COPY Cargo.toml ./
COPY async-nats/Cargo.toml async-nats/Cargo.toml
COPY nats/Cargo.toml nats/Cargo.toml
COPY nats-server/Cargo.toml nats-server/Cargo.toml

# Create dummy source files so cargo can resolve dependencies
RUN mkdir -p async-nats/src nats/src nats-server/src && \
    echo "fn main() {}" > async-nats/src/lib.rs && \
    echo "fn main() {}" > nats/src/lib.rs && \
    echo "fn main() {}" > nats-server/src/lib.rs && \
    mkdir -p async-nats/examples && \
    echo "fn main() {}" > async-nats/examples/leaf_server.rs

# Build dependencies only (cached layer)
RUN cargo build --release --no-default-features \
    --features "server_2_10,server_2_11,server_2_12" \
    -p async-nats --example leaf_server 2>/dev/null || true

# Copy actual source
COPY . .

# Touch sources to invalidate the dummy builds
RUN touch async-nats/src/lib.rs async-nats/examples/leaf_server.rs

# Build the real binary
RUN cargo build --release --no-default-features \
    --features "server_2_10,server_2_11,server_2_12" \
    --example leaf_server

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/examples/leaf_server /usr/local/bin/leaf-server

EXPOSE 4222

ENTRYPOINT ["leaf-server"]
CMD ["--port", "4222"]
