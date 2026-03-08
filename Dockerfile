# Stage 1: Build
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Copy manifests first for dependency caching.
COPY Cargo.toml Cargo.lock ./

# Create dummy source files so cargo can resolve the package
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs

# Build dependencies only (cached layer)
RUN cargo build --release 2>/dev/null || true

# Copy actual source
COPY . .

# Touch sources to invalidate the dummy builds
RUN touch src/main.rs src/lib.rs

# Build the real binary
RUN cargo build --release

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/open-wire /usr/local/bin/open-wire

EXPOSE 4222
EXPOSE 4223

ENTRYPOINT ["open-wire"]
CMD ["--port", "4222"]
