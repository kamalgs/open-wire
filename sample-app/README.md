# NATS Chat — Browser-based IRC over NATS

A single-file browser chat app that connects directly to the leaf node gateway via WebSocket using `nats.ws`.

## Quick Start

### 1. Run the leaf server with WebSocket support

```bash
cargo run -p open-wire --features websockets --example leaf_server -- --port 4222 --ws-port 4223
```

### 2. Serve the app

```bash
cd sample-app
python3 -m http.server 8080
```

### 3. Chat

Open two browser tabs to http://localhost:8080. Pick different usernames, join the same channel, and exchange messages.

## How It Works

- Browsers connect directly to the leaf server's WebSocket port using the `nats.ws` client (loaded from CDN)
- Messages are published/subscribed on `chat.<channel>` subjects as JSON: `{"user":"alice","text":"hello","ts":1709...}`
- No backend or build step required — just static HTML + JS
