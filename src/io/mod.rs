//! I/O primitives: adaptive buffers, cross-worker delivery, WebSocket codec.

pub(crate) mod buf;
pub(crate) mod msg_writer;
pub(crate) mod websocket;
