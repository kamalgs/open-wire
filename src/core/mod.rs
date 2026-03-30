//! Shared infrastructure: server, workers, protocol parsing, subscriptions, I/O.

pub mod config;
pub(crate) mod handler;
pub(crate) mod io;
pub mod protocol;
pub mod pubsub;
pub mod server;
pub(crate) mod worker;

// Convenience re-exports so external paths stay one level deep (e.g. `crate::core::nats_proto`).
pub(crate) use io::buf;
pub(crate) use io::msg_writer;
pub(crate) use io::websocket;
pub use protocol::nats_proto;
pub use protocol::types;
pub use pubsub::sub_list;
