//! Shared infrastructure: server, workers, protocol parsing, subscriptions, I/O.

pub(crate) mod buf;
pub mod config;
pub(crate) mod msg_writer;
pub mod nats_proto;
pub mod server;
pub mod sub_list;
pub mod types;
pub(crate) mod websocket;
pub(crate) mod worker;
