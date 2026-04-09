//! open-wire — High-performance NATS-compatible message relay.
//!
//! Speaks the standard NATS client, leaf node, route, and gateway protocols.
//! Routes messages between local clients, optionally bridges traffic to an
//! upstream NATS hub, and can form full-mesh clusters with peer nodes.

// Deny unwrap in production code — forces explicit error handling.
// Test code uses #[cfg_attr(test, allow(clippy::unwrap_used))].
#![deny(clippy::unwrap_used)]

// Internal module visibility is controlled at the item level within each module.
// Modules are `pub` to allow re-exports from lib.rs; internal items use `pub(crate)`.
pub mod config;
pub mod connector;
pub mod core;
pub(crate) mod handler;
pub(crate) mod io;
pub mod protocol;
pub mod pubsub;
pub(crate) mod util;

pub(crate) use io::buf;
pub(crate) use io::msg_writer;
pub(crate) use io::websocket;
pub use protocol::nats_proto;
pub use protocol::types;
pub use pubsub::sub_list;

pub use core::server;

#[cfg(feature = "subject-mapping")]
pub use connector::leaf::SubjectMapping;
pub use core::server::ClusterConfig;
pub use core::server::GatewayConfig;
pub use core::server::GatewayRemote;
pub use core::server::HubConfig;
pub use core::server::HubCredentials;
pub use core::server::HubRemote;
pub use core::server::InboundLeafConfig;
#[cfg(feature = "accounts")]
pub use core::server::{AccountConfig, AccountId, AccountRegistry};
pub use core::server::{ClientAuth, Permission, Permissions, Server, ServerConfig, UserConfig};
pub use core::server::{Limits, TlsConfig};
