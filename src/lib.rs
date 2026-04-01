//! open-wire — High-performance NATS-compatible message relay.
//!
//! Speaks the standard NATS client, leaf node, route, and gateway protocols.
//! Routes messages between local clients, optionally bridges traffic to an
//! upstream NATS hub, and can form full-mesh clusters with peer nodes.

// Internal module visibility is controlled at the item level within each module.
// Modules are `pub` to allow re-exports from lib.rs; internal items use `pub(crate)`.
pub mod config;
pub mod connector;
pub mod core;
pub(crate) mod handler;
pub(crate) mod io;
pub mod protocol;
pub mod pubsub;

pub(crate) use io::buf;
pub(crate) use io::msg_writer;
pub(crate) use io::websocket;
pub use protocol::nats_proto;
pub use protocol::types;
pub use pubsub::sub_list;

pub use core::server;

#[cfg(all(feature = "leaf", feature = "subject-mapping"))]
pub use connector::leaf::SubjectMapping;
#[cfg(feature = "mesh")]
pub use core::server::ClusterConfig;
#[cfg(feature = "gateway")]
pub use core::server::GatewayConfig;
#[cfg(feature = "gateway")]
pub use core::server::GatewayRemote;
#[cfg(feature = "leaf")]
pub use core::server::HubConfig;
#[cfg(feature = "leaf")]
pub use core::server::HubCredentials;
#[cfg(feature = "leaf")]
pub use core::server::HubRemote;
#[cfg(feature = "hub")]
pub use core::server::InboundLeafConfig;
#[cfg(feature = "accounts")]
pub use core::server::{AccountConfig, AccountId, AccountRegistry};
pub use core::server::{ClientAuth, Permission, Permissions, Server, ServerConfig, UserConfig};
pub use core::server::{Limits, TlsConfig};
