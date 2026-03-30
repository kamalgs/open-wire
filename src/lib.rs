//! open-wire — High-performance NATS-compatible message relay.
//!
//! Speaks the standard NATS client, leaf node, route, and gateway protocols.
//! Routes messages between local clients, optionally bridges traffic to an
//! upstream NATS hub, and can form full-mesh clusters with peer nodes.

// Internal module visibility is controlled at the item level within each module.
// Modules are `pub` to allow re-exports from lib.rs; internal items use `pub(crate)`.
pub mod connector;
pub mod core;

// Public re-exports for external consumers (main.rs, tests, benchmarks).
pub use core::config;
pub use core::nats_proto;
pub use core::server;
pub use core::sub_list;
pub use core::types;

#[cfg(all(feature = "leaf", feature = "subject-mapping"))]
pub use connector::leaf::SubjectMapping;
#[cfg(feature = "gateway")]
pub use core::server::GatewayRemote;
#[cfg(feature = "leaf")]
pub use core::server::HubCredentials;
#[cfg(feature = "leaf")]
pub use core::server::HubRemote;
#[cfg(feature = "accounts")]
pub use core::server::{AccountConfig, AccountId, AccountRegistry};
pub use core::server::{
    ClientAuth, LeafServer, LeafServerConfig, Permission, Permissions, UserConfig,
};
