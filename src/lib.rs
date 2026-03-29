//! open-wire — High-performance NATS-compatible message relay.
//!
//! Speaks the standard NATS client, leaf node, route, and gateway protocols.
//! Routes messages between local clients, optionally bridges traffic to an
//! upstream NATS hub, and can form full-mesh clusters with peer nodes.

// Internal module visibility is controlled at the item level within each module.
// Modules are `pub` to allow re-exports from lib.rs; internal items use `pub(crate)`.
pub mod infra;

pub(crate) mod handler;

#[cfg(feature = "cluster")]
pub(crate) mod cluster;
#[cfg(feature = "gateway")]
pub(crate) mod gateway;
#[cfg(any(feature = "leaf", feature = "hub"))]
pub mod leaf;

// Public re-exports for external consumers (main.rs, tests, benchmarks).
pub use infra::config;
pub use infra::nats_proto;
pub use infra::server;
pub use infra::sub_list;
pub use infra::types;

#[cfg(feature = "gateway")]
pub use infra::server::GatewayRemote;
#[cfg(feature = "leaf")]
pub use infra::server::HubCredentials;
#[cfg(feature = "leaf")]
pub use infra::server::HubRemote;
#[cfg(feature = "accounts")]
pub use infra::server::{AccountConfig, AccountId, AccountRegistry};
pub use infra::server::{
    ClientAuth, LeafServer, LeafServerConfig, Permission, Permissions, UserConfig,
};
#[cfg(all(feature = "leaf", feature = "subject-mapping"))]
pub use leaf::SubjectMapping;
