//! open-wire — High-performance NATS-compatible message relay.
//!
//! Speaks the standard NATS client, leaf node, route, and gateway protocols.
//! Routes messages between local clients, optionally bridges traffic to an
//! upstream NATS hub, and can form full-mesh clusters with peer nodes.

pub mod config;
#[cfg(feature = "leaf")]
pub(crate) mod interest;
pub mod nats_proto;
pub(crate) mod protocol;
pub mod server;
pub mod sub_list;
pub mod types;
#[cfg(feature = "leaf")]
pub(crate) mod upstream;
pub(crate) mod websocket;
pub(crate) mod worker;

pub(crate) mod client_handler;
#[cfg(feature = "gateway")]
pub(crate) mod gateway_conn;
#[cfg(feature = "gateway")]
pub(crate) mod gateway_handler;
pub(crate) mod handler;
#[cfg(feature = "hub")]
pub(crate) mod leaf_handler;
#[cfg(feature = "cluster")]
pub(crate) mod route_conn;
#[cfg(feature = "cluster")]
pub(crate) mod route_handler;

#[cfg(all(feature = "leaf", feature = "subject-mapping"))]
pub use interest::SubjectMapping;
#[cfg(feature = "gateway")]
pub use server::GatewayRemote;
#[cfg(feature = "leaf")]
pub use server::HubCredentials;
#[cfg(feature = "leaf")]
pub use server::HubRemote;
#[cfg(feature = "accounts")]
pub use server::{AccountConfig, AccountId, AccountRegistry};
pub use server::{ClientAuth, LeafServer, LeafServerConfig, Permission, Permissions, UserConfig};
