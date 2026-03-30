//! Shared handler types and delivery functions for protocol command dispatch.
//!
//! Organized into submodules:
//! - `conn` — Connection types and handler interface (ConnCtx, ConnExt, ConnectionHandler)
//! - `delivery` — Message delivery pipeline (Msg, MessageDeliveryHub, deliver_to_subs, publish)
//! - `client` — Client protocol dispatch (PUB/SUB/UNSUB/PING/PONG)
//! - `propagation` — Interest propagation (LS+/LS-, RS+/RS-) + gateway reply rewriting

pub(crate) mod client;
mod conn;
mod delivery;
pub(crate) mod propagation;

pub(crate) use conn::*;
pub(crate) use delivery::*;
