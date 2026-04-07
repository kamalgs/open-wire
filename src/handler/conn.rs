//! Connection types and handler interface.
//!
//! Defines per-connection context (`ConnCtx`), the `ConnExt` discriminator
//! for connection types, the `ConnectionHandler` trait, and shared enums.

use std::collections::HashMap;
use std::io;

use bytes::{Bytes, BytesMut};

use crate::core::server::Permissions;
use crate::sub_list::MsgWriter;

#[cfg(feature = "leaf")]
use crate::connector::leaf::UpstreamCmd;
#[cfg(feature = "leaf")]
use std::sync::mpsc;

/// Trait unifying protocol handlers for different connection types.
///
/// Each connection kind (client, leaf, route, gateway) implements this trait
/// to provide both protocol parsing and operation dispatch. This enables
/// the worker to use a single generic `process_active<H>()` method instead
/// of duplicating the parse → handle → cleanup pipeline for each type.
pub(crate) trait ConnectionHandler {
    /// Protocol operation type produced by the parser.
    type Op;

    /// Parse the next protocol operation from the read buffer.
    ///
    /// Returns `Ok(Some(op))` on success, `Ok(None)` if more data is needed,
    /// or `Err` on parse error (caller should disconnect).
    fn parse_op(buf: &mut BytesMut) -> io::Result<Option<Self::Op>>;

    /// Dispatch a parsed operation, returning the result and any expired subs.
    fn handle_op(
        conn: &mut ConnCtx<'_>,
        wctx: &mut super::MessageDeliveryHub<'_>,
        op: Self::Op,
    ) -> (HandleResult, Vec<(u64, u64)>);
}

/// Per-connection state view. Borrows disjointly from `ClientState` fields
/// so the handler can read/write connection-specific data without holding
/// a mutable borrow on the entire `Worker`.
pub(crate) struct ConnCtx<'a> {
    pub conn_id: u64,
    pub write_buf: &'a mut BytesMut,
    pub direct_writer: &'a MsgWriter,
    pub echo: bool,
    /// When true, send 503 no-responders status for request-reply with zero subscribers.
    pub no_responders: bool,
    pub sub_count: &'a mut usize,
    #[cfg(feature = "leaf")]
    pub upstream_txs: &'a mut Vec<mpsc::Sender<UpstreamCmd>>,
    pub permissions: &'a Option<Permissions>,
    pub ext: &'a mut ConnExt,
    /// Whether the connection is in Draining phase.
    pub draining: bool,
    /// Account this connection belongs to. 0 = `$G` (global/default).
    #[cfg(feature = "accounts")]
    pub account_id: crate::core::server::AccountId,
}

/// Per-connection extension state, replacing the flat `ConnectionKind` enum
/// plus the `leaf_sid_counter` / `leaf_sids` fields on `ClientState`.
pub(crate) enum ConnExt {
    /// Normal NATS client connection (uses MSG/HMSG for delivery).
    Client,
    /// Inbound leaf node connection (uses LMSG for delivery, LS+/LS- for interest).
    #[cfg(feature = "hub")]
    Leaf {
        leaf_sid_counter: u64,
        leaf_sids: HashMap<(Bytes, Option<Bytes>), u64>,
    },
    /// Inbound route connection (uses RMSG for delivery, RS+/RS- for interest).
    #[cfg(feature = "mesh")]
    Route {
        route_sid_counter: u64,
        route_sids: HashMap<(Bytes, Option<Bytes>), u64>,
        /// Peer's server_id, stored for cleanup deregistration from RoutePeerRegistry.
        peer_server_id: Option<String>,
        /// Whether this connection uses binary framing (open-wire binary protocol).
        binary: bool,
    },
    /// Inbound gateway connection (uses RMSG for delivery, RS+/RS- for interest).
    #[cfg(feature = "gateway")]
    Gateway {
        gateway_sid_counter: u64,
        gateway_sids: HashMap<(Bytes, Option<Bytes>), u64>,
        /// Secondary index: subject → list of (queue, sid) for O(1) unsub lookup.
        gateway_sids_by_subject: HashMap<Bytes, Vec<(Option<Bytes>, u64)>>,
        /// Remote cluster's gateway name.
        peer_gateway_name: Option<String>,
    },
    /// Binary-protocol client: uses the binary wire format for pub/sub/deliver.
    #[cfg(feature = "binary-client")]
    BinaryClient,
}

/// Lightweight tag for dispatch — always compiled, no cfg gates on the enum itself.
pub(crate) enum ConnKind {
    /// Normal NATS client connection.
    Client,
    /// Inbound leaf node connection.
    #[cfg(feature = "hub")]
    Leaf,
    /// Inbound route connection.
    #[cfg(feature = "mesh")]
    Route,
    /// Inbound gateway connection.
    #[cfg(feature = "gateway")]
    Gateway,
    /// Binary-protocol client connection (`binary-client` feature).
    #[cfg(feature = "binary-client")]
    BinaryClient,
}

impl ConnExt {
    /// Returns a lightweight discriminant tag for dispatch.
    pub(crate) fn kind_tag(&self) -> ConnKind {
        match self {
            Self::Client => ConnKind::Client,
            #[cfg(feature = "hub")]
            Self::Leaf { .. } => ConnKind::Leaf,
            #[cfg(feature = "mesh")]
            Self::Route { .. } => ConnKind::Route,
            #[cfg(feature = "gateway")]
            Self::Gateway { .. } => ConnKind::Gateway,
            #[cfg(feature = "binary-client")]
            Self::BinaryClient => ConnKind::BinaryClient,
        }
    }

    /// Returns `true` for inbound leaf connections.
    #[cfg(feature = "hub")]
    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf { .. })
    }

    /// Returns `true` for inbound leaf connections.
    #[cfg(not(feature = "hub"))]
    pub fn is_leaf(&self) -> bool {
        false
    }

    /// Returns `true` for inbound route connections.
    #[cfg(feature = "mesh")]
    pub fn is_route(&self) -> bool {
        matches!(self, Self::Route { .. })
    }

    /// Returns `true` for inbound route connections.
    #[cfg(not(feature = "mesh"))]
    #[allow(dead_code)]
    pub fn is_route(&self) -> bool {
        false
    }

    /// Returns `true` for inbound gateway connections.
    #[cfg(feature = "gateway")]
    pub fn is_gateway(&self) -> bool {
        matches!(self, Self::Gateway { .. })
    }

    /// Returns `true` for inbound gateway connections.
    #[cfg(not(feature = "gateway"))]
    #[allow(dead_code)]
    pub fn is_gateway(&self) -> bool {
        false
    }

    /// Connection type label for metrics and logging.
    #[cfg(any(feature = "hub", feature = "mesh", feature = "gateway"))]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Client => "client",
            #[cfg(feature = "hub")]
            Self::Leaf { .. } => "leaf",
            #[cfg(feature = "mesh")]
            Self::Route { .. } => "route",
            #[cfg(feature = "gateway")]
            Self::Gateway { .. } => "gateway",
            #[cfg(feature = "binary-client")]
            Self::BinaryClient => "binary",
        }
    }
}

/// Result of handling a single protocol operation.
pub(crate) enum HandleResult {
    /// Operation handled successfully, continue processing.
    Ok,
    /// Operation handled, connection should be flushed.
    Flush,
    /// Protocol error — disconnect the connection.
    Disconnect,
}

/// Convert `Bytes` to `&str` without UTF-8 validation.
/// NATS subjects are restricted to ASCII printable characters.
#[inline]
pub(crate) fn bytes_to_str(b: &Bytes) -> &str {
    // SAFETY: NATS protocol subjects/reply-to are always ASCII
    unsafe { std::str::from_utf8_unchecked(b) }
}
