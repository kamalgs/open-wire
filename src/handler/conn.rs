//! Connection types and handler interface.
//!
//! Defines per-connection context (`ConnCtx`), the `ConnExt` discriminator
//! for connection types, the `ConnectionHandler` trait, and shared enums.

use std::collections::HashMap;
use std::io;

use bytes::{Bytes, BytesMut};

use crate::core::server::Permissions;
use crate::sub_list::MsgWriter;

use crate::connector::leaf::UpstreamCmd;
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
    Leaf {
        leaf_sid_counter: u64,
        leaf_sids: HashMap<(Bytes, Option<Bytes>), u64>,
    },
    /// Inbound route connection (uses RMSG for delivery, RS+/RS- for interest).
    Route {
        route_sid_counter: u64,
        route_sids: HashMap<(Bytes, Option<Bytes>), u64>,
        /// Peer's server_id, stored for cleanup deregistration from RoutePeerRegistry.
        peer_server_id: Option<String>,
        /// Whether this connection uses binary framing (open-wire binary protocol).
        binary: bool,
    },
    /// Inbound gateway connection (uses RMSG for delivery, RS+/RS- for interest).
    Gateway {
        gateway_sid_counter: u64,
        gateway_sids: HashMap<(Bytes, Option<Bytes>), u64>,
        /// Secondary index: subject → list of (queue, sid) for O(1) unsub lookup.
        gateway_sids_by_subject: HashMap<Bytes, Vec<(Option<Bytes>, u64)>>,
        /// Remote cluster's gateway name.
        peer_gateway_name: Option<String>,
    },
    /// Binary-protocol client: uses the binary wire format for pub/sub/deliver.
    BinaryClient,
}

/// Lightweight tag for dispatch — always compiled, no cfg gates on the enum itself.
pub(crate) enum ConnKind {
    /// Normal NATS client connection.
    Client,
    /// Inbound leaf node connection.
    Leaf,
    /// Inbound route connection.
    Route,
    /// Inbound gateway connection.
    Gateway,
    /// Binary-protocol client connection.
    BinaryClient,
}

impl ConnExt {
    /// Returns a lightweight discriminant tag for dispatch.
    pub(crate) fn kind_tag(&self) -> ConnKind {
        match self {
            Self::Client => ConnKind::Client,
            Self::Leaf { .. } => ConnKind::Leaf,
            Self::Route { .. } => ConnKind::Route,
            Self::Gateway { .. } => ConnKind::Gateway,
            Self::BinaryClient => ConnKind::BinaryClient,
        }
    }

    /// Returns `true` for inbound leaf connections.
    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf { .. })
    }

    /// Returns `true` for inbound route connections.
    pub fn is_route(&self) -> bool {
        matches!(self, Self::Route { .. })
    }

    /// Returns `true` for inbound gateway connections.
    pub fn is_gateway(&self) -> bool {
        matches!(self, Self::Gateway { .. })
    }

    /// Connection type label for metrics and logging.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Leaf { .. } => "leaf",
            Self::Route { .. } => "route",
            Self::Gateway { .. } => "gateway",
            Self::BinaryClient => "binary",
        }
    }

    /// Allocate the next SID for an interest-propagated subscription (leaf/route/gateway).
    ///
    /// Increments the internal counter and records the (subject, queue) → sid mapping
    /// so that a later unsub can look it up. Returns `None` for Client/BinaryClient
    /// (which receive SIDs from the protocol, not generated internally).
    pub(crate) fn next_sid(&mut self, subject: &Bytes, queue: &Option<Bytes>) -> Option<u64> {
        match self {
            Self::Leaf {
                leaf_sid_counter,
                leaf_sids,
            } => {
                *leaf_sid_counter += 1;
                let sid = *leaf_sid_counter;
                leaf_sids.insert((subject.clone(), queue.clone()), sid);
                Some(sid)
            }
            Self::Route {
                route_sid_counter,
                route_sids,
                ..
            } => {
                *route_sid_counter += 1;
                let sid = *route_sid_counter;
                route_sids.insert((subject.clone(), queue.clone()), sid);
                Some(sid)
            }
            Self::Gateway {
                gateway_sid_counter,
                gateway_sids,
                ..
            } => {
                *gateway_sid_counter += 1;
                let sid = *gateway_sid_counter;
                gateway_sids.insert((subject.clone(), queue.clone()), sid);
                Some(sid)
            }
            Self::Client | Self::BinaryClient => None,
        }
    }

    /// Look up and remove a SID by (subject, queue) key. Used for unsub.
    pub(crate) fn remove_sid(&mut self, subject: &Bytes, queue: &Option<Bytes>) -> Option<u64> {
        let key = (subject.clone(), queue.clone());
        match self {
            Self::Leaf { leaf_sids, .. } => leaf_sids.remove(&key),
            Self::Route { route_sids, .. } => route_sids.remove(&key),
            Self::Gateway { gateway_sids, .. } => gateway_sids.remove(&key),
            Self::Client | Self::BinaryClient => None,
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
