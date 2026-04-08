//! Message delivery pipeline.
//!
//! Contains the full publish → deliver → notify flow:
//! - `MessageDeliveryHub::publish()` — entry point from all handlers
//! - `deliver_to_subs()` — iterate subs, dispatch to each
//! - `deliver_to_sub_inner()` — write MSG/LMSG/RMSG per sub type
//! - `forward_to_optimistic_gateways()` — optimistic gateway delivery
//! - `deliver_cross_account()` — cross-account forwarding
//! - `forward_to_upstream()` — send to hub via mpsc
//! - `handle_expired_subs()` — cleanup after delivery

use std::os::fd::RawFd;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;

use bytes::Bytes;
use metrics::gauge;
use tracing::debug;

use super::ConnCtx;
use crate::connector::leaf::UpstreamCmd;
use crate::core::server::ServerState;
use crate::sub_list::MsgWriter;
use crate::types::HeaderMap;

/// Worker-level context for delivery and notification.
pub(crate) struct MessageDeliveryHub<'a> {
    pub state: &'a Arc<ServerState>,
    pub event_fd: RawFd,
    pub pending_notify: &'a mut [RawFd; 16],
    pub pending_notify_count: &'a mut usize,
    pub msgs_received: &'a mut u64,
    pub msgs_received_bytes: &'a mut u64,
    pub msgs_delivered: &'a mut u64,
    pub msgs_delivered_bytes: &'a mut u64,
    pub worker_label: &'a str,
    /// Worker index within the worker pool (0-based).
    #[cfg(feature = "worker-affinity")]
    pub worker_index: usize,
    /// Set to true when a delivery hits a congested route MsgWriter.
    /// The worker reads this after processing a client's PUBs to reduce
    /// that client's read budget (non-blocking TCP flow control).
    pub route_congested: bool,
}

impl MessageDeliveryHub<'_> {
    /// Queue an eventfd notification for a remote worker, deduplicating within the batch.
    ///
    /// Skips notification for our own worker (flush_pending handles it).
    #[inline]
    pub(crate) fn queue_notify(&mut self, fd: RawFd) {
        if fd == self.event_fd {
            return;
        }
        if !self.pending_notify[..*self.pending_notify_count].contains(&fd)
            && *self.pending_notify_count < self.pending_notify.len()
        {
            self.pending_notify[*self.pending_notify_count] = fd;
            *self.pending_notify_count += 1;
        }
    }

    /// Record a delivered message in worker-level metrics.
    #[inline]
    pub(crate) fn record_delivery(&mut self, payload_len: u64) {
        *self.msgs_delivered += 1;
        *self.msgs_delivered_bytes += payload_len;
    }
}

/// Message descriptor with owned, ref-counted subject and payload.
///
/// Using `Bytes` for subject/payload eliminates two memcpy calls per routed
/// message: the binary route writer path can clone the `Bytes` (O(1) refcount
/// increment) instead of copying into the shared `direct_buf`.
pub(crate) struct Msg<'a> {
    pub subject: Bytes,
    pub reply: Option<Bytes>,
    pub headers: Option<&'a HeaderMap>,
    pub payload: Bytes,
}

impl<'a> Msg<'a> {
    /// Create a new message descriptor.
    ///
    /// `subject` and `payload` accept anything that converts to `Bytes`
    /// (e.g. `&[u8]`, `Bytes`, `Vec<u8>`), so existing `b"literal"` call
    /// sites continue to compile via `Bytes::copy_from_slice`.
    #[inline]
    pub(crate) fn new(
        subject: impl Into<Bytes>,
        reply: Option<Bytes>,
        headers: Option<&'a HeaderMap>,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
            subject: subject.into(),
            reply,
            headers,
            payload: payload.into(),
        }
    }

    /// Return the subject as a `&str`.
    ///
    /// # Safety
    /// NATS subjects are always valid ASCII (a subset of UTF-8).
    #[inline]
    pub(crate) fn subject_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.subject) }
    }
}

/// Controls which subscription types are skipped during delivery.
pub(crate) struct DeliveryScope {
    /// Skip delivery back to the publishing connection.
    pub skip_echo: bool,
    /// Skip route subscriptions (one-hop enforcement from routes).
    pub skip_routes: bool,
    /// Skip gateway subscriptions (one-hop enforcement from gateways).
    pub skip_gateways: bool,
}

impl DeliveryScope {
    /// Scope for client PUB / leaf LMSG: all peer types receive the message.
    #[inline]
    pub(crate) fn local(skip_echo: bool) -> Self {
        Self {
            skip_echo,
            skip_routes: false,
            skip_gateways: false,
        }
    }

    /// Scope for messages arriving from a route peer (one-hop: skip routes).
    #[inline]
    pub(crate) fn from_route() -> Self {
        Self {
            skip_echo: true,
            skip_routes: true,
            skip_gateways: false,
        }
    }

    /// Scope for messages arriving from a gateway peer (one-hop: skip routes + gateways).
    #[inline]
    pub(crate) fn from_gateway() -> Self {
        Self {
            skip_echo: true,
            skip_routes: true,
            skip_gateways: true,
        }
    }
}

impl MessageDeliveryHub<'_> {
    /// Publish a message: deliver to local subs, optimistic gateways, and cross-account.
    ///
    /// Encapsulates the 3-step pipeline that every handler's publish path uses.
    #[allow(unused_variables)]
    pub(crate) fn publish(
        &mut self,
        msg: &Msg<'_>,
        skip_conn_id: u64,
        scope: &DeliveryScope,
        #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
    ) -> (usize, Vec<(u64, u64)>) {
        let (delivered, expired) = deliver_to_subs(
            self,
            msg,
            skip_conn_id,
            scope,
            #[cfg(feature = "accounts")]
            account_id,
        );

        if !scope.skip_gateways {
            forward_to_optimistic_gateways(
                self,
                msg,
                #[cfg(feature = "accounts")]
                self.state.account_name(account_id).as_bytes(),
            );
        }

        #[cfg(feature = "accounts")]
        let expired = {
            let mut expired = expired;
            let cross_expired = deliver_cross_account(self, msg, account_id);
            expired.extend(cross_expired);
            expired
        };

        (delivered, expired)
    }
}

/// Inner dispatch for writing a message to a single subscription based on its type.
///
/// Returns `true` if the message was delivered, `false` if it was filtered
/// (e.g., by leaf subscribe permissions).
#[inline]
#[allow(unused_variables)]
pub(crate) fn deliver_to_sub_inner(
    sub: &crate::sub_list::Subscription,
    msg: &Msg<'_>,
    #[cfg(feature = "accounts")] account_name: &[u8],
) -> bool {
    if sub.is_route() {
        sub.writer.write_rmsg(
            &msg.subject,
            msg.reply.as_deref(),
            msg.headers,
            &msg.payload,
            #[cfg(feature = "accounts")]
            account_name,
        );
        return true;
    }
    if sub.is_leaf() {
        // Check subscribe permissions: don't deliver to leaf if the subject
        // is not in the leaf's allowed subscribe set.
        if let Some(ref perms) = sub.leaf_perms {
            let subject_str = msg.subject_str();
            if !perms.subscribe.is_allowed(subject_str) {
                return false;
            }
        }
        sub.writer.write_lmsg(
            &msg.subject,
            msg.reply.as_deref(),
            msg.headers,
            &msg.payload,
        );
        return true;
    }
    // Binary-protocol client subscriptions: deliver via binary Msg frame.
    if sub.is_binary_client() {
        sub.writer.write_rmsg(
            &msg.subject,
            msg.reply.as_deref(),
            msg.headers,
            &msg.payload,
            #[cfg(feature = "accounts")]
            account_name,
        );
        return true;
    }

    sub.writer.write_msg(
        &msg.subject,
        &sub.sid_bytes,
        msg.reply.as_deref(),
        msg.headers,
        &msg.payload,
    );
    true
}

/// Shared delivery iteration: filter subs by scope, dispatch to writer, call `on_deliver`.
///
/// Extracts the common filter → dispatch → callback pipeline shared by both the
/// worker (`deliver_to_subs`) and upstream (`deliver_to_subs_upstream_inner`) paths.
/// Zero runtime overhead — monomorphized and inlined per call site.
#[inline]
#[allow(unused_variables)]
fn deliver_to_subs_core<F>(
    subs: &crate::sub_list::SubscriptionManager,
    msg: &Msg<'_>,
    skip_conn_id: u64,
    scope: &DeliveryScope,
    #[cfg(feature = "accounts")] acct_name: &[u8],
    state: &ServerState,
    mut on_deliver: F,
) -> (usize, Vec<(u64, u64)>)
where
    F: FnMut(&crate::sub_list::Subscription),
{
    let mut delivered: usize = 0;
    let (_match_count, expired) = subs.for_each_match(
        msg.subject_str(),
        |sub| {
            // Pre-filter: exclude subs that must not participate in queue-group
            // round-robin — if they are picked as the sole group member and then
            // filtered post-selection, the message would be silently dropped.
            if scope.skip_echo && sub.conn_id == skip_conn_id {
                return false;
            }

            if scope.skip_routes && sub.is_route() {
                return false;
            }

            if scope.skip_gateways && sub.is_gateway() {
                return false;
            }
            true
        },
        |sub| {
            if sub.is_gateway() {
                let gw_reply =
                    crate::handler::propagation::rewrite_gateway_reply(msg.reply.as_deref(), state);
                sub.writer.write_rmsg(
                    &msg.subject,
                    gw_reply.as_deref(),
                    msg.headers,
                    &msg.payload,
                    #[cfg(feature = "accounts")]
                    acct_name,
                );
            } else if !deliver_to_sub_inner(
                sub,
                msg,
                #[cfg(feature = "accounts")]
                acct_name,
            ) {
                return;
            }
            delivered += 1;
            on_deliver(sub);
        },
    );
    (delivered, expired)
}

/// Deliver a message to all matching subscriptions in the global sub list.
///
/// Writes MSG (for client subs) or LMSG (for leaf subs) directly to each
/// matching subscription's `MsgWriter`. Accumulates eventfd notifications
/// for remote workers (deduplicating within the batch).
///
/// Returns `(delivered_count, expired_subs)` where `delivered_count` is the
/// number of subscriptions that actually received the message (excluding skipped
/// subs), and expired_subs contains `(conn_id, sid)` pairs for subscriptions
/// that reached their delivery limit.
pub(crate) fn deliver_to_subs(
    wctx: &mut MessageDeliveryHub<'_>,
    msg: &Msg<'_>,
    skip_conn_id: u64,
    scope: &DeliveryScope,
    #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
) -> (usize, Vec<(u64, u64)>) {
    let payload_len = msg.payload.len() as u64;

    #[cfg(feature = "accounts")]
    let acct_name_str = wctx.state.account_name(account_id);
    #[cfg(feature = "accounts")]
    let acct_name = acct_name_str.as_bytes();

    let gw_state = Arc::clone(wctx.state);

    let subs = wctx
        .state
        .get_subs(
            #[cfg(feature = "accounts")]
            account_id,
        )
        .read()
        .unwrap();

    let (delivered, expired) = deliver_to_subs_core(
        &subs,
        msg,
        skip_conn_id,
        scope,
        #[cfg(feature = "accounts")]
        acct_name,
        &gw_state,
        |sub| {
            wctx.record_delivery(payload_len);
            wctx.queue_notify(sub.writer.event_raw_fd());
            // Signal route congestion so the worker can reduce this client's read budget.

            if sub.is_route() && sub.writer.congestion() >= 1 {
                wctx.route_congested = true;
            }
        },
    );
    drop(subs);
    (delivered, expired)
}

/// Deliver a message to all matching subscriptions from the upstream reader thread.
///
/// Unlike `deliver_to_subs`, this variant collects dirty writers for batch
/// notification (since the upstream reader runs outside the worker event loop)
/// and does not track worker-level metrics.
pub(crate) fn deliver_to_subs_upstream(
    state: &ServerState,
    msg: &Msg<'_>,
    dirty_writers: &mut Vec<MsgWriter>,
    #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
) -> (usize, Vec<(u64, u64)>) {
    deliver_to_subs_upstream_inner(
        state,
        msg,
        dirty_writers,
        &DeliveryScope::local(false),
        #[cfg(feature = "accounts")]
        account_id,
    )
}

/// Deliver to local subs from an upstream-like source (hub or route reader thread).
/// Respects `scope` for one-hop enforcement (skip_routes, skip_gateways).
///
/// Returns `(delivered_count, expired_subs)`.
#[allow(unused_variables)]
pub(crate) fn deliver_to_subs_upstream_inner(
    state: &ServerState,
    msg: &Msg<'_>,
    dirty_writers: &mut Vec<MsgWriter>,
    scope: &DeliveryScope,
    #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
) -> (usize, Vec<(u64, u64)>) {
    #[cfg(feature = "accounts")]
    #[allow(unused)]
    let acct_name_str = state.account_name(account_id);
    #[cfg(feature = "accounts")]
    #[allow(unused)]
    let acct_name = acct_name_str.as_bytes();

    let subs = state
        .get_subs(
            #[cfg(feature = "accounts")]
            account_id,
        )
        .read()
        .unwrap();
    let (delivered, expired) = deliver_to_subs_core(
        &subs,
        msg,
        0,
        scope,
        #[cfg(feature = "accounts")]
        acct_name,
        state,
        |sub| {
            dirty_writers.push(sub.writer.clone());
        },
    );
    drop(subs);
    (delivered, expired)
}

/// Forward a publish to all upstream hubs. If a writer thread has gone,
/// refresh the senders from global state.
pub(crate) fn forward_to_upstream(
    upstream_txs: &mut Vec<mpsc::Sender<UpstreamCmd>>,
    state: &ServerState,
    subject: Bytes,
    reply: Option<Bytes>,
    headers: Option<HeaderMap>,
    payload: Bytes,
) {
    if upstream_txs.is_empty() {
        return;
    }
    let mut any_failed = false;
    for tx in upstream_txs.iter() {
        if tx
            .send(UpstreamCmd::Publish {
                subject: subject.clone(),
                reply: reply.clone(),
                headers: headers.clone(),
                payload: payload.clone(),
            })
            .is_err()
        {
            any_failed = true;
        }
    }
    if any_failed {
        // At least one writer thread gone — refresh from global state (may have reconnected)
        *upstream_txs = state.leaf.upstream_txs.read().unwrap().clone();
    }
}

impl ConnCtx<'_> {
    /// Forward a publish to all upstream hubs via the connection's cached senders.
    #[inline]
    pub(crate) fn forward_to_upstream(
        &mut self,
        state: &ServerState,
        subject: Bytes,
        reply: Option<Bytes>,
        headers: Option<HeaderMap>,
        payload: Bytes,
    ) {
        forward_to_upstream(self.upstream_txs, state, subject, reply, headers, payload);
    }
}

/// Handle expired subscriptions after delivery.
///
/// Removes expired subs from the global sub list, decrements `sub_count` on
/// their owning connections, removes upstream interest, and updates metrics.
pub(crate) fn handle_expired_subs(
    expired: &[(u64, u64)],
    state: &ServerState,
    conns: &mut rustc_hash::FxHashMap<u64, crate::core::worker::ClientState>,
    worker_label: &str,
    #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
) {
    if expired.is_empty() {
        return;
    }
    let mut subs = state
        .get_subs(
            #[cfg(feature = "accounts")]
            account_id,
        )
        .write()
        .unwrap();
    for (exp_conn_id, exp_sid) in expired {
        if let Some(removed) = subs.remove(*exp_conn_id, *exp_sid) {
            if let Some(client) = conns.get_mut(exp_conn_id) {
                client.sub_count = client.sub_count.saturating_sub(1);
            }

            {
                let mut upstreams = state.leaf.upstreams.write().unwrap();
                for up in upstreams.iter_mut() {
                    up.remove_interest(&removed.subject, removed.queue.as_deref());
                }
            }
            let _ = &removed;
            gauge!(
                "subscriptions_active",
                "worker" => worker_label.to_string()
            )
            .decrement(1.0);
            debug!(
                conn_id = exp_conn_id,
                sid = exp_sid,
                subject = %removed.subject,
                "auto-unsubscribed (max reached)"
            );
        }
    }
    state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
}

/// Handle expired subscriptions from the upstream reader thread.
///
/// Similar to `handle_expired_subs` but without access to worker connections
/// (upstream runs in its own thread).
pub(crate) fn handle_expired_subs_upstream(
    expired: &[(u64, u64)],
    state: &ServerState,
    #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
) {
    if expired.is_empty() {
        return;
    }
    let mut subs = state
        .get_subs(
            #[cfg(feature = "accounts")]
            account_id,
        )
        .write()
        .unwrap();
    for (conn_id, sid) in expired {
        if let Some(removed) = subs.remove(*conn_id, *sid) {
            let mut upstreams = state.leaf.upstreams.write().unwrap();
            for up in upstreams.iter_mut() {
                up.remove_interest(&removed.subject, removed.queue.as_deref());
            }
        }
    }
    state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
}

/// Forward a message to outbound gateways in optimistic mode.
///
/// Called after `deliver_to_subs` when gateway subs may not exist in SubscriptionManager
/// (because the remote hasn't sent RS+ yet). In optimistic mode, we forward
/// unless the subject is in the gateway's negative interest set.
pub(crate) fn forward_to_optimistic_gateways(
    wctx: &mut MessageDeliveryHub<'_>,
    msg: &Msg<'_>,
    #[cfg(feature = "accounts")] account: &[u8],
) {
    use crate::core::server::GatewayInterestMode;

    let gi = wctx.state.gateway.interest.read().unwrap();
    if gi.is_empty() {
        return;
    }

    let payload_len = msg.payload.len() as u64;

    for gis in gi.values() {
        if gis.mode != GatewayInterestMode::Optimistic {
            continue;
        }
        // Skip if subject is in the negative interest set.
        if gis.ni.contains(msg.subject_str()) {
            continue;
        }

        // Rewrite reply with _GR_ prefix before forwarding across gateway.
        let gw_reply =
            crate::handler::propagation::rewrite_gateway_reply(msg.reply.as_deref(), wctx.state);
        gis.writer.write_rmsg(
            &msg.subject,
            gw_reply.as_deref(),
            msg.headers,
            &msg.payload,
            #[cfg(feature = "accounts")]
            account,
        );

        wctx.record_delivery(payload_len);
        wctx.queue_notify(gis.writer.event_raw_fd());
    }
}

/// Deliver a message to cross-account subscribers (worker context).
///
/// Called after same-account `deliver_to_subs()`. For each cross-account route
/// whose export pattern matches the published subject, delivers to the
/// destination account's SubscriptionManager (optionally remapping the subject).
///
/// Single-hop: cross-account delivery does NOT recursively trigger more
/// cross-account forwarding.
#[cfg(feature = "accounts")]
pub(crate) fn deliver_cross_account(
    wctx: &mut MessageDeliveryHub<'_>,
    msg: &Msg<'_>,
    src_account_id: crate::core::server::AccountId,
) -> Vec<(u64, u64)> {
    let routes = match wctx.state.cross_account_routes.get(src_account_id as usize) {
        Some(r) if !r.is_empty() => r,
        _ => return Vec::new(),
    };

    let payload_len = msg.payload.len() as u64;
    let mut all_expired = Vec::new();

    for route in routes {
        if !crate::sub_list::subject_matches(&route.export_pattern, msg.subject_str()) {
            continue;
        }

        let (dst_subject_str, dst_subject_bytes);
        match &route.remap {
            Some(r) => {
                dst_subject_str = crate::sub_list::remap_subject(
                    &r.from_pattern,
                    &r.to_pattern,
                    msg.subject_str(),
                );
                dst_subject_bytes = Bytes::copy_from_slice(dst_subject_str.as_bytes());
            }
            None => {
                dst_subject_str = msg.subject_str().to_string();
                dst_subject_bytes = msg.subject.clone();
            }
        };

        let dst_acct_name = wctx.state.account_name(route.dst_account_id).as_bytes();

        let dst_msg = Msg::new(
            dst_subject_bytes,
            msg.reply.clone(),
            msg.headers,
            msg.payload.clone(),
        );

        let subs = wctx.state.get_subs(route.dst_account_id).read().unwrap();
        let (_count, expired) = subs.for_each_match(
            &dst_subject_str,
            |_| true,
            |sub| {
                let did_deliver = deliver_to_sub_inner(sub, &dst_msg, dst_acct_name);
                if !did_deliver {
                    return;
                }
                wctx.record_delivery(payload_len);
                wctx.queue_notify(sub.writer.event_raw_fd());
            },
        );
        drop(subs);
        all_expired.extend(expired);
    }

    all_expired
}

/// Deliver a message to cross-account subscribers (upstream/route reader thread).
///
/// Same as `deliver_cross_account` but for contexts outside the worker event loop.
/// Accumulates dirty writers for batch notification.
#[cfg(feature = "accounts")]
pub(crate) fn deliver_cross_account_upstream(
    state: &ServerState,
    msg: &Msg<'_>,
    dirty_writers: &mut Vec<MsgWriter>,
    src_account_id: crate::core::server::AccountId,
) -> Vec<(u64, u64)> {
    let routes = match state.cross_account_routes.get(src_account_id as usize) {
        Some(r) if !r.is_empty() => r,
        _ => return Vec::new(),
    };

    let mut all_expired = Vec::new();

    for route in routes {
        if !crate::sub_list::subject_matches(&route.export_pattern, msg.subject_str()) {
            continue;
        }

        let (dst_subject_str, dst_subject_bytes);
        match &route.remap {
            Some(r) => {
                dst_subject_str = crate::sub_list::remap_subject(
                    &r.from_pattern,
                    &r.to_pattern,
                    msg.subject_str(),
                );
                dst_subject_bytes = Bytes::copy_from_slice(dst_subject_str.as_bytes());
            }
            None => {
                dst_subject_str = msg.subject_str().to_string();
                dst_subject_bytes = msg.subject.clone();
            }
        };

        #[allow(unused)]
        let dst_acct_name = state.account_name(route.dst_account_id).as_bytes();

        let dst_msg = Msg::new(
            dst_subject_bytes,
            msg.reply.clone(),
            msg.headers,
            msg.payload.clone(),
        );

        let subs = state.get_subs(route.dst_account_id).read().unwrap();
        let (_count, expired) = subs.for_each_match(
            &dst_subject_str,
            |_| true,
            |sub| {
                if !deliver_to_sub_inner(sub, &dst_msg, dst_acct_name) {
                    return;
                }
                dirty_writers.push(sub.writer.clone());
            },
        );
        drop(subs);
        all_expired.extend(expired);
    }

    all_expired
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize};
    use std::sync::Arc;

    use crate::sub_list::{DirectWriter, SubKind, SubList, Subscription};

    use super::*;

    fn test_server_state() -> crate::core::server::ServerState {
        use rustc_hash::FxHashMap;
        #[allow(unused_imports)]
        use std::collections::HashMap;

        crate::core::server::ServerState {
            info: Default::default(),
            auth: Default::default(),
            ping_interval_ms: AtomicU64::new(0),
            auth_timeout_ms: AtomicU64::new(0),
            max_pings_outstanding: AtomicU32::new(0),
            #[cfg(not(feature = "accounts"))]
            subs: std::sync::RwLock::new(SubList::new()),
            #[cfg(feature = "accounts")]
            account_subs: vec![std::sync::RwLock::new(SubList::new())],
            #[cfg(feature = "accounts")]
            account_registry: crate::core::server::AccountRegistry::new(&[]),
            #[cfg(feature = "accounts")]
            account_configs: Vec::new(),
            #[cfg(feature = "accounts")]
            cross_account_routes: Vec::new(),
            #[cfg(feature = "accounts")]
            reverse_imports: Vec::new(),

            has_subs: AtomicBool::new(false),
            buf_config: Default::default(),
            next_cid: AtomicU64::new(1),
            tls_config: None,
            active_connections: AtomicU64::new(0),
            max_connections: AtomicUsize::new(0),
            max_payload: AtomicUsize::new(1024 * 1024),
            max_control_line: AtomicUsize::new(4096),
            max_subscriptions: AtomicUsize::new(0),
            stats: Default::default(),
            #[cfg(feature = "worker-affinity")]
            affinity: crate::core::server::AffinityMap::new(1),
            leaf: crate::core::server::LeafState {
                upstreams: std::sync::RwLock::new(Vec::new()),
                upstream_txs: std::sync::RwLock::new(Vec::new()),
                port: None,
                inbound_writers: std::sync::RwLock::new(FxHashMap::default()),
                inbound_auth: Default::default(),
            },
            cluster: crate::core::server::ClusterState {
                route_writers: std::sync::RwLock::new(FxHashMap::default()),
                port: None,
                name: None,
                seeds: Vec::new(),
                route_peers: std::sync::Mutex::new(crate::core::server::RoutePeerRegistry {
                    connected: HashMap::new(),
                    known_urls: std::collections::HashSet::new(),
                }),
                connect_tx: std::sync::Mutex::new(None),
            },
            gateway: crate::core::server::GatewayState {
                writers: std::sync::RwLock::new(FxHashMap::default()),
                port: None,
                name: None,
                remotes: Vec::new(),
                peers: std::sync::Mutex::new(crate::core::server::GatewayPeerRegistry {
                    connected: HashMap::new(),
                    known_urls: std::collections::HashSet::new(),
                }),
                connect_tx: std::sync::Mutex::new(None),
                reply_prefix: b"_GR_.abc.def.".to_vec(),
                cached_info: std::sync::Mutex::new(String::new()),
                interest: std::sync::RwLock::new(FxHashMap::default()),
                has_interest: AtomicBool::new(false),
            },
        }
    }

    /// Create a Subscription with a specific writer for verifying delivery.
    fn sub_with_writer(
        conn_id: u64,
        sid: u64,
        subject: &str,
        queue: Option<&str>,
        writer: &DirectWriter,
    ) -> Subscription {
        Subscription::new(
            conn_id,
            sid,
            subject.to_string(),
            queue.map(|s| s.to_string()),
            writer.clone(),
            SubKind::Client,
            #[cfg(feature = "accounts")]
            0,
        )
    }

    /// Helper to run deliver_to_subs_core directly against a SubList.
    fn run_core_delivery(
        subs: &SubList,
        subject: &str,
        payload: &[u8],
        skip_conn_id: u64,
        scope: &DeliveryScope,
    ) -> (usize, Vec<(u64, u64)>) {
        let msg = Msg::new(
            Bytes::copy_from_slice(subject.as_bytes()),
            None,
            None,
            Bytes::copy_from_slice(payload),
        );

        let state = test_server_state();
        deliver_to_subs_core(
            subs,
            &msg,
            skip_conn_id,
            scope,
            #[cfg(feature = "accounts")]
            b"$G",
            &state,
            |_sub| {},
        )
    }

    #[test]
    fn test_deliver_to_subs_writes_msg() {
        let writer = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo.bar", None, &writer));

        let msg = Msg::new(
            Bytes::from_static(b"foo.bar"),
            None,
            None,
            Bytes::from_static(b"hello"),
        );

        let state = test_server_state();
        let (delivered, expired) = deliver_to_subs_core(
            &subs,
            &msg,
            0,
            &DeliveryScope::local(false),
            #[cfg(feature = "accounts")]
            b"$G",
            &state,
            |_sub| {},
        );

        assert_eq!(delivered, 1);
        assert!(expired.is_empty());
        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.contains("MSG foo.bar 1"), "expected MSG: {s}");
        assert!(s.contains("hello"), "expected payload: {s}");
    }

    #[test]
    fn test_deliver_to_subs_fanout() {
        let w1 = DirectWriter::new_dummy();
        let w2 = DirectWriter::new_dummy();
        let w3 = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo.bar", None, &w1));
        subs.insert(sub_with_writer(2, 1, "foo.bar", None, &w2));
        subs.insert(sub_with_writer(3, 1, "foo.bar", None, &w3));

        let (delivered, _) =
            run_core_delivery(&subs, "foo.bar", b"data", 0, &DeliveryScope::local(false));

        assert_eq!(delivered, 3);
        assert!(w1.drain().is_some());
        assert!(w2.drain().is_some());
        assert!(w3.drain().is_some());
    }

    #[test]
    fn test_deliver_to_subs_no_match() {
        let writer = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo.bar", None, &writer));

        let (delivered, _) =
            run_core_delivery(&subs, "baz.qux", b"data", 0, &DeliveryScope::local(false));

        assert_eq!(delivered, 0);
        assert!(writer.drain().is_none());
    }

    #[test]
    fn test_skip_echo_suppresses_publisher() {
        let writer = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(42, 1, "foo", None, &writer));

        let (delivered, _) =
            run_core_delivery(&subs, "foo", b"data", 42, &DeliveryScope::local(true));

        assert_eq!(delivered, 0, "echo should be suppressed");
        assert!(writer.drain().is_none());
    }

    #[test]
    fn test_skip_echo_false_delivers_to_publisher() {
        let writer = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(42, 1, "foo", None, &writer));

        let (delivered, _) =
            run_core_delivery(&subs, "foo", b"data", 42, &DeliveryScope::local(false));

        assert_eq!(delivered, 1, "echo not suppressed when skip_echo=false");
        assert!(writer.drain().is_some());
    }

    #[test]

    fn test_from_route_skips_route_subs() {
        let writer = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        let mut sub = sub_with_writer(1, 1, "foo", None, &writer);
        sub.kind = SubKind::Route;
        subs.insert(sub);

        let (delivered, _) =
            run_core_delivery(&subs, "foo", b"data", 0, &DeliveryScope::from_route());

        assert_eq!(delivered, 0, "route sub should be skipped from route scope");
        assert!(writer.drain().is_none());
    }

    #[test]

    fn test_from_route_delivers_to_client_subs() {
        let client_w = DirectWriter::new_dummy();
        let route_w = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo", None, &client_w));
        let mut route_sub = sub_with_writer(2, 1, "foo", None, &route_w);
        route_sub.kind = SubKind::Route;
        subs.insert(route_sub);

        let (delivered, _) =
            run_core_delivery(&subs, "foo", b"data", 0, &DeliveryScope::from_route());

        assert_eq!(delivered, 1, "only client sub should receive");
        assert!(client_w.drain().is_some());
        assert!(route_w.drain().is_none());
    }

    #[test]

    fn test_from_gateway_skips_route_and_gateway_subs() {
        let client_w = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo", None, &client_w));

        {
            let route_w = DirectWriter::new_dummy();
            let mut route_sub = sub_with_writer(2, 1, "foo", None, &route_w);
            route_sub.kind = SubKind::Route;
            subs.insert(route_sub);
        }

        let gw_w = DirectWriter::new_dummy();
        let mut gw_sub = sub_with_writer(3, 1, "foo", None, &gw_w);
        gw_sub.kind = SubKind::Gateway;
        subs.insert(gw_sub);

        let (delivered, _) =
            run_core_delivery(&subs, "foo", b"data", 0, &DeliveryScope::from_gateway());

        assert_eq!(delivered, 1, "only client sub should receive");
        assert!(client_w.drain().is_some());
    }

    #[test]
    fn test_local_scope_delivers_to_all_types() {
        let client_w = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo", None, &client_w));

        #[allow(unused_mut)]
        let mut total_expected = 1;

        {
            let route_w = DirectWriter::new_dummy();
            let mut route_sub = sub_with_writer(2, 1, "foo", None, &route_w);
            route_sub.kind = SubKind::Route;
            subs.insert(route_sub);
            total_expected += 1;
        }

        let (delivered, _) =
            run_core_delivery(&subs, "foo", b"data", 0, &DeliveryScope::local(false));

        assert_eq!(delivered, total_expected);
    }

    #[test]

    fn test_deliver_to_leaf_sub_writes_lmsg() {
        let writer = DirectWriter::new_dummy();
        let mut sub = sub_with_writer(1, 1, "foo.bar", None, &writer);
        sub.kind = SubKind::Leaf;

        let msg = Msg::new(
            Bytes::from_static(b"foo.bar"),
            None,
            None,
            Bytes::from_static(b"hello"),
        );
        let delivered = deliver_to_sub_inner(
            &sub,
            &msg,
            #[cfg(feature = "accounts")]
            b"$G",
        );

        assert!(delivered, "leaf sub should be delivered");
        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.starts_with("LMSG"), "expected LMSG format: {s}");
    }

    #[test]

    fn test_deliver_leaf_sub_filtered_by_permissions() {
        use crate::core::server::{Permission, Permissions};

        let writer = DirectWriter::new_dummy();
        let mut sub = sub_with_writer(1, 1, "secret.data", None, &writer);
        sub.kind = SubKind::Leaf;
        sub.leaf_perms = Some(Arc::new(Permissions {
            publish: Permission {
                allow: Vec::new(),
                deny: Vec::new(),
            },
            subscribe: Permission {
                allow: Vec::new(),
                deny: vec!["secret.>".to_string()],
            },
        }));

        let msg = Msg::new(
            Bytes::from_static(b"secret.data"),
            None,
            None,
            Bytes::from_static(b"payload"),
        );
        let delivered = deliver_to_sub_inner(
            &sub,
            &msg,
            #[cfg(feature = "accounts")]
            b"$G",
        );

        assert!(!delivered, "should be filtered by leaf perms");
        assert!(writer.drain().is_none());
    }

    #[test]

    fn test_deliver_to_route_sub_writes_rmsg() {
        let writer = DirectWriter::new_dummy();
        let mut sub = sub_with_writer(1, 1, "foo.bar", None, &writer);
        sub.kind = SubKind::Route;

        let msg = Msg::new(
            Bytes::from_static(b"foo.bar"),
            None,
            None,
            Bytes::from_static(b"hello"),
        );
        let delivered = deliver_to_sub_inner(
            &sub,
            &msg,
            #[cfg(feature = "accounts")]
            b"$G",
        );

        assert!(delivered, "route sub should be delivered");
        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.starts_with("RMSG"), "expected RMSG format: {s}");
    }

    #[test]
    fn test_deliver_to_subs_returns_expired() {
        let writer = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        let mut sub = sub_with_writer(10, 5, "foo", None, &writer);
        sub.max_msgs = AtomicU64::new(1);
        subs.insert(sub);

        let (delivered, expired) =
            run_core_delivery(&subs, "foo", b"data", 0, &DeliveryScope::local(false));

        assert_eq!(delivered, 1);
        assert_eq!(expired.len(), 1, "sub should be expired after 1 delivery");
        assert_eq!(expired[0], (10, 5));
    }

    #[test]
    fn test_queue_group_delivers_to_one() {
        let w1 = DirectWriter::new_dummy();
        let w2 = DirectWriter::new_dummy();
        let w3 = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo", Some("q1"), &w1));
        subs.insert(sub_with_writer(2, 1, "foo", Some("q1"), &w2));
        subs.insert(sub_with_writer(3, 1, "foo", Some("q1"), &w3));

        let (delivered, _) =
            run_core_delivery(&subs, "foo", b"data", 0, &DeliveryScope::local(false));

        assert_eq!(delivered, 1, "queue group should deliver to exactly one");
        let got_data = [
            w1.drain().is_some(),
            w2.drain().is_some(),
            w3.drain().is_some(),
        ];
        assert_eq!(
            got_data.iter().filter(|&&b| b).count(),
            1,
            "exactly one writer should have data"
        );
    }

    #[test]
    fn test_queue_and_non_queue_mix() {
        let non_q_w = DirectWriter::new_dummy();
        let q1_w = DirectWriter::new_dummy();
        let q2_w = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo", None, &non_q_w));
        subs.insert(sub_with_writer(2, 1, "foo", Some("q1"), &q1_w));
        subs.insert(sub_with_writer(3, 1, "foo", Some("q1"), &q2_w));

        let (delivered, _) =
            run_core_delivery(&subs, "foo", b"data", 0, &DeliveryScope::local(false));

        assert_eq!(delivered, 2, "non-queue + 1 queue member");
        assert!(non_q_w.drain().is_some(), "non-queue sub always receives");
        let q_got = [q1_w.drain().is_some(), q2_w.drain().is_some()];
        assert_eq!(
            q_got.iter().filter(|&&b| b).count(),
            1,
            "exactly one queue member should receive"
        );
    }

    #[test]
    fn test_deliver_to_subs_upstream_collects_dirty_writers() {
        let state = test_server_state();
        let w1 = DirectWriter::new_dummy();
        let w2 = DirectWriter::new_dummy();

        {
            #[cfg(not(feature = "accounts"))]
            let mut subs = state.subs.write().unwrap();
            #[cfg(feature = "accounts")]
            let mut subs = state.account_subs[0].write().unwrap();
            subs.insert(sub_with_writer(1, 1, "foo", None, &w1));
            subs.insert(sub_with_writer(2, 1, "foo", None, &w2));
        }

        let msg = Msg::new(
            Bytes::from_static(b"foo"),
            None,
            None,
            Bytes::from_static(b"data"),
        );
        let mut dirty_writers = Vec::new();
        let (delivered, _) = deliver_to_subs_upstream_inner(
            &state,
            &msg,
            &mut dirty_writers,
            &DeliveryScope::local(false),
            #[cfg(feature = "accounts")]
            0,
        );

        assert_eq!(delivered, 2);
        assert_eq!(dirty_writers.len(), 2);
        assert!(w1.drain().is_some());
        assert!(w2.drain().is_some());
    }

    #[test]
    fn test_publish_delivers_to_local_subs() {
        let state = Arc::new(test_server_state());
        let writer = DirectWriter::new_dummy();

        {
            #[cfg(not(feature = "accounts"))]
            let mut subs = state.subs.write().unwrap();
            #[cfg(feature = "accounts")]
            let mut subs = state.account_subs[0].write().unwrap();
            subs.insert(sub_with_writer(1, 1, "foo", None, &writer));
        }

        let mut pending_notify = [0i32; 16];
        let mut pending_notify_count = 0usize;
        let mut msgs_received = 0u64;
        let mut msgs_received_bytes = 0u64;
        let mut msgs_delivered = 0u64;
        let mut msgs_delivered_bytes = 0u64;
        let mut wctx = MessageDeliveryHub {
            state: &state,
            event_fd: -1,
            pending_notify: &mut pending_notify,
            pending_notify_count: &mut pending_notify_count,
            msgs_received: &mut msgs_received,
            msgs_received_bytes: &mut msgs_received_bytes,
            msgs_delivered: &mut msgs_delivered,
            msgs_delivered_bytes: &mut msgs_delivered_bytes,
            worker_label: "test",
            #[cfg(feature = "worker-affinity")]
            worker_index: 0,
            route_congested: false,
        };

        let msg = Msg::new(
            Bytes::from_static(b"foo"),
            None,
            None,
            Bytes::from_static(b"hello"),
        );
        let (delivered, expired) = wctx.publish(
            &msg,
            0,
            &DeliveryScope::local(false),
            #[cfg(feature = "accounts")]
            0,
        );

        assert_eq!(delivered, 1);
        assert!(expired.is_empty());
        assert_eq!(msgs_delivered, 1);
        assert_eq!(msgs_delivered_bytes, 5);
        assert!(writer.drain().is_some());
    }

    #[test]
    fn test_deliver_wildcard_match() {
        let writer = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo.*", None, &writer));

        let (delivered, _) =
            run_core_delivery(&subs, "foo.bar", b"data", 0, &DeliveryScope::local(false));

        assert_eq!(delivered, 1);
        assert!(writer.drain().is_some());
    }

    #[test]
    fn test_deliver_gt_wildcard_match() {
        let writer = DirectWriter::new_dummy();
        let mut subs = SubList::new();
        subs.insert(sub_with_writer(1, 1, "foo.>", None, &writer));

        let (delivered, _) = run_core_delivery(
            &subs,
            "foo.bar.baz",
            b"data",
            0,
            &DeliveryScope::local(false),
        );

        assert_eq!(delivered, 1);
        assert!(writer.drain().is_some());
    }
}
