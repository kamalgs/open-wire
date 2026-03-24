//! Client protocol handler: PUB/HPUB, SUB, UNSUB, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Client`.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use metrics::{counter, gauge};
use tracing::{debug, warn};

#[cfg(feature = "accounts")]
use crate::handler::deliver_cross_account;
#[cfg(feature = "leaf")]
use crate::handler::forward_to_upstream;
use crate::handler::{bytes_to_str, deliver_to_subs, ConnCtx, HandleResult, WorkerCtx};
#[cfg(feature = "gateway")]
use crate::handler::{propagate_gateway_sub, propagate_gateway_unsub};
#[cfg(feature = "hub")]
use crate::handler::{propagate_leaf_sub, propagate_leaf_unsub};
#[cfg(feature = "cluster")]
use crate::handler::{propagate_route_sub, propagate_route_unsub};
use crate::nats_proto::{self, ClientOp};
use crate::sub_list::Subscription;

/// Handles client protocol operations (PUB, SUB, UNSUB, PING, PONG).
pub(crate) struct ClientHandler;

impl ClientHandler {
    /// Dispatch a parsed client operation.
    ///
    /// Returns `(HandleResult, expired_subs)`. Expired subs must be cleaned up
    /// by the worker after regaining `&mut self` access to the connections map.
    pub(crate) fn handle_op(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        op: ClientOp,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        match op {
            ClientOp::Ping => {
                conn.write_buf.extend_from_slice(b"PONG\r\n");
                (HandleResult::Flush, Vec::new())
            }
            ClientOp::Pong => {
                // Pings outstanding is reset by the worker after handler returns.
                // We signal Ok here — the worker handles the pong bookkeeping.
                (HandleResult::Ok, Vec::new())
            }
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                let result = Self::handle_sub(conn, wctx, sid, subject, queue_group);
                (result, Vec::new())
            }
            ClientOp::Unsubscribe { sid, max } => {
                let result = Self::handle_unsub(conn, wctx, sid, max);
                (result, Vec::new())
            }
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
                ..
            } => Self::handle_pub(conn, wctx, subject, payload, respond, headers),
            ClientOp::Connect(_) => {
                // Duplicate CONNECT in active phase, ignore.
                (HandleResult::Ok, Vec::new())
            }
        }
    }

    fn handle_sub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        sid: u64,
        subject: Bytes,
        queue_group: Option<Bytes>,
    ) -> HandleResult {
        // Silently reject SUB during drain
        if conn.draining {
            return HandleResult::Ok;
        }

        // Check subscribe permissions
        if let Some(ref perms) = conn.permissions {
            let subj = bytes_to_str(&subject);
            if !perms.subscribe.is_allowed(subj) {
                counter!("subscriptions_rejected_total", "reason" => "permissions").increment(1);
                conn.write_buf
                    .extend_from_slice(b"-ERR 'Permissions Violation for Subscription'\r\n");
                return HandleResult::Flush;
            }
        }

        let max_subs = wctx.state.max_subscriptions.load(Ordering::Relaxed);
        if max_subs > 0 && *conn.sub_count >= max_subs {
            warn!(
                conn_id = conn.conn_id,
                max_subscriptions = max_subs,
                "maximum subscriptions exceeded"
            );
            counter!("subscriptions_rejected_total").increment(1);
            conn.write_buf
                .extend_from_slice(b"-ERR 'Maximum Subscriptions Exceeded'\r\n");
            return HandleResult::Flush;
        }

        let subject_str = bytes_to_str(&subject);
        let queue_str = queue_group.as_ref().map(|q| bytes_to_str(q).to_string());

        // Clone for upstream + leaf propagation before moving into Subscription
        #[cfg(feature = "leaf")]
        let upstream_queue = queue_str.clone();
        #[cfg(feature = "hub")]
        let leaf_queue = queue_str.clone();

        let sub = Subscription {
            conn_id: conn.conn_id,
            sid,
            sid_bytes: nats_proto::sid_to_bytes(sid),
            subject: subject_str.to_string(),
            queue: queue_str,
            writer: conn.direct_writer.clone(),
            max_msgs: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            is_leaf: false,
            #[cfg(feature = "cluster")]
            is_route: false,
            #[cfg(feature = "gateway")]
            is_gateway: false,
            #[cfg(feature = "accounts")]
            account_id: conn.account_id,
            #[cfg(feature = "hub")]
            leaf_perms: None,
        };

        {
            let mut subs = wctx
                .state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    conn.account_id,
                )
                .write()
                .unwrap();
            subs.insert(sub);
            wctx.state.has_subs.store(true, Ordering::Relaxed);
        }

        #[cfg(feature = "leaf")]
        {
            let mut upstreams = wctx.state.upstreams.write().unwrap();
            for up in upstreams.iter_mut() {
                if let Err(e) = up.add_interest(subject_str.to_string(), upstream_queue.clone()) {
                    warn!(error = %e, "failed to add upstream interest");
                }
            }
        }

        *conn.sub_count += 1;

        // Propagate LS+ to inbound leaf connections.
        #[cfg(feature = "hub")]
        propagate_leaf_sub(
            wctx.state,
            subject_str.as_bytes(),
            leaf_queue.as_deref().map(|q| q.as_bytes()),
        );

        // Propagate RS+ to inbound route connections.
        #[cfg(feature = "cluster")]
        propagate_route_sub(
            wctx.state,
            subject_str.as_bytes(),
            queue_group.as_deref(),
            #[cfg(feature = "accounts")]
            wctx.state.account_name(conn.account_id).as_bytes(),
        );

        // Propagate RS+ to gateway connections.
        #[cfg(feature = "gateway")]
        propagate_gateway_sub(
            wctx.state,
            subject_str.as_bytes(),
            queue_group.as_deref(),
            #[cfg(feature = "accounts")]
            wctx.state.account_name(conn.account_id).as_bytes(),
        );

        // Reverse interest: if this subscription matches an import's local pattern,
        // propagate interest in the source account's namespace so remote peers
        // forward messages to us.
        #[cfg(feature = "accounts")]
        {
            if let Some(reverses) = wctx.state.reverse_imports.get(conn.account_id as usize) {
                for ri in reverses {
                    if crate::sub_list::subject_matches(&ri.local_pattern, subject_str) {
                        let src_acct_name = wctx.state.account_name(ri.src_account_id).as_bytes();
                        #[cfg(feature = "leaf")]
                        {
                            let mut upstreams = wctx.state.upstreams.write().unwrap();
                            for up in upstreams.iter_mut() {
                                let _ = up.add_interest(ri.src_pattern.clone(), None);
                            }
                        }
                        #[cfg(feature = "hub")]
                        propagate_leaf_sub(wctx.state, ri.src_pattern.as_bytes(), None);
                        #[cfg(feature = "cluster")]
                        propagate_route_sub(
                            wctx.state,
                            ri.src_pattern.as_bytes(),
                            None,
                            src_acct_name,
                        );
                        #[cfg(feature = "gateway")]
                        propagate_gateway_sub(
                            wctx.state,
                            ri.src_pattern.as_bytes(),
                            None,
                            src_acct_name,
                        );
                    }
                }
            }
        }

        gauge!(
            "subscriptions_active",
            "worker" => wctx.worker_label.to_string()
        )
        .increment(1.0);
        debug!(conn_id = conn.conn_id, sid, subject = %subject_str, "client subscribed");

        HandleResult::Ok
    }

    fn handle_unsub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        sid: u64,
        max: Option<u64>,
    ) -> HandleResult {
        if let Some(n) = max {
            // UNSUB with max: set delivery limit, auto-remove when reached.
            let subs = wctx
                .state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    conn.account_id,
                )
                .read()
                .unwrap();
            let found = subs.set_unsub_max(conn.conn_id, sid, n);
            let already_expired = found && subs.is_expired(conn.conn_id, sid);
            drop(subs);

            if already_expired {
                let mut subs = wctx
                    .state
                    .get_subs(
                        #[cfg(feature = "accounts")]
                        conn.account_id,
                    )
                    .write()
                    .unwrap();
                if let Some(removed) = subs.remove(conn.conn_id, sid) {
                    wctx.state
                        .has_subs
                        .store(!subs.is_empty(), Ordering::Relaxed);
                    *conn.sub_count = conn.sub_count.saturating_sub(1);
                    #[cfg(feature = "leaf")]
                    {
                        let mut upstreams = wctx.state.upstreams.write().unwrap();
                        for up in upstreams.iter_mut() {
                            up.remove_interest(&removed.subject, removed.queue.as_deref());
                        }
                    }
                    gauge!(
                        "subscriptions_active",
                        "worker" => wctx.worker_label.to_string()
                    )
                    .decrement(1.0);
                    #[cfg(feature = "hub")]
                    propagate_leaf_unsub(
                        wctx.state,
                        removed.subject.as_bytes(),
                        removed.queue.as_deref().map(|q| q.as_bytes()),
                    );
                    #[cfg(feature = "cluster")]
                    propagate_route_unsub(
                        wctx.state,
                        removed.subject.as_bytes(),
                        removed.queue.as_deref().map(|q| q.as_bytes()),
                        #[cfg(feature = "accounts")]
                        wctx.state.account_name(conn.account_id).as_bytes(),
                    );
                    #[cfg(feature = "gateway")]
                    propagate_gateway_unsub(
                        wctx.state,
                        removed.subject.as_bytes(),
                        removed.queue.as_deref().map(|q| q.as_bytes()),
                        #[cfg(feature = "accounts")]
                        wctx.state.account_name(conn.account_id).as_bytes(),
                    );
                    // Reverse interest unsub for cross-account imports.
                    #[cfg(feature = "accounts")]
                    propagate_reverse_unsub(wctx, conn.account_id, &removed.subject);
                    debug!(
                        conn_id = conn.conn_id,
                        sid,
                        subject = %removed.subject,
                        "auto-unsubscribed (max already reached)"
                    );
                }
            }
        } else {
            // Immediate unsubscribe
            let removed = {
                let mut subs = wctx
                    .state
                    .get_subs(
                        #[cfg(feature = "accounts")]
                        conn.account_id,
                    )
                    .write()
                    .unwrap();
                let r = subs.remove(conn.conn_id, sid);
                wctx.state
                    .has_subs
                    .store(!subs.is_empty(), Ordering::Relaxed);
                r
            };

            if let Some(ref removed) = removed {
                *conn.sub_count = conn.sub_count.saturating_sub(1);
                #[cfg(feature = "leaf")]
                {
                    let mut upstreams = wctx.state.upstreams.write().unwrap();
                    for up in upstreams.iter_mut() {
                        up.remove_interest(&removed.subject, removed.queue.as_deref());
                    }
                }
                gauge!(
                    "subscriptions_active",
                    "worker" => wctx.worker_label.to_string()
                )
                .decrement(1.0);
                #[cfg(feature = "hub")]
                propagate_leaf_unsub(
                    wctx.state,
                    removed.subject.as_bytes(),
                    removed.queue.as_deref().map(|q| q.as_bytes()),
                );
                #[cfg(feature = "cluster")]
                propagate_route_unsub(
                    wctx.state,
                    removed.subject.as_bytes(),
                    removed.queue.as_deref().map(|q| q.as_bytes()),
                    #[cfg(feature = "accounts")]
                    wctx.state.account_name(conn.account_id).as_bytes(),
                );
                #[cfg(feature = "gateway")]
                propagate_gateway_unsub(
                    wctx.state,
                    removed.subject.as_bytes(),
                    removed.queue.as_deref().map(|q| q.as_bytes()),
                    #[cfg(feature = "accounts")]
                    wctx.state.account_name(conn.account_id).as_bytes(),
                );
                // Reverse interest unsub for cross-account imports.
                #[cfg(feature = "accounts")]
                propagate_reverse_unsub(wctx, conn.account_id, &removed.subject);
                debug!(
                    conn_id = conn.conn_id,
                    sid,
                    subject = %removed.subject,
                    "client unsubscribed"
                );
            }
        }

        HandleResult::Ok
    }

    fn handle_pub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        subject: Bytes,
        payload: Bytes,
        respond: Option<Bytes>,
        headers: Option<crate::types::HeaderMap>,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        // Check publish permissions
        if let Some(ref perms) = conn.permissions {
            let subj = bytes_to_str(&subject);
            if !perms.publish.is_allowed(subj) {
                counter!("messages_rejected_total", "reason" => "permissions").increment(1);
                conn.write_buf
                    .extend_from_slice(b"-ERR 'Permissions Violation for Publish'\r\n");
                return (HandleResult::Disconnect, Vec::new());
            }
        }

        let max_payload = wctx.state.max_payload.load(Ordering::Relaxed);
        if max_payload > 0 && payload.len() > max_payload {
            warn!(
                conn_id = conn.conn_id,
                payload_len = payload.len(),
                max_payload,
                "maximum payload violation"
            );
            counter!("messages_rejected_total", "reason" => "max_payload").increment(1);
            conn.write_buf
                .extend_from_slice(b"-ERR 'Maximum Payload Violation'\r\n");
            return (HandleResult::Disconnect, Vec::new());
        }

        let payload_len = payload.len() as u64;
        *wctx.msgs_received += 1;
        *wctx.msgs_received_bytes += payload_len;

        let subject_str = bytes_to_str(&subject);

        // No-responders: if the client opted in (no_responders && headers) and
        // this PUB has a reply-to subject, check whether any subscriber exists.
        // If not, send a 503 status HMSG back on the reply subject.
        // Skip this check when connected to an upstream hub — let the hub handle it.
        if let Some(ref reply_bytes) = respond {
            if conn.no_responders {
                let has_upstream = {
                    #[cfg(feature = "leaf")]
                    {
                        !conn.upstream_txs.is_empty()
                    }
                    #[cfg(not(feature = "leaf"))]
                    {
                        false
                    }
                };
                if !has_upstream {
                    let subs = wctx
                        .state
                        .get_subs(
                            #[cfg(feature = "accounts")]
                            conn.account_id,
                        )
                        .read()
                        .unwrap();
                    let has_sub = subs.has_any_subscriber(subject_str);
                    drop(subs);

                    if !has_sub {
                        // Build 503 No Responders header and deliver to reply subject.
                        let mut hdr = crate::types::HeaderMap::new();
                        hdr.set_status(503, None);
                        let reply_str = bytes_to_str(reply_bytes);
                        deliver_to_subs(
                            wctx,
                            reply_bytes,
                            reply_str,
                            None,
                            Some(&hdr),
                            &Bytes::new(),
                            conn.conn_id,
                            false, // don't skip echo — deliver to the publishing client
                            #[cfg(feature = "cluster")]
                            false,
                            #[cfg(feature = "gateway")]
                            false,
                            #[cfg(feature = "accounts")]
                            conn.account_id,
                        );
                        return (HandleResult::Ok, Vec::new());
                    }
                }
            }
        }

        let (_delivered, expired) = deliver_to_subs(
            wctx,
            &subject,
            subject_str,
            respond.as_deref(),
            headers.as_ref(),
            &payload,
            conn.conn_id,
            !conn.echo,
            #[cfg(feature = "cluster")]
            false, // don't skip routes — client pubs forward to route peers
            #[cfg(feature = "gateway")]
            false, // don't skip gateways — client pubs forward to gateway peers
            #[cfg(feature = "accounts")]
            conn.account_id,
        );

        // Forward to optimistic gateways when no gateway sub matched.
        #[cfg(feature = "gateway")]
        crate::handler::forward_to_optimistic_gateways(
            wctx,
            &subject,
            subject_str,
            respond.as_deref(),
            headers.as_ref(),
            &payload,
            #[cfg(feature = "accounts")]
            wctx.state.account_name(conn.account_id).as_bytes(),
        );

        // Cross-account forwarding: deliver to destination accounts' SubLists.
        #[cfg(feature = "accounts")]
        let expired = {
            let mut expired = expired;
            let cross_expired = deliver_cross_account(
                wctx,
                &subject,
                subject_str,
                respond.as_deref(),
                headers.as_ref(),
                &payload,
                conn.account_id,
            );
            expired.extend(cross_expired);
            expired
        };

        #[cfg(feature = "leaf")]
        forward_to_upstream(
            conn.upstream_txs,
            wctx.state,
            subject,
            respond,
            headers,
            payload,
        );

        (HandleResult::Ok, expired)
    }
}

/// Propagate reverse interest removal for cross-account imports.
///
/// When a client unsubscribes from a subject that matches an import's local pattern,
/// remove the corresponding interest in the source account's namespace from
/// upstream/leaf/route/gateway peers.
#[cfg(feature = "accounts")]
fn propagate_reverse_unsub(
    wctx: &mut WorkerCtx<'_>,
    account_id: crate::server::AccountId,
    subject: &str,
) {
    if let Some(reverses) = wctx.state.reverse_imports.get(account_id as usize) {
        for ri in reverses {
            if crate::sub_list::subject_matches(&ri.local_pattern, subject) {
                #[allow(unused)]
                let src_acct_name = wctx.state.account_name(ri.src_account_id).as_bytes();
                #[cfg(feature = "leaf")]
                {
                    let mut upstreams = wctx.state.upstreams.write().unwrap();
                    for up in upstreams.iter_mut() {
                        up.remove_interest(&ri.src_pattern, None);
                    }
                }
                #[cfg(feature = "hub")]
                propagate_leaf_unsub(wctx.state, ri.src_pattern.as_bytes(), None);
                #[cfg(feature = "cluster")]
                propagate_route_unsub(wctx.state, ri.src_pattern.as_bytes(), None, src_acct_name);
                #[cfg(feature = "gateway")]
                propagate_gateway_unsub(wctx.state, ri.src_pattern.as_bytes(), None, src_acct_name);
            }
        }
    }
}
