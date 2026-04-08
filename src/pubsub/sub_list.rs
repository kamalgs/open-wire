use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;

pub(crate) use crate::msg_writer::{create_eventfd, DirectBuf, MsgWriter};

pub(crate) use crate::msg_writer::{BinSeg, BinSegBuf};

/// Backward-compatible alias for `MsgWriter`.
pub(crate) type DirectWriter = MsgWriter;

/// What kind of endpoint this subscription targets.
/// Determines the wire format used for delivery and scope-filtering behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubKind {
    /// Local NATS text-protocol client (MSG).
    Client,
    /// Inbound leaf node (LMSG).
    Leaf,
    /// Route peer in the same cluster (RMSG text or binary).
    Route,
    /// Gateway peer in a different cluster (RMSG).
    Gateway,
    /// Binary-protocol client (binary Msg frame via write_rmsg).
    BinaryClient,
}

impl SubKind {
    #[inline]
    pub fn is_route(self) -> bool {
        matches!(self, Self::Route)
    }
    #[inline]
    pub fn is_gateway(self) -> bool {
        matches!(self, Self::Gateway)
    }
    #[inline]
    pub fn is_leaf(self) -> bool {
        matches!(self, Self::Leaf)
    }
    #[inline]
    pub fn is_local(self) -> bool {
        matches!(self, Self::Client | Self::BinaryClient)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Subscription {
    pub conn_id: u64,
    pub sid: u64,
    /// Pre-computed ASCII bytes of the SID (e.g. `b"42"`), cloned (atomic inc)
    /// at delivery time instead of heap-allocating via `sid_to_bytes()` per message.
    pub sid_bytes: Bytes,
    pub subject: String,
    pub queue: Option<String>,
    /// Message writer to the client's shared write buffer.
    pub(crate) writer: MsgWriter,
    /// Maximum messages to deliver before auto-unsubscribe. 0 = no limit.
    pub(crate) max_msgs: AtomicU64,
    /// Number of messages delivered so far (only incremented when max_msgs > 0).
    pub(crate) delivered: AtomicU64,
    /// What kind of endpoint this subscription targets.
    pub kind: SubKind,
    /// Account this subscription belongs to. 0 = `$G` (global/default).
    #[cfg(feature = "accounts")]
    pub account_id: crate::core::server::AccountId,
    /// Per-leaf publish permissions (leaf subs only).
    pub leaf_perms: Option<std::sync::Arc<crate::core::server::Permissions>>,
}

impl Subscription {
    /// Create a subscription for the given kind.
    pub(crate) fn new(
        conn_id: u64,
        sid: u64,
        subject: String,
        queue: Option<String>,
        writer: MsgWriter,
        kind: SubKind,
        #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
    ) -> Self {
        Self {
            conn_id,
            sid,
            sid_bytes: crate::nats_proto::sid_to_bytes(sid),
            subject,
            queue,
            writer,
            max_msgs: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            kind,
            #[cfg(feature = "accounts")]
            account_id,
            leaf_perms: None,
        }
    }
}

impl Clone for Subscription {
    fn clone(&self) -> Self {
        Self {
            conn_id: self.conn_id,
            sid: self.sid,
            sid_bytes: self.sid_bytes.clone(),
            subject: self.subject.clone(),
            queue: self.queue.clone(),
            writer: self.writer.clone(),
            max_msgs: AtomicU64::new(self.max_msgs.load(Ordering::Relaxed)),
            delivered: AtomicU64::new(self.delivered.load(Ordering::Relaxed)),
            kind: self.kind,
            #[cfg(feature = "accounts")]
            account_id: self.account_id,
            leaf_perms: self.leaf_perms.clone(),
        }
    }
}

impl Subscription {
    /// Create a subscription without a real DirectWriter (for benchmarks/tests).
    pub fn new_dummy(conn_id: u64, sid: u64, subject: String, queue: Option<String>) -> Self {
        Self::new(
            conn_id,
            sid,
            subject,
            queue,
            DirectWriter::new_dummy(),
            SubKind::Client,
            #[cfg(feature = "accounts")]
            0,
        )
    }

    // Backward-compat helpers used in delivery.rs and sub_list.rs tests.
    #[inline]
    pub fn is_route(&self) -> bool {
        self.kind.is_route()
    }
    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.kind.is_leaf()
    }
    #[inline]
    pub fn is_gateway(&self) -> bool {
        self.kind.is_gateway()
    }
    #[inline]
    pub fn is_binary_client(&self) -> bool {
        matches!(self.kind, SubKind::BinaryClient)
    }
}

/// Returns true if the subject pattern contains wildcard characters (`*` or `>`).
#[inline]
fn is_wildcard(subject: &str) -> bool {
    memchr::memchr2(b'*', b'>', subject.as_bytes()).is_some()
}

/// A trie node for wildcard subject matching.
///
/// Each level corresponds to one token in a dotted subject (e.g. `foo.bar.baz`).
/// Literal tokens index into `children`, `*` wildcards use the `star` branch,
/// and `>` (full wildcard) subs are stored in `gt_subs` at the parent level.
#[derive(Debug, Default)]
struct TrieNode {
    /// Subs whose pattern terminates at this node (non-`>` terminal).
    subs: Vec<Subscription>,
    /// Children indexed by literal token.
    children: HashMap<String, TrieNode>,
    /// Child node for `*` wildcard token.
    star: Option<Box<TrieNode>>,
    /// Subs whose pattern ends with `>` at this level.
    gt_subs: Vec<Subscription>,
}

/// Trie-based wildcard subscription storage (real implementation).
///
/// Provides O(depth) matching instead of O(N) linear scan. Each wildcard
/// subscription is inserted into a trie keyed by its dotted subject tokens.
/// Matching walks only the branches that could match the publish subject.
#[derive(Debug, Default)]
struct WildTrieInner {
    root: TrieNode,
    len: usize,
    /// (conn_id, sid) → subject pattern. For O(1) remove lookup.
    sub_index: HashMap<(u64, u64), String>,
    /// conn_id → list of sids. For connection-level removal.
    conn_index: HashMap<u64, Vec<u64>>,
}

impl WildTrieInner {
    fn insert(&mut self, sub: Subscription) {
        let subject = sub.subject.clone();
        let conn_id = sub.conn_id;
        let sid = sub.sid;

        let tokens: Vec<&str> = subject.split('.').collect();
        let mut node = &mut self.root;

        // Check if the last token is `>` — if so, walk to parent and store in gt_subs.
        let (walk_tokens, is_gt) = if tokens.last() == Some(&">") {
            (&tokens[..tokens.len() - 1], true)
        } else {
            (&tokens[..], false)
        };

        for token in walk_tokens {
            if *token == "*" {
                node = node
                    .star
                    .get_or_insert_with(|| Box::new(TrieNode::default()));
            } else {
                node = node.children.entry(token.to_string()).or_default();
            }
        }

        if is_gt {
            node.gt_subs.push(sub);
        } else {
            node.subs.push(sub);
        }

        self.len += 1;
        self.sub_index.insert((conn_id, sid), subject);
        self.conn_index.entry(conn_id).or_default().push(sid);
    }

    fn remove(&mut self, conn_id: u64, sid: u64) -> Option<Subscription> {
        let subject = self.sub_index.remove(&(conn_id, sid))?;

        let mut node = &mut self.root;

        for token in subject.split('.') {
            if token == ">" {
                if let Some(pos) = node
                    .gt_subs
                    .iter()
                    .position(|s| s.conn_id == conn_id && s.sid == sid)
                {
                    let removed = node.gt_subs.swap_remove(pos);
                    self.len -= 1;
                    if let Some(sids) = self.conn_index.get_mut(&conn_id) {
                        sids.retain(|&s| s != sid);
                        if sids.is_empty() {
                            self.conn_index.remove(&conn_id);
                        }
                    }
                    return Some(removed);
                }
                return None;
            }
            if token == "*" {
                match node.star {
                    Some(ref mut star) => node = star,
                    None => return None,
                }
            } else {
                match node.children.get_mut(token) {
                    Some(child) => node = child,
                    None => return None,
                }
            }
        }

        // Terminal node (non->)
        if let Some(pos) = node
            .subs
            .iter()
            .position(|s| s.conn_id == conn_id && s.sid == sid)
        {
            let removed = node.subs.swap_remove(pos);
            self.len -= 1;
            if let Some(sids) = self.conn_index.get_mut(&conn_id) {
                sids.retain(|&s| s != sid);
                if sids.is_empty() {
                    self.conn_index.remove(&conn_id);
                }
            }
            Some(removed)
        } else {
            None
        }
    }

    fn remove_conn(&mut self, conn_id: u64) -> Vec<Subscription> {
        let sids = match self.conn_index.remove(&conn_id) {
            Some(sids) => sids,
            None => return Vec::new(),
        };

        let mut removed = Vec::with_capacity(sids.len());
        for sid in sids {
            if let Some(subject) = self.sub_index.remove(&(conn_id, sid)) {
                let mut node = &mut self.root;
                let mut found = false;

                for token in subject.split('.') {
                    if token == ">" {
                        if let Some(pos) = node
                            .gt_subs
                            .iter()
                            .position(|s| s.conn_id == conn_id && s.sid == sid)
                        {
                            removed.push(node.gt_subs.swap_remove(pos));
                            self.len -= 1;
                            found = true;
                        }
                        break;
                    }
                    if token == "*" {
                        match node.star {
                            Some(ref mut star) => node = star,
                            None => break,
                        }
                    } else {
                        match node.children.get_mut(token) {
                            Some(child) => node = child,
                            None => break,
                        }
                    }
                }

                if !found {
                    if let Some(pos) = node
                        .subs
                        .iter()
                        .position(|s| s.conn_id == conn_id && s.sid == sid)
                    {
                        removed.push(node.subs.swap_remove(pos));
                        self.len -= 1;
                    }
                }
            }
        }

        removed
    }

    /// Iterate over all wildcard subscriptions matching a publish subject.
    fn for_each_match<'a>(&'a self, subject: &str, mut f: impl FnMut(&'a Subscription)) {
        let tokens: Vec<&str> = subject.split('.').collect();
        let depth = tokens.len();
        let mut stack: Vec<(&TrieNode, usize)> = Vec::with_capacity(depth * 2);
        stack.push((&self.root, 0));

        while let Some((node, idx)) = stack.pop() {
            // `>` matches one or more remaining tokens
            if idx < depth {
                for sub in &node.gt_subs {
                    f(sub);
                }
            }

            if idx == depth {
                // Terminal match
                for sub in &node.subs {
                    f(sub);
                }
            } else {
                let token = tokens[idx];
                // Literal child match
                if let Some(child) = node.children.get(token) {
                    stack.push((child, idx + 1));
                }
                // Star wildcard match (any single token)
                if let Some(ref star) = node.star {
                    stack.push((star, idx + 1));
                }
            }
        }
    }

    /// DFS traversal visiting all subscriptions in the trie.
    fn for_each_sub<'a>(&'a self, mut f: impl FnMut(&'a Subscription)) {
        let mut stack: Vec<&TrieNode> = vec![&self.root];
        while let Some(node) = stack.pop() {
            for sub in &node.subs {
                f(sub);
            }
            for sub in &node.gt_subs {
                f(sub);
            }
            for child in node.children.values() {
                stack.push(child);
            }
            if let Some(ref star) = node.star {
                stack.push(star);
            }
        }
    }

    /// Find a subscription by conn_id and sid.
    fn find(&self, conn_id: u64, sid: u64) -> Option<&Subscription> {
        let subject = self.sub_index.get(&(conn_id, sid))?;

        let mut node = &self.root;

        for token in subject.split('.') {
            if token == ">" {
                return node
                    .gt_subs
                    .iter()
                    .find(|s| s.conn_id == conn_id && s.sid == sid);
            }
            if token == "*" {
                match node.star {
                    Some(ref star) => node = star,
                    None => return None,
                }
            } else {
                match node.children.get(token) {
                    Some(child) => node = child,
                    None => return None,
                }
            }
        }

        node.subs
            .iter()
            .find(|s| s.conn_id == conn_id && s.sid == sid)
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Null-object wrapper around the wildcard trie.
///
/// Starts as `Empty` — all query methods are zero-cost no-ops with no heap
/// allocation. Transitions to `Active` on the first wildcard `insert`, and
/// back to `Empty` when the last wildcard subscription is removed, freeing
/// the trie memory.
#[derive(Debug, Default)]
enum WildTrie {
    #[default]
    Empty,
    Active(Box<WildTrieInner>),
}

impl WildTrie {
    fn insert(&mut self, sub: Subscription) {
        if matches!(self, WildTrie::Empty) {
            *self = WildTrie::Active(Box::default());
        }
        if let WildTrie::Active(inner) = self {
            inner.insert(sub);
        }
    }

    fn remove(&mut self, conn_id: u64, sid: u64) -> Option<Subscription> {
        let WildTrie::Active(inner) = self else {
            return None;
        };
        let result = inner.remove(conn_id, sid);
        if inner.len == 0 {
            *self = WildTrie::Empty;
        }
        result
    }

    fn remove_conn(&mut self, conn_id: u64) -> Vec<Subscription> {
        let WildTrie::Active(inner) = self else {
            return Vec::new();
        };
        let result = inner.remove_conn(conn_id);
        if inner.len == 0 {
            *self = WildTrie::Empty;
        }
        result
    }

    fn for_each_match<'a>(&'a self, subject: &str, f: impl FnMut(&'a Subscription)) {
        if let WildTrie::Active(inner) = self {
            inner.for_each_match(subject, f);
        }
    }

    fn for_each_sub<'a>(&'a self, f: impl FnMut(&'a Subscription)) {
        if let WildTrie::Active(inner) = self {
            inner.for_each_sub(f);
        }
    }

    fn find(&self, conn_id: u64, sid: u64) -> Option<&Subscription> {
        match self {
            WildTrie::Active(inner) => inner.find(conn_id, sid),
            WildTrie::Empty => None,
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self, WildTrie::Empty)
    }
}

/// Manages subscription lifecycle (insert/remove) and subject-based lookup.
///
/// Splits subscriptions into exact (HashMap) and wildcard (trie) for fast
/// lookups: exact subjects get O(1) HashMap lookup, wildcard patterns use
/// O(depth) trie traversal.
#[derive(Debug, Default)]
pub struct SubscriptionManager {
    /// Exact (non-wildcard) subscriptions indexed by subject.
    exact: HashMap<String, Vec<Subscription>>,
    /// Wildcard subscriptions (patterns with `*` or `>`) stored in a trie.
    wild: WildTrie,
    /// Round-robin counter for queue group delivery.
    queue_counter: AtomicUsize,
}

/// Backward-compatible alias for `SubscriptionManager`.
pub type SubList = SubscriptionManager;

impl SubscriptionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, sub: Subscription) {
        if is_wildcard(&sub.subject) {
            self.wild.insert(sub);
        } else {
            self.exact.entry(sub.subject.clone()).or_default().push(sub);
        }
    }

    pub fn remove(&mut self, conn_id: u64, sid: u64) -> Option<Subscription> {
        if let Some(removed) = self.wild.remove(conn_id, sid) {
            return Some(removed);
        }
        for subs in self.exact.values_mut() {
            if let Some(pos) = subs
                .iter()
                .position(|s| s.conn_id == conn_id && s.sid == sid)
            {
                let removed = subs.swap_remove(pos);
                return Some(removed);
            }
        }
        None
    }

    pub fn remove_conn(&mut self, conn_id: u64) -> Vec<Subscription> {
        let mut removed = self.wild.remove_conn(conn_id);

        self.exact.retain(|_, subs| {
            let mut i = 0;
            while i < subs.len() {
                if subs[i].conn_id == conn_id {
                    removed.push(subs.swap_remove(i));
                } else {
                    i += 1;
                }
            }
            !subs.is_empty()
        });

        removed
    }

    pub fn match_subject(&self, subject: &str) -> Vec<&Subscription> {
        let mut result = Vec::new();
        if let Some(subs) = self.exact.get(subject) {
            result.extend(subs.iter());
        }
        self.wild.for_each_match(subject, |sub| {
            result.push(sub);
        });
        result
    }

    /// Set the auto-unsubscribe max on an existing subscription.
    /// Works under a read lock (uses atomics). Returns `true` if the sub was found.
    /// If the sub has already delivered >= max messages, returns `true` but caller
    /// should remove it under a write lock.
    pub fn set_unsub_max(&self, conn_id: u64, sid: u64, max: u64) -> bool {
        for subs in self.exact.values() {
            for sub in subs {
                if sub.conn_id == conn_id && sub.sid == sid {
                    sub.max_msgs.store(max, Ordering::Relaxed);
                    return true;
                }
            }
        }
        if let Some(sub) = self.wild.find(conn_id, sid) {
            sub.max_msgs.store(max, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Check if a subscription has already reached its delivery limit.
    pub fn is_expired(&self, conn_id: u64, sid: u64) -> bool {
        let find = |sub: &Subscription| -> bool {
            let max = sub.max_msgs.load(Ordering::Relaxed);
            max > 0 && sub.delivered.load(Ordering::Relaxed) >= max
        };
        for subs in self.exact.values() {
            for sub in subs {
                if sub.conn_id == conn_id && sub.sid == sid {
                    return find(sub);
                }
            }
        }
        if let Some(sub) = self.wild.find(conn_id, sid) {
            return find(sub);
        }
        false
    }

    /// Iterate over matching subscriptions without allocating a Vec.
    /// Returns `(match_count, expired_subs)` where expired_subs is a list of
    /// `(conn_id, sid)` pairs for subscriptions that reached their max delivery limit.
    ///
    /// Queue group semantics: non-queue subs receive every message,
    /// while each queue group delivers to exactly one member (round-robin).
    ///
    /// `pre_filter`: called before a subscription is considered for delivery or
    /// added to a queue group. Returning `false` excludes it entirely — including
    /// from the queue-group round-robin pool, so it never "steals" a slot.
    pub fn for_each_match(
        &self,
        subject: &str,
        pre_filter: impl Fn(&Subscription) -> bool,
        mut f: impl FnMut(&Subscription),
    ) -> (usize, Vec<(u64, u64)>) {
        let mut count = 0;
        let mut expired: Vec<(u64, u64)> = Vec::new();
        let mut queue_groups: Vec<(&str, Vec<&Subscription>)> = Vec::new();

        // Check max_msgs limit, deliver if allowed, track expired.
        macro_rules! deliver_sub {
            ($sub:expr) => {{
                let max = $sub.max_msgs.load(Ordering::Relaxed);
                if max > 0 {
                    let prev = $sub.delivered.fetch_add(1, Ordering::Relaxed);
                    if prev >= max {
                        // Already over limit — undo increment, skip delivery
                        $sub.delivered.fetch_sub(1, Ordering::Relaxed);
                    } else {
                        if prev + 1 >= max {
                            expired.push(($sub.conn_id, $sub.sid));
                        }
                        f($sub);
                        count += 1;
                    }
                } else {
                    f($sub);
                    count += 1;
                }
            }};
        }

        macro_rules! route_sub {
            ($sub:expr) => {
                if !pre_filter($sub) {
                    // excluded — do not add to queue group or deliver
                } else if let Some(ref q) = $sub.queue {
                    if let Some(group) = queue_groups
                        .iter_mut()
                        .find(|(name, _)| *name == q.as_str())
                    {
                        group.1.push($sub);
                    } else {
                        queue_groups.push((q.as_str(), vec![$sub]));
                    }
                } else {
                    deliver_sub!($sub);
                }
            };
        }

        if let Some(subs) = self.exact.get(subject) {
            for sub in subs.iter() {
                route_sub!(sub);
            }
        }
        self.wild.for_each_match(subject, |sub| {
            route_sub!(sub);
        });

        // Deliver to exactly one member per queue group (round-robin)
        if !queue_groups.is_empty() {
            let rr = self.queue_counter.fetch_add(1, Ordering::Relaxed);
            for (_name, members) in &queue_groups {
                let idx = rr % members.len();
                let sub = members[idx];
                deliver_sub!(sub);
            }
        }

        (count, expired)
    }

    pub fn has_any_subscriber(&self, subject: &str) -> bool {
        if let Some(subs) = self.exact.get(subject) {
            if !subs.is_empty() {
                return true;
            }
        }
        // O(depth) trie traversal for wildcard matches, early exit on first hit
        let mut found = false;
        self.wild.for_each_match(subject, |_sub| {
            if found {
                return;
            }
            found = true;
        });
        found
    }

    /// Returns true if the sublist has no subscriptions at all.
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.wild.is_empty()
    }

    pub fn unique_subjects(&self) -> HashSet<&str> {
        let mut subjects: HashSet<&str> = self.exact.keys().map(|s| s.as_str()).collect();
        self.wild.for_each_sub(|sub| {
            subjects.insert(&sub.subject);
        });
        subjects
    }

    /// Returns true if any local (non-route, non-gateway) subscription matches the subject.
    /// Cheap boolean check — no delivery, no queue routing.
    pub fn has_local_interest(&self, subject: &str) -> bool {
        if let Some(subs) = self.exact.get(subject) {
            for sub in subs {
                if sub.is_route() {
                    continue;
                }
                if sub.is_gateway() {
                    continue;
                }
                return true;
            }
        }
        // Check wildcard subs via trie
        let mut found = false;
        self.wild.for_each_match(subject, |sub| {
            if found {
                return;
            }

            if sub.is_route() {
                return;
            }
            if sub.is_gateway() {
                return;
            }
            found = true;
        });
        found
    }

    /// Returns unique non-leaf, non-route, non-gateway (subject, queue) pairs for leaf interest
    /// propagation. Only includes subscriptions from client connections.
    pub fn client_interests(&self) -> Vec<(&str, Option<&str>)> {
        let mut set: HashSet<(&str, Option<&str>)> = HashSet::new();
        for (subj, subs) in &self.exact {
            for sub in subs {
                if !sub.is_leaf() {
                    if sub.is_route() {
                        continue;
                    }

                    if sub.is_gateway() {
                        continue;
                    }
                    set.insert((subj.as_str(), sub.queue.as_deref()));
                }
            }
        }
        self.wild.for_each_sub(|sub| {
            if !sub.is_leaf() {
                if sub.is_route() {
                    return;
                }

                if sub.is_gateway() {
                    return;
                }
                set.insert((sub.subject.as_str(), sub.queue.as_deref()));
            }
        });
        set.into_iter().collect()
    }

    /// Returns unique local (client + leaf) (subject, queue) pairs for route/gateway interest
    /// propagation. Excludes route and gateway subscriptions (avoids loops).
    pub fn local_interests(&self) -> Vec<(&str, Option<&str>)> {
        let mut set: HashSet<(&str, Option<&str>)> = HashSet::new();
        for (subj, subs) in &self.exact {
            for sub in subs {
                if sub.is_route() {
                    continue;
                }

                if sub.is_gateway() {
                    continue;
                }
                set.insert((subj.as_str(), sub.queue.as_deref()));
            }
        }
        self.wild.for_each_sub(|sub| {
            if sub.is_route() {
                return;
            }

            if sub.is_gateway() {
                return;
            }
            set.insert((sub.subject.as_str(), sub.queue.as_deref()));
        });
        set.into_iter().collect()
    }

    /// Returns unique (subject, queue) pairs for upstream interest sync.
    /// Non-queue subs have `queue = None`. Queue subs are deduplicated
    /// by (subject, queue_name) so each group is announced once.
    pub fn unique_interests(&self) -> Vec<(&str, Option<&str>)> {
        let mut set: HashSet<(&str, Option<&str>)> = HashSet::new();
        for (subj, subs) in &self.exact {
            for sub in subs {
                set.insert((subj.as_str(), sub.queue.as_deref()));
            }
        }
        self.wild.for_each_sub(|sub| {
            set.insert((sub.subject.as_str(), sub.queue.as_deref()));
        });
        set.into_iter().collect()
    }
}

/// NATS wildcard matching.
/// `*` matches a single token, `>` matches one or more tokens (tail match).
///
/// Uses byte-level dot scanning instead of `str::split('.')` to avoid
/// the `SplitInternal` iterator machinery (which was ~6% of CPU in profiles).
pub fn subject_matches(pattern: &str, subject: &str) -> bool {
    subject_matches_bytes(pattern.as_bytes(), subject.as_bytes())
}

#[inline]
fn subject_matches_bytes(pattern: &[u8], subject: &[u8]) -> bool {
    let mut pp = 0; // pattern position
    let mut sp = 0; // subject position

    loop {
        if pp >= pattern.len() {
            return sp >= subject.len();
        }
        if sp >= subject.len() {
            return false;
        }

        // Find end of current pattern token
        let pe = match memchr::memchr(b'.', &pattern[pp..]) {
            Some(i) => pp + i,
            None => pattern.len(),
        };

        // Check for ">"
        if pe - pp == 1 && pattern[pp] == b'>' {
            return true;
        }

        // Find end of current subject token
        let se = match memchr::memchr(b'.', &subject[sp..]) {
            Some(i) => sp + i,
            None => subject.len(),
        };

        // Check match: "*" matches any single token, otherwise exact
        let is_star = pe - pp == 1 && pattern[pp] == b'*';
        if !is_star && pattern[pp..pe] != subject[sp..se] {
            return false;
        }

        // Advance past token + dot
        pp = if pe < pattern.len() { pe + 1 } else { pe };
        sp = if se < subject.len() { se + 1 } else { se };
    }
}

/// Remap a concrete subject using a from/to pattern pair.
///
/// Tokenizes both patterns by `.`, extracts wildcard captures (`*` or `>`)
/// from the `from_pattern` matched against the subject, and substitutes
/// them positionally into the `to_pattern`.
///
/// Example: from=`events.>`, to=`team_a.events.>`, subject=`events.order.created`
/// → result: `team_a.events.order.created`
#[cfg(feature = "accounts")]
pub fn remap_subject(from_pattern: &str, to_pattern: &str, subject: &str) -> String {
    let from_tokens: Vec<&str> = from_pattern.split('.').collect();
    let subject_tokens: Vec<&str> = subject.split('.').collect();
    let to_tokens: Vec<&str> = to_pattern.split('.').collect();

    // Extract captures from from_pattern vs subject
    let mut captures: Vec<String> = Vec::new();
    let mut si = 0;
    for ft in &from_tokens {
        if si >= subject_tokens.len() {
            break;
        }
        if *ft == ">" {
            // Capture remaining tokens
            captures.push(subject_tokens[si..].join("."));
            break;
        } else if *ft == "*" {
            captures.push(subject_tokens[si].to_string());
            si += 1;
        } else {
            // Literal token — no capture
            si += 1;
        }
    }

    // Build result by substituting captures into to_pattern
    let mut result_parts: Vec<String> = Vec::new();
    let mut ci = 0;
    for tt in &to_tokens {
        if *tt == ">" {
            if ci < captures.len() {
                result_parts.push(captures[ci].clone());
            }
            break;
        } else if *tt == "*" {
            if ci < captures.len() {
                result_parts.push(captures[ci].clone());
                ci += 1;
            } else {
                result_parts.push("*".to_string());
            }
        } else {
            result_parts.push(tt.to_string());
        }
    }

    result_parts.join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test subscription with a dummy DirectWriter.
    fn test_sub(conn_id: u64, sid: u64, subject: &str) -> Subscription {
        Subscription::new_dummy(conn_id, sid, subject.to_string(), None)
    }

    #[test]
    fn test_exact_match() {
        assert!(subject_matches("foo.bar.baz", "foo.bar.baz"));
        assert!(!subject_matches("foo.bar.baz", "foo.bar.qux"));
        assert!(!subject_matches("foo.bar", "foo.bar.baz"));
        assert!(!subject_matches("foo.bar.baz", "foo.bar"));
    }

    #[test]
    fn test_star_wildcard() {
        assert!(subject_matches("foo.*", "foo.bar"));
        assert!(subject_matches("foo.*.baz", "foo.bar.baz"));
        assert!(!subject_matches("foo.*", "foo.bar.baz"));
        assert!(subject_matches("*.*.*", "a.b.c"));
        assert!(!subject_matches("*.*", "a.b.c"));
    }

    #[test]
    fn test_gt_wildcard() {
        assert!(subject_matches("foo.>", "foo.bar"));
        assert!(subject_matches("foo.>", "foo.bar.baz"));
        assert!(subject_matches("foo.>", "foo.bar.baz.qux"));
        assert!(!subject_matches("foo.>", "bar.baz"));
        assert!(subject_matches(">", "anything.at.all"));
        assert!(subject_matches(">", "single"));
    }

    #[test]
    fn test_combined_wildcards() {
        assert!(subject_matches("foo.*.>", "foo.bar.baz"));
        assert!(subject_matches("foo.*.>", "foo.bar.baz.qux"));
        assert!(!subject_matches("foo.*.>", "foo.bar"));
        // `>` requires at least one token
    }

    #[test]
    fn test_sublist_insert_match() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.bar"));
        sl.insert(test_sub(1, 2, "foo.*"));
        sl.insert(test_sub(2, 1, "baz.>"));

        let matches = sl.match_subject("foo.bar");
        assert_eq!(matches.len(), 2);

        let matches = sl.match_subject("foo.qux");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].subject, "foo.*");

        let matches = sl.match_subject("baz.one.two");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].subject, "baz.>");
    }

    #[test]
    fn test_sublist_remove() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_sub(1, 2, "bar"));

        let removed = sl.remove(1, 1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().subject, "foo");

        let matches = sl.match_subject("foo");
        assert!(matches.is_empty());
    }

    #[test]
    fn test_sublist_remove_conn() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_sub(1, 2, "bar"));
        sl.insert(test_sub(2, 1, "foo"));

        let removed = sl.remove_conn(1);
        assert_eq!(removed.len(), 2);

        let matches = sl.match_subject("foo");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].conn_id, 2);
    }

    #[test]
    fn test_unique_subjects() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_sub(2, 1, "foo"));
        sl.insert(test_sub(1, 2, "bar"));

        let subjects = sl.unique_subjects();
        assert_eq!(subjects.len(), 2);
        assert!(subjects.contains("foo"));
        assert!(subjects.contains("bar"));
    }

    #[test]
    fn test_exact_vs_wildcard_split() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.bar"));
        sl.insert(test_sub(1, 2, "foo.*"));
        sl.insert(test_sub(1, 3, "baz.>"));
        sl.insert(test_sub(2, 1, "foo.bar"));

        // "foo.bar" should match 2 exact + 1 wildcard
        let matches = sl.match_subject("foo.bar");
        assert_eq!(matches.len(), 3);

        // "foo.qux" should match only the wildcard "foo.*"
        let matches = sl.match_subject("foo.qux");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].subject, "foo.*");

        // is_empty
        assert!(!sl.is_empty());
        let empty = SubList::new();
        assert!(empty.is_empty());
    }

    // DirectWriter behavioral tests live in direct_writer.rs.

    fn test_queue_sub(conn_id: u64, sid: u64, subject: &str, queue: &str) -> Subscription {
        Subscription::new_dummy(conn_id, sid, subject.to_string(), Some(queue.to_string()))
    }

    #[test]
    fn test_queue_group_delivers_to_one() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));
        sl.insert(test_queue_sub(3, 1, "foo", "q1"));

        let mut delivered = Vec::new();
        let (count, _expired) = sl.for_each_match(
            "foo",
            |_| true,
            |sub| {
                delivered.push(sub.conn_id);
            },
        );
        assert_eq!(count, 1);
        assert_eq!(delivered.len(), 1);
    }

    #[test]
    fn test_queue_group_round_robin() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));

        let mut counts = [0u32; 3]; // conn_id 1 and 2
        for _ in 0..100 {
            let _ = sl.for_each_match(
                "foo",
                |_| true,
                |sub| {
                    counts[sub.conn_id as usize] += 1;
                },
            );
        }
        // Both should get roughly half
        assert!(counts[1] > 0);
        assert!(counts[2] > 0);
        assert_eq!(counts[1] + counts[2], 100);
    }

    #[test]
    fn test_queue_and_non_queue_mix() {
        let mut sl = SubList::new();
        // 2 queue subs in group "q1"
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));
        // 1 non-queue sub
        sl.insert(test_sub(3, 1, "foo"));

        let mut delivered = Vec::new();
        let (count, _) = sl.for_each_match(
            "foo",
            |_| true,
            |sub| {
                delivered.push(sub.conn_id);
            },
        );
        // Non-queue sub always delivered + 1 from queue group
        assert_eq!(count, 2);
        assert!(delivered.contains(&3)); // non-queue always present
    }

    #[test]
    fn test_multiple_queue_groups() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));
        sl.insert(test_queue_sub(3, 1, "foo", "q2"));
        sl.insert(test_queue_sub(4, 1, "foo", "q2"));

        let mut delivered = Vec::new();
        let (count, _) = sl.for_each_match(
            "foo",
            |_| true,
            |sub| {
                delivered.push(sub.conn_id);
            },
        );
        // 1 from q1 + 1 from q2
        assert_eq!(count, 2);
        assert_eq!(delivered.len(), 2);
    }

    #[test]
    fn test_queue_group_with_wildcard() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo.*", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo.*", "q1"));

        let mut delivered = Vec::new();
        let (count, _) = sl.for_each_match(
            "foo.bar",
            |_| true,
            |sub| {
                delivered.push(sub.conn_id);
            },
        );
        assert_eq!(count, 1);
    }

    #[test]
    fn test_unique_interests() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_sub(2, 2, "foo"));
        sl.insert(test_queue_sub(3, 1, "bar", "q1"));
        sl.insert(test_queue_sub(4, 2, "bar", "q1"));
        sl.insert(test_queue_sub(5, 3, "bar", "q2"));

        let interests = sl.unique_interests();
        // foo (no queue) + bar/q1 + bar/q2 = 3
        assert_eq!(interests.len(), 3);
        assert!(interests.contains(&("foo", None)));
        assert!(interests.contains(&("bar", Some("q1"))));
        assert!(interests.contains(&("bar", Some("q2"))));
    }

    #[test]
    fn test_unsub_max_delivery_limit() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.set_unsub_max(1, 1, 3);

        // Deliver 3 messages — all should succeed
        for _ in 0..3 {
            let (count, _) = sl.for_each_match("foo", |_| true, |_sub| {});
            assert_eq!(count, 1);
        }

        // 4th message should be skipped and sub expired
        let (count, expired) = sl.for_each_match("foo", |_| true, |_sub| {});
        assert_eq!(count, 0);
        assert!(expired.is_empty()); // already expired on 3rd delivery
    }

    #[test]
    fn test_unsub_max_single_shot() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));
        sl.set_unsub_max(1, 1, 1);

        let (count, expired) = sl.for_each_match("foo", |_| true, |_sub| {});
        assert_eq!(count, 1);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], (1, 1));

        // Next delivery should skip
        let (count, _) = sl.for_each_match("foo", |_| true, |_sub| {});
        assert_eq!(count, 0);
    }

    #[test]
    fn test_unsub_max_with_queue_group() {
        let mut sl = SubList::new();
        sl.insert(test_queue_sub(1, 1, "foo", "q1"));
        sl.insert(test_queue_sub(2, 1, "foo", "q1"));
        sl.set_unsub_max(1, 1, 2);

        // Deliver enough messages that conn_id=1 gets 2
        let mut conn1_count = 0u32;
        for _ in 0..100 {
            let (_, expired) = sl.for_each_match(
                "foo",
                |_| true,
                |sub| {
                    if sub.conn_id == 1 {
                        conn1_count += 1;
                    }
                },
            );
            // Remove expired
            for (cid, sid) in expired {
                sl.remove(cid, sid);
            }
        }
        // conn_id=1 should have received exactly 2
        assert_eq!(conn1_count, 2);
    }

    #[test]
    fn test_unsub_max_already_expired() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));

        // Set max=3, then deliver 3 messages to reach the limit
        sl.set_unsub_max(1, 1, 3);
        for _ in 0..3 {
            let _ = sl.for_each_match("foo", |_| true, |_sub| {});
        }

        // Should now be expired
        assert!(sl.is_expired(1, 1));

        // Increasing the max should un-expire it
        sl.set_unsub_max(1, 1, 5);
        assert!(!sl.is_expired(1, 1));
    }

    #[test]

    fn test_client_interests_excludes_leaf_subs() {
        let mut sl = SubList::new();
        // Client subs
        sl.insert(test_sub(1, 1, "foo"));
        sl.insert(test_queue_sub(2, 1, "bar", "q1"));

        // Leaf sub — should be excluded from client_interests
        let mut leaf_sub = Subscription::new_dummy(10, 1, "foo".to_string(), None);
        leaf_sub.kind = SubKind::Leaf;
        sl.insert(leaf_sub);

        let mut leaf_queue_sub =
            Subscription::new_dummy(11, 1, "bar".to_string(), Some("q1".to_string()));
        leaf_queue_sub.kind = SubKind::Leaf;
        sl.insert(leaf_queue_sub);

        let interests = sl.client_interests();
        // Should only have client interests: ("foo", None) and ("bar", Some("q1"))
        assert_eq!(interests.len(), 2);
        assert!(interests.contains(&("foo", None)));
        assert!(interests.contains(&("bar", Some("q1"))));

        // Verify leaf subs are still in the list for matching
        let matches = sl.match_subject("foo");
        assert_eq!(matches.len(), 2); // client + leaf
    }

    #[test]
    fn test_has_any_subscriber_exact() {
        let mut sl = SubList::new();
        assert!(!sl.has_any_subscriber("foo"));

        sl.insert(test_sub(1, 1, "foo"));
        assert!(sl.has_any_subscriber("foo"));
        assert!(!sl.has_any_subscriber("bar"));
    }

    #[test]
    fn test_has_any_subscriber_wildcard() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.*"));
        sl.insert(test_sub(1, 2, "bar.>"));

        assert!(sl.has_any_subscriber("foo.baz"));
        assert!(sl.has_any_subscriber("bar.a.b"));
        assert!(!sl.has_any_subscriber("qux"));
    }

    #[test]
    fn test_has_any_subscriber_empty() {
        let sl = SubList::new();
        assert!(!sl.has_any_subscriber("anything"));
    }

    #[test]

    fn test_has_local_interest_exact() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo"));

        assert!(sl.has_local_interest("foo"));
        assert!(!sl.has_local_interest("bar"));
    }

    #[test]

    fn test_has_local_interest_wildcard() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.*"));
        sl.insert(test_sub(1, 2, "bar.>"));

        assert!(sl.has_local_interest("foo.baz"));
        assert!(sl.has_local_interest("bar.a.b"));
        assert!(!sl.has_local_interest("qux"));
    }

    #[test]

    fn test_has_local_interest_excludes_gateway_subs() {
        let mut sl = SubList::new();

        // Gateway sub only — should NOT count as local interest
        let mut gw_sub = Subscription::new_dummy(10, 1, "foo".to_string(), None);
        gw_sub.kind = SubKind::Gateway;
        sl.insert(gw_sub);

        assert!(!sl.has_local_interest("foo"));

        // Add a client sub — now should have local interest
        sl.insert(test_sub(1, 1, "foo"));
        assert!(sl.has_local_interest("foo"));
    }

    #[test]
    fn test_set_unsub_max_wildcard() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.*"));
        assert!(sl.set_unsub_max(1, 1, 2));

        let (count, _) = sl.for_each_match("foo.bar", |_| true, |_sub| {});
        assert_eq!(count, 1);
        let (count, expired) = sl.for_each_match("foo.baz", |_| true, |_sub| {});
        assert_eq!(count, 1);
        assert_eq!(expired.len(), 1);
    }

    #[test]
    fn test_wildcard_star_matches_one_level() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.*"));

        let matches = sl.match_subject("foo.bar");
        assert_eq!(matches.len(), 1);

        let matches = sl.match_subject("foo.bar.baz");
        assert!(matches.is_empty(), "* should not match multiple levels");
    }

    #[test]
    fn test_wildcard_gt_matches_all_levels() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.>"));

        let one = sl.match_subject("foo.bar");
        assert_eq!(one.len(), 1);

        let two = sl.match_subject("foo.bar.baz");
        assert_eq!(two.len(), 1);

        let none = sl.match_subject("foo");
        assert!(
            none.is_empty(),
            "> requires at least one token after prefix"
        );
    }

    #[test]
    fn test_wildcard_no_false_positives() {
        let mut sl = SubList::new();
        sl.insert(test_sub(1, 1, "foo.*"));

        let matches = sl.match_subject("bar.baz");
        assert!(matches.is_empty(), "foo.* should not match bar.baz");

        let matches = sl.match_subject("foo");
        assert!(matches.is_empty(), "foo.* should not match bare foo");
    }

    #[cfg(feature = "accounts")]
    mod remap_tests {
        use super::super::remap_subject;

        #[test]
        fn test_remap_gt_prefix() {
            // events.> → team_a.events.>
            let result = remap_subject("events.>", "team_a.events.>", "events.order.created");
            assert_eq!(result, "team_a.events.order.created");
        }

        #[test]
        fn test_remap_gt_single_token() {
            // events.> → team_a.events.>
            let result = remap_subject("events.>", "team_a.events.>", "events.order");
            assert_eq!(result, "team_a.events.order");
        }

        #[test]
        fn test_remap_star() {
            // orders.* → team.orders.*
            let result = remap_subject("orders.*", "team.orders.*", "orders.new");
            assert_eq!(result, "team.orders.new");
        }

        #[test]
        fn test_remap_no_wildcard() {
            // exact.subject → other.subject
            let result = remap_subject("exact.subject", "other.subject", "exact.subject");
            assert_eq!(result, "other.subject");
        }

        #[test]
        fn test_remap_multi_star() {
            // *.*.events → prefix.*.*.events
            let result = remap_subject("*.*.events", "prefix.*.*.events", "a.b.events");
            assert_eq!(result, "prefix.a.b.events");
        }

        #[test]
        fn test_remap_identity() {
            // Same pattern: events.> → events.>
            let result = remap_subject("events.>", "events.>", "events.foo.bar");
            assert_eq!(result, "events.foo.bar");
        }
    }
}
