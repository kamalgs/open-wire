//! Interest transform pipeline for upstream leaf subscriptions.
//!
//! Two-stage pipeline that transforms subjects before sending upstream:
//! 1. **Subject mapping** (`subject-mapping` feature) — stateless prefix rewrite
//! 2. **Interest collapse** (`interest-collapse` feature) — stateful N:1 aggregation
//!
//! Both stages are `#[cfg]`-gated and compile to no-ops when disabled.

use std::collections::HashSet;

#[cfg(feature = "interest-collapse")]
use std::collections::HashMap;

#[cfg(feature = "subject-mapping")]
use crate::core::sub_list::subject_matches;

/// A subject mapping rule: rewrite subjects matching `from` pattern to `to` pattern.
///
/// Supports two forms:
/// - **Prefix mapping**: `from: "local.>"` / `to: "prod.>"` — rewrites the prefix
/// - **Exact mapping**: `from: "foo.bar"` / `to: "baz.qux"` — exact subject replacement
#[cfg(feature = "subject-mapping")]
#[derive(Debug, Clone)]
pub struct SubjectMapping {
    pub from: String,
    pub to: String,
}

/// Stateless subject rewriter. Applies prefix or exact mappings in order.
#[cfg(feature = "subject-mapping")]
struct SubjectMapper {
    mappings: Vec<SubjectMapping>,
}

#[cfg(feature = "subject-mapping")]
impl SubjectMapper {
    fn new(mappings: Vec<SubjectMapping>) -> Self {
        Self { mappings }
    }

    /// Apply the first matching mapping to the subject, or return it unchanged.
    fn map_subject(&self, subject: &str) -> String {
        for m in &self.mappings {
            if !subject_matches(&m.from, subject) {
                continue;
            }
            // Prefix mapping: from ends with `>`, to ends with `>`
            if m.from.ends_with('>') && m.to.ends_with('>') {
                let from_prefix = &m.from[..m.from.len() - 1];
                let to_prefix = &m.to[..m.to.len() - 1];
                if let Some(suffix) = subject.strip_prefix(from_prefix) {
                    return format!("{to_prefix}{suffix}");
                }
            }
            // Exact mapping
            return m.to.clone();
        }
        subject.to_string()
    }
}

/// Stateful interest collapser. Merges many exact subs into wildcard subs
/// using templates, tracking refcounts per collapse key.
#[cfg(feature = "interest-collapse")]
struct InterestCollapser {
    templates: Vec<String>,
    counts: HashMap<String, u32>,
}

#[cfg(feature = "interest-collapse")]
impl InterestCollapser {
    fn new(templates: Vec<String>) -> Self {
        Self {
            templates,
            counts: HashMap::new(),
        }
    }

    /// On subscribe: returns the collapse key if this is the first sub under it.
    /// Returns `None` if suppressed (not the first) or no template matches.
    fn on_subscribe(&mut self, subject: &str) -> CollapseResult {
        if let Some(ckey) = collapse_key(subject, &self.templates) {
            let count = self.counts.entry(ckey.clone()).or_insert(0);
            *count += 1;
            if *count == 1 {
                CollapseResult::Collapsed(ckey)
            } else {
                CollapseResult::Suppressed
            }
        } else {
            CollapseResult::PassThrough
        }
    }

    /// On unsubscribe: returns the collapse key if this was the last sub under it.
    /// Returns `None` if suppressed (still subs remaining) or no template matches.
    fn on_unsubscribe(&mut self, subject: &str) -> CollapseResult {
        if let Some(ckey) = collapse_key(subject, &self.templates) {
            if let Some(count) = self.counts.get_mut(&ckey) {
                *count -= 1;
                if *count == 0 {
                    self.counts.remove(&ckey);
                    return CollapseResult::Collapsed(ckey);
                }
            }
            CollapseResult::Suppressed
        } else {
            CollapseResult::PassThrough
        }
    }
}

/// Result of attempting to collapse a subject.
#[cfg(feature = "interest-collapse")]
enum CollapseResult {
    /// Subject was collapsed to this key (send it upstream).
    Collapsed(String),
    /// Subject matched a template but was suppressed (already counted).
    Suppressed,
    /// No template matched — pass through as-is.
    PassThrough,
}

/// Two-stage interest transform pipeline.
///
/// Stage 1: Subject mapping (stateless rewrite)
/// Stage 2: Interest collapse (stateful refcounting)
///
/// When features are disabled, the corresponding fields are absent and methods
/// become identity/no-ops at zero cost.
pub(crate) struct InterestPipeline {
    #[cfg(feature = "subject-mapping")]
    mapper: Option<SubjectMapper>,
    #[cfg(feature = "interest-collapse")]
    collapser: Option<InterestCollapser>,
}

impl InterestPipeline {
    /// Build a pipeline from configuration.
    ///
    /// Empty vecs disable the corresponding stage (even when the feature is compiled in).
    pub(crate) fn new(
        #[cfg(feature = "interest-collapse")] collapse_templates: Vec<String>,
        #[cfg(feature = "subject-mapping")] subject_mappings: Vec<SubjectMapping>,
    ) -> Self {
        Self {
            #[cfg(feature = "subject-mapping")]
            mapper: if subject_mappings.is_empty() {
                None
            } else {
                Some(SubjectMapper::new(subject_mappings))
            },
            #[cfg(feature = "interest-collapse")]
            collapser: if collapse_templates.is_empty() {
                None
            } else {
                Some(InterestCollapser::new(collapse_templates))
            },
        }
    }

    /// Process a new subscription through the pipeline.
    ///
    /// Returns `Some((subject, queue))` if an `LS+` should be sent upstream,
    /// or `None` if the subscription should be suppressed.
    pub(crate) fn on_subscribe(
        &mut self,
        subject: &str,
        queue: Option<&str>,
    ) -> Option<(String, Option<String>)> {
        // Queue group subs bypass the pipeline entirely
        if queue.is_some() {
            return Some((subject.to_string(), queue.map(|q| q.to_string())));
        }

        // Stage 1: mapping
        let mapped = self.map_subject(subject);

        // Stage 2: collapse
        #[cfg(feature = "interest-collapse")]
        if let Some(ref mut collapser) = self.collapser {
            return match collapser.on_subscribe(&mapped) {
                CollapseResult::Collapsed(ckey) => Some((ckey, None)),
                CollapseResult::Suppressed => None,
                CollapseResult::PassThrough => Some((mapped, None)),
            };
        }

        Some((mapped, None))
    }

    /// Process an unsubscription through the pipeline.
    ///
    /// Returns `Some((subject, queue))` if an `LS-` should be sent upstream,
    /// or `None` if the unsubscription should be suppressed.
    pub(crate) fn on_unsubscribe(
        &mut self,
        subject: &str,
        queue: Option<&str>,
    ) -> Option<(String, Option<String>)> {
        // Queue group subs bypass the pipeline entirely
        if queue.is_some() {
            return Some((subject.to_string(), queue.map(|q| q.to_string())));
        }

        // Stage 1: mapping
        let mapped = self.map_subject(subject);

        // Stage 2: collapse
        #[cfg(feature = "interest-collapse")]
        if let Some(ref mut collapser) = self.collapser {
            return match collapser.on_unsubscribe(&mapped) {
                CollapseResult::Collapsed(ckey) => Some((ckey, None)),
                CollapseResult::Suppressed => None,
                CollapseResult::PassThrough => Some((mapped, None)),
            };
        }

        Some((mapped, None))
    }

    /// Stateless mapping only (for sync — collapse dedup handled separately).
    pub(crate) fn transform_for_sync(&self, subject: &str) -> String {
        self.map_subject(subject)
    }

    /// Collapse dedup for sync: returns the collapse key if not yet sent, or `None`.
    #[cfg(feature = "interest-collapse")]
    pub(crate) fn collapse_for_sync(
        &self,
        subject: &str,
        sent_keys: &mut HashSet<String>,
    ) -> Option<String> {
        if let Some(ref collapser) = self.collapser {
            if let Some(ckey) = collapse_key(subject, &collapser.templates) {
                if sent_keys.insert(ckey.clone()) {
                    return Some(ckey);
                }
                return None;
            }
        }
        // No collapse — return subject as-is
        Some(subject.to_string())
    }

    /// Collapse dedup for sync (no-op when feature disabled — always returns subject).
    #[cfg(not(feature = "interest-collapse"))]
    pub(crate) fn collapse_for_sync(
        &self,
        subject: &str,
        _sent_keys: &mut HashSet<String>,
    ) -> Option<String> {
        Some(subject.to_string())
    }

    /// Apply subject mapping (stage 1). Identity when no mapper configured.
    fn map_subject(&self, subject: &str) -> String {
        #[cfg(feature = "subject-mapping")]
        if let Some(ref mapper) = self.mapper {
            return mapper.map_subject(subject);
        }
        subject.to_string()
    }
}

/// Compute the collapse key for a subject given a set of templates.
///
/// For each template, checks if the subject matches. If so, walks the template
/// and subject token-by-token: literal and `*` positions take the subject's actual
/// token, `>` replaces all trailing tokens with `>`.
///
/// Returns `None` if no template matches (use exact interest).
///
/// Example: template `app.*.sessions.>`, subject `app.node1.sessions.abc123`
/// → key `app.node1.sessions.>`
#[cfg(feature = "interest-collapse")]
fn collapse_key(subject: &str, templates: &[String]) -> Option<String> {
    use crate::core::sub_list::subject_matches;

    for template in templates {
        if !subject_matches(template, subject) {
            continue;
        }

        let mut key = String::new();
        let mut titer = template.split('.');
        let mut siter = subject.split('.');

        loop {
            match titer.next() {
                Some(">") => {
                    // `>` replaces all remaining subject tokens
                    if !key.is_empty() {
                        key.push('.');
                    }
                    key.push('>');
                    break;
                }
                Some(_ttok) => {
                    // `*` or literal — take the subject's actual token
                    if let Some(stok) = siter.next() {
                        if !key.is_empty() {
                            key.push('.');
                        }
                        key.push_str(stok);
                    }
                }
                None => break,
            }
        }

        return Some(key);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- collapse_key ---

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn collapse_key_matching_gt() {
        let templates = vec!["app.*.sessions.>".to_string()];
        let key = collapse_key("app.node1.sessions.abc123", &templates);
        assert_eq!(key.as_deref(), Some("app.node1.sessions.>"));
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn collapse_key_matching_star() {
        let templates = vec!["telemetry.*.metrics.*".to_string()];
        let key = collapse_key("telemetry.host1.metrics.cpu", &templates);
        assert_eq!(key.as_deref(), Some("telemetry.host1.metrics.cpu"));
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn collapse_key_no_match() {
        let templates = vec!["app.*.sessions.>".to_string()];
        let key = collapse_key("other.subject", &templates);
        assert!(key.is_none());
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn collapse_key_gt_preserves_prefix() {
        let templates = vec!["data.*.>".to_string()];
        let key = collapse_key("data.region1.metrics.cpu.load", &templates);
        assert_eq!(key.as_deref(), Some("data.region1.>"));
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn collapse_key_multiple_templates() {
        let templates = vec!["app.*.sessions.>".to_string(), "telemetry.>".to_string()];
        assert_eq!(
            collapse_key("app.n1.sessions.s1", &templates).as_deref(),
            Some("app.n1.sessions.>")
        );
        assert_eq!(
            collapse_key("telemetry.host1.cpu", &templates).as_deref(),
            Some("telemetry.>")
        );
        assert!(collapse_key("other.thing", &templates).is_none());
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn collapse_key_exact_template() {
        let templates = vec!["foo.bar.baz".to_string()];
        let key = collapse_key("foo.bar.baz", &templates);
        assert_eq!(key.as_deref(), Some("foo.bar.baz"));
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn collapse_key_empty_templates() {
        let templates: Vec<String> = vec![];
        assert!(collapse_key("anything", &templates).is_none());
    }

    // --- SubjectMapper ---

    #[cfg(feature = "subject-mapping")]
    #[test]
    fn mapper_prefix_rewrite() {
        let mapper = SubjectMapper::new(vec![SubjectMapping {
            from: "local.>".into(),
            to: "prod.region1.>".into(),
        }]);
        assert_eq!(mapper.map_subject("local.foo.bar"), "prod.region1.foo.bar");
    }

    #[cfg(feature = "subject-mapping")]
    #[test]
    fn mapper_exact_rewrite() {
        let mapper = SubjectMapper::new(vec![SubjectMapping {
            from: "old.subject".into(),
            to: "new.subject".into(),
        }]);
        assert_eq!(mapper.map_subject("old.subject"), "new.subject");
    }

    #[cfg(feature = "subject-mapping")]
    #[test]
    fn mapper_no_match_passthrough() {
        let mapper = SubjectMapper::new(vec![SubjectMapping {
            from: "local.>".into(),
            to: "prod.>".into(),
        }]);
        assert_eq!(mapper.map_subject("other.foo"), "other.foo");
    }

    #[cfg(feature = "subject-mapping")]
    #[test]
    fn mapper_first_match_wins() {
        let mapper = SubjectMapper::new(vec![
            SubjectMapping {
                from: "local.foo.>".into(),
                to: "specific.>".into(),
            },
            SubjectMapping {
                from: "local.>".into(),
                to: "general.>".into(),
            },
        ]);
        assert_eq!(mapper.map_subject("local.foo.bar"), "specific.bar");
        assert_eq!(mapper.map_subject("local.baz.qux"), "general.baz.qux");
    }

    // --- InterestPipeline ---

    #[test]
    fn pipeline_no_transforms_passthrough() {
        let mut pipeline = InterestPipeline::new(
            #[cfg(feature = "interest-collapse")]
            vec![],
            #[cfg(feature = "subject-mapping")]
            vec![],
        );
        let result = pipeline.on_subscribe("foo.bar", None);
        assert_eq!(result, Some(("foo.bar".to_string(), None)));

        let result = pipeline.on_unsubscribe("foo.bar", None);
        assert_eq!(result, Some(("foo.bar".to_string(), None)));
    }

    #[test]
    fn pipeline_queue_group_bypasses() {
        let mut pipeline = InterestPipeline::new(
            #[cfg(feature = "interest-collapse")]
            vec!["app.*.>".to_string()],
            #[cfg(feature = "subject-mapping")]
            vec![],
        );
        let result = pipeline.on_subscribe("app.n1.foo", Some("q1"));
        assert_eq!(
            result,
            Some(("app.n1.foo".to_string(), Some("q1".to_string())))
        );
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn pipeline_collapse_subscribe_unsubscribe() {
        let mut pipeline = InterestPipeline::new(
            vec!["app.*.sessions.>".to_string()],
            #[cfg(feature = "subject-mapping")]
            vec![],
        );

        // First sub under collapse key → sends wildcard
        let r = pipeline.on_subscribe("app.n1.sessions.s1", None);
        assert_eq!(r, Some(("app.n1.sessions.>".to_string(), None)));

        // Second sub under same key → suppressed
        let r = pipeline.on_subscribe("app.n1.sessions.s2", None);
        assert!(r.is_none());

        // Remove first → still one left, suppressed
        let r = pipeline.on_unsubscribe("app.n1.sessions.s1", None);
        assert!(r.is_none());

        // Remove last → sends unsub
        let r = pipeline.on_unsubscribe("app.n1.sessions.s2", None);
        assert_eq!(r, Some(("app.n1.sessions.>".to_string(), None)));
    }

    #[cfg(feature = "subject-mapping")]
    #[test]
    fn pipeline_mapping_only() {
        let mut pipeline = InterestPipeline::new(
            #[cfg(feature = "interest-collapse")]
            vec![],
            vec![SubjectMapping {
                from: "local.>".into(),
                to: "prod.>".into(),
            }],
        );

        let r = pipeline.on_subscribe("local.foo.bar", None);
        assert_eq!(r, Some(("prod.foo.bar".to_string(), None)));
    }

    #[cfg(all(feature = "subject-mapping", feature = "interest-collapse"))]
    #[test]
    fn pipeline_mapping_then_collapse() {
        let mut pipeline = InterestPipeline::new(
            vec!["prod.*.>".to_string()],
            vec![SubjectMapping {
                from: "local.>".into(),
                to: "prod.>".into(),
            }],
        );

        // local.foo.bar → mapped to prod.foo.bar → collapsed to prod.foo.>
        let r = pipeline.on_subscribe("local.foo.bar", None);
        assert_eq!(r, Some(("prod.foo.>".to_string(), None)));

        // local.foo.baz → mapped to prod.foo.baz → collapsed (suppressed)
        let r = pipeline.on_subscribe("local.foo.baz", None);
        assert!(r.is_none());

        // Unsubscribe first → suppressed
        let r = pipeline.on_unsubscribe("local.foo.bar", None);
        assert!(r.is_none());

        // Unsubscribe last → sends unsub with collapse key
        let r = pipeline.on_unsubscribe("local.foo.baz", None);
        assert_eq!(r, Some(("prod.foo.>".to_string(), None)));
    }

    #[test]
    fn pipeline_transform_for_sync() {
        let pipeline = InterestPipeline::new(
            #[cfg(feature = "interest-collapse")]
            vec![],
            #[cfg(feature = "subject-mapping")]
            vec![],
        );
        assert_eq!(pipeline.transform_for_sync("foo.bar"), "foo.bar");
    }

    #[cfg(feature = "subject-mapping")]
    #[test]
    fn pipeline_transform_for_sync_with_mapping() {
        let pipeline = InterestPipeline::new(
            #[cfg(feature = "interest-collapse")]
            vec![],
            vec![SubjectMapping {
                from: "local.>".into(),
                to: "prod.>".into(),
            }],
        );
        assert_eq!(pipeline.transform_for_sync("local.foo"), "prod.foo");
    }

    #[test]
    fn pipeline_collapse_for_sync_no_templates() {
        let pipeline = InterestPipeline::new(
            #[cfg(feature = "interest-collapse")]
            vec![],
            #[cfg(feature = "subject-mapping")]
            vec![],
        );
        let mut sent = std::collections::HashSet::new();
        assert_eq!(
            pipeline.collapse_for_sync("foo.bar", &mut sent),
            Some("foo.bar".to_string())
        );
    }

    #[cfg(feature = "interest-collapse")]
    #[test]
    fn pipeline_collapse_for_sync_dedup() {
        let pipeline = InterestPipeline::new(
            vec!["app.*.>".to_string()],
            #[cfg(feature = "subject-mapping")]
            vec![],
        );
        let mut sent = HashSet::new();

        // First → sends collapse key
        assert_eq!(
            pipeline.collapse_for_sync("app.n1.foo", &mut sent),
            Some("app.n1.>".to_string())
        );
        // Second under same key → suppressed
        assert_eq!(pipeline.collapse_for_sync("app.n1.bar", &mut sent), None);
        // Different key → sends
        assert_eq!(
            pipeline.collapse_for_sync("app.n2.foo", &mut sent),
            Some("app.n2.>".to_string())
        );
    }
}
