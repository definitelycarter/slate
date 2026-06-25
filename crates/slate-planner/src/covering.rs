//! Covering-index analysis (RFC: Covering Index Scans, Part B — phases 1–2).
//!
//! A conservative post-pass over a lowered read [`Plan::Query`](crate::plan::Plan)
//! tree. When every field the query reads is a component of the scanned index or
//! the primary key, the document fetch is pure overhead: the index entry already
//! carries the value(s) and the doc-id. This pass proves that condition for two
//! shapes — a single-field `IndexScan` (phase 1) or a multi-field
//! `CompoundIndexScan` (phase 2), each under a `KeyLookup` beneath a linear chain
//! of binding-aware nodes — and, when it holds, flips the scan to covering and
//! drops the `KeyLookup`. The executor then synthesizes each row from the entry
//! instead of fetching the document.
//!
//! ## The one invariant
//!
//! *A missed cover is a lost optimization; a wrong cover is wrong results.* So
//! the analysis is one-directional: it returns **uncoverable** the instant it
//! sees any node, binding, or expression shape it does not fully understand. It
//! covers only when it can name every referenced path and show each **exactly
//! equals** an index component or the pk. It handles two index sources:
//!
//! - a single-field scan (`KeyLookup(IndexScan)`) or a compound scan
//!   (`KeyLookup(CompoundIndexScan)`) — not `IndexMerge`, not a multikey (`.[]`)
//!   scan (which lowers under a `Distinct`);
//! - **scalar** index components — top-level (`status`) or dotted paths
//!   (`user.id`), synthesized as nested objects (`{user: {id: value}}`, merging
//!   shared prefixes for a compound index); never a multikey `.[]` component,
//!   whose array fan-out a single scalar entry can't rebuild;
//! - **one alias**, bound by [`RowBinding::Alias`] — an [`RowBinding::Env`] row
//!   means a join / unwind / aggregate / subquery, all out of scope here;
//! - references that are **clean alias-rooted member chains** (`alias.a`,
//!   `alias.a.b`, …) whose reconstructed dotted path *string-equals* the index
//!   field or the pk. A bare `alias` (whole row), an `alias[i]` index, a
//!   `PathGet`/`MultikeyEq`, or a correlated subquery on the alias all force
//!   uncoverable — as does a parent, extension, or sibling of any index component
//!   (`user` / `user.id.x` / `user.name` against an index on `user.id`).
//!
//! Covered aggregates are a later phase (they add the aggregate binding-shift);
//! the RFC has the full design.

use std::collections::BTreeSet;

use slate_ast::Expression;

use crate::plan::{Node, RowBinding};
use crate::sargable::CollectionMeta;

/// Rewrite `node` so a provably-covered single-field index scan serves the query
/// from index entries (no `KeyLookup`).
///
/// Two phases, so a query that *can't* be covered — the common case — keeps its
/// plan untouched and pays only a cheap read-only walk: [`is_coverable`] decides
/// by reference (no allocation), and only a coverable plan is rebuilt by
/// [`rewrite`]. The optimization never taxes the paths it doesn't help.
pub(crate) fn apply(node: Node, meta: &CollectionMeta) -> Node {
    // The synthesized covering document carries the pk under its flat `pk_path`
    // key; a dotted pk would need a nested object the entry's scalar doc-id can't
    // reproduce, so a dotted pk is never coverable.
    if meta.pk_path.contains('.') {
        return node;
    }
    if is_coverable(&node, meta) {
        rewrite(node, &meta.compound_indexes)
    } else {
        node
    }
}

/// Decide, without mutating or allocating beyond the analysis, whether `node` is
/// a coverable single-field- or compound-index plan.
fn is_coverable(node: &Node, meta: &CollectionMeta) -> bool {
    let mut analysis = Analysis {
        pk_path: &meta.pk_path,
        compound_indexes: &meta.compound_indexes,
        alias: None,
        refs: Refs::Fields(BTreeSet::new()),
    };
    analyze(node, &mut analysis)
}

/// Accumulated coverage state while descending the wrapper chain by reference.
/// The alias is *borrowed* from the plan — the walk holds the tree, so no clone.
struct Analysis<'a> {
    pk_path: &'a str,
    /// Compound-index `(identity, components)` pairs — to resolve a compound
    /// scan's opaque joined `field` back to the component paths it carries.
    compound_indexes: &'a [(String, Vec<String>)],
    /// The single alias the binding-aware nodes read, pinned at the first one.
    alias: Option<&'a str>,
    /// Field references gathered so far, or the uncoverable poison state.
    refs: Refs,
}

/// Alias-field references collected from the wrapper expressions, or a sticky
/// "uncoverable" state once an unsupported shape is seen.
enum Refs {
    Fields(BTreeSet<String>),
    Uncoverable,
}

impl<'a> Analysis<'a> {
    /// Record a node's binding: pin the alias on the first `Alias`, and poison on
    /// an `Env` binding (joins/aggregates/subqueries) or a second, different
    /// alias (which a single-source plan never has, but bail rather than guess).
    fn note_binding(&mut self, binding: &'a RowBinding) {
        match binding {
            RowBinding::Alias(a) => match self.alias {
                None => self.alias = Some(a.as_str()),
                Some(prev) if prev == a => {}
                Some(_) => self.refs = Refs::Uncoverable,
            },
            RowBinding::Env => self.refs = Refs::Uncoverable,
        }
    }

    /// Fold an expression's alias references into the running set.
    fn collect(&mut self, expr: &Expression) {
        if matches!(self.refs, Refs::Uncoverable) {
            return;
        }
        let Some(alias) = self.alias else {
            // A field-bearing node with no pinned alias — can't reason; bail.
            self.refs = Refs::Uncoverable;
            return;
        };
        collect_alias_refs(expr, alias, &mut self.refs);
    }

    /// Whether a single-field scan on `field` covers everything collected so far.
    /// `field` may be a dotted scalar path (`user.id`); every collected reference
    /// must string-equal it or the pk (the settled exact-path-match rule).
    fn covers(&self, field: &str) -> bool {
        if self.alias.is_none() {
            return false;
        }
        // A multikey array path (`tags.[]`) fans one document out across many
        // entries; a single scalar entry value can't reconstruct the array.
        if field.contains("[]") {
            return false;
        }
        // A dotted field is synthesized as a nested object rooted at its first
        // segment. If that root collides with the (always top-level) pk key,
        // synthesis would emit two conflicting top-level keys — leave it to the fetch.
        if let Some((root, _)) = field.split_once('.')
            && root == self.pk_path
        {
            return false;
        }
        match &self.refs {
            Refs::Uncoverable => false,
            Refs::Fields(paths) => paths.iter().all(|p| p == field || p == self.pk_path),
        }
    }

    /// Whether the compound index with joined `identity` covers everything
    /// collected so far. Resolves the identity to its ordered component paths,
    /// then — exactly like the single-field [`covers`](Self::covers) — requires
    /// every collected reference to string-equal one of those components or the
    /// pk (the settled exact-path-match rule, now over a set of components).
    fn covers_compound(&self, identity: &str) -> bool {
        if self.alias.is_none() {
            return false;
        }
        let Some((_, components)) = self.compound_indexes.iter().find(|(id, _)| id == identity)
        else {
            return false;
        };
        for c in components {
            // A multikey component (`tags.[]`) fans one document out across many
            // entries; a single scalar value can't rebuild the array.
            if c.contains("[]") {
                return false;
            }
            // A component whose synthesized top-level key collides with the
            // (always top-level) pk key would emit two conflicting keys — leave it
            // to the fetch. Covers `_id` itself and any `_id.*` dotted component.
            if c == self.pk_path {
                return false;
            }
            if let Some((root, _)) = c.split_once('.')
                && root == self.pk_path
            {
                return false;
            }
        }
        match &self.refs {
            Refs::Uncoverable => false,
            Refs::Fields(paths) => paths
                .iter()
                .all(|p| p == self.pk_path || components.iter().any(|c| c == p)),
        }
    }
}

/// Descend the linear wrapper chain by reference, collecting refs, and report
/// whether the index source at the bottom is a single-field scan that covers
/// every reference. Any node outside the recognized chain ⇒ not coverable.
fn analyze<'a>(node: &'a Node, a: &mut Analysis<'a>) -> bool {
    match node {
        // Pass-through wrappers that read no alias fields themselves.
        Node::Limit { source, .. } | Node::Distinct { source, .. } => analyze(source, a),

        // Binding-aware wrappers: pin the alias, gather their references, recurse.
        Node::Project {
            expr,
            binding,
            source,
        } => {
            a.note_binding(binding);
            a.collect(expr);
            analyze(source, a)
        }
        Node::Sort {
            keys,
            binding,
            source,
        } => {
            a.note_binding(binding);
            for key in keys {
                a.collect(&key.expr);
            }
            analyze(source, a)
        }
        Node::Filter {
            predicate,
            binding,
            source,
        } => {
            a.note_binding(binding);
            a.collect(predicate);
            analyze(source, a)
        }

        // The source: coverable iff it is a non-covering single-field or compound
        // index scan and every collected reference is one of its components or the
        // pk. A compound scan's `field` is the opaque joined identity; `covers_compound`
        // resolves it back to component paths via the metadata.
        Node::KeyLookup { source, .. } => match source.as_ref() {
            Node::IndexScan {
                field,
                covering: false,
                ..
            } => a.covers(field),
            Node::CompoundIndexScan {
                field,
                covering: None,
                ..
            } => a.covers_compound(field),
            _ => false,
        },

        // Anything else (Bind, Unwind, Aggregate, Subquery, Scan, a bare
        // IndexScan/CompoundIndexScan with no KeyLookup, IndexMerge, …) is not the
        // coverable chain.
        _ => false,
    }
}

/// Rebuild a plan [`is_coverable`] has approved: drop the `KeyLookup` and flip its
/// single-field `IndexScan` or compound `CompoundIndexScan` to covering, leaving
/// the rest of the spine intact. Only ever called on a coverable plan, so the
/// `KeyLookup(scan)` is present. `compound_indexes` resolves a compound scan's
/// joined identity back to the component paths the covering marker carries.
fn rewrite(node: Node, compound_indexes: &[(String, Vec<String>)]) -> Node {
    match node {
        Node::Limit { skip, take, source } => Node::Limit {
            skip,
            take,
            source: Box::new(rewrite(*source, compound_indexes)),
        },
        Node::Distinct { source, flatten } => Node::Distinct {
            source: Box::new(rewrite(*source, compound_indexes)),
            flatten,
        },
        Node::Project {
            expr,
            binding,
            source,
        } => Node::Project {
            expr,
            binding,
            source: Box::new(rewrite(*source, compound_indexes)),
        },
        Node::Sort {
            keys,
            binding,
            source,
        } => Node::Sort {
            keys,
            binding,
            source: Box::new(rewrite(*source, compound_indexes)),
        },
        Node::Filter {
            predicate,
            binding,
            source,
        } => Node::Filter {
            predicate,
            binding,
            source: Box::new(rewrite(*source, compound_indexes)),
        },
        Node::KeyLookup { collection, source } => match *source {
            Node::IndexScan {
                collection: ic,
                field,
                range,
                direction,
                limit,
                covering: false,
            } => Node::IndexScan {
                collection: ic,
                field,
                range,
                direction,
                limit,
                covering: true,
            },
            Node::CompoundIndexScan {
                collection: ic,
                field,
                range,
                direction,
                limit,
                covering: None,
            } => match compound_indexes.iter().find(|(id, _)| *id == field) {
                // Carry the component paths on the marker — the node's `field` is
                // the opaque joined identity, so the executor can't recover them.
                // `is_coverable` already proved the identity is present; the clone
                // is an owned copy of 2–4 short component strings (the metadata is
                // only borrowed here), mirroring `field`/`range` ownership.
                Some((_, components)) => Node::CompoundIndexScan {
                    collection: ic,
                    field,
                    range,
                    direction,
                    limit,
                    covering: Some(components.clone()),
                },
                // Defensive: identity unexpectedly absent — keep the fetch.
                None => Node::KeyLookup {
                    collection,
                    source: Box::new(Node::CompoundIndexScan {
                        collection: ic,
                        field,
                        range,
                        direction,
                        limit,
                        covering: None,
                    }),
                },
            },
            // Defensive: a coverable plan always has a recognized scan here, so
            // this only guards against an unexpected shape — re-wrap intact.
            other => Node::KeyLookup {
                collection,
                source: Box::new(other),
            },
        },
        other => other,
    }
}

/// Fold `alias.<field>` references in `expr` into `refs`, or poison `refs` to
/// `Uncoverable` on any shape the flat index entry can't serve.
fn collect_alias_refs(expr: &Expression, alias: &str, refs: &mut Refs) {
    if matches!(refs, Refs::Uncoverable) {
        return;
    }
    match expr {
        // A bare reference to the bound row needs every field of the document.
        Expression::Identifier(name) => {
            if name == alias {
                *refs = Refs::Uncoverable;
            }
        }
        Expression::Literal(_) | Expression::Value(_) | Expression::Parameter(_) => {}
        Expression::Member { base, .. } => {
            // A clean alias-rooted member chain (`alias.a`, `alias.a.b`, …) is one
            // dotted-path reference; collect its full reconstructed path so the
            // exact-match in `covers` can compare it to the index field. Owning the
            // path string is intrinsic — we collect from a borrowed expression tree.
            if let Some(path) = alias_rooted_path(expr, alias) {
                if let Refs::Fields(paths) = refs {
                    paths.insert(path);
                }
            } else {
                // Not alias-rooted (e.g. `f(alias.x).y`): descend into the base so
                // any nested alias reference is still collected — or poisoned, if
                // it reaches the alias through an index / path-get / subquery the
                // scalar entry can't serve.
                collect_alias_refs(base, alias, refs);
            }
        }
        // Indexing into / array-distributing over the alias can't be served from
        // a scalar component value.
        Expression::Index { base, index } => {
            if touches_alias(base, alias) {
                *refs = Refs::Uncoverable;
            } else {
                collect_alias_refs(index, alias, refs);
            }
        }
        Expression::PathGet { base, .. } => {
            if touches_alias(base, alias) {
                *refs = Refs::Uncoverable;
            } else {
                collect_alias_refs(base, alias, refs);
            }
        }
        Expression::MultikeyEq { base, value, .. } => {
            if touches_alias(base, alias) {
                *refs = Refs::Uncoverable;
            } else {
                collect_alias_refs(value, alias, refs);
            }
        }
        Expression::Unary { expr, .. } => collect_alias_refs(expr, alias, refs),
        Expression::Binary { lhs, rhs, .. } => {
            collect_alias_refs(lhs, alias, refs);
            collect_alias_refs(rhs, alias, refs);
        }
        Expression::Function { args, .. } | Expression::Array(args) => {
            for e in args {
                collect_alias_refs(e, alias, refs);
            }
        }
        Expression::Object(fields) => {
            for (_, e) in fields {
                collect_alias_refs(e, alias, refs);
            }
        }
        // A correlated subquery can reference the alias in ways the entry can't
        // serve; never cover through one.
        Expression::Subquery { .. } => *refs = Refs::Uncoverable,
    }
}

/// If `expr` is a clean alias-rooted member chain (`alias.a`, `alias.a.b`, …),
/// return the dotted path below the alias (`a`, `a.b`, …). `None` for the bare
/// alias or any shape that isn't a pure `Member`-over-`alias` chain — cases
/// [`collect_alias_refs`] handles (and poisons) on its own. Mirrors the planner's
/// `path_of`, so a covered reference reconstructs the same path string the index
/// field carries, making the exact-match in [`Analysis::covers`] sound.
fn alias_rooted_path(expr: &Expression, alias: &str) -> Option<String> {
    match expr {
        Expression::Member { base, field } => match base.as_ref() {
            Expression::Identifier(a) if a == alias => Some(field.clone()),
            base => alias_rooted_path(base, alias).map(|p| format!("{p}.{field}")),
        },
        _ => None,
    }
}

/// Whether `alias` appears anywhere in `expr` — used to reject any alias access
/// that isn't a clean member chain.
fn touches_alias(expr: &Expression, alias: &str) -> bool {
    match expr {
        Expression::Identifier(name) => name == alias,
        Expression::Literal(_) | Expression::Value(_) | Expression::Parameter(_) => false,
        Expression::Member { base, .. } | Expression::PathGet { base, .. } => {
            touches_alias(base, alias)
        }
        Expression::Index { base, index } => {
            touches_alias(base, alias) || touches_alias(index, alias)
        }
        Expression::Unary { expr, .. } => touches_alias(expr, alias),
        Expression::Binary { lhs, rhs, .. } => {
            touches_alias(lhs, alias) || touches_alias(rhs, alias)
        }
        Expression::MultikeyEq { base, value, .. } => {
            touches_alias(base, alias) || touches_alias(value, alias)
        }
        Expression::Function { args, .. } | Expression::Array(args) => {
            args.iter().any(|e| touches_alias(e, alias))
        }
        Expression::Object(fields) => fields.iter().any(|(_, e)| touches_alias(e, alias)),
        // Conservative: assume a subquery may correlate to the alias.
        Expression::Subquery { .. } => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lower::lower;
    use crate::plan::{CollectionRef, Plan};
    use crate::sargable::CollectionMeta;

    fn container() -> CollectionRef {
        CollectionRef {
            cf: "default".into(),
            collection: "c".into(),
        }
    }

    fn meta(indexes: &[&str]) -> CollectionMeta {
        CollectionMeta {
            indexes: indexes.iter().map(|s| s.to_string()).collect(),
            compound_indexes: Vec::new(),
            pk_path: "_id".into(),
        }
    }

    /// Lower `sql` against `indexes` and return the covering flag of the first
    /// index scan in the plan: `Some(true)` covered (no `KeyLookup`), `Some(false)`
    /// indexed-but-fetched, `None` no index scan at all.
    fn covering_flag(sql: &str, indexes: &[&str]) -> Option<bool> {
        let Plan::Query(node) = lower(slate_sql::parse(sql).unwrap(), container(), &meta(indexes))
        else {
            panic!("lower always produces a Query");
        };
        fn find(node: &Node) -> Option<bool> {
            match node {
                Node::IndexScan { covering, .. } => Some(*covering),
                Node::KeyLookup { source, .. }
                | Node::Project { source, .. }
                | Node::Filter { source, .. }
                | Node::Sort { source, .. }
                | Node::Limit { source, .. }
                | Node::Distinct { source, .. }
                | Node::Aggregate { source, .. }
                | Node::Bind { source, .. } => find(source),
                _ => None,
            }
        }
        find(&node)
    }

    /// Whether the plan still contains a `KeyLookup` (the document fetch).
    fn has_key_lookup(sql: &str, indexes: &[&str]) -> bool {
        let Plan::Query(node) = lower(slate_sql::parse(sql).unwrap(), container(), &meta(indexes))
        else {
            panic!("lower always produces a Query");
        };
        fn walk(node: &Node) -> bool {
            match node {
                Node::KeyLookup { .. } => true,
                Node::Project { source, .. }
                | Node::Filter { source, .. }
                | Node::Sort { source, .. }
                | Node::Limit { source, .. }
                | Node::Distinct { source, .. }
                | Node::Aggregate { source, .. }
                | Node::Bind { source, .. } => walk(source),
                _ => false,
            }
        }
        walk(&node)
    }

    #[test]
    fn single_indexed_field_projection_is_covered() {
        // Reads only `status` (indexed) → covered, no KeyLookup.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c.status FROM c WHERE c.status = 'active'",
                &["status"]
            ),
            Some(true)
        );
        assert!(!has_key_lookup(
            "SELECT VALUE c.status FROM c WHERE c.status = 'active'",
            &["status"]
        ));
    }

    #[test]
    fn pk_reference_is_covered() {
        // The pk is carried by the entry's doc-id, so projecting it stays covered.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c._id FROM c WHERE c.status = 'active'",
                &["status"]
            ),
            Some(true)
        );
    }

    #[test]
    fn whole_row_projection_is_not_covered() {
        // `SELECT VALUE c` needs every field — must keep the document fetch.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c FROM c WHERE c.status = 'active'",
                &["status"]
            ),
            Some(false)
        );
        assert!(has_key_lookup(
            "SELECT VALUE c FROM c WHERE c.status = 'active'",
            &["status"]
        ));
    }

    #[test]
    fn unindexed_referenced_field_is_not_covered() {
        // `name` is read but isn't the index field or pk → fetch required.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c.name FROM c WHERE c.status = 'active'",
                &["status"]
            ),
            Some(false)
        );
    }

    #[test]
    fn deeper_path_is_not_covered() {
        // `c.status.x` navigates into the value; the scalar entry can't reproduce it.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c.status.x FROM c WHERE c.status = 'active'",
                &["status"]
            ),
            Some(false)
        );
    }

    #[test]
    fn covering_composes_with_a_retained_residual_filter() {
        // STARTSWITH keeps a residual Filter on the indexed field; it reads
        // `c.name` from the synthesized row, so the query is still covered.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c.name FROM c WHERE STARTSWITH(c.name, 'a')",
                &["name"]
            ),
            Some(true)
        );
    }

    #[test]
    fn computed_projection_over_only_the_indexed_field_is_covered() {
        // `UPPER(c.status)` reads only `status` (depth-1) — covered.
        assert_eq!(
            covering_flag(
                "SELECT VALUE UPPER(c.status) FROM c WHERE c.status = 'active'",
                &["status"]
            ),
            Some(true)
        );
    }

    #[test]
    fn aggregate_is_not_covered_in_phase_1() {
        // GROUP BY lowers to an Env-bound Aggregate → the pass bails (deferred to
        // the covered-aggregate phase).
        assert_eq!(
            covering_flag(
                "SELECT c.status, COUNT(1) FROM c WHERE c.status = 'active' GROUP BY c.status",
                &["status"]
            ),
            Some(false)
        );
    }

    // ── Dotted-path covering: exact-path-string match ────────────────────────

    #[test]
    fn dotted_indexed_path_projection_is_covered() {
        // Index on `user.id`; the query reads only `c.user.id`. Its reconstructed
        // path string-equals the index field → covered, KeyLookup dropped.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c.user.id FROM c WHERE c.user.id = 'x'",
                &["user.id"]
            ),
            Some(true)
        );
        assert!(!has_key_lookup(
            "SELECT VALUE c.user.id FROM c WHERE c.user.id = 'x'",
            &["user.id"]
        ));
    }

    #[test]
    fn dotted_pk_reference_alongside_dotted_index_is_covered() {
        // The pk (`_id`) rides on the entry's doc-id, so a covered dotted query
        // can also project it.
        assert_eq!(
            covering_flag(
                "SELECT c.user.id, c._id FROM c WHERE c.user.id = 'x'",
                &["user.id"]
            ),
            Some(true)
        );
    }

    #[test]
    fn parent_subdoc_of_dotted_index_is_not_covered() {
        // Projecting `c.user` (the whole subdoc) needs more than `user.id` carries
        // — the user called this case out explicitly.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c.user FROM c WHERE c.user.id = 'x'",
                &["user.id"]
            ),
            Some(false)
        );
        assert!(has_key_lookup(
            "SELECT VALUE c.user FROM c WHERE c.user.id = 'x'",
            &["user.id"]
        ));
    }

    #[test]
    fn extension_of_dotted_index_is_not_covered() {
        // `c.user.id.x` navigates past the scalar value the entry stores.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c.user.id.x FROM c WHERE c.user.id = 'x'",
                &["user.id"]
            ),
            Some(false)
        );
    }

    #[test]
    fn sibling_of_dotted_index_is_not_covered() {
        // `c.user.name` is a sibling path the `user.id` entry doesn't carry.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c.user.name FROM c WHERE c.user.id = 'x'",
                &["user.id"]
            ),
            Some(false)
        );
    }

    #[test]
    fn multikey_path_index_is_not_covered() {
        // A multikey (`tags.[]`) scan fans out under a Distinct; the array can't
        // be rebuilt from one scalar entry, so it keeps the fetch.
        assert_eq!(
            covering_flag(
                "SELECT VALUE c.tags FROM c WHERE ARRAY_CONTAINS(c.tags, 'x')",
                &["tags.[]"]
            ),
            Some(false)
        );
        assert!(has_key_lookup(
            "SELECT VALUE c.tags FROM c WHERE ARRAY_CONTAINS(c.tags, 'x')",
            &["tags.[]"]
        ));
    }

    // ── Compound covering (phase 2): same exact-path match, N components ──────

    /// A meta whose only indexes are the given compound indexes, each `&[&str]`
    /// its ordered component paths. The identity mirrors `join_index_fields`
    /// (components joined by `\x01`), so the lowered `CompoundIndexScan.field`
    /// matches what the covering pass resolves against `compound_indexes`.
    fn meta_compound(compound: &[&[&str]]) -> CollectionMeta {
        let compound_indexes = compound
            .iter()
            .map(|components| {
                let comps: Vec<String> = components.iter().map(|s| s.to_string()).collect();
                (comps.join("\u{1}"), comps)
            })
            .collect();
        CollectionMeta {
            indexes: Vec::new(),
            compound_indexes,
            pk_path: "_id".into(),
        }
    }

    /// Lower `sql` against `compound` and report the covering state of the first
    /// compound scan: `Some(true)` covered, `Some(false)` indexed-but-fetched,
    /// `None` no compound scan at all.
    fn compound_covering(sql: &str, compound: &[&[&str]]) -> Option<bool> {
        let Plan::Query(node) = lower(
            slate_sql::parse(sql).unwrap(),
            container(),
            &meta_compound(compound),
        ) else {
            panic!("lower always produces a Query");
        };
        fn find(node: &Node) -> Option<bool> {
            match node {
                Node::CompoundIndexScan { covering, .. } => Some(covering.is_some()),
                Node::KeyLookup { source, .. }
                | Node::Project { source, .. }
                | Node::Filter { source, .. }
                | Node::Sort { source, .. }
                | Node::Limit { source, .. }
                | Node::Distinct { source, .. }
                | Node::Aggregate { source, .. }
                | Node::Bind { source, .. } => find(source),
                _ => None,
            }
        }
        find(&node)
    }

    /// Whether a compound-indexed plan still contains the document fetch.
    fn compound_has_key_lookup(sql: &str, compound: &[&[&str]]) -> bool {
        let Plan::Query(node) = lower(
            slate_sql::parse(sql).unwrap(),
            container(),
            &meta_compound(compound),
        ) else {
            panic!("lower always produces a Query");
        };
        fn walk(node: &Node) -> bool {
            match node {
                Node::KeyLookup { .. } => true,
                Node::Project { source, .. }
                | Node::Filter { source, .. }
                | Node::Sort { source, .. }
                | Node::Limit { source, .. }
                | Node::Distinct { source, .. }
                | Node::Aggregate { source, .. }
                | Node::Bind { source, .. } => walk(source),
                _ => false,
            }
        }
        walk(&node)
    }

    #[test]
    fn compound_all_components_projection_is_covered() {
        // Reads both components of `(status, created_at)` → covered, no fetch.
        assert_eq!(
            compound_covering(
                "SELECT c.status, c.created_at FROM c WHERE c.status = 'active'",
                &[&["status", "created_at"]]
            ),
            Some(true)
        );
        assert!(!compound_has_key_lookup(
            "SELECT c.status, c.created_at FROM c WHERE c.status = 'active'",
            &[&["status", "created_at"]]
        ));
    }

    #[test]
    fn compound_leading_component_only_is_covered() {
        // A leftmost-prefix query that reads only the leading component is covered
        // (the entry carries every component regardless of which the query reads).
        assert_eq!(
            compound_covering(
                "SELECT VALUE c.status FROM c WHERE c.status = 'active'",
                &[&["status", "created_at"]]
            ),
            Some(true)
        );
    }

    #[test]
    fn compound_pk_reference_is_covered() {
        // `c._id` rides on the entry's doc-id, so a `SELECT VALUE c._id` over a
        // compound leading-prefix scan is covered.
        assert_eq!(
            compound_covering(
                "SELECT VALUE c._id FROM c WHERE c.status = 'active'",
                &[&["status", "created_at"]]
            ),
            Some(true)
        );
    }

    #[test]
    fn compound_unindexed_field_is_not_covered() {
        // `name` is neither a component nor the pk → the fetch stays.
        assert_eq!(
            compound_covering(
                "SELECT VALUE c.name FROM c WHERE c.status = 'active'",
                &[&["status", "created_at"]]
            ),
            Some(false)
        );
        assert!(compound_has_key_lookup(
            "SELECT VALUE c.name FROM c WHERE c.status = 'active'",
            &[&["status", "created_at"]]
        ));
    }

    #[test]
    fn compound_whole_row_is_not_covered() {
        // `SELECT VALUE c` needs every field — keep the fetch.
        assert_eq!(
            compound_covering(
                "SELECT VALUE c FROM c WHERE c.status = 'active'",
                &[&["status", "created_at"]]
            ),
            Some(false)
        );
    }

    #[test]
    fn compound_dotted_components_are_covered_by_exact_match() {
        // Dotted components covered by the same exact-path rule; `c.user.id`
        // string-equals component `user.id`.
        assert_eq!(
            compound_covering(
                "SELECT c.user.id, c.status FROM c WHERE c.user.id = 'x'",
                &[&["user.id", "status"]]
            ),
            Some(true)
        );
    }

    #[test]
    fn compound_parent_of_dotted_component_is_not_covered() {
        // Projecting the parent subdoc `c.user` needs more than the `user.id`
        // component carries — bail, exactly like single-field dotted covering.
        assert_eq!(
            compound_covering(
                "SELECT c.user, c.status FROM c WHERE c.user.id = 'x'",
                &[&["user.id", "status"]]
            ),
            Some(false)
        );
        assert!(compound_has_key_lookup(
            "SELECT c.user, c.status FROM c WHERE c.user.id = 'x'",
            &[&["user.id", "status"]]
        ));
    }
}
