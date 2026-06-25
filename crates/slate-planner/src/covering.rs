//! Covering-index analysis (RFC: Covering Index Scans, Part B — phase 1).
//!
//! A conservative post-pass over a lowered read [`Plan::Query`](crate::plan::Plan)
//! tree. When every field the query reads is the scanned index field or the
//! primary key, the document fetch is pure overhead: the index entry already
//! carries the value and the doc-id. This pass proves that condition for the
//! simplest shape — a single-field `IndexScan` under a `KeyLookup`, beneath a
//! linear chain of binding-aware nodes — and, when it holds, flips the scan to
//! [`covering`](crate::plan::Node::IndexScan::covering) and drops the
//! `KeyLookup`. The executor then synthesizes each row from the entry instead of
//! fetching the document.
//!
//! ## The one invariant
//!
//! *A missed cover is a lost optimization; a wrong cover is wrong results.* So
//! the analysis is one-directional: it returns **uncoverable** the instant it
//! sees any node, binding, or expression shape it does not fully understand. It
//! covers only when it can name every referenced field and show each is the
//! index field or the pk. Phase 1 deliberately handles only:
//!
//! - a **single** index source (`KeyLookup(IndexScan)`) — not `IndexMerge`, not a
//!   compound scan, not a multikey (`.[]`) scan (which lowers under a `Distinct`);
//! - a **top-level scalar** index field (no dot-path / `.[]` — the flat entry
//!   value can't reproduce nested navigation);
//! - **one alias**, bound by [`RowBinding::Alias`] — an [`RowBinding::Env`] row
//!   means a join / unwind / aggregate / subquery, all out of scope here;
//! - **depth-1** field references (`alias.field`) only — a bare `alias` (whole
//!   row), a deeper `alias.a.b`, an `alias[i]` index, a `PathGet`/`MultikeyEq`,
//!   or a correlated subquery on the alias all force uncoverable.
//!
//! Compound covering and covered aggregates are later phases (they add component
//! synthesis and the aggregate binding-shift, respectively); the RFC has the
//! full design.

use std::collections::BTreeSet;

use slate_ast::Expression;

use crate::plan::{Node, RowBinding};

/// Rewrite `node` so a provably-covered single-field index scan serves the query
/// from index entries (no `KeyLookup`).
///
/// Two phases, so a query that *can't* be covered — the common case — keeps its
/// plan untouched and pays only a cheap read-only walk: [`is_coverable`] decides
/// by reference (no allocation), and only a coverable plan is rebuilt by
/// [`rewrite`]. The optimization never taxes the paths it doesn't help.
pub(crate) fn apply(node: Node, pk_path: &str) -> Node {
    // The synthesized covering document carries the pk under its flat `pk_path`
    // key; a dotted pk would need a nested object the entry's scalar doc-id can't
    // reproduce, so a dotted pk is never coverable in phase 1.
    if pk_path.contains('.') {
        return node;
    }
    if is_coverable(&node, pk_path) {
        rewrite(node)
    } else {
        node
    }
}

/// Decide, without mutating or allocating beyond the analysis, whether `node` is
/// a coverable single-field-index plan.
fn is_coverable(node: &Node, pk_path: &str) -> bool {
    let mut analysis = Analysis {
        pk_path,
        alias: None,
        refs: Refs::Fields(BTreeSet::new()),
    };
    analyze(node, &mut analysis)
}

/// Accumulated coverage state while descending the wrapper chain by reference.
/// The alias is *borrowed* from the plan — the walk holds the tree, so no clone.
struct Analysis<'a> {
    pk_path: &'a str,
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
    fn covers(&self, field: &str) -> bool {
        // A dotted / `.[]` index field can't be rebuilt from the flat entry value.
        if field.contains('.') || self.alias.is_none() {
            return false;
        }
        match &self.refs {
            Refs::Uncoverable => false,
            Refs::Fields(fields) => fields.iter().all(|f| f == field || f == self.pk_path),
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

        // The source: coverable iff it is a single-field, non-covering index scan
        // and every collected reference is its field or the pk.
        Node::KeyLookup { source, .. } => match source.as_ref() {
            Node::IndexScan {
                field,
                covering: false,
                ..
            } => a.covers(field),
            _ => false,
        },

        // Anything else (Bind, Unwind, Aggregate, Subquery, Scan, a bare
        // IndexScan, CompoundIndexScan, IndexMerge, …) is not the coverable chain.
        _ => false,
    }
}

/// Rebuild a plan [`is_coverable`] has approved: drop the `KeyLookup` and flip its
/// single-field `IndexScan` to covering, leaving the rest of the spine intact.
/// Only ever called on a coverable plan, so the `KeyLookup(IndexScan)` is present.
fn rewrite(node: Node) -> Node {
    match node {
        Node::Limit { skip, take, source } => Node::Limit {
            skip,
            take,
            source: Box::new(rewrite(*source)),
        },
        Node::Distinct { source, flatten } => Node::Distinct {
            source: Box::new(rewrite(*source)),
            flatten,
        },
        Node::Project {
            expr,
            binding,
            source,
        } => Node::Project {
            expr,
            binding,
            source: Box::new(rewrite(*source)),
        },
        Node::Sort {
            keys,
            binding,
            source,
        } => Node::Sort {
            keys,
            binding,
            source: Box::new(rewrite(*source)),
        },
        Node::Filter {
            predicate,
            binding,
            source,
        } => Node::Filter {
            predicate,
            binding,
            source: Box::new(rewrite(*source)),
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
            // Defensive: a coverable plan always has the single-field scan here,
            // so this only guards against an unexpected shape — re-wrap intact.
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
        Expression::Member { base, field } => match base.as_ref() {
            // `alias.field` — a clean depth-1 scalar reference. Owning the field
            // name is intrinsic: we collect from a borrowed expression tree.
            Expression::Identifier(a) if a == alias => {
                if let Refs::Fields(fields) = refs {
                    fields.insert(field.clone());
                }
            }
            // `<alias-rooted>.field` (depth ≥ 2) — nested navigation the scalar
            // entry value can't reproduce.
            base if touches_alias(base, alias) => *refs = Refs::Uncoverable,
            // The base never touches the alias — nothing alias-related to collect.
            base => collect_alias_refs(base, alias, refs),
        },
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

/// Whether `alias` appears anywhere in `expr` — used to reject any alias access
/// that isn't a clean depth-1 member.
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
}
