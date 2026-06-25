//! Sargability: choosing an index-scan source and the residual predicate.
//!
//! The `WHERE` predicate is split: conjuncts that reduce to a point/range (or a
//! boolean merge of them) over some index's key order are *pushed* into an
//! index source (wrapped by `KeyLookup` to fetch documents), and primary-key
//! equality becomes a direct `KeyLookup`. Everything else stays a residual
//! `Filter`. A predicate that wraps the field in a computation
//! (`UPPER(c.x) = ...`, `c.x + 1 = ...`) is not sargable and falls through.
//!
//! ## The unified recogniser
//!
//! One entry point — [`sargable`] — recognises each indexable predicate shape
//! and returns a declarative [`IndexAccess`] (the index *decision*), which
//! [`lower_access`] turns into the physical `IndexScan` / `IndexMerge` /
//! `KeyLookup` nodes. Adding a sargable shape is "add an arm to `sargable`,"
//! not "thread a new path through the planner." [`plan_source`] orchestrates:
//! it owns conjunct splitting, the pk fast-path, the cross-conjunct range
//! combine ([`field_index_scan`]), and the residual/recheck bookkeeping (the
//! `(scan_node, residual)` contract) — and calls `sargable` for everything
//! else.
//!
//! Each arm also decides whether the conjunct is *consumed* (provably exact —
//! dropped from the recheck) or *retained* (the index narrows; the residual
//! `Filter` keeps the result precise) — see [`Residual`].

use bson::{Bson, RawBson};
use slate_ast::{BinOp, Expression, Literal};

use crate::plan::{
    CollectionRef, CompoundScanRange, CompoundScanTail, IndexScanRange, LogicalOp, Node,
    ScanDirection,
};

/// Index metadata for the queried collection, used to choose a scan source.
#[derive(Debug, Clone, Default)]
pub struct CollectionMeta {
    /// Single-field indexed paths (e.g. `"age"`, `"address.city"`, `"tags.[]"`).
    pub indexes: Vec<String>,
    /// Compound (multi-field) indexes as `(identity, components)` pairs: the
    /// engine's stored identity string (the join the executor scans by) and its
    /// ordered component paths (e.g. `["status", "created_at"]`, matched by the
    /// leftmost-prefix rule). The identity is passed through opaquely so the
    /// planner never has to know the join encoding.
    pub compound_indexes: Vec<(String, Vec<String>)>,
    /// Primary-key field path (e.g. `"_id"`).
    pub pk_path: String,
}

/// What an index can do for one predicate — the declarative decision the
/// recogniser returns and [`lower_access`] lowers into plan nodes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IndexAccess {
    /// Point or range scan on one scalar index field.
    Scan {
        field: String,
        range: IndexScanRange,
    },
    /// Leftmost-prefix scan on a compound index. `field` is the joined identity;
    /// `range` carries the equality prefix and the trailing predicate.
    Compound {
        field: String,
        range: CompoundScanRange,
    },
    /// A `.[]` element scan. Lowers to a *deduped* id stream — a multikey scan
    /// emits one id per matching element, so a repeated element would otherwise
    /// produce a duplicate result row the recheck can't remove.
    Multikey { field: String, value: Bson },
    /// Boolean combination of accesses: `And` → `IndexMerge(And)` (intersect),
    /// `Or` → `IndexMerge(Or)` (union). Both dedup doc-ids.
    Merge {
        op: LogicalOp,
        parts: Vec<IndexAccess>,
    },
}

/// Whether a recognised conjunct must stay as a residual recheck.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Residual {
    /// Provably exact — drop the conjunct from the recheck.
    Consumed,
    /// Keep the original predicate as a residual `Filter` (the index is a
    /// conservative superset; the recheck keeps results precise).
    Retained,
}

// ── Sargability ─────────────────────────────────────────────────

/// Decide the scan source and the residual (non-pushed) predicate.
///
/// - A top-level `OR` whose every branch is indexable → `IndexMerge(Or)` (with
///   the full predicate kept as a residual recheck).
/// - Otherwise the predicate is treated as a conjunction: pk equality wins
///   (direct key lookup); else each indexed field's atoms become one
///   `IndexScan` (range bounds combined), and the [unified recogniser](sargable)
///   handles the remaining shapes (Mongo eq, multikey eq, `ARRAY_CONTAINS`
///   family, `OR` sub-groups) — all intersected via `IndexMerge(And)` when more
///   than one applies. Consumed conjuncts leave the residual; the rest stay a
///   `Filter`.
pub(crate) fn plan_source(
    filter: Option<Expression>,
    alias: &str,
    container: &CollectionRef,
    meta: &CollectionMeta,
) -> (Node, Option<Expression>) {
    let Some(expr) = filter else {
        return (scan(container), None);
    };

    // Top-level OR → IndexMerge(Or) when fully indexable; recheck the full OR.
    // A whole-filter Mongo implicit-equality (`f = v OR ARRAY_CONTAINS(f, v)`)
    // is *not* a real disjunction — it is sargable as an equality on `f`, so it
    // falls through to the conjunction path below rather than taking this route.
    if matches!(expr, Expression::Binary { op: BinOp::Or, .. })
        && as_mongo_eq(&expr, alias).is_none()
    {
        return match sargable(&expr, alias, meta) {
            Some((access, _)) => (
                key_lookup(container, lower_access(&access, container)),
                Some(expr),
            ),
            None => (scan(container), Some(expr)),
        };
    }

    let mut conjuncts = Vec::new();
    flatten_and(expr, &mut conjuncts);

    // Priority 1: primary-key equality (a plain `Eq` or the Mongo idiom) →
    // direct point lookup. The pk is never an array, so the `ARRAY_CONTAINS`
    // branch is always false and the equality is exact (safe to consume).
    for i in 0..conjuncts.len() {
        if let Some(value) = pk_eq_value(&conjuncts[i], alias, &meta.pk_path)
            && let Some(id) = bson_to_raw(&value)
        {
            let source = Node::KeyLookup {
                collection: container.clone(),
                source: Box::new(Node::Values(vec![id])),
            };
            return (source, residual_excluding(conjuncts, &[i]));
        }
    }

    // Collect index accesses from conjuncts, tracking which are consumed.
    let mut accesses: Vec<IndexAccess> = Vec::new();
    let mut consumed: Vec<usize> = Vec::new();

    // Priority 2: compound indexes. Pick the one whose leftmost prefix covers the
    // most predicates (a longer prefix is strictly more selective than a single
    // field). The covered conjuncts are *consumed*: the executor's
    // `CompoundIndexScan` node already rechecks each leading equality and the
    // trailing range exactly against the index entry — dropping the byte seek's
    // conservative over-read — with the same coercing `compare_bson` comparator
    // `WHERE` uses. A residual `Filter` over the fetched document would re-check
    // the identical predicate, so it is pure redundancy. This mirrors single-field
    // `Eq`, whose in-scan recheck likewise consumes the atom (RFC: Covering Index
    // Scans & Engine-Level Recheck, Part A).
    if let Some((access, used)) = best_compound_scan(&conjuncts, alias, &meta.compound_indexes) {
        consumed.extend(used);
        accesses.push(access);
    }

    // Per indexed scalar field: combine its comparison atoms into a single
    // IndexScan (Eq wins; otherwise range bounds merge). These atoms are
    // consumed — the scan (with the executor's coercing post-filter) is exact.
    // Skip fields already claimed by the compound source (now in `consumed`).
    for field in &meta.indexes {
        if let Some((access, used)) = field_index_scan(&conjuncts, &consumed, alias, field) {
            accesses.push(access);
            consumed.extend(used);
        }
    }

    // Remaining conjuncts: the unified recogniser handles the non-atom shapes
    // (Mongo eq, multikey eq, the `ARRAY_CONTAINS` family, `OR` sub-groups).
    // Plain comparison atoms are the field-scan loop's domain above — skip them
    // so a same-field range left for the residual (`age = 41 AND age > 30`)
    // isn't picked up as a second source.
    for (i, conjunct) in conjuncts.iter().enumerate() {
        if consumed.contains(&i) || as_atom(conjunct, alias).is_some() {
            continue;
        }
        if let Some((access, residual)) = sargable(conjunct, alias, meta) {
            accesses.push(access);
            if matches!(residual, Residual::Consumed) {
                consumed.push(i);
            }
        }
    }

    let residual = residual_excluding(conjuncts, &consumed);
    let sources: Vec<Node> = accesses
        .iter()
        .map(|a| lower_access(a, container))
        .collect();
    match merge_sources(container, LogicalOp::And, sources) {
        Some(ids) => (key_lookup(container, ids), residual),
        None => (scan(container), residual),
    }
}

/// The unified recogniser: map one predicate to the [`IndexAccess`] that serves
/// it, plus whether the conjunct is [`Consumed`](Residual::Consumed) or
/// [`Retained`](Residual::Retained). `None` ⇒ not sargable ⇒ Filter.
///
/// One arm per recognised shape, dispatched in priority order; the default is
/// `None`. Used by [`plan_source`] for the non-atom conjuncts and recursively
/// for the branches of an `OR`.
fn sargable(
    pred: &Expression,
    alias: &str,
    meta: &CollectionMeta,
) -> Option<(IndexAccess, Residual)> {
    // Mongo implicit-equality idiom (`f = v OR ARRAY_CONTAINS(f, v)`) on a
    // scalar index → a consumed Eq. Checked *before* the generic OR arm so the
    // idiom is recognised whole, not split into its un-indexable
    // `ARRAY_CONTAINS` branch. A scalar index holds no array entries, so the
    // idiom is vacuously true over the candidate set — safe to consume.
    if let Some((field, value)) = as_mongo_eq(pred, alias) {
        return meta.indexes.contains(&field).then(|| {
            (
                IndexAccess::Scan {
                    field,
                    range: IndexScanRange::Eq(value),
                },
                Residual::Consumed,
            )
        });
    }

    // A disjunction → union of its branches; sargable only when *every* branch
    // is (the merge can't drop a branch's rows). Kept as a recheck.
    if let Expression::Binary {
        op: BinOp::Or,
        lhs,
        rhs,
    } = pred
    {
        let (l, _) = sargable(lhs, alias, meta)?;
        let (r, _) = sargable(rhs, alias, meta)?;
        return Some((merge(LogicalOp::Or, vec![l, r]), Residual::Retained));
    }

    // Explicit multikey equality `MultikeyEq` (Mongo `{f.[]: v}`) → a deduped
    // element scan on the verbatim `.[]` index, kept as a recheck.
    if let Some((field, value)) = as_multikey_eq(pred, alias) {
        return meta
            .indexes
            .contains(&field)
            .then_some((IndexAccess::Multikey { field, value }, Residual::Retained));
    }

    // The `ARRAY_CONTAINS` / `_ANY` / `_ALL` family (SQL) → multikey element
    // access on the derived `.[]` index, kept as a recheck.
    if let Some(access) = array_containment(pred, alias, meta) {
        return Some((access, Residual::Retained));
    }

    // `STRINGEQUALS(x, lit)` (2-arg) → a tight `Eq` seek on a scalar string
    // index. Consumed: it is plain `==`, so on a correct string index the seek is
    // exact — identical to `x = 'lit'` (which is also consumed). The string
    // value/doc-id boundary fix is what makes this sound.
    if let Some((field, value)) = as_string_eq(pred, alias) {
        return meta.indexes.contains(&field).then_some((
            IndexAccess::Scan {
                field,
                range: IndexScanRange::Eq(value),
            },
            Residual::Consumed,
        ));
    }

    // `STARTSWITH(x, "pre")` / `LIKE 'pre%'` (an anchored-prefix REGEXMATCH) → a
    // half-open `[pre, pre⁺)` range on a scalar string index. Retained: the byte
    // range can sweep cross-type entries whose sortable bytes share the prefix
    // (and a `LIKE` pattern's tail past the prefix), which the recheck drops.
    if let Some((field, prefix)) = as_string_prefix(pred, alias) {
        return meta.indexes.contains(&field).then_some((
            IndexAccess::Scan {
                field,
                range: IndexScanRange::StringPrefix(prefix),
            },
            Residual::Retained,
        ));
    }

    // A bare comparison atom on a scalar index → point/range scan. At the
    // conjunct level `plan_source` routes atoms through `field_index_scan`
    // (range-bound combining); this arm serves the OR-branch recursion above.
    if let Some((field, op, value)) = as_atom(pred, alias) {
        if !meta.indexes.contains(&field) {
            return None;
        }
        let (range, residual) = match op {
            BinOp::Eq => (IndexScanRange::Eq(value), Residual::Consumed),
            BinOp::Gt => (range_bound(Some((value, false)), None), Residual::Retained),
            BinOp::Gte => (range_bound(Some((value, true)), None), Residual::Retained),
            BinOp::Lt => (range_bound(None, Some((value, false))), Residual::Retained),
            BinOp::Lte => (range_bound(None, Some((value, true))), Residual::Retained),
            _ => return None,
        };
        return Some((IndexAccess::Scan { field, range }, residual));
    }

    None
}

/// Lower an [`IndexAccess`] into the id-yielding plan node `KeyLookup` consumes.
fn lower_access(access: &IndexAccess, container: &CollectionRef) -> Node {
    match access {
        IndexAccess::Scan { field, range } => index_scan(container, field, range.clone()),
        IndexAccess::Compound { field, range } => Node::CompoundIndexScan {
            collection: container.clone(),
            field: field.clone(),
            range: range.clone(),
            direction: ScanDirection::Forward,
            limit: None,
        },
        IndexAccess::Multikey { field, value } => dedup_ids(index_scan(
            container,
            field,
            IndexScanRange::Eq(value.clone()),
        )),
        IndexAccess::Merge { op, parts } => {
            let sources: Vec<Node> = parts.iter().map(|p| lower_access(p, container)).collect();
            // A `Merge` is always built with at least one part; the fallback is
            // unreachable but keeps this panic-free.
            merge_sources(container, *op, sources).unwrap_or_else(|| scan(container))
        }
    }
}

/// Build one `IndexScan` covering all of `field`'s atoms (`Eq` wins; otherwise
/// range bounds are combined across conjuncts). Returns the access and the
/// consumed conjunct indices, or `None` if `field` has no usable atom here.
fn field_index_scan(
    conjuncts: &[Expression],
    consumed: &[usize],
    alias: &str,
    field: &str,
) -> Option<(IndexAccess, Vec<usize>)> {
    let mut eq: Option<(Bson, usize)> = None;
    let mut lower: Option<(Bson, bool, usize)> = None;
    let mut upper: Option<(Bson, bool, usize)> = None;

    for (i, conjunct) in conjuncts.iter().enumerate() {
        if consumed.contains(&i) {
            continue;
        }
        let Some((f, op, value)) = as_atom(conjunct, alias) else {
            continue;
        };
        if f != field {
            continue;
        }
        match op {
            BinOp::Eq if eq.is_none() => eq = Some((value, i)),
            BinOp::Gt if lower.is_none() => lower = Some((value, false, i)),
            BinOp::Gte if lower.is_none() => lower = Some((value, true, i)),
            BinOp::Lt if upper.is_none() => upper = Some((value, false, i)),
            BinOp::Lte if upper.is_none() => upper = Some((value, true, i)),
            _ => {}
        }
    }

    if let Some((value, i)) = eq {
        // Eq is the most selective; leave any range atoms to the residual.
        return Some((
            IndexAccess::Scan {
                field: field.to_string(),
                range: IndexScanRange::Eq(value),
            },
            vec![i],
        ));
    }
    if lower.is_none() && upper.is_none() {
        return None;
    }
    let mut used = Vec::new();
    let lo = lower.map(|(v, incl, i)| {
        used.push(i);
        (v, incl)
    });
    let hi = upper.map(|(v, incl, i)| {
        used.push(i);
        (v, incl)
    });
    Some((
        IndexAccess::Scan {
            field: field.to_string(),
            range: range_bound(lo, hi),
        },
        used,
    ))
}

/// Choose the best compound index for the conjuncts: the one whose leftmost
/// prefix consumes the most predicates. Returns the access and the conjunct
/// indices it claims, or `None` if no compound index has a usable leading
/// component. Ties are broken by first declared (stable).
fn best_compound_scan(
    conjuncts: &[Expression],
    alias: &str,
    compound_indexes: &[(String, Vec<String>)],
) -> Option<(IndexAccess, Vec<usize>)> {
    let mut best: Option<(IndexAccess, Vec<usize>)> = None;
    for (identity, fields) in compound_indexes {
        // Pass an empty `consumed` — the compound source is chosen first, so no
        // prior index source has claimed any conjunct yet.
        if let Some((access, used)) = compound_index_scan(conjuncts, &[], alias, identity, fields) {
            let better = best.as_ref().is_none_or(|(_, b)| used.len() > b.len());
            if better {
                best = Some((access, used));
            }
        }
    }
    best
}

/// Build a leftmost-prefix [`IndexAccess::Compound`] for one compound index.
///
/// Walks the index's component fields left to right, consuming an equality atom
/// per field for as long as they chain. The first field with only a range atom
/// (no equality) becomes the trailing range and terminates the prefix — fields
/// after a range can't be sought. Returns the access and the consumed conjunct
/// indices, or `None` if the leading field has no usable atom (the leftmost-
/// prefix rule: the first component must be constrained).
///
/// Conjuncts already `consumed` by an earlier (higher-priority) source are
/// skipped, so two index sources never claim the same predicate.
fn compound_index_scan(
    conjuncts: &[Expression],
    consumed: &[usize],
    alias: &str,
    identity: &str,
    fields: &[String],
) -> Option<(IndexAccess, Vec<usize>)> {
    let mut eq_prefix: Vec<Bson> = Vec::new();
    let mut used: Vec<usize> = Vec::new();
    let mut tail = CompoundScanTail::Unbounded;

    for field in fields {
        // Gather this component's atoms (skipping ones already consumed or used
        // by an earlier component in this same index).
        let mut eq: Option<(Bson, usize)> = None;
        let mut lower: Option<(Bson, bool, usize)> = None;
        let mut upper: Option<(Bson, bool, usize)> = None;
        for (i, conjunct) in conjuncts.iter().enumerate() {
            if consumed.contains(&i) || used.contains(&i) {
                continue;
            }
            let Some((f, op, value)) = as_atom(conjunct, alias) else {
                continue;
            };
            if f != *field {
                continue;
            }
            match op {
                BinOp::Eq if eq.is_none() => eq = Some((value, i)),
                BinOp::Gt if lower.is_none() => lower = Some((value, false, i)),
                BinOp::Gte if lower.is_none() => lower = Some((value, true, i)),
                BinOp::Lt if upper.is_none() => upper = Some((value, false, i)),
                BinOp::Lte if upper.is_none() => upper = Some((value, true, i)),
                _ => {}
            }
        }

        if let Some((value, i)) = eq {
            // Equality extends the prefix; keep walking to the next component.
            eq_prefix.push(value);
            used.push(i);
            continue;
        }
        if lower.is_some() || upper.is_some() {
            // A range on this component is the trailing predicate — the prefix
            // stops here (later components can't be sought).
            let lo = lower.map(|(v, incl, i)| {
                used.push(i);
                (v, incl)
            });
            let hi = upper.map(|(v, incl, i)| {
                used.push(i);
                (v, incl)
            });
            tail = CompoundScanTail::Range {
                lower: lo,
                upper: hi,
            };
        }
        // No usable atom on this component → the prefix ends before it.
        break;
    }

    // The leftmost-prefix rule: the leading component must be constrained, so we
    // need at least one equality (or a range that became the tail on field 0).
    if eq_prefix.is_empty() && matches!(tail, CompoundScanTail::Unbounded) {
        return None;
    }

    Some((
        IndexAccess::Compound {
            field: identity.to_string(),
            range: CompoundScanRange { eq_prefix, tail },
        },
        used,
    ))
}

/// Recognise the SQL `ARRAY_CONTAINS` / `_ANY` / `_ALL` shapes over a
/// `.[]`-indexed path. `None` (⇒ Filter) unless the form is the indexable one:
/// the array argument is a member path with a registered `.[]` index and every
/// needle is a scalar literal (not 3-arg partial, not a non-scalar/non-literal
/// needle). `_ANY` → `Merge(Or)`, `_ALL` → `Merge(And)` of per-value scans.
fn array_containment(pred: &Expression, alias: &str, meta: &CollectionMeta) -> Option<IndexAccess> {
    let Expression::Function { name, args } = pred else {
        return None;
    };
    match name.to_ascii_uppercase().as_str() {
        // 2-arg scalar-literal form only — the optional 3rd `partial` arg makes
        // the match broader than the indexed elements, so it stays a Filter.
        "ARRAY_CONTAINS" => {
            if args.len() != 2 {
                return None;
            }
            let field = multikey_index_for(&args[0], alias, meta)?;
            let value = scalar_needle(&args[1])?;
            Some(IndexAccess::Multikey { field, value })
        }
        // `_ANY` → union, `_ALL` → intersection of per-value element scans. Any
        // non-scalar/non-literal needle disqualifies the *whole* predicate.
        name @ ("ARRAY_CONTAINS_ANY" | "ARRAY_CONTAINS_ALL") => {
            if args.len() < 2 {
                return None;
            }
            let field = multikey_index_for(&args[0], alias, meta)?;
            let mut parts = Vec::with_capacity(args.len() - 1);
            for needle in &args[1..] {
                parts.push(IndexAccess::Multikey {
                    field: field.clone(),
                    value: scalar_needle(needle)?,
                });
            }
            let op = if name == "ARRAY_CONTAINS_ANY" {
                LogicalOp::Or
            } else {
                LogicalOp::And
            };
            Some(merge(op, parts))
        }
        _ => None,
    }
}

/// The `.[]` index name for an `ARRAY_CONTAINS(alias.<path>, …)` array argument:
/// SQL carries the bare path `tags`, the index is registered as `tags.[]`. Some
/// only when that derived index exists.
fn multikey_index_for(arr: &Expression, alias: &str, meta: &CollectionMeta) -> Option<String> {
    let index = format!("{}.[]", path_of(arr, alias)?);
    meta.indexes.contains(&index).then_some(index)
}

/// A literal needle that the sparse element index can actually answer — only the
/// scalar types it stores (`bson_value.rs`). A `null`/object/array needle leaves
/// no index entry, so pushing it down would be a false negative the recheck
/// can't repair; those stay a Filter.
fn scalar_needle(expr: &Expression) -> Option<Bson> {
    let value = as_literal(expr)?;
    is_indexable_scalar(&value).then_some(value)
}

fn is_indexable_scalar(b: &Bson) -> bool {
    matches!(
        b,
        Bson::String(_)
            | Bson::Int32(_)
            | Bson::Int64(_)
            | Bson::Double(_)
            | Bson::Boolean(_)
            | Bson::DateTime(_)
            | Bson::ObjectId(_)
    )
}

/// Combine [`IndexAccess`]es under `op`, flattening any nested same-op merge so
/// an `OR`/`IN` chain (or `_ANY`/`_ALL` value list) builds one flat union/
/// intersection.
fn merge(op: LogicalOp, parts: Vec<IndexAccess>) -> IndexAccess {
    let mut flat = Vec::with_capacity(parts.len());
    for part in parts {
        match part {
            IndexAccess::Merge {
                op: inner,
                parts: inner_parts,
            } if inner == op => flat.extend(inner_parts),
            other => flat.push(other),
        }
    }
    IndexAccess::Merge { op, parts: flat }
}

fn range_bound(lower: Option<(Bson, bool)>, upper: Option<(Bson, bool)>) -> IndexScanRange {
    IndexScanRange::Range { lower, upper }
}

/// Fold ID sources into an `IndexMerge` tree (`None` if empty, the source
/// itself if a single one).
fn merge_sources(
    container: &CollectionRef,
    logical: LogicalOp,
    sources: Vec<Node>,
) -> Option<Node> {
    let mut iter = sources.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, node| Node::IndexMerge {
        collection: container.clone(),
        logical,
        lhs: Box::new(acc),
        rhs: Box::new(node),
    }))
}

pub(crate) fn scan(container: &CollectionRef) -> Node {
    Node::Scan {
        collection: container.clone(),
    }
}

fn index_scan(container: &CollectionRef, field: &str, range: IndexScanRange) -> Node {
    Node::IndexScan {
        collection: container.clone(),
        field: field.to_string(),
        range,
        direction: ScanDirection::Forward,
        limit: None,
    }
}

/// Deduplicate an id stream by value identity before `KeyLookup`. A `.[]`
/// element scan emits one id per matching element, so a doc with a repeated
/// matching element (`tags: ["db","db"]`) yields its id more than once;
/// `KeyLookup` does not dedup, so without this the duplicate ids become
/// duplicate result rows — which the residual recheck can't remove (it filters
/// values, not multiplicities). Reuses the streaming `Distinct` dedup (ids are
/// bare scalars; `flatten: false` treats each as one whole value).
fn dedup_ids(source: Node) -> Node {
    Node::Distinct {
        source: Box::new(source),
        flatten: false,
    }
}

/// Wrap an ID-yielding source in a `KeyLookup` to fetch the documents.
fn key_lookup(container: &CollectionRef, ids: Node) -> Node {
    Node::KeyLookup {
        collection: container.clone(),
        source: Box::new(ids),
    }
}

/// Interpret a conjunct as `alias.<path> <cmp> <literal>` (either operand
/// order), returning the field path, comparison op, and literal value.
fn as_atom(expr: &Expression, alias: &str) -> Option<(String, BinOp, Bson)> {
    let Expression::Binary { op, lhs, rhs } = expr else {
        return None;
    };
    if !is_comparison(*op) {
        return None;
    }
    let (path, op, lit) = if let (Some(path), Some(lit)) =
        (path_of(lhs.as_ref(), alias), as_literal(rhs.as_ref()))
    {
        (path, *op, lit)
    } else if let (Some(lit), Some(path)) = (as_literal(lhs.as_ref()), path_of(rhs.as_ref(), alias))
    {
        (path, flip(*op), lit)
    } else {
        return None;
    };
    // A sparse index stores only scalar non-null values (`is_indexable_scalar`), so a
    // comparison against a null / non-scalar literal can never be answered from it —
    // e.g. `x = null` would scan an index that by construction holds no nulls,
    // returning the wrong rows (the complement). Not sargable → falls back to a
    // Filter, which evaluates it correctly.
    is_indexable_scalar(&lit).then_some((path, op, lit))
}

// ── Mongo idiom recognition ─────────────────────────────────────
//
// The find front-end has no `=`; it emits `{field: v}` as the disjunction
// `field = v OR ARRAY_CONTAINS(field, v)` and a `.[]` match as `MultikeyEq`.
// These recognizers let the sargability pass above treat those as a plain
// indexed equality rather than an un-indexable `OR`/function call.

/// Recognize the Mongo implicit-equality idiom the find front-end emits:
/// `alias.field = lit OR ARRAY_CONTAINS(alias.field, lit)` (same field, same
/// literal). Returns the field path and value — it is sargable as an equality
/// on `field`, because an index/pk lookup for `lit` finds both the
/// scalar-equal and the array-containing documents.
fn as_mongo_eq(expr: &Expression, alias: &str) -> Option<(String, Bson)> {
    let Expression::Binary {
        op: BinOp::Or,
        lhs,
        rhs,
    } = expr
    else {
        return None;
    };
    // lhs: `field = lit`
    let (eq_field, BinOp::Eq, eq_val) = as_atom(lhs.as_ref(), alias)? else {
        return None;
    };
    // rhs: `ARRAY_CONTAINS(field, lit)` over the same field and value
    let (ac_field, ac_val) = as_array_contains(rhs.as_ref(), alias)?;
    if ac_field == eq_field && ac_val == eq_val {
        Some((eq_field, eq_val))
    } else {
        None
    }
}

/// Interpret `ARRAY_CONTAINS(alias.<path>, <literal>)`, returning the field
/// path and the literal value.
fn as_array_contains(expr: &Expression, alias: &str) -> Option<(String, Bson)> {
    let Expression::Function { name, args } = expr else {
        return None;
    };
    if !name.eq_ignore_ascii_case("ARRAY_CONTAINS") || args.len() != 2 {
        return None;
    }
    Some((path_of(&args[0], alias)?, as_literal(&args[1])?))
}

/// Interpret `STRINGEQUALS(alias.<path>, <string-literal>)` — the 2-arg,
/// case-sensitive form. The optional 3rd `ignoreCase` arg makes the match
/// case-insensitive, which a case-sensitive index can't bound, so it is excluded
/// (falls back to a `Filter`). `stringequals.rs` is plain `==`, and the literal
/// must be a string — the function is `undefined` for a non-string operand,
/// which a scalar string index never matches anyway.
fn as_string_eq(expr: &Expression, alias: &str) -> Option<(String, Bson)> {
    let Expression::Function { name, args } = expr else {
        return None;
    };
    if !name.eq_ignore_ascii_case("STRINGEQUALS") || args.len() != 2 {
        return None;
    }
    let field = path_of(&args[0], alias)?;
    let value = as_literal(&args[1])?;
    matches!(value, Bson::String(_)).then_some((field, value))
}

/// Recognise the two prefix-predicate shapes over `alias.<path>`, returning the
/// field path and the non-empty literal prefix:
///
/// - `STARTSWITH(x, "pre")` — 2-arg, case-sensitive (the 3-arg `ignoreCase` form
///   a case-sensitive index can't bound).
/// - `REGEXMATCH(x, "^pre…")` — the anchored-literal form `LIKE 'pre%'` desugars
///   to before the planner (`%`→`.*`, `_`→`.`, `^…$`-anchored, case-sensitive).
///
/// Both feed the same `StringPrefix` range — see the RFC's prefix-range proof.
fn as_string_prefix(expr: &Expression, alias: &str) -> Option<(String, String)> {
    let Expression::Function { name, args } = expr else {
        return None;
    };
    let (field, prefix) = match name.to_ascii_uppercase().as_str() {
        "STARTSWITH" if args.len() == 2 => {
            let field = path_of(&args[0], alias)?;
            let Bson::String(p) = as_literal(&args[1])? else {
                return None;
            };
            (field, p)
        }
        "REGEXMATCH" if args.len() == 2 => {
            let field = path_of(&args[0], alias)?;
            let Bson::String(pat) = as_literal(&args[1])? else {
                return None;
            };
            (field, regex_literal_prefix(&pat)?)
        }
        _ => return None,
    };
    // An empty prefix means "every string" — a full scan, not an index range.
    (!prefix.is_empty()).then_some((field, prefix))
}

/// The leading literal prefix of an anchored regex (`^pre.*$` → `"pre"`), or
/// `None` when the pattern is not `^`-anchored or has no literal prefix (`^.*…`,
/// or a leading `(?i)` case-insensitive flag — which never starts with `^`).
/// Stops at the first *unescaped* regex metacharacter; `\<meta>` contributes the
/// literal character (matching `like_to_regex`'s escaping). A trailing lone `\`
/// disqualifies (returns `None`).
fn regex_literal_prefix(pattern: &str) -> Option<String> {
    let mut chars = pattern.strip_prefix('^')?.chars();
    let mut prefix = String::new();
    while let Some(c) = chars.next() {
        match c {
            '\\' => prefix.push(chars.next()?),
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' => break,
            other => prefix.push(other),
        }
    }
    (!prefix.is_empty()).then_some(prefix)
}

/// Recognize a [`Expression::MultikeyEq`] on `alias` — explicit multikey
/// equality the find front-end emits for a `.[]` path. Returns the verbatim
/// `.[]` path (which is also the index name) and the literal value, so it can
/// be matched to a multikey index.
fn as_multikey_eq(expr: &Expression, alias: &str) -> Option<(String, Bson)> {
    let Expression::MultikeyEq {
        base,
        index_path,
        value,
    } = expr
    else {
        return None;
    };
    if !matches!(base.as_ref(), Expression::Identifier(a) if a == alias) {
        return None;
    }
    let value = as_literal(value)?;
    // Same sparse-index reasoning as `scalar_needle`/`as_atom`: a null/non-scalar
    // element is never indexed, so `{x.[]: null}` can't be answered from the element
    // index. Not sargable → Filter.
    is_indexable_scalar(&value).then_some((index_path.clone(), value))
}

/// The value of a primary-key equality on `pk` — a plain `Eq` atom or the Mongo
/// idiom (whose `ARRAY_CONTAINS` branch is vacuous for a non-array pk).
fn pk_eq_value(expr: &Expression, alias: &str, pk: &str) -> Option<Bson> {
    if let Some((field, BinOp::Eq, value)) = as_atom(expr, alias)
        && field == pk
    {
        return Some(value);
    }
    match as_mongo_eq(expr, alias) {
        Some((field, value)) if field == pk => Some(value),
        _ => None,
    }
}

/// The dotted field path of an `alias.a.b.c` access (`None` for bare `alias`,
/// computed expressions, or a different root).
fn path_of(expr: &Expression, alias: &str) -> Option<String> {
    match expr {
        Expression::Member { base, field } => match base.as_ref() {
            Expression::Identifier(a) if a == alias => Some(field.clone()),
            other => path_of(other, alias).map(|p| format!("{p}.{field}")),
        },
        _ => None,
    }
}

fn as_literal(expr: &Expression) -> Option<Bson> {
    match expr {
        Expression::Literal(lit) => Some(literal_to_bson(lit)),
        // A materialized value preserves its exact BSON type — important here,
        // since an index bound must match the stored key's numeric type.
        Expression::Value(b) => Some(b.clone()),
        _ => None,
    }
}

fn literal_to_bson(lit: &Literal) -> Bson {
    match lit {
        Literal::Null => Bson::Null,
        Literal::Bool(b) => Bson::Boolean(*b),
        Literal::Int(i) => Bson::Int64(*i),
        Literal::Float(f) => Bson::Double(*f),
        Literal::Str(s) => Bson::String(s.clone()),
    }
}

// Explicit match (not `.ok()`) per the crate's no-silent-discard convention; a
// non-representable value simply skips the index optimization.
#[allow(clippy::manual_ok_err)]
fn bson_to_raw(value: &Bson) -> Option<RawBson> {
    match RawBson::try_from(value.clone()) {
        Ok(raw) => Some(raw),
        Err(_) => None, // unrepresentable → skip the optimization
    }
}

fn is_comparison(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::Eq | BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte
    )
}

/// Flip a comparison so the field is on the left (`40 < c.age` → `c.age > 40`).
fn flip(op: BinOp) -> BinOp {
    match op {
        BinOp::Gt => BinOp::Lt,
        BinOp::Gte => BinOp::Lte,
        BinOp::Lt => BinOp::Gt,
        BinOp::Lte => BinOp::Gte,
        other => other,
    }
}

fn flatten_and(expr: Expression, out: &mut Vec<Expression>) {
    match expr {
        Expression::Binary {
            op: BinOp::And,
            lhs,
            rhs,
        } => {
            flatten_and(*lhs, out);
            flatten_and(*rhs, out);
        }
        other => out.push(other),
    }
}

/// Rebuild an `AND` from the conjuncts not in `used`, or `None` if none remain.
fn residual_excluding(conjuncts: Vec<Expression>, used: &[usize]) -> Option<Expression> {
    let remaining: Vec<Expression> = conjuncts
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !used.contains(i))
        .map(|(_, c)| c)
        .collect();
    rebuild_and(remaining)
}

fn rebuild_and(conjuncts: Vec<Expression>) -> Option<Expression> {
    let mut iter = conjuncts.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, e| Expression::Binary {
        op: BinOp::And,
        lhs: Box::new(acc),
        rhs: Box::new(e),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(indexes: &[&str]) -> CollectionMeta {
        CollectionMeta {
            indexes: indexes.iter().map(|s| s.to_string()).collect(),
            compound_indexes: Vec::new(),
            pk_path: "_id".into(),
        }
    }

    /// A `CollectionMeta` with compound indexes — each given as its ordered
    /// component fields; the identity is the components joined by `\x01`.
    fn meta_compound(compound: &[&[&str]]) -> CollectionMeta {
        let compound_indexes = compound
            .iter()
            .map(|fields| {
                let components: Vec<String> = fields.iter().map(|s| s.to_string()).collect();
                (components.join("\u{1}"), components)
            })
            .collect();
        CollectionMeta {
            indexes: Vec::new(),
            compound_indexes,
            pk_path: "_id".into(),
        }
    }

    fn parse_where(sql: &str) -> Expression {
        slate_sql::parse(sql)
            .unwrap()
            .filter
            .expect("query has a WHERE")
    }

    /// Recognise the single-predicate `WHERE` of `sql` against `indexes`.
    fn recognise(sql: &str, indexes: &[&str]) -> Option<(IndexAccess, Residual)> {
        sargable(&parse_where(sql), "c", &meta(indexes))
    }

    /// Plan the `WHERE` of `sql` against `compound` indexes and return the chosen
    /// compound access (if any).
    fn plan_compound(sql: &str, compound: &[&[&str]]) -> Option<(IndexAccess, Vec<usize>)> {
        let mut conjuncts = Vec::new();
        flatten_and(parse_where(sql), &mut conjuncts);
        best_compound_scan(&conjuncts, "c", &meta_compound(compound).compound_indexes)
    }

    /// Plan the `WHERE` of `sql` against `compound` indexes and return the residual
    /// `Filter` expression `plan_source` leaves behind (`None` when every conjunct
    /// is consumed).
    fn plan_residual(sql: &str, compound: &[&[&str]]) -> Option<Expression> {
        let container = CollectionRef {
            cf: "default_cf".into(),
            collection: "c".into(),
        };
        let (_, residual) = plan_source(
            Some(parse_where(sql)),
            "c",
            &container,
            &meta_compound(compound),
        );
        residual
    }

    /// A compound index that covers every conjunct (a leading equality plus a
    /// trailing range) leaves no residual `Filter`: the executor's
    /// `CompoundIndexScan` rechecks both exactly against the index entry, so a
    /// document-level recheck would be redundant (RFC Part A).
    #[test]
    fn compound_scan_consumes_covered_conjuncts() {
        let residual = plan_residual(
            "SELECT VALUE c FROM c WHERE c.status = 'active' AND c.created_at > 5",
            &[&["status", "created_at"]],
        );
        assert!(
            residual.is_none(),
            "fully-covered compound query kept a redundant residual: {residual:?}"
        );
    }

    /// Only the compound-covered conjuncts are consumed: a predicate on a field
    /// outside the index stays in the residual `Filter`, while the covered
    /// equality is dropped.
    #[test]
    fn compound_scan_keeps_uncovered_conjunct_in_residual() {
        let residual = plan_residual(
            "SELECT VALUE c FROM c WHERE c.status = 'active' AND c.note = 'keep'",
            &[&["status", "created_at"]],
        )
        .expect("the uncovered `note` predicate must remain");
        let shown = format!("{residual:?}");
        assert!(
            shown.contains("note"),
            "residual should keep `note`: {shown}"
        );
        assert!(
            !shown.contains("status"),
            "residual should drop the compound-covered `status`: {shown}"
        );
    }

    #[test]
    fn array_contains_derives_multikey_index_name() {
        let (access, residual) = recognise(
            "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, 'x')",
            &["tags.[]"],
        )
        .expect("sargable");
        assert_eq!(
            access,
            IndexAccess::Multikey {
                field: "tags.[]".into(),
                value: Bson::String("x".into()),
            }
        );
        // Retained as a recheck (the element index is a superset).
        assert_eq!(residual, Residual::Retained);
    }

    #[test]
    fn array_contains_without_index_is_not_sargable() {
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, 'x')",
                &[]
            )
            .is_none()
        );
    }

    #[test]
    fn array_contains_partial_arg_is_not_sargable() {
        // The 3-arg partial form matches more than the indexed elements.
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, 'x', true)",
                &["tags.[]"]
            )
            .is_none()
        );
    }

    #[test]
    fn array_contains_non_literal_needle_is_not_sargable() {
        // A field-reference needle can't be turned into an index bound.
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, c.other)",
                &["tags.[]"]
            )
            .is_none()
        );
    }

    #[test]
    fn array_contains_non_scalar_needle_is_not_sargable() {
        // An object needle leaves no scalar element entry to probe.
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, {\"a\": 1})",
                &["tags.[]"]
            )
            .is_none()
        );
    }

    #[test]
    fn array_contains_any_unions_per_value_multikeys() {
        let (access, _) = recognise(
            "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, 'a', 'b')",
            &["tags.[]"],
        )
        .expect("sargable");
        assert_eq!(
            access,
            IndexAccess::Merge {
                op: LogicalOp::Or,
                parts: vec![
                    IndexAccess::Multikey {
                        field: "tags.[]".into(),
                        value: Bson::String("a".into())
                    },
                    IndexAccess::Multikey {
                        field: "tags.[]".into(),
                        value: Bson::String("b".into())
                    },
                ],
            }
        );
    }

    #[test]
    fn array_contains_all_intersects_per_value_multikeys() {
        let (access, _) = recognise(
            "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, 'a', 'b')",
            &["tags.[]"],
        )
        .expect("sargable");
        let IndexAccess::Merge { op, parts } = access else {
            panic!("expected a merge");
        };
        assert_eq!(op, LogicalOp::And);
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn array_contains_any_with_non_scalar_value_filters_whole() {
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, 'a', {\"x\": 1})",
                &["tags.[]"]
            )
            .is_none()
        );
    }

    #[test]
    fn array_contains_numeric_needle_keeps_its_type() {
        // The Int64 literal stays Int64 so the executor's numeric Eq scan
        // sweeps cross-numeric-type elements (`7` vs `7.0`).
        let (access, _) = recognise(
            "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, 7)",
            &["tags.[]"],
        )
        .expect("sargable");
        assert_eq!(
            access,
            IndexAccess::Multikey {
                field: "tags.[]".into(),
                value: Bson::Int64(7),
            }
        );
    }

    // ── Increment C: STRINGEQUALS → Eq ──────────────────────────

    #[test]
    fn string_equals_is_an_eq_seek_consumed() {
        let (access, residual) = recognise(
            "SELECT VALUE c FROM c WHERE STRINGEQUALS(c.name, 'acme')",
            &["name"],
        )
        .expect("sargable");
        assert_eq!(
            access,
            IndexAccess::Scan {
                field: "name".into(),
                range: IndexScanRange::Eq(Bson::String("acme".into())),
            }
        );
        // Plain `==` on a correct string index is exact — drop the recheck.
        assert_eq!(residual, Residual::Consumed);
    }

    #[test]
    fn string_equals_without_index_is_not_sargable() {
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE STRINGEQUALS(c.name, 'acme')",
                &[]
            )
            .is_none()
        );
    }

    #[test]
    fn string_equals_ignore_case_is_not_sargable() {
        // The 3-arg case-insensitive form can't be bounded by a case-sensitive
        // index.
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE STRINGEQUALS(c.name, 'acme', true)",
                &["name"]
            )
            .is_none()
        );
    }

    #[test]
    fn string_equals_non_string_literal_is_not_sargable() {
        // STRINGEQUALS is undefined for a non-string operand; never push it down.
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE STRINGEQUALS(c.name, 5)",
                &["name"]
            )
            .is_none()
        );
    }

    #[test]
    fn string_equals_non_literal_arg_is_not_sargable() {
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE STRINGEQUALS(c.name, c.other)",
                &["name"]
            )
            .is_none()
        );
    }

    // ── Increment B: STARTSWITH / LIKE → prefix range ───────────

    fn assert_prefix(sql: &str, prefix: &str) {
        let (access, residual) = recognise(sql, &["name"]).expect("sargable");
        assert_eq!(
            access,
            IndexAccess::Scan {
                field: "name".into(),
                range: IndexScanRange::StringPrefix(prefix.into()),
            },
            "`{sql}`"
        );
        // The byte range can over-return (cross-type, LIKE tail) → keep the recheck.
        assert_eq!(residual, Residual::Retained, "`{sql}`");
    }

    #[test]
    fn startswith_is_a_prefix_range_retained() {
        assert_prefix("SELECT VALUE c FROM c WHERE STARTSWITH(c.name, 'al')", "al");
    }

    #[test]
    fn like_prefix_is_a_prefix_range() {
        // `LIKE 'al%'` desugars to REGEXMATCH(c.name, "^al.*$") before the planner.
        assert_prefix("SELECT VALUE c FROM c WHERE c.name LIKE 'al%'", "al");
    }

    #[test]
    fn like_escaped_metachar_in_prefix() {
        // `LIKE 'a.b%'`: the `.` is a LIKE literal, escaped to `\.` in the regex,
        // so the extracted prefix keeps the literal dot.
        assert_prefix("SELECT VALUE c FROM c WHERE c.name LIKE 'a.b%'", "a.b");
    }

    #[test]
    fn anchored_regexmatch_is_a_prefix_range() {
        assert_prefix(
            "SELECT VALUE c FROM c WHERE REGEXMATCH(c.name, '^al')",
            "al",
        );
    }

    #[test]
    fn startswith_without_index_is_not_sargable() {
        assert!(recognise("SELECT VALUE c FROM c WHERE STARTSWITH(c.name, 'al')", &[]).is_none());
    }

    #[test]
    fn startswith_ignore_case_is_not_sargable() {
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE STARTSWITH(c.name, 'al', true)",
                &["name"]
            )
            .is_none()
        );
    }

    #[test]
    fn startswith_empty_prefix_is_not_sargable() {
        // The empty prefix matches every string — a full scan, not an index range.
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE STARTSWITH(c.name, '')",
                &["name"]
            )
            .is_none()
        );
    }

    #[test]
    fn like_leading_wildcard_is_not_sargable() {
        // `LIKE '%al'` → REGEXMATCH(c.name, "^.*al$") has no literal prefix.
        assert!(recognise("SELECT VALUE c FROM c WHERE c.name LIKE '%al'", &["name"]).is_none());
    }

    #[test]
    fn case_insensitive_regex_is_not_sargable() {
        // `(?i)` prefixes the anchor, so the pattern isn't `^`-anchored → no prefix.
        assert!(
            recognise(
                "SELECT VALUE c FROM c WHERE REGEXMATCH(c.name, '(?i)^al')",
                &["name"]
            )
            .is_none()
        );
    }

    #[test]
    fn regex_literal_prefix_extraction() {
        assert_eq!(regex_literal_prefix("^abc.*$").as_deref(), Some("abc"));
        assert_eq!(regex_literal_prefix("^a\\.b.*$").as_deref(), Some("a.b"));
        assert_eq!(regex_literal_prefix("^abc$").as_deref(), Some("abc"));
        assert_eq!(regex_literal_prefix("^.*x$"), None); // empty literal run
        assert_eq!(regex_literal_prefix("abc"), None); // not anchored
        assert_eq!(regex_literal_prefix("(?i)^ad"), None); // case-insensitive
        assert_eq!(regex_literal_prefix("^\\"), None); // trailing lone backslash
    }

    // ── Compound index selection ────────────────────────────────

    fn compound_access(sql: &str, compound: &[&[&str]]) -> (CompoundScanRange, Vec<usize>) {
        let (access, used) = plan_compound(sql, compound).expect("a compound access");
        let IndexAccess::Compound { range, .. } = access else {
            panic!("expected a compound access");
        };
        (range, used)
    }

    #[test]
    fn compound_two_equalities_consumes_both() {
        let (range, used) = compound_access(
            "SELECT VALUE c FROM c WHERE c.status = 'active' AND c.created_at = 5",
            &[&["status", "created_at"]],
        );
        assert_eq!(
            range.eq_prefix,
            vec![Bson::String("active".into()), Bson::Int64(5)]
        );
        assert!(matches!(range.tail, CompoundScanTail::Unbounded));
        assert_eq!(used.len(), 2);
    }

    #[test]
    fn compound_eq_then_range_uses_prefix_and_tail() {
        let (range, used) = compound_access(
            "SELECT VALUE c FROM c WHERE c.status = 'active' AND c.created_at > 5",
            &[&["status", "created_at"]],
        );
        assert_eq!(range.eq_prefix, vec![Bson::String("active".into())]);
        assert_eq!(
            range.tail,
            CompoundScanTail::Range {
                lower: Some((Bson::Int64(5), false)),
                upper: None,
            }
        );
        assert_eq!(used.len(), 2);
    }

    #[test]
    fn compound_leading_field_only_is_a_prefix_scan() {
        // Only the leading field is constrained → a single-equality prefix, tail
        // unbounded (the leftmost-prefix rule lets a compound index serve `{a}`).
        let (range, used) = compound_access(
            "SELECT VALUE c FROM c WHERE c.status = 'active'",
            &[&["status", "created_at"]],
        );
        assert_eq!(range.eq_prefix, vec![Bson::String("active".into())]);
        assert!(matches!(range.tail, CompoundScanTail::Unbounded));
        assert_eq!(used.len(), 1);
    }

    #[test]
    fn compound_non_leading_field_alone_is_not_sargable() {
        // `{created_at}` alone cannot use an index whose leading field is status.
        assert!(
            plan_compound(
                "SELECT VALUE c FROM c WHERE c.created_at = 5",
                &[&["status", "created_at"]],
            )
            .is_none()
        );
    }

    #[test]
    fn compound_range_on_leading_field_is_the_tail() {
        // A range on the leading field is itself the trailing predicate (no eq
        // prefix) — still a valid compound prefix scan.
        let (range, used) = compound_access(
            "SELECT VALUE c FROM c WHERE c.status > 'a'",
            &[&["status", "created_at"]],
        );
        assert!(range.eq_prefix.is_empty());
        assert_eq!(
            range.tail,
            CompoundScanTail::Range {
                lower: Some((Bson::String("a".into()), false)),
                upper: None,
            }
        );
        assert_eq!(used.len(), 1);
    }

    #[test]
    fn compound_prefers_longer_prefix_index() {
        // Two candidate indexes; the one covering both equalities wins over the
        // one covering only the first.
        let (range, used) = compound_access(
            "SELECT VALUE c FROM c WHERE c.a = 1 AND c.b = 2",
            &[&["a", "x"], &["a", "b"]],
        );
        assert_eq!(range.eq_prefix, vec![Bson::Int64(1), Bson::Int64(2)]);
        assert_eq!(used.len(), 2);
    }

    #[test]
    fn compound_identity_is_the_joined_components() {
        let (access, _) = plan_compound(
            "SELECT VALUE c FROM c WHERE c.status = 'active'",
            &[&["status", "created_at"]],
        )
        .unwrap();
        let IndexAccess::Compound { field, .. } = access else {
            panic!("expected compound");
        };
        assert_eq!(field, "status\u{1}created_at");
    }
}
