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

use crate::plan::{CollectionRef, IndexScanRange, LogicalOp, Node, ScanDirection};

/// Index metadata for the queried collection, used to choose a scan source.
#[derive(Debug, Clone, Default)]
pub struct CollectionMeta {
    /// Indexed field paths (e.g. `"age"`, `"address.city"`, `"tags.[]"`).
    pub indexes: Vec<String>,
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

    // Per indexed scalar field: combine its comparison atoms into a single
    // IndexScan (Eq wins; otherwise range bounds merge). These atoms are
    // consumed — the scan (with the executor's coercing post-filter) is exact.
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
    if let (Some(path), Some(lit)) = (path_of(lhs.as_ref(), alias), as_literal(rhs.as_ref())) {
        Some((path, *op, lit))
    } else if let (Some(lit), Some(path)) = (as_literal(lhs.as_ref()), path_of(rhs.as_ref(), alias))
    {
        Some((path, flip(*op), lit))
    } else {
        None
    }
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
    Some((index_path.clone(), as_literal(value)?))
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
}
