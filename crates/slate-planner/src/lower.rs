//! Lowering: a parsed [`slate_ast::Query`] into an executable [`Plan`].
//!
//! The query's `FROM` clause supplies only the alias — the container is chosen
//! by the caller (matching Cosmos) and passed in as `container`, along with its
//! index metadata ([`CollectionMeta`]) so the planner can choose an index.
//!
//! Pipeline shape:
//!
//! ```text
//! FROM c            → <source> → Bind(c)       (source = Scan, or an index path)
//! JOIN t IN <arr>   → Unwind(t, <arr>)
//! <residual WHERE>  → Filter(e)                (the non-sargable remainder)
//! ORDER BY ...      → Sort(...)
//! SELECT VALUE <e>  → Project(e)
//! OFFSET/LIMIT      → Limit(...)
//! ```
//!
//! ## Sargability
//!
//! The `WHERE` predicate is split: conjuncts of the form `c.<indexed-field>
//! <cmp> <literal>` are *pushed* into an `IndexScan` (wrapped by `KeyLookup` to
//! fetch documents), and primary-key equality becomes a direct `KeyLookup`.
//! Everything else stays a residual `Filter`. A predicate that wraps the field
//! in a computation (`UPPER(c.x) = ...`, `c.x + 1 = ...`) is not sargable and
//! falls through to the residual.

use bson::{Bson, RawBson};
use slate_ast::{BinOp, FromSource, Literal, Query, ScalarExpr, SelectClause};

use crate::plan::{
    CollectionRef, IndexScanRange, LogicalOp, Node, Plan, RowBinding, ScanDirection,
};

/// Index metadata for the queried collection, used to choose a scan source.
#[derive(Debug, Clone, Default)]
pub struct CollectionMeta {
    /// Indexed field paths (e.g. `"age"`, `"address.city"`).
    pub indexes: Vec<String>,
    /// Primary-key field path (e.g. `"_id"`).
    pub pk_path: String,
}

/// Lower `query` into a plan that reads from `container`.
pub fn lower(query: Query, container: CollectionRef, meta: &CollectionMeta) -> Plan {
    let Query {
        select,
        from,
        filter,
        order_by,
        offset,
        limit,
    } = query;

    let FromSource::ImplicitContainer { alias } = from.source;

    // Choose a source (Scan or an index path), pushing sargable predicates in.
    let (source, residual) = plan_source(filter, &alias, &container, meta);

    // Without joins, the bound rows are the bare source documents and binding
    // is a zero-cost single alias. With joins we materialize a row environment:
    // `Bind(c)` attaches the first alias, `Unwind` adds one per array element,
    // and the binding-aware nodes read its fields.
    let (binding, mut node) = if from.joins.is_empty() {
        (RowBinding::Alias(alias), source)
    } else {
        let mut node = Node::Bind {
            alias,
            source: Box::new(source),
        };
        for join in from.joins {
            node = Node::Unwind {
                alias: join.alias,
                array: join.array,
                source: Box::new(node),
            };
        }
        (RowBinding::Env, node)
    };

    // Residual WHERE  →  Filter
    if let Some(predicate) = residual {
        node = Node::Filter {
            predicate,
            binding: binding.clone(),
            source: Box::new(node),
        };
    }

    // ORDER BY  →  Sort (before projection — keys reference the row environment)
    if !order_by.is_empty() {
        node = Node::Sort {
            keys: order_by,
            binding: binding.clone(),
            source: Box::new(node),
        };
    }

    // SELECT VALUE  →  Project
    let SelectClause::Value(expr) = select;
    node = Node::Project {
        expr,
        binding,
        source: Box::new(node),
    };

    // OFFSET / LIMIT  →  Limit (after projection — on result rows)
    if offset.is_some() || limit.is_some() {
        node = Node::Limit {
            skip: offset.unwrap_or(0) as usize,
            take: limit.map(|n| n as usize),
            source: Box::new(node),
        };
    }

    Plan::Query(node)
}

// ── Sargability ─────────────────────────────────────────────────

/// Decide the scan source and the residual (non-pushed) predicate.
///
/// - A top-level `OR` whose every branch is indexable → `IndexMerge(Or)` (with
///   the full predicate kept as a residual recheck).
/// - Otherwise the predicate is treated as a conjunction: pk equality wins
///   (direct key lookup); else each indexed field's atoms become one
///   `IndexScan` (range bounds combined), and `OR` sub-groups become
///   `IndexMerge(Or)` — all intersected via `IndexMerge(And)` when more than
///   one applies. Consumed atoms leave the residual; the rest stay a `Filter`.
fn plan_source(
    filter: Option<ScalarExpr>,
    alias: &str,
    container: &CollectionRef,
    meta: &CollectionMeta,
) -> (Node, Option<ScalarExpr>) {
    let Some(expr) = filter else {
        return (scan(container), None);
    };

    // Top-level OR → IndexMerge(Or) when fully indexable; recheck the full OR.
    // A whole-filter Mongo implicit-equality (`f = v OR ARRAY_CONTAINS(f, v)`)
    // is *not* a real disjunction — it is sargable as an equality on `f`, so it
    // falls through to the conjunction path below rather than taking this route.
    if matches!(expr, ScalarExpr::Binary { op: BinOp::Or, .. })
        && as_mongo_eq(&expr, alias).is_none()
    {
        return match index_source_for(&expr, alias, container, meta) {
            Some(ids) => (key_lookup(container, ids), Some(expr)),
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

    // Collect ID sources from conjuncts, tracking which conjuncts are consumed.
    let mut sources: Vec<Node> = Vec::new();
    let mut consumed: Vec<usize> = Vec::new();

    // Per indexed field: combine its atoms into a single IndexScan.
    for field in &meta.indexes {
        if let Some((scan, used)) = field_index_scan(&conjuncts, &consumed, alias, container, field)
        {
            sources.push(scan);
            consumed.extend(used);
        }
    }

    // Mongo implicit-equality on a scalar-indexed field → an index Eq lookup,
    // and the conjunct is *consumed* (no residual recheck). A scalar index holds
    // only scalar entries — an array-valued field produces none (see
    // `index_record::extract_all`) — so every candidate the `Eq` returns already
    // has `field == value`; the idiom's recheck (`field = v OR ARRAY_CONTAINS`)
    // is therefore always true over the candidate set and is pure per-row
    // overhead. (Array-valued documents are absent from a scalar index whether
    // or not the recheck runs, so consuming it changes no results — pinned by
    // the v1↔v2 differential `diff_fuzz`/`parity_audit`.)
    //
    // Multikey (`.[]`) equality is handled separately below and stays a recheck.
    for (i, conjunct) in conjuncts.iter().enumerate() {
        if consumed.contains(&i) {
            continue;
        }
        if let Some((field, value)) = as_mongo_eq(conjunct, alias)
            && meta.indexes.contains(&field)
        {
            sources.push(index_scan(container, &field, IndexScanRange::Eq(value)));
            consumed.push(i);
        }
    }

    // Explicit multikey equality on an indexed `.[]` path → index Eq lookup
    // (kept as a residual recheck, like the Mongo idiom above). The index name
    // is the verbatim `.[]` path the predicate carries.
    for conjunct in &conjuncts {
        if let Some((field, value)) = as_multikey_eq(conjunct, alias)
            && meta.indexes.contains(&field)
        {
            sources.push(index_scan(container, &field, IndexScanRange::Eq(value)));
        }
    }

    // OR sub-groups that are fully indexable become IndexMerge(Or) inputs.
    // The conjunct is NOT consumed: it stays as a residual recheck, because an
    // index merge can over-return (e.g. a range bound against a field holding
    // mixed numeric types). The index narrows the candidate set; the recheck
    // keeps the result precise — matching how the top-level-OR path behaves.
    for (i, conjunct) in conjuncts.iter().enumerate() {
        if consumed.contains(&i) {
            continue;
        }
        if as_mongo_eq(conjunct, alias).is_some() {
            continue; // already handled above
        }
        if matches!(conjunct, ScalarExpr::Binary { op: BinOp::Or, .. })
            && let Some(ids) = index_source_for(conjunct, alias, container, meta)
        {
            sources.push(ids);
        }
    }

    let residual = residual_excluding(conjuncts, &consumed);
    match merge_sources(container, LogicalOp::And, sources) {
        Some(ids) => (key_lookup(container, ids), residual),
        None => (scan(container), residual),
    }
}

/// Build one `IndexScan` covering all of `field`'s atoms (`Eq` wins; otherwise
/// range bounds are combined). Returns the scan and the consumed conjunct
/// indices, or `None` if `field` has no usable atom here.
fn field_index_scan(
    conjuncts: &[ScalarExpr],
    consumed: &[usize],
    alias: &str,
    container: &CollectionRef,
    field: &str,
) -> Option<(Node, Vec<usize>)> {
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
            index_scan(container, field, IndexScanRange::Eq(value)),
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
        index_scan(
            container,
            field,
            IndexScanRange::Range {
                lower: lo,
                upper: hi,
            },
        ),
        used,
    ))
}

/// Build an ID-yielding source for a single predicate, or `None` if it is not
/// fully indexable: an atom on an indexed field → `IndexScan`; an `OR` whose
/// every branch is indexable → `IndexMerge(Or)`.
fn index_source_for(
    expr: &ScalarExpr,
    alias: &str,
    container: &CollectionRef,
    meta: &CollectionMeta,
) -> Option<Node> {
    // The Mongo implicit-equality idiom (`field = lit OR ARRAY_CONTAINS(field,
    // lit)`) is itself an `Or`. Recognize it as a single indexed Eq *before*
    // treating a generic `Or` as a disjunction to merge — otherwise flattening
    // would split it and expose the lone, un-indexable `ARRAY_CONTAINS` branch,
    // poisoning the whole merge (so an OR/IN of `{field: value}` fell back to a
    // full scan).
    if let Some((field, value)) = as_mongo_eq(expr, alias) {
        return meta
            .indexes
            .contains(&field)
            .then(|| index_scan(container, &field, IndexScanRange::Eq(value)));
    }

    if matches!(expr, ScalarExpr::Binary { op: BinOp::Or, .. }) {
        let mut branches = Vec::new();
        collect_or(expr, alias, &mut branches);
        let mut sources = Vec::with_capacity(branches.len());
        for branch in branches {
            sources.push(index_source_for(branch, alias, container, meta)?);
        }
        return merge_sources(container, LogicalOp::Or, sources);
    }

    let (field, op, value) = as_atom(expr, alias)?;
    if !meta.indexes.contains(&field) {
        return None;
    }
    let range = match op {
        BinOp::Eq => IndexScanRange::Eq(value),
        BinOp::Gt => IndexScanRange::Range {
            lower: Some((value, false)),
            upper: None,
        },
        BinOp::Gte => IndexScanRange::Range {
            lower: Some((value, true)),
            upper: None,
        },
        BinOp::Lt => IndexScanRange::Range {
            lower: None,
            upper: Some((value, false)),
        },
        BinOp::Lte => IndexScanRange::Range {
            lower: None,
            upper: Some((value, true)),
        },
        _ => return None,
    };
    Some(index_scan(container, &field, range))
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

/// Flatten an `Or` into its branches, but treat a Mongo implicit-equality idiom
/// as one indivisible branch (its inner `Eq OR ARRAY_CONTAINS` must not be split
/// — `index_source_for` recognizes the whole idiom as an indexed Eq).
fn collect_or<'a>(expr: &'a ScalarExpr, alias: &str, out: &mut Vec<&'a ScalarExpr>) {
    if as_mongo_eq(expr, alias).is_some() {
        out.push(expr);
        return;
    }
    if let ScalarExpr::Binary {
        op: BinOp::Or,
        lhs,
        rhs,
    } = expr
    {
        collect_or(lhs, alias, out);
        collect_or(rhs, alias, out);
    } else {
        out.push(expr);
    }
}

fn scan(container: &CollectionRef) -> Node {
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

/// Wrap an ID-yielding source in a `KeyLookup` to fetch the documents.
fn key_lookup(container: &CollectionRef, ids: Node) -> Node {
    Node::KeyLookup {
        collection: container.clone(),
        source: Box::new(ids),
    }
}

/// Interpret a conjunct as `alias.<path> <cmp> <literal>` (either operand
/// order), returning the field path, comparison op, and literal value.
fn as_atom(expr: &ScalarExpr, alias: &str) -> Option<(String, BinOp, Bson)> {
    let ScalarExpr::Binary { op, lhs, rhs } = expr else {
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

/// Recognize the Mongo implicit-equality idiom the find front-end emits:
/// `alias.field = lit OR ARRAY_CONTAINS(alias.field, lit)` (same field, same
/// literal). Returns the field path and value — it is sargable as an equality
/// on `field`, because an index/pk lookup for `lit` finds both the
/// scalar-equal and the array-containing documents.
fn as_mongo_eq(expr: &ScalarExpr, alias: &str) -> Option<(String, Bson)> {
    let ScalarExpr::Binary {
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
fn as_array_contains(expr: &ScalarExpr, alias: &str) -> Option<(String, Bson)> {
    let ScalarExpr::Function { name, args } = expr else {
        return None;
    };
    if !name.eq_ignore_ascii_case("ARRAY_CONTAINS") || args.len() != 2 {
        return None;
    }
    Some((path_of(&args[0], alias)?, as_literal(&args[1])?))
}

/// Recognize a [`ScalarExpr::MultikeyEq`] on `alias` — explicit multikey
/// equality the find front-end emits for a `.[]` path. Returns the verbatim
/// `.[]` path (which is also the index name) and the literal value, so it can
/// be matched to a multikey index.
fn as_multikey_eq(expr: &ScalarExpr, alias: &str) -> Option<(String, Bson)> {
    let ScalarExpr::MultikeyEq {
        base,
        index_path,
        value,
    } = expr
    else {
        return None;
    };
    if !matches!(base.as_ref(), ScalarExpr::Identifier(a) if a == alias) {
        return None;
    }
    Some((index_path.clone(), as_literal(value)?))
}

/// The value of a primary-key equality on `pk` — a plain `Eq` atom or the Mongo
/// idiom (whose `ARRAY_CONTAINS` branch is vacuous for a non-array pk).
fn pk_eq_value(expr: &ScalarExpr, alias: &str, pk: &str) -> Option<Bson> {
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
fn path_of(expr: &ScalarExpr, alias: &str) -> Option<String> {
    match expr {
        ScalarExpr::Member { base, field } => match base.as_ref() {
            ScalarExpr::Identifier(a) if a == alias => Some(field.clone()),
            other => path_of(other, alias).map(|p| format!("{p}.{field}")),
        },
        _ => None,
    }
}

fn as_literal(expr: &ScalarExpr) -> Option<Bson> {
    match expr {
        ScalarExpr::Literal(lit) => Some(literal_to_bson(lit)),
        // A materialized value preserves its exact BSON type — important here,
        // since an index bound must match the stored key's numeric type.
        ScalarExpr::Value(b) => Some(b.clone()),
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

fn flatten_and(expr: ScalarExpr, out: &mut Vec<ScalarExpr>) {
    match expr {
        ScalarExpr::Binary {
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
fn residual_excluding(conjuncts: Vec<ScalarExpr>, used: &[usize]) -> Option<ScalarExpr> {
    let remaining: Vec<ScalarExpr> = conjuncts
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !used.contains(i))
        .map(|(_, c)| c)
        .collect();
    rebuild_and(remaining)
}

fn rebuild_and(conjuncts: Vec<ScalarExpr>) -> Option<ScalarExpr> {
    let mut iter = conjuncts.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, e| ScalarExpr::Binary {
        op: BinOp::And,
        lhs: Box::new(acc),
        rhs: Box::new(e),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn container() -> CollectionRef {
        CollectionRef {
            cf: "default".into(),
            collection: "people".into(),
        }
    }

    fn no_index() -> CollectionMeta {
        CollectionMeta {
            indexes: vec![],
            pk_path: "_id".into(),
        }
    }

    fn age_indexed() -> CollectionMeta {
        CollectionMeta {
            indexes: vec!["age".into()],
            pk_path: "_id".into(),
        }
    }

    fn lower_with(sql: &str, meta: &CollectionMeta) -> Node {
        let Plan::Query(node) = lower(slate_sql::parse(sql).unwrap(), container(), meta) else {
            panic!("lower always produces a Query");
        };
        node
    }

    fn lower_sql(sql: &str) -> Node {
        lower_with(sql, &no_index())
    }

    // ── Structural lowering (no index) ──────────────────────────

    #[test]
    fn minimal_is_project_scan() {
        // No joins → no Bind wrapper; the alias binds the bare scan rows.
        match lower_sql("SELECT VALUE c FROM c") {
            Node::Project {
                source, binding, ..
            } => {
                assert_eq!(binding, RowBinding::Alias("c".into()));
                assert!(matches!(*source, Node::Scan { .. }));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn full_pipeline_nesting() {
        // Limit(Project(Sort(Filter(Scan)))) — no Bind without joins.
        let node = lower_sql(
            "SELECT VALUE c.name FROM c WHERE c.age > 40 ORDER BY c.age OFFSET 1 LIMIT 2",
        );
        let Node::Limit { skip, take, source } = node else {
            panic!("expected Limit");
        };
        assert_eq!(skip, 1);
        assert_eq!(take, Some(2));
        let Node::Project { source, .. } = *source else {
            panic!("expected Project");
        };
        let Node::Sort { source, .. } = *source else {
            panic!("expected Sort");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }

    #[test]
    fn join_lowers_to_unwind() {
        let Node::Project { source, .. } = lower_sql("SELECT VALUE t FROM c JOIN t IN c.tags")
        else {
            panic!("expected Project");
        };
        let Node::Unwind { alias, source, .. } = *source else {
            panic!("expected Unwind");
        };
        assert_eq!(alias, "t");
        assert!(matches!(*source, Node::Bind { .. }));
    }

    // ── Sargability ─────────────────────────────────────────────

    /// Unwrap `Project(<source>)` — or `Project(Filter(<source>))` — and return
    /// the scan/index source. With no joins there is no `Bind` wrapper.
    fn source_under_bind(node: Node) -> Node {
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        match *source {
            Node::Filter { source, .. } => *source,
            other => other,
        }
    }

    #[test]
    fn eq_on_indexed_field_uses_index() {
        let node = lower_with("SELECT VALUE c FROM c WHERE c.age = 41", &age_indexed());
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexScan {
                    range: IndexScanRange::Eq(_),
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexScan), got {other:?}"),
        }
    }

    #[test]
    fn range_on_indexed_field_uses_index() {
        let node = lower_with("SELECT VALUE c FROM c WHERE 40 < c.age", &age_indexed());
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexScan {
                    range: IndexScanRange::Range { .. },
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexScan range), got {other:?}"),
        }
    }

    #[test]
    fn pk_equality_uses_direct_key_lookup() {
        let node = lower_with(r#"SELECT VALUE c FROM c WHERE c._id = "2""#, &age_indexed());
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(*source, Node::Values(_))),
            other => panic!("expected KeyLookup(Values), got {other:?}"),
        }
    }

    #[test]
    fn non_indexed_predicate_falls_back_to_scan() {
        // name is not indexed → Scan + residual Filter
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.name = "ada""#,
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected residual Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }

    #[test]
    fn partial_pushdown_keeps_residual() {
        // c.age > 40 pushed to index; c.name = "x" stays a residual Filter.
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.age > 40 AND c.name = "x""#,
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        // Residual Filter present...
        let Node::Filter { source, .. } = *source else {
            panic!("expected residual Filter");
        };
        // ...and the source is the index path.
        assert!(matches!(*source, Node::KeyLookup { .. }));
    }

    #[test]
    fn computed_field_is_not_sargable() {
        // c.age + 1 = 42 wraps the field → not sargable → Scan + Filter.
        let node = lower_with("SELECT VALUE c FROM c WHERE c.age + 1 = 42", &age_indexed());
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }

    // ── OR / multi-index ────────────────────────────────────────

    fn age_status_indexed() -> CollectionMeta {
        CollectionMeta {
            indexes: vec!["age".into(), "status".into()],
            pk_path: "_id".into(),
        }
    }

    #[test]
    fn or_of_indexed_atoms_uses_index_merge_or() {
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE c.age = 41 OR c.age = 44",
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexMerge {
                    logical: LogicalOp::Or,
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexMerge Or), got {other:?}"),
        }
    }

    #[test]
    fn or_of_implicit_equality_idioms_uses_index_merge_or() {
        // The Mongo `{field: v}` form lowers to `field = v OR ARRAY_CONTAINS(
        // field, v)`. An OR/IN of those must still merge indexes — the regression
        // was that flattening the outer OR split each idiom and exposed the lone,
        // un-indexable `ARRAY_CONTAINS` branch, forcing a full scan.
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE (c.age = 41 OR ARRAY_CONTAINS(c.age, 41)) \
             OR (c.age = 44 OR ARRAY_CONTAINS(c.age, 44))",
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexMerge {
                    logical: LogicalOp::Or,
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexMerge Or), got {other:?}"),
        }
    }

    #[test]
    fn and_of_two_indexed_fields_uses_index_merge_and() {
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.age = 41 AND c.status = "active""#,
            &age_status_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexMerge {
                    logical: LogicalOp::And,
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexMerge And), got {other:?}"),
        }
    }

    #[test]
    fn range_bounds_on_one_field_combine_into_one_scan() {
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE c.age > 40 AND c.age < 50",
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => match *source {
                Node::IndexScan {
                    range: IndexScanRange::Range { lower, upper },
                    ..
                } => {
                    assert!(lower.is_some() && upper.is_some());
                }
                other => panic!("expected single IndexScan range, got {other:?}"),
            },
            other => panic!("expected KeyLookup(IndexScan), got {other:?}"),
        }
    }

    #[test]
    fn or_with_non_indexed_branch_falls_back_to_scan() {
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.age = 41 OR c.name = "x""#,
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }

    // ── Mongo implicit-equality idiom (`f = v OR ARRAY_CONTAINS(f, v)`) ──

    #[test]
    fn mongo_eq_on_pk_uses_point_lookup() {
        // The find front-end's `{_id: "2"}`. Must be a direct pk lookup, not a
        // scan — and the (vacuous) idiom is consumed, so no residual Filter.
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c._id = "2" OR ARRAY_CONTAINS(c._id, "2")"#,
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(*source, Node::Values(_))),
            other => panic!("expected KeyLookup(Values), got {other:?}"),
        }
    }

    #[test]
    fn mongo_eq_on_indexed_field_uses_index_and_consumes_recheck() {
        // `{age: 41}` on a scalar-indexed field → index Eq, with the idiom
        // *consumed* (no residual Filter): a scalar index returns only docs
        // whose `age` already equals 41, so the `OR ARRAY_CONTAINS` recheck is
        // always true over the candidates and is dropped.
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE c.age = 41 OR ARRAY_CONTAINS(c.age, 41)",
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        match *source {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexScan {
                    range: IndexScanRange::Eq(_),
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexScan) with no residual Filter, got {other:?}"),
        }
    }

    #[test]
    fn mongo_eq_on_unindexed_field_scans() {
        // `{name: "x"}` — name not indexed → scan + residual recheck.
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.name = "x" OR ARRAY_CONTAINS(c.name, "x")"#,
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }
}
