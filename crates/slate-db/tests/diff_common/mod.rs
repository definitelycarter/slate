//! Shared harness for v1 ↔ v2 differential tests.
//!
//! Seeds two identical engines — a v1 [`Database`] and a raw `KvEngine` for v2 —
//! runs the same find request through each pipeline, and compares the results.
//! The common input is the find request `(filter_doc, FindOptions)` (matching
//! v1's public API and the existing benches); the v2 side translates that
//! request into a [`slate_planner::Plan`].
//!
//! Untranslatable requests (e.g. `$regex`, which v2's evaluator can't yet
//! express) translate to `None`, letting a test skip — those gaps are exactly
//! what differential testing surfaces.

#![allow(dead_code)]

use bson::raw::RawBsonRef;
use bson::{Document, RawDocument, RawDocumentBuf, doc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_engine::{Catalog, Engine, EngineTransaction, KvEngine};
use slate_query::FindOptions;
use slate_store::MemoryStore;

use slate_ast::{BinOp, Literal, OrderByItem, ScalarExpr, SortDirection, UnaryOp};
use slate_planner::{CollectionRef, Node, Plan, RowBinding, UpsertMode};
use slate_query::DistinctOptions;

pub const COLL: &str = "people";

/// The shared dataset.
pub fn dataset() -> Vec<Document> {
    vec![
        doc! { "_id": "1", "name": "ada", "age": 36, "status": "active", "tags": ["math", "logic"] },
        doc! { "_id": "2", "name": "alan", "age": 41, "status": "inactive", "tags": ["computing"] },
        doc! { "_id": "3", "name": "grace", "age": 44, "status": "active", "tags": ["computing", "naval"] },
    ]
}

fn indexes() -> &'static [&'static str] {
    &["age", "status"]
}

// ── Engines ─────────────────────────────────────────────────────

pub fn v1_db() -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLL.into(),
        ..Default::default()
    })
    .unwrap();
    for f in indexes() {
        txn.create_index(DEFAULT_CF, COLL, f).unwrap();
    }
    txn.insert_many(DEFAULT_CF, COLL, dataset())
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();
    db
}

pub fn v2_engine() -> KvEngine<MemoryStore> {
    let engine = KvEngine::new(MemoryStore::new());
    {
        let txn = engine.begin(false).unwrap();
        txn.create_collection(DEFAULT_CF, COLL, &Default::default())
            .unwrap();
        for f in indexes() {
            txn.create_index(DEFAULT_CF, COLL, f).unwrap();
        }
        txn.commit().unwrap();
    }
    {
        let txn = engine.begin(false).unwrap();
        let handle = txn.collection(DEFAULT_CF, COLL).unwrap();
        for d in dataset() {
            let raw = RawDocumentBuf::try_from(&d).unwrap();
            txn.put(&handle, &raw).unwrap();
        }
        txn.commit().unwrap();
    }
    engine
}

// ── Runners ─────────────────────────────────────────────────────

pub fn run_v1(db: &Database<MemoryStore>, filter: Document, options: FindOptions) -> Vec<Document> {
    let txn = db.begin(true).unwrap();
    txn.find(DEFAULT_CF, COLL, filter, options)
        .unwrap()
        .iter_raw()
        .unwrap()
        .map(|r| to_doc(r.unwrap()))
        .collect()
}

pub fn run_v2(engine: &KvEngine<MemoryStore>, plan: Plan) -> Vec<Document> {
    let txn = engine.begin(true).unwrap();
    slate_executor::Executor::new(&txn)
        .execute_collect(plan)
        .unwrap()
        .into_iter()
        .map(|raw| match raw {
            bson::RawBson::Document(buf) => to_doc(buf),
            other => panic!("expected document, got {other:?}"),
        })
        .collect()
}

fn to_doc(buf: RawDocumentBuf) -> Document {
    bson::deserialize_from_slice(buf.as_bytes()).unwrap()
}

/// Run the same find request through both pipelines and assert identical
/// results, compared as an unordered set. Skips silently if the request can't
/// be translated to v2 (a known gap).
pub fn assert_same(filter: Document, options: FindOptions) {
    let Some((v1, v2)) = run_both(filter, options) else {
        return;
    };
    assert_same_set(v1, v2);
}

/// Like [`assert_same`] but compares **in order** (for `ORDER BY`/`LIMIT`).
pub fn assert_same_ordered(filter: Document, options: FindOptions) {
    let Some((v1, v2)) = run_both(filter, options) else {
        return;
    };
    let a: Vec<_> = v1.iter().map(canon).collect();
    let b: Vec<_> = v2.iter().map(canon).collect();
    assert_eq!(a, b, "v1 (left) and v2 (right) differ in order/content");
}

fn run_both(filter: Document, options: FindOptions) -> Option<(Vec<Document>, Vec<Document>)> {
    let plan = v2_plan(&filter, &options)?;
    let v1 = run_v1(&v1_db(), filter, options);
    let v2 = run_v2(&v2_engine(), plan);
    Some((v1, v2))
}

/// Canonical, field-order-insensitive view of a document (top-level keys).
fn canon(d: &Document) -> std::collections::BTreeMap<String, bson::Bson> {
    d.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn id_of(m: &std::collections::BTreeMap<String, bson::Bson>) -> String {
    m.get("_id").and_then(|v| v.as_str()).unwrap_or("").into()
}

pub fn assert_same_set(v1: Vec<Document>, v2: Vec<Document>) {
    let mut a: Vec<_> = v1.iter().map(canon).collect();
    let mut b: Vec<_> = v2.iter().map(canon).collect();
    a.sort_by_key(id_of);
    b.sort_by_key(id_of);
    assert_eq!(a, b, "v1 (left) and v2 (right) differ");
}

// ── Request → v2 Plan translation ───────────────────────────────

/// Translate a find request into a v2 plan, or `None` if untranslatable.
pub fn v2_plan(filter: &Document, options: &FindOptions) -> Option<Plan> {
    // Single-binding (`Alias`) mode, matching what the planner emits for a
    // join-free find: the scan rows are bound directly to `c`, no `Bind`.
    let binding = RowBinding::Alias("c".into());
    let mut node = Node::Scan {
        collection: CollectionRef {
            cf: DEFAULT_CF.into(),
            collection: COLL.into(),
        },
    };

    let raw = RawDocumentBuf::try_from(filter).ok()?;
    if let Some(predicate) = translate_filter(&raw)? {
        node = Node::Filter {
            predicate,
            binding: binding.clone(),
            source: Box::new(node),
        };
    }

    if !options.sort.is_empty() {
        let keys = options
            .sort
            .iter()
            .map(|s| OrderByItem {
                expr: path(&s.field),
                direction: match s.direction {
                    slate_query::SortDirection::Asc => SortDirection::Asc,
                    slate_query::SortDirection::Desc => SortDirection::Desc,
                },
            })
            .collect();
        node = Node::Sort {
            keys,
            binding: binding.clone(),
            source: Box::new(node),
        };
    }

    // Projection: v1 always includes the pk plus the selected columns.
    let proj = match &options.columns {
        None => ScalarExpr::Identifier("c".into()),
        Some(cols) => {
            let mut fields = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for col in std::iter::once("_id").chain(cols.iter().map(|s| s.as_str())) {
                if seen.insert(col.to_string()) {
                    fields.push((col.to_string(), path(col)));
                }
            }
            ScalarExpr::Object(fields)
        }
    };
    node = Node::Project {
        expr: proj,
        binding,
        source: Box::new(node),
    };

    if options.skip.is_some() || options.take.is_some() {
        node = Node::Limit {
            skip: options.skip.unwrap_or(0),
            take: options.take,
            source: Box::new(node),
        };
    }

    Some(Plan::Query(node))
}

/// `Some(None)` = match-all (no predicate). `None` = untranslatable.
fn translate_filter(doc: &RawDocument) -> Option<Option<ScalarExpr>> {
    let mut conjuncts = Vec::new();
    for entry in doc.iter() {
        let (key, value) = entry.ok()?;
        match key.as_str() {
            "$and" => conjuncts.push(translate_logical(value, BinOp::And)?),
            "$or" => conjuncts.push(translate_logical(value, BinOp::Or)?),
            k if k.starts_with('$') => return None,
            field => conjuncts.push(translate_field(field, value)?),
        }
    }
    Some(fold(conjuncts, BinOp::And))
}

fn translate_logical(value: RawBsonRef, op: BinOp) -> Option<ScalarExpr> {
    let RawBsonRef::Array(arr) = value else {
        return None;
    };
    let mut parts = Vec::new();
    for elem in arr.into_iter() {
        let RawBsonRef::Document(sub) = elem.ok()? else {
            return None;
        };
        parts.push(translate_filter(sub)??);
    }
    fold(parts, op)
}

fn translate_field(field: &str, value: RawBsonRef) -> Option<ScalarExpr> {
    // Operator sub-document if the first key starts with `$`.
    if let RawBsonRef::Document(sub) = value
        && let Some(Ok((first, _))) = sub.iter().next()
        && first.as_str().starts_with('$')
    {
        return translate_operators(field, sub);
    }
    eq_or_contains(field, value)
}

/// Mongo `{field: value}` equality: matches a scalar field *or* an array field
/// containing the value. Mirrors v1's implicit array-aware equality.
fn eq_or_contains(field: &str, value: RawBsonRef) -> Option<ScalarExpr> {
    let lit = literal(value)?;
    let eq = binary(BinOp::Eq, path(field), lit.clone());
    let contains = ScalarExpr::Function {
        name: "ARRAY_CONTAINS".into(),
        args: vec![path(field), lit],
    };
    Some(binary(BinOp::Or, eq, contains))
}

fn translate_operators(field: &str, doc: &RawDocument) -> Option<ScalarExpr> {
    // Special-case $regex (with optional $options) → REGEXMATCH.
    let mut pattern: Option<String> = None;
    let mut options: Option<String> = None;
    let mut has_other = false;
    for entry in doc.iter() {
        let (op, value) = entry.ok()?;
        match op.as_str() {
            "$regex" => match value {
                RawBsonRef::String(s) => pattern = Some(s.to_string()),
                _ => return None,
            },
            "$options" => match value {
                RawBsonRef::String(s) => options = Some(s.to_string()),
                _ => return None,
            },
            _ => has_other = true,
        }
    }
    if let Some(pat) = pattern {
        if has_other {
            return None; // $regex mixed with other operators: skip
        }
        let full = match options {
            Some(opts) => format!("(?{opts}){pat}"),
            None => pat,
        };
        return Some(ScalarExpr::Function {
            name: "REGEXMATCH".into(),
            args: vec![path(field), ScalarExpr::Literal(Literal::Str(full))],
        });
    }

    let mut conds = Vec::new();
    for entry in doc.iter() {
        let (op, value) = entry.ok()?;
        let cond = match op.as_str() {
            "$eq" => eq_or_contains(field, value)?,
            "$gt" => binary(BinOp::Gt, path(field), literal(value)?),
            "$gte" => binary(BinOp::Gte, path(field), literal(value)?),
            "$lt" => binary(BinOp::Lt, path(field), literal(value)?),
            "$lte" => binary(BinOp::Lte, path(field), literal(value)?),
            "$exists" => {
                let RawBsonRef::Boolean(b) = value else {
                    return None;
                };
                let is_def = ScalarExpr::Function {
                    name: "IS_DEFINED".into(),
                    args: vec![path(field)],
                };
                if b {
                    is_def
                } else {
                    ScalarExpr::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(is_def),
                    }
                }
            }
            _ => return None, // $regex and friends: not expressible in v2 yet
        };
        conds.push(cond);
    }
    fold(conds, BinOp::And)
}

fn path(field: &str) -> ScalarExpr {
    let mut expr = ScalarExpr::Identifier("c".into());
    for part in field.split('.') {
        expr = ScalarExpr::Member {
            base: Box::new(expr),
            field: part.into(),
        };
    }
    expr
}

fn literal(value: RawBsonRef) -> Option<ScalarExpr> {
    let lit = match value {
        RawBsonRef::String(s) => Literal::Str(s.to_string()),
        RawBsonRef::Int32(i) => Literal::Int(i as i64),
        RawBsonRef::Int64(i) => Literal::Int(i),
        RawBsonRef::Double(f) => Literal::Float(f),
        RawBsonRef::Boolean(b) => Literal::Bool(b),
        RawBsonRef::Null => Literal::Null,
        _ => return None,
    };
    Some(ScalarExpr::Literal(lit))
}

fn binary(op: BinOp, lhs: ScalarExpr, rhs: ScalarExpr) -> ScalarExpr {
    ScalarExpr::Binary {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

fn collection_ref() -> CollectionRef {
    CollectionRef {
        cf: DEFAULT_CF.into(),
        collection: COLL.into(),
    }
}

/// `Scan → [Filter] → Project(c)` — the matched documents for a write, in
/// single-binding mode.
fn matched_source(filter: &Document) -> Option<Node> {
    let binding = RowBinding::Alias("c".into());
    let mut node = Node::Scan {
        collection: collection_ref(),
    };
    let raw = RawDocumentBuf::try_from(filter).ok()?;
    if let Some(pred) = translate_filter(&raw)? {
        node = Node::Filter {
            predicate: pred,
            binding: binding.clone(),
            source: Box::new(node),
        };
    }
    Some(Node::Project {
        expr: ScalarExpr::Identifier("c".into()),
        binding,
        source: Box::new(node),
    })
}

// ── Write-state comparison ──────────────────────────────────────

/// v1 collection state after applying a mutation closure, as `_id`-ordered docs.
fn v1_state_after(
    mutate: impl FnOnce(&slate_db::DatabaseTransaction<MemoryStore>),
) -> Vec<Document> {
    let db = v1_db();
    {
        let txn = db.begin(false).unwrap();
        mutate(&txn);
        txn.commit().unwrap();
    }
    run_v1(&db, doc! {}, FindOptions::default())
}

/// v2 collection state after running a write plan.
fn v2_state_after(plan: Plan) -> Vec<Document> {
    let engine = v2_engine();
    {
        let txn = engine.begin(false).unwrap();
        slate_executor::Executor::new(&txn)
            .execute_collect(plan)
            .unwrap();
        txn.commit().unwrap();
    }
    run_v2(&engine, v2_plan(&doc! {}, &FindOptions::default()).unwrap())
}

pub fn assert_same_delete(filter: Document) {
    let Some(source) = matched_source(&filter) else {
        return;
    };
    let plan = Plan::Delete {
        collection: collection_ref(),
        source,
    };
    let v1 = v1_state_after(|txn| {
        txn.delete_many(DEFAULT_CF, COLL, filter.clone())
            .unwrap()
            .drain()
            .unwrap();
    });
    assert_same_set(v1, v2_state_after(plan));
}

pub fn assert_same_update(filter: Document, update: Document) {
    let Some(source) = matched_source(&filter) else {
        return;
    };
    let update_raw = RawDocumentBuf::try_from(&update).unwrap();
    let Ok(mutation) = slate_mutation::parse_mutation(&update_raw, "_id") else {
        return;
    };
    let plan = Plan::Update {
        collection: collection_ref(),
        mutation,
        source,
    };
    let v1 = v1_state_after(|txn| {
        txn.update_many(DEFAULT_CF, COLL, filter.clone(), update.clone())
            .unwrap()
            .drain()
            .unwrap();
    });
    assert_same_set(v1, v2_state_after(plan));
}

pub fn assert_same_replace(filter: Document, replacement: Document) {
    let Some(source) = matched_source(&filter) else {
        return;
    };
    let plan = Plan::Replace {
        collection: collection_ref(),
        replacement: RawDocumentBuf::try_from(&replacement).unwrap(),
        source,
    };
    let v1 = v1_state_after(|txn| {
        txn.replace_one(DEFAULT_CF, COLL, filter.clone(), replacement.clone())
            .unwrap()
            .drain()
            .unwrap();
    });
    assert_same_set(v1, v2_state_after(plan));
}

pub fn assert_same_upsert(docs: Vec<Document>, merge: bool) {
    let raws: Vec<bson::RawBson> = docs
        .iter()
        .map(|d| bson::RawBson::Document(RawDocumentBuf::try_from(d).unwrap()))
        .collect();
    let plan = Plan::Upsert {
        collection: collection_ref(),
        mode: if merge {
            UpsertMode::Merge
        } else {
            UpsertMode::Replace
        },
        hooks: vec![],
        source: Node::Values(raws),
    };
    let v1 = v1_state_after(|txn| {
        if merge {
            txn.merge_many(DEFAULT_CF, COLL, docs.clone())
                .unwrap()
                .drain()
                .unwrap();
        } else {
            txn.upsert_many(DEFAULT_CF, COLL, docs.clone())
                .unwrap()
                .drain()
                .unwrap();
        }
    });
    assert_same_set(v1, v2_state_after(plan));
}

// ── Distinct comparison ─────────────────────────────────────────

pub fn assert_same_distinct(field: &str, filter: Document) {
    // v1: distinct returns a single array of values.
    let db = v1_db();
    let txn = db.begin(true).unwrap();
    let arr = txn
        .distinct(
            DEFAULT_CF,
            COLL,
            field,
            filter.clone(),
            DistinctOptions::default(),
        )
        .unwrap();
    let mut v1: Vec<bson::Bson> = match arr {
        bson::RawBson::Array(a) => a
            .into_iter()
            .map(|r| bson::Bson::try_from(r.unwrap()).unwrap())
            .collect(),
        _ => panic!("distinct did not return an array"),
    };

    // v2: Scan → [Filter] → Project(c.field) → Distinct (single-binding mode).
    let binding = RowBinding::Alias("c".into());
    let mut node = Node::Scan {
        collection: collection_ref(),
    };
    let raw = RawDocumentBuf::try_from(&filter).unwrap();
    if let Some(Some(pred)) = Some(translate_filter(&raw)).flatten() {
        node = Node::Filter {
            predicate: pred,
            binding: binding.clone(),
            source: Box::new(node),
        };
    }
    node = Node::Project {
        expr: path(field),
        binding,
        source: Box::new(node),
    };
    node = Node::Distinct {
        source: Box::new(node),
    };

    let engine = v2_engine();
    let vtxn = engine.begin(true).unwrap();
    let mut v2: Vec<bson::Bson> = slate_executor::Executor::new(&vtxn)
        .execute_collect(Plan::Query(node))
        .unwrap()
        .into_iter()
        .map(|r| bson::Bson::try_from(r).unwrap())
        .collect();

    let key = |b: &bson::Bson| format!("{b:?}");
    v1.sort_by_key(key);
    v2.sort_by_key(key);
    assert_eq!(v1, v2, "distinct values differ");
}

fn fold(mut parts: Vec<ScalarExpr>, op: BinOp) -> Option<ScalarExpr> {
    match parts.len() {
        0 => None,
        1 => Some(parts.pop().unwrap()),
        _ => {
            let mut iter = parts.into_iter();
            let first = iter.next().unwrap();
            Some(iter.fold(first, |acc, e| binary(op, acc, e)))
        }
    }
}
