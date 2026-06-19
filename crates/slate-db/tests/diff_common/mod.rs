//! Shared harness for v1 ↔ v2 differential tests.
//!
//! Seeds two identical engines — a v1 [`Database`] and a raw `KvEngine` for v2 —
//! runs the same find request through each pipeline, and compares the results.
//! The common input is the find request `(filter_doc, FindOptions)` (matching
//! v1's public API); the v2 side runs it through the **real** production path —
//! `slate_query::find_to_query` (the Mongo front-end) then `slate_planner::lower`
//! — so these tests validate exactly what `slate-db` will route `find` through,
//! index paths included.
//!
//! Untranslatable requests (a filter the front-end can't yet express, e.g. a
//! document-valued comparison operand) translate to `None`, letting a test
//! skip — those gaps are exactly what differential testing surfaces.

#![allow(dead_code)]

use bson::{Document, RawDocumentBuf, doc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_engine::{Catalog, Engine, EngineTransaction, KvEngine};
use slate_query::FindOptions;
use slate_store::MemoryStore;

use slate_ast::ScalarExpr;
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
///
/// This is the **real production path**: the Mongo front-end (`slate-query`)
/// translates the request into the shared AST, and `slate-planner` lowers it —
/// the same code `slate-db` will route `find` through. `None` means the filter
/// hits a known gap (e.g. a non-scalar literal), so the test skips.
pub fn v2_plan(filter: &Document, options: &FindOptions) -> Option<Plan> {
    let raw = RawDocumentBuf::try_from(filter).ok()?;
    let query = slate_query::find_to_query(&raw, options).ok()?;
    Some(slate_planner::lower(query, collection_ref(), &meta()))
}

/// Index/pk metadata matching the seeded collection, so `lower` can choose
/// index paths. (Differential results must match regardless of the path taken.)
fn meta() -> slate_planner::CollectionMeta {
    slate_planner::CollectionMeta {
        indexes: indexes().iter().map(|s| s.to_string()).collect(),
        pk_path: "_id".into(),
    }
}

/// Member path `c.a.b` for a dotted field — used by the distinct builder, which
/// constructs a `Project` over the field directly.
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

fn collection_ref() -> CollectionRef {
    CollectionRef {
        cf: DEFAULT_CF.into(),
        collection: COLL.into(),
    }
}

/// The read-source node for a write: translate the filter through the real
/// front-end and lower it, then unwrap the resulting `Plan::Query` node (the
/// `Scan → [Filter] → Project(c)` tree) to wrap in a write plan — exactly the
/// shape `slate-db`'s write APIs will build.
fn matched_source(filter: &Document) -> Option<Node> {
    let raw = RawDocumentBuf::try_from(filter).ok()?;
    let query = slate_query::find_to_query(&raw, &FindOptions::default()).ok()?;
    match slate_planner::lower(query, collection_ref(), &meta()) {
        Plan::Query(node) => Some(node),
        _ => None,
    }
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
    if let Ok(Some(pred)) = slate_query::translate_filter(&raw) {
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
