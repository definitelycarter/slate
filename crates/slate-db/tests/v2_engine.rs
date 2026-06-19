//! End-to-end check that `Database::find` under `QueryEngine::V2` returns the
//! same results as `V1`, through the real public API (not just the plan layer,
//! which `diff_*` covers). This is the production read path: find → slate-query
//! → slate-planner → slate-executor.

use bson::{Document, doc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder, QueryEngine};
use slate_query::{FindOptions, Sort, SortDirection};
use slate_store::MemoryStore;

const COLL: &str = "people";

fn dataset() -> Vec<Document> {
    vec![
        doc! { "_id": "1", "name": "ada", "age": 36, "status": "active", "tags": ["math", "logic"] },
        doc! { "_id": "2", "name": "alan", "age": 41, "status": "inactive", "tags": ["computing"] },
        doc! { "_id": "3", "name": "grace", "age": 44, "status": "active", "tags": ["computing"] },
        doc! { "_id": "4", "name": "edsger", "age": 52, "status": "active", "tags": ["rigour"] },
    ]
}

fn db(engine: QueryEngine) -> Database<MemoryStore> {
    let db = DatabaseBuilder::new()
        .query_engine(engine)
        .open(MemoryStore::new())
        .unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLL.into(),
        ..Default::default()
    })
    .unwrap();
    for f in ["age", "status"] {
        txn.create_index(DEFAULT_CF, COLL, f).unwrap();
    }
    txn.insert_many(DEFAULT_CF, COLL, dataset())
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();
    db
}

fn run(db: &Database<MemoryStore>, filter: Document, options: FindOptions) -> Vec<Document> {
    let txn = db.begin(true).unwrap();
    txn.find(DEFAULT_CF, COLL, filter, options)
        .unwrap()
        .iter::<Document>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// Compare v1 and v2 results as an `_id`-keyed set (find order is unspecified
/// without ORDER BY).
fn assert_same(filter: Document, options: FindOptions) {
    let v1 = db(QueryEngine::V1);
    let v2 = db(QueryEngine::V2);
    let mut a = run(&v1, filter.clone(), options.clone());
    let mut b = run(&v2, filter, options);
    let key = |d: &Document| d.get_str("_id").unwrap_or("").to_string();
    a.sort_by_key(key);
    b.sort_by_key(key);
    assert_eq!(a, b, "v1 (left) and v2 (right) differ");
}

/// Compare in order (for ORDER BY).
fn assert_same_ordered(filter: Document, options: FindOptions) {
    let v1 = db(QueryEngine::V1);
    let v2 = db(QueryEngine::V2);
    let a = run(&v1, filter.clone(), options.clone());
    let b = run(&v2, filter, options);
    assert_eq!(a, b, "v1 (left) and v2 (right) differ in order");
}

fn sorted(field: &str, dir: SortDirection) -> FindOptions {
    FindOptions {
        sort: vec![Sort {
            field: field.into(),
            direction: dir,
        }],
        ..Default::default()
    }
}

#[test]
fn scan_all() {
    assert_same(doc! {}, FindOptions::default());
}

#[test]
fn implicit_eq_indexed() {
    assert_same(doc! { "age": 41 }, FindOptions::default());
}

#[test]
fn implicit_eq_unindexed() {
    assert_same(doc! { "name": "grace" }, FindOptions::default());
}

#[test]
fn range_on_index() {
    assert_same(doc! { "age": { "$gt": 40 } }, FindOptions::default());
    assert_same(
        doc! { "age": { "$gte": 41, "$lt": 52 } },
        FindOptions::default(),
    );
}

#[test]
fn and_across_indexed_fields() {
    assert_same(
        doc! { "$and": [ { "status": "active" }, { "age": { "$gt": 40 } } ] },
        FindOptions::default(),
    );
}

#[test]
fn or_on_index() {
    assert_same(
        doc! { "$or": [ { "age": 36 }, { "age": 52 } ] },
        FindOptions::default(),
    );
}

#[test]
fn no_match_returns_empty() {
    assert_same(doc! { "age": { "$gt": 1000 } }, FindOptions::default());
}

#[test]
fn sort_indexed_and_unindexed() {
    assert_same_ordered(doc! {}, sorted("age", SortDirection::Desc));
    assert_same_ordered(doc! {}, sorted("name", SortDirection::Asc));
}

#[test]
fn sort_with_skip_take() {
    let opts = FindOptions {
        sort: vec![Sort {
            field: "age".into(),
            direction: SortDirection::Asc,
        }],
        skip: Some(1),
        take: Some(2),
        ..Default::default()
    };
    assert_same_ordered(doc! {}, opts);
}

#[test]
fn projection_columns() {
    let opts = FindOptions {
        columns: Some(vec!["name".into()]),
        ..Default::default()
    };
    assert_same(doc! { "status": "active" }, opts);
}

#[test]
fn unknown_operator_errors_in_both_engines() {
    // `$in` is unknown to both engines. The v2 path can't translate it and
    // falls back to v1, which also rejects it — so the error is consistent and
    // the fall-back never masks a failure.
    for engine in [QueryEngine::V1, QueryEngine::V2] {
        let d = db(engine);
        let txn = d.begin(true).unwrap();
        let r = txn.find(
            DEFAULT_CF,
            COLL,
            doc! { "age": { "$in": [36, 44] } },
            FindOptions::default(),
        );
        assert!(r.is_err(), "{engine:?} should reject $in");
    }
}
