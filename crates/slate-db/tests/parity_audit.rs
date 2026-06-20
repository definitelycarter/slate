//! Feature-parity audit: v1 vs v2 through the **real Database API**, over
//! edge-case data the curated `diff_*` suite doesn't cover (null vs missing,
//! mixed numeric types, nested docs, array range/multikey, nested logical,
//! string-coerced queries).
//!
//! Comparing through the Database means a filter v2 can't translate falls back
//! to v1 (so it trivially matches) — a *divergence only shows when v2 actually
//! runs the query and produces different rows*. That's exactly what we want to
//! find.

use bson::{Bson, Document, doc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder, QueryEngine};
use slate_query::FindOptions;
use slate_store::MemoryStore;

const COLL: &str = "people";

/// Edge-case dataset: explicit null, a missing field, mixed numeric types,
/// nested docs, and arrays with several elements.
fn dataset() -> Vec<Document> {
    vec![
        doc! { "_id": "1", "n": "ada",   "age": 30_i32, "tags": ["x", "y"],  "addr": { "city": "nyc" }, "maybe": Bson::Null, "events": [{ "kind": "click" }, { "kind": "view" }] },
        doc! { "_id": "2", "n": "alan",  "age": 40_i64, "tags": ["y", "z"],  "addr": { "city": "ldn" }, "events": [{ "kind": "view" }] },
        doc! { "_id": "3", "n": "grace", "age": 50.0,   "tags": ["z"] /* no addr, no maybe */ },
        doc! { "_id": "4", "n": "kurt",  "age": 30_i64, "tags": [],          "maybe": "set" },
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
    txn.create_index(DEFAULT_CF, COLL, "age").unwrap();
    txn.insert_many(DEFAULT_CF, COLL, dataset())
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();
    db
}

/// Matching `_id`s (sorted) for a filter on the given engine.
fn ids(engine: QueryEngine, filter: Document) -> Vec<String> {
    let db = db(engine);
    let txn = db.begin(true).unwrap();
    let mut out: Vec<String> = txn
        .find(DEFAULT_CF, COLL, filter, FindOptions::default())
        .unwrap()
        .iter::<Document>()
        .unwrap()
        .map(|d| d.unwrap().get_str("_id").unwrap().to_string())
        .collect();
    out.sort();
    out
}

/// v1 and v2 must return the same documents for `filter`.
fn assert_parity(filter: Document) {
    let v1 = ids(QueryEngine::V1, filter.clone());
    let v2 = ids(QueryEngine::V2, filter.clone());
    assert_eq!(v1, v2, "v1 vs v2 differ for filter {filter:?}");
}

// ── Behaviors that SHOULD match ──────────────────────────────────

#[test]
fn mixed_numeric_equality_coerces() {
    // age is stored as Int32 (1,4) / Int64 (2) / Double (3). A numeric query
    // value should match across representations.
    assert_parity(doc! { "age": 30 });
    assert_parity(doc! { "age": 40 });
    assert_parity(doc! { "age": 50 });
}

#[test]
fn numeric_range_across_types() {
    assert_parity(doc! { "age": { "$gt": 35 } });
    assert_parity(doc! { "age": { "$gte": 40, "$lt": 50 } });
}

#[test]
fn exists_true_counts_null_as_present() {
    // `maybe` is null in doc 1, a string in doc 4, absent in 2 & 3.
    assert_parity(doc! { "maybe": { "$exists": true } });
}

#[test]
fn exists_false_on_missing() {
    assert_parity(doc! { "maybe": { "$exists": false } });
}

#[test]
fn array_multikey_membership() {
    assert_parity(doc! { "tags": "y" });
    assert_parity(doc! { "tags": "z" });
    assert_parity(doc! { "tags": "absent" });
}

#[test]
fn nested_logical_and_or() {
    assert_parity(doc! {
        "$and": [
            { "$or": [ { "age": { "$lt": 35 } }, { "age": { "$gt": 45 } } ] },
            { "tags": "y" },
        ]
    });
}

#[test]
fn ne_is_rejected_by_both_engines() {
    // Neither engine supports `$ne` (v2 deliberately doesn't translate it, so it
    // falls back to v1, which rejects it) — so the behavior is consistent.
    for engine in [QueryEngine::V1, QueryEngine::V2] {
        let d = db(engine);
        let txn = d.begin(true).unwrap();
        let r = txn.find(
            DEFAULT_CF,
            COLL,
            doc! { "n": { "$ne": "ada" } },
            FindOptions::default(),
        );
        assert!(r.is_err(), "{engine:?} should reject $ne");
    }
}

#[test]
fn subdocument_equality() {
    // v2 can't express a document-valued operand → falls back to v1, so the
    // results must still match.
    assert_parity(doc! { "addr": { "city": "nyc" } });
}

// ── Suspected divergences (these document v2 gaps) ───────────────

#[test]
fn null_equality_matches_missing_and_null() {
    // Mongo/v1: `{maybe: null}` matches BOTH explicit null (doc 1) and missing
    // (docs 2, 3). v2 must agree.
    assert_parity(doc! { "maybe": Bson::Null });
}

#[test]
fn multikey_range_on_array_is_a_known_gap() {
    // KNOWN GAP: Mongo/v1 applies range operators element-wise to arrays, so
    // `{tags: {$gt: "y"}}` matches docs whose array has any element > "y"
    // (docs 2 ["y","z"], 3 ["z"]). v2's translator emits a plain `c.tags > "y"`,
    // which compares the whole array (not a scalar) → no match. Tracked for the
    // future; documented here so a fix flips this test.
    assert_eq!(
        ids(QueryEngine::V1, doc! { "tags": { "$gt": "y" } }),
        vec!["2", "3"]
    );
    assert_eq!(
        ids(QueryEngine::V2, doc! { "tags": { "$gt": "y" } }),
        Vec::<String>::new()
    );
}

#[test]
fn string_coerced_numeric_query() {
    // v1 and v2 agree here (a string operand does not match a numeric field).
    assert_parity(doc! { "age": "30" });
}

#[test]
fn dotted_path_into_array_filter_matches() {
    // For FILTERS, neither engine traverses a dotted path into an array of
    // subdocuments — `{"events.kind": "click"}` matches nothing in v1 OR v2, so
    // they agree. (Distinct differs: v1's distinct DOES traverse array paths and
    // v2's doesn't yet — tracked in the v1-removal blockers, exercised by
    // `tests/distinct.rs::distinct_array_of_sub_documents` under v1.)
    assert_parity(doc! { "events.kind": "click" });
}
