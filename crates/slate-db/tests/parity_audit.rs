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
use slate_query::{FindOptions, Sort, SortDirection};
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
    // age is stored as Int32 (1) / Int64 (2, 4) / Double (3). A numeric query
    // value matches across representations. v1 is NOT a clean oracle here: its
    // index-Eq post-filter only coerces Int32/Int64 (it drops Double), so we
    // pin v2 against the correct result rather than against v1.
    assert_eq!(ids(QueryEngine::V2, doc! { "age": 30 }), vec!["1", "4"]);
    assert_eq!(ids(QueryEngine::V2, doc! { "age": 40 }), vec!["2"]);
    assert_eq!(ids(QueryEngine::V2, doc! { "age": 50 }), vec!["3"]); // Double-stored
}

#[test]
fn numeric_range_across_types() {
    // Cross-type numeric ranges over an indexed field. v1 is NOT a clean oracle:
    // a typed index range scan over-returns when the bound's type differs from
    // the stored values, so pin v2 == correct. ages: 30(i32) 40(i64) 50.0(f64) 30(i64).
    assert_eq!(
        ids(QueryEngine::V2, doc! { "age": { "$gt": 35 } }),
        vec!["2", "3"]
    );
    assert_eq!(
        ids(QueryEngine::V2, doc! { "age": { "$gte": 40, "$lt": 50 } }),
        vec!["2"]
    );
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
fn dotted_column_projection_nests() {
    // Projecting `addr.city` must NEST — `{addr: {city: ...}}` with only the
    // requested subfield — not a flat `{"addr.city": ...}` key. Scoped to docs
    // that HAVE `addr`: when the parent is missing, v1 omits the key while v2
    // emits an empty `{addr: {}}` (a minor Mongo-vs-Cosmos object-construction
    // difference, tracked in the v1-removal blockers).
    let opts = FindOptions {
        columns: Some(vec!["addr.city".into()]),
        ..Default::default()
    };
    let canon = |engine| -> Vec<std::collections::BTreeMap<String, Bson>> {
        let d = db(engine);
        let txn = d.begin(true).unwrap();
        let mut out: Vec<_> = txn
            .find(
                DEFAULT_CF,
                COLL,
                doc! { "addr": { "$exists": true } },
                opts.clone(),
            )
            .unwrap()
            .iter::<Document>()
            .unwrap()
            .map(|r| {
                r.unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .collect();
        out.sort_by_key(|m: &std::collections::BTreeMap<String, Bson>| {
            m.get("_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        });
        out
    };
    assert_eq!(canon(QueryEngine::V1), canon(QueryEngine::V2));
}

#[test]
fn distinct_traverses_array_path() {
    // distinct over `events.kind` (an array of subdocs) traverses into the
    // array and collects each element's `kind` — like v1.
    let vals = |engine| -> Vec<Bson> {
        let d = db(engine);
        let txn = d.begin(true).unwrap();
        let raw = txn
            .distinct(
                DEFAULT_CF,
                COLL,
                "events.kind",
                doc! {},
                slate_query::DistinctOptions::default(),
            )
            .unwrap();
        let mut v: Vec<Bson> = match raw {
            bson::RawBson::Array(a) => a
                .into_iter()
                .map(|r| Bson::try_from(r.unwrap()).unwrap())
                .collect(),
            other => panic!("not an array: {other:?}"),
        };
        v.sort_by_key(|b| format!("{b:?}"));
        v
    };
    let v1 = vals(QueryEngine::V1);
    assert_eq!(v1, vals(QueryEngine::V2));
    // sanity: it actually found the nested values (not empty)
    assert!(v1.contains(&Bson::String("click".into())));
    assert!(v1.contains(&Bson::String("view".into())));
}

#[test]
fn explicit_multikey_index_query() {
    // `create_index("tags.[]")` + `{tags.[]: x}` — explicit multikey array
    // indexing. v1 only works *via* the index; v2 works with or without it.
    // Compare v1 and v2 over a freshly-seeded indexed collection.
    fn seed(engine: QueryEngine) -> (Database<MemoryStore>, &'static str) {
        let db = DatabaseBuilder::new()
            .query_engine(engine)
            .open(MemoryStore::new())
            .unwrap();
        let txn = db.begin(false).unwrap();
        txn.create_collection(&CollectionConfig {
            name: "posts".into(),
            ..Default::default()
        })
        .unwrap();
        txn.create_index(DEFAULT_CF, "posts", "tags.[]").unwrap();
        txn.create_index(DEFAULT_CF, "posts", "items.[].sku")
            .unwrap();
        txn.insert_many(
            DEFAULT_CF,
            "posts",
            vec![
                doc! { "_id": "a", "tags": ["rust", "db"], "items": [{ "sku": "A1" }] },
                doc! { "_id": "b", "tags": ["go", "api"],  "items": [{ "sku": "B2" }, { "sku": "A1" }] },
                doc! { "_id": "c", "tags": ["rust", "api"], "items": [{ "sku": "C3" }] },
            ],
        )
        .unwrap()
        .drain()
        .unwrap();
        txn.commit().unwrap();
        (db, "posts")
    }
    let q = |engine, filter: Document| -> Vec<String> {
        let (db, coll) = seed(engine);
        let txn = db.begin(true).unwrap();
        let mut out: Vec<String> = txn
            .find(DEFAULT_CF, coll, filter, FindOptions::default())
            .unwrap()
            .iter::<Document>()
            .unwrap()
            .map(|d| d.unwrap().get_str("_id").unwrap().to_string())
            .collect();
        out.sort();
        out
    };
    // scalar array element
    let v1 = q(QueryEngine::V1, doc! { "tags.[]": "rust" });
    assert_eq!(v1, q(QueryEngine::V2, doc! { "tags.[]": "rust" }));
    assert_eq!(v1, vec!["a", "c"]);
    // nested path into array of subdocs
    let v1n = q(QueryEngine::V1, doc! { "items.[].sku": "A1" });
    assert_eq!(v1n, q(QueryEngine::V2, doc! { "items.[].sku": "A1" }));
    assert_eq!(v1n, vec!["a", "b"]);
}

// ── Cases where v2 is MORE correct than v1 (v1 bugs, found by the fuzzer) ──

#[test]
fn sort_take_on_missing_field_v2_keeps_doc() {
    // v1's index-ordered `sort + take` reads from the (indexed) sort field's
    // index, which has NO entry for a doc missing that field — so v1 silently
    // drops it. v2 full-sorts and keeps it (missing sorts first, Mongo-style).
    // v2 is correct; this documents the intentional divergence.
    fn run(engine: QueryEngine) -> Vec<String> {
        let db = DatabaseBuilder::new()
            .query_engine(engine)
            .open(MemoryStore::new())
            .unwrap();
        let txn = db.begin(false).unwrap();
        txn.create_collection(&CollectionConfig {
            name: "s".into(),
            ..Default::default()
        })
        .unwrap();
        txn.create_index(DEFAULT_CF, "s", "name").unwrap();
        txn.insert_many(
            DEFAULT_CF,
            "s",
            vec![
                doc! { "_id": "1", "name": "b" },
                doc! { "_id": "2", "name": "a" },
                doc! { "_id": "3" }, // no name
            ],
        )
        .unwrap()
        .drain()
        .unwrap();
        txn.commit().unwrap();
        let opts = FindOptions {
            sort: vec![Sort {
                field: "name".into(),
                direction: SortDirection::Asc,
            }],
            take: Some(2),
            ..Default::default()
        };
        let txn = db.begin(true).unwrap();
        txn.find(DEFAULT_CF, "s", doc! {}, opts)
            .unwrap()
            .iter::<Document>()
            .unwrap()
            .map(|d| d.unwrap().get_str("_id").unwrap().to_string())
            .collect()
    }
    // v1 drops the missing-name doc (3); v2 keeps it, sorted first.
    assert_eq!(run(QueryEngine::V1), vec!["2", "1"]);
    assert_eq!(run(QueryEngine::V2), vec!["3", "2"]);
}

#[test]
fn multikey_inside_logical_v2_evaluates_it() {
    // A `.[]` predicate inside `$or`/`$and` is *evaluated*, not index-looked-up.
    // v1's RawField resolver can't resolve a `.[]` path, so it's always-false
    // there (v1's `.[]` only works as a standalone indexed equality). v2
    // evaluates it correctly. v2 is correct.
    fn run(engine: QueryEngine) -> Vec<String> {
        let db = DatabaseBuilder::new()
            .query_engine(engine)
            .open(MemoryStore::new())
            .unwrap();
        let txn = db.begin(false).unwrap();
        txn.create_collection(&CollectionConfig {
            name: "m".into(),
            ..Default::default()
        })
        .unwrap();
        txn.create_index(DEFAULT_CF, "m", "tags.[]").unwrap();
        txn.insert_many(
            DEFAULT_CF,
            "m",
            vec![
                doc! { "_id": "1", "tags": ["z"] },
                doc! { "_id": "2", "tags": ["q"] },
            ],
        )
        .unwrap()
        .drain()
        .unwrap();
        txn.commit().unwrap();
        let txn = db.begin(true).unwrap();
        let mut out: Vec<String> = txn
            .find(
                DEFAULT_CF,
                "m",
                doc! { "$or": [ { "tags.[]": "z" }, { "_id": "2" } ] },
                FindOptions::default(),
            )
            .unwrap()
            .iter::<Document>()
            .unwrap()
            .map(|d| d.unwrap().get_str("_id").unwrap().to_string())
            .collect();
        out.sort();
        out
    }
    // v1 only matches via `_id` (the `.[]` branch is always-false); v2 matches both.
    assert_eq!(run(QueryEngine::V1), vec!["2"]);
    assert_eq!(run(QueryEngine::V2), vec!["1", "2"]);
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
