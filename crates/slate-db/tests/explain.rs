//! `Transaction::explain` — lower a query to a plan and render the plan tree
//! without running it. These pin that the rendered plan reflects the planner's
//! real index choice: an indexed equality becomes an `IndexScan`→`KeyLookup`,
//! while an unindexed predicate stays a `Scan`→`Filter`.

use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_store::MemoryStore;

fn seeded() -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "people".into(),
        ..Default::default()
    })
    .unwrap();
    // `name` is indexed; `age` deliberately is not, so the two predicates below
    // take different plan shapes.
    txn.create_index(DEFAULT_CF, "people", "name").unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "people",
        vec![
            bson::doc! { "_id": "1", "name": "ada", "age": 36 },
            bson::doc! { "_id": "2", "name": "alan", "age": 41 },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

fn explain(db: &Database<MemoryStore>, sql: &str) -> String {
    let txn = db.begin(true).unwrap();
    let plan = txn.explain(DEFAULT_CF, "people", sql).unwrap();
    txn.rollback().unwrap();
    plan
}

#[test]
fn indexed_equality_uses_index_scan_then_key_lookup() {
    // Projecting `c.age` (not indexed) forces the document fetch — so this keeps
    // the IndexScan→KeyLookup pairing. (Projecting only the indexed `c.name`
    // would drop the KeyLookup as a covering scan; see `covering_index.rs`.)
    let plan = explain(
        &seeded(),
        "SELECT VALUE c.age FROM c WHERE c.name = \"ada\"",
    );
    // The sargable `name` equality is pushed into an index scan whose IDs the
    // key lookup resolves to documents — the index is consulted, not the heap.
    assert!(plan.contains("IndexScan"), "expected an index scan: {plan}");
    assert!(plan.contains("KeyLookup"), "expected a key lookup: {plan}");
    assert!(
        plan.contains("people.name"),
        "index scan should name the indexed field: {plan}"
    );
    // The key-lookup feeds from the index scan: IndexScan is more deeply
    // indented (a child) than KeyLookup.
    let key_lookup = plan.find("KeyLookup").unwrap();
    let index_scan = plan.find("IndexScan").unwrap();
    assert!(
        key_lookup < index_scan,
        "KeyLookup should sit above IndexScan: {plan}"
    );
}

#[test]
fn unindexed_predicate_stays_a_filtered_scan() {
    let plan = explain(&seeded(), "SELECT VALUE c.name FROM c WHERE c.age = 36");
    // `age` has no index, so the predicate cannot be pushed down — a full scan
    // gated by a filter.
    assert!(plan.contains("Scan"), "expected a scan: {plan}");
    assert!(
        !plan.contains("IndexScan"),
        "an unindexed predicate must not produce an index scan: {plan}"
    );
    assert!(
        plan.contains("Filter c.age = 36"),
        "expected the residual filter: {plan}"
    );
}

/// A collection whose `tags.[]` multikey index lets `ARRAY_CONTAINS` push down.
fn seeded_tags() -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "posts".into(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "posts", "tags.[]").unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "posts",
        vec![
            bson::doc! { "_id": "1", "tags": ["rust", "db"] },
            bson::doc! { "_id": "2", "tags": ["go", "api"] },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

fn explain_tags(db: &Database<MemoryStore>, sql: &str) -> String {
    let txn = db.begin(true).unwrap();
    let plan = txn.explain(DEFAULT_CF, "posts", sql).unwrap();
    txn.rollback().unwrap();
    plan
}

#[test]
fn array_contains_over_multikey_index_plans_as_index_scan() {
    // SQL `ARRAY_CONTAINS(c.tags, 'x')` over a `tags.[]` index reaches the
    // multikey element index — an IndexScan on `tags.[]`, not a full scan — with
    // the original predicate retained as a residual recheck.
    let plan = explain_tags(
        &seeded_tags(),
        "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, \"rust\")",
    );
    assert!(plan.contains("IndexScan"), "expected an index scan: {plan}");
    assert!(
        plan.contains("posts.tags.[]"),
        "index scan should name the multikey field: {plan}"
    );
    assert!(
        plan.contains("Filter ARRAY_CONTAINS(c.tags, \"rust\")"),
        "the original predicate should be retained as a recheck: {plan}"
    );
}

#[test]
fn array_contains_three_arg_partial_stays_a_filtered_scan() {
    // The 3-arg partial form matches more than the indexed elements, so it is
    // not pushed down: a full scan gated by the filter, no IndexScan.
    let plan = explain_tags(
        &seeded_tags(),
        "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, \"rust\", true)",
    );
    assert!(
        !plan.contains("IndexScan"),
        "the 3-arg partial form must not use the index: {plan}"
    );
    assert!(
        plan.contains("Filter ARRAY_CONTAINS(c.tags, \"rust\", true)"),
        "expected the residual filter: {plan}"
    );
}

#[test]
fn references_to_parameters_are_rejected_like_query() {
    // `explain` binds no parameters (matching `query`), so a query that names an
    // `@parameter` is an error rather than a misleading plan.
    let db = seeded();
    let txn = db.begin(true).unwrap();
    let err = txn
        .explain(
            DEFAULT_CF,
            "people",
            "SELECT VALUE c.name FROM c WHERE c.age = @min",
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("@min"),
        "error should name the missing parameter: {err}"
    );
}
