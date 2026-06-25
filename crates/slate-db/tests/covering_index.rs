//! Covering index scans (RFC: Covering Index Scans, Part B — phase 1).
//!
//! When a query reads only an indexed field (and the pk), the scan serves it
//! from index entries and the planner drops the document fetch. The optimization
//! must be **invisible to results**: a covered query returns exactly what the
//! materialized (unindexed → full-scan) plan returns. And it must **not** fire
//! when the query reads anything else (the whole row, an unindexed field, or a
//! deeper path) — those keep the `KeyLookup`.
//!
//! These pin both halves: a differential (`indexed == unindexed`) for the value
//! invariant, and `EXPLAIN` assertions that the covering scan actually replaces
//! the `KeyLookup` (so the differential can't pass vacuously by never covering).

use bson::doc;
use bson::raw::RawDocumentBuf;
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_query::FindOptions;
use slate_store::MemoryStore;

/// Seed `people` with a scalar `status`, indexed only when `indexed`. The other
/// fields are deliberately present so a covered projection must avoid reading
/// them: `name` (an unindexed sibling), `meta.note` (the deeper-path tripwire),
/// and the `Active` row (case-sensitivity). Row 4 omits `meta` entirely.
fn seed(indexed: bool) -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "people".into(),
        ..Default::default()
    })
    .unwrap();
    if indexed {
        txn.create_index(DEFAULT_CF, "people", "status").unwrap();
    }
    txn.insert_many(
        DEFAULT_CF,
        "people",
        vec![
            doc! { "_id": "1", "status": "active", "name": "ada", "meta": { "note": "x" } },
            doc! { "_id": "2", "status": "inactive", "name": "bob", "meta": { "note": "y" } },
            doc! { "_id": "3", "status": "active", "name": "cy", "meta": { "note": "z" } },
            doc! { "_id": "4", "status": "Active", "name": "dee" },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

/// Run `find(filter, columns)` and return the result documents sorted by `_id`
/// (the two plans stream rows in different orders, so sort before comparing).
fn find_sorted(
    db: &Database<MemoryStore>,
    filter: bson::Document,
    columns: Option<Vec<String>>,
) -> Vec<RawDocumentBuf> {
    let txn = db.begin(true).unwrap();
    let options = FindOptions {
        columns,
        ..FindOptions::default()
    };
    let mut docs = txn
        .find(DEFAULT_CF, "people", filter, options)
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<RawDocumentBuf>, _>>()
        .unwrap();
    docs.sort_by(|a, b| id_of(a).cmp(&id_of(b)));
    docs
}

/// The `_id` string of a result document (empty when absent).
fn id_of(doc: &RawDocumentBuf) -> String {
    doc.get_str("_id").unwrap_or_default().to_string()
}

/// Run SQL and return result values sorted by their debug form (heterogeneous
/// values across queries — documents, strings — so a canonical string sort).
fn query_sorted(db: &Database<MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let mut out = txn
        .query(DEFAULT_CF, "people", sql)
        .unwrap()
        .iter_values::<bson::Bson>()
        .unwrap()
        .map(|r| format!("{:?}", r.unwrap()))
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn explain(db: &Database<MemoryStore>, sql: &str) -> String {
    let txn = db.begin(true).unwrap();
    txn.explain(DEFAULT_CF, "people", sql).unwrap()
}

// ── The value invariant: covered == materialized ────────────────────────────

#[test]
fn covered_projection_matches_materialized() {
    let filter = doc! { "status": "active" };
    let columns = Some(vec!["status".to_string()]);
    let covered = find_sorted(&seed(true), filter.clone(), columns.clone());
    let materialized = find_sorted(&seed(false), filter, columns);

    // Same rows, byte-for-byte (the projection rebuilds `{_id, status}` in both
    // plans; the synthesized row is only the Project's input).
    assert_eq!(covered, materialized);
    // Sanity: the two `active` rows, projected to just `_id` + `status`.
    assert_eq!(covered.len(), 2);
    for d in &covered {
        assert_eq!(d.iter().count(), 2, "projected only _id + status: {d:?}");
        assert!(d.get_str("_id").is_ok(), "carries the pk: {d:?}");
        assert!(
            d.get_str("status").is_ok(),
            "carries the indexed field: {d:?}"
        );
    }
}

#[test]
fn covered_range_projection_matches_materialized() {
    // A range predicate on the indexed field is still covered (the synthesized
    // value passes the executor's in-scan post-filter, and the projection reads
    // only `status`).
    let filter = doc! { "status": { "$gte": "a" } };
    let columns = Some(vec!["status".to_string()]);
    assert_eq!(
        find_sorted(&seed(true), filter.clone(), columns.clone()),
        find_sorted(&seed(false), filter, columns),
    );
}

#[test]
fn bail_cases_match_materialized() {
    // Whole row, an unindexed field, and a deeper path: each must return the same
    // values with or without the index (the pass bails, keeping the fetch).
    for sql in [
        "SELECT VALUE c FROM c WHERE c.status = 'active'",
        "SELECT VALUE c.name FROM c WHERE c.status = 'active'",
        "SELECT VALUE c.meta.note FROM c WHERE c.status = 'active'",
    ] {
        assert_eq!(
            query_sorted(&seed(true), sql),
            query_sorted(&seed(false), sql),
            "indexed/unindexed mismatch for `{sql}`",
        );
    }
}

// ── The plan actually changes: covering replaces the KeyLookup ───────────────

#[test]
fn covered_query_plan_drops_key_lookup() {
    let plan = explain(
        &seed(true),
        "SELECT VALUE c.status FROM c WHERE c.status = 'active'",
    );
    assert!(
        plan.contains("covering"),
        "expected a covering scan:\n{plan}"
    );
    assert!(
        !plan.contains("KeyLookup"),
        "a covered plan must drop the KeyLookup:\n{plan}"
    );
}

#[test]
fn covered_plan_keeps_a_retained_residual_filter() {
    // STARTSWITH keeps a residual Filter on the indexed field; covering still
    // applies (the Filter reads `status` from the synthesized row).
    let plan = explain(
        &seed(true),
        "SELECT VALUE c.status FROM c WHERE STARTSWITH(c.status, 'a')",
    );
    assert!(
        plan.contains("covering"),
        "expected a covering scan:\n{plan}"
    );
    assert!(
        plan.contains("Filter"),
        "expected the retained residual:\n{plan}"
    );
    assert!(!plan.contains("KeyLookup"), "covered, no fetch:\n{plan}");
}

#[test]
fn whole_row_query_keeps_key_lookup() {
    let plan = explain(
        &seed(true),
        "SELECT VALUE c FROM c WHERE c.status = 'active'",
    );
    assert!(
        !plan.contains("covering"),
        "whole-row read must not cover:\n{plan}"
    );
    assert!(
        plan.contains("KeyLookup"),
        "whole-row read needs the fetch:\n{plan}"
    );
}

#[test]
fn unindexed_field_query_keeps_key_lookup() {
    let plan = explain(
        &seed(true),
        "SELECT VALUE c.name FROM c WHERE c.status = 'active'",
    );
    assert!(
        !plan.contains("covering"),
        "reading `name` can't cover:\n{plan}"
    );
    assert!(
        plan.contains("KeyLookup"),
        "must fetch to read `name`:\n{plan}"
    );
}
