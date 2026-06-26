//! Compound (multi-field) indexes, end to end.
//!
//! A compound index on `["status", "created_at"]` is matched by the
//! leftmost-prefix rule: it serves `{status}` and `{status, created_at}` but not
//! `{created_at}` alone. The pushdown is *invisible to results*: an indexed
//! query returns exactly the same rows as a full scan. These pin that invariant
//! against the tripwires — a leading value in a prefix relationship
//! (`active` ⊂ `active2`), so the byte seek over-reads and the recheck must drop
//! the spillover.

use bson::{Bson, doc};
use slate_db::v2::IndexOptions;
use slate_db::{Database, DatabaseBuilder};
use slate_store::MemoryStore;

/// Seed `orders` with `status` + `created_at`, optionally with a compound index
/// on `["status", "created_at"]`. The `active`/`active2` pair exercises the
/// leading-string byte-prefix over-read.
fn seed(indexed: bool) -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    db.collections().create("orders").execute(&txn).unwrap();
    db.collection("orders")
        .insert_many(vec![
            doc! { "_id": "a", "status": "active", "created_at": 10 },
            doc! { "_id": "b", "status": "active", "created_at": 20 },
            doc! { "_id": "c", "status": "active", "created_at": 30 },
            doc! { "_id": "d", "status": "active2", "created_at": 15 },
            doc! { "_id": "e", "status": "archived", "created_at": 25 },
            doc! { "_id": "f", "status": "archived", "created_at": 5 },
        ])
        .execute(&txn)
        .unwrap();
    // Create the index after data exists (also tests backfill).
    if indexed {
        db.collection("orders")
            .indexes()
            .create(["status", "created_at"], IndexOptions::default())
            .execute(&txn)
            .unwrap();
    }
    txn.commit().unwrap();
    db
}

/// The sorted `_id`s a `SELECT VALUE c._id … WHERE` selects from `orders`.
fn ids(db: &Database<MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let mut got: Vec<String> = db
        .collection("orders")
        .query(sql)
        .iter::<String>(&txn)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    got.sort();
    got
}

fn explain(db: &Database<MemoryStore>, sql: &str) -> String {
    let txn = db.begin(true).unwrap();
    let plan = db.collection("orders").query(sql).explain(&txn).unwrap();
    txn.rollback().unwrap();
    plan
}

// ── Plan shape ──────────────────────────────────────────────────

#[test]
fn two_equalities_use_compound_scan() {
    let db = seed(true);
    let plan = explain(
        &db,
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.created_at = 20",
    );
    assert!(
        plan.contains("CompoundIndexScan"),
        "expected a CompoundIndexScan, got:\n{plan}"
    );
}

#[test]
fn eq_then_range_uses_compound_scan() {
    let db = seed(true);
    let plan = explain(
        &db,
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.created_at > 15",
    );
    assert!(
        plan.contains("CompoundIndexScan"),
        "expected a CompoundIndexScan, got:\n{plan}"
    );
}

#[test]
fn leading_field_only_uses_compound_scan() {
    let db = seed(true);
    let plan = explain(&db, "SELECT VALUE c._id FROM c WHERE c.status = 'active'");
    assert!(
        plan.contains("CompoundIndexScan"),
        "leftmost-prefix should serve a leading-field-only query:\n{plan}"
    );
}

#[test]
fn non_leading_field_falls_back_to_scan() {
    let db = seed(true);
    let plan = explain(&db, "SELECT VALUE c._id FROM c WHERE c.created_at = 20");
    assert!(
        !plan.contains("CompoundIndexScan"),
        "a non-leading field alone cannot use the compound index:\n{plan}"
    );
    assert!(plan.contains("Scan"), "expected a full scan, got:\n{plan}");
}

// ── Results identical to a full scan ────────────────────────────

#[test]
fn indexed_results_match_full_scan() {
    let indexed = seed(true);
    let plain = seed(false);
    for sql in [
        "SELECT VALUE c._id FROM c WHERE c.status = 'active'",
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.created_at = 20",
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.created_at > 15",
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.created_at >= 20 AND c.created_at < 30",
        "SELECT VALUE c._id FROM c WHERE c.status = 'active2'",
        "SELECT VALUE c._id FROM c WHERE c.status = 'archived' AND c.created_at = 5",
    ] {
        assert_eq!(ids(&indexed, sql), ids(&plain, sql), "mismatch for `{sql}`");
    }
}

#[test]
fn leading_prefix_collision_is_excluded() {
    // status = "active" must NOT pick up the "active2" doc, even though the seek
    // over-reads its byte prefix.
    let db = seed(true);
    assert_eq!(
        ids(&db, "SELECT VALUE c._id FROM c WHERE c.status = 'active'"),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
}

#[test]
fn eq_then_range_selects_the_right_rows() {
    let db = seed(true);
    assert_eq!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.created_at > 15"
        ),
        vec!["b".to_string(), "c".to_string()]
    );
}

// ── Covering: serve the query from index entries (RFC Part B, phase 2) ──────
//
// A compound scan whose query reads only the index's components (plus the pk)
// is *covering*: the planner drops the `KeyLookup` and the executor synthesizes
// each row from the entry's per-component values. As with phase 1, this must be
// invisible to results — a covered query returns exactly what the materialized
// (unindexed full-scan) plan returns — and must NOT fire when the query reads
// the whole row or any non-component field.

/// Sorted debug forms of the values a SQL query returns from `orders`, for
/// comparing a covered plan against the materialized (unindexed) one.
fn vals(db: &Database<MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let mut out = db
        .collection("orders")
        .query(sql)
        .iter::<bson::Bson>(&txn)
        .unwrap()
        .map(|r| format!("{:?}", r.unwrap()))
        .collect::<Vec<_>>();
    out.sort();
    out
}

#[test]
fn covered_compound_projection_matches_materialized() {
    // Both components projected, filtered on the leading one — covered. The
    // synthesized rows must match the materialized full-scan plan exactly.
    for sql in [
        "SELECT c.status, c.created_at FROM c WHERE c.status = 'active'",
        "SELECT VALUE c.created_at FROM c WHERE c.status = 'active' AND c.created_at > 15",
        "SELECT c.status, c.created_at, c._id FROM c WHERE c.status = 'archived'",
    ] {
        assert_eq!(
            vals(&seed(true), sql),
            vals(&seed(false), sql),
            "covered/materialized mismatch for `{sql}`",
        );
    }
}

#[test]
fn covered_compound_plan_drops_key_lookup() {
    let plan = explain(
        &seed(true),
        "SELECT c.status, c.created_at FROM c WHERE c.status = 'active'",
    );
    assert!(
        plan.contains("CompoundIndexScan") && plan.contains("covering"),
        "expected a covering compound scan:\n{plan}"
    );
    assert!(
        !plan.contains("KeyLookup"),
        "a covered plan must drop the KeyLookup:\n{plan}"
    );
}

#[test]
fn whole_row_compound_query_keeps_key_lookup() {
    // `SELECT VALUE c` needs every field — the compound entry can't serve it, so
    // the fetch stays (the bail half of the invariant).
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

// ── Index maintenance on update / delete ────────────────────────

#[test]
fn update_maintains_compound_index() {
    let db = seed(true);
    // Move "b" out of the (active, 20) slot.
    {
        let txn = db.begin(false).unwrap();
        let filter = eq("_id", Bson::String("b".into()));
        db.collection("orders")
            .find(&filter)
            .update(doc! { "status": "archived" })
            .one()
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();
    }
    // status = "active" now omits "b".
    assert_eq!(
        ids(&db, "SELECT VALUE c._id FROM c WHERE c.status = 'active'"),
        vec!["a".to_string(), "c".to_string()]
    );
    // And "b" appears under archived.
    assert!(
        ids(&db, "SELECT VALUE c._id FROM c WHERE c.status = 'archived'")
            .contains(&"b".to_string())
    );
}

#[test]
fn delete_maintains_compound_index() {
    let db = seed(true);
    {
        let txn = db.begin(false).unwrap();
        let filter = eq("_id", Bson::String("a".into()));
        db.collection("orders")
            .find(&filter)
            .delete()
            .one()
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();
    }
    assert_eq!(
        ids(&db, "SELECT VALUE c._id FROM c WHERE c.status = 'active'"),
        vec!["b".to_string(), "c".to_string()]
    );
}

// ── Sparsity: a missing component yields no entry ───────────────

#[test]
fn missing_component_is_not_indexed() {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    {
        let txn = db.begin(false).unwrap();
        db.collections().create("orders").execute(&txn).unwrap();
        db.collection("orders")
            .insert_many(vec![
                doc! { "_id": "a", "status": "active", "created_at": 10 },
                // No `created_at` → not in the compound index.
                doc! { "_id": "b", "status": "active" },
            ])
            .execute(&txn)
            .unwrap();
        db.collection("orders")
            .indexes()
            .create(["status", "created_at"], IndexOptions::default())
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();
    }
    // The compound-index path returns only "a"; the residual recheck applied by
    // the planner over the index source keeps "b" out (it has no created_at).
    assert_eq!(
        ids(&db, "SELECT VALUE c._id FROM c WHERE c.status = 'active'"),
        vec!["a".to_string()]
    );
    // But a full-scan equality on just `status` still sees both (no index used).
    let txn = db.begin(true).unwrap();
    let found = db
        .collection("orders")
        .find(eq("status", Bson::String("active".into())))
        .iter_raw(&txn)
        .unwrap()
        .count();
    assert_eq!(found, 2);
}

// ── list_indexes reports the joined identity ────────────────────

#[test]
fn list_indexes_reports_compound_identity() {
    let db = seed(true);
    let txn = db.begin(true).unwrap();
    let indexes = db.collection("orders").indexes().list(&txn).unwrap();
    assert!(
        indexes
            .iter()
            .any(|i| i.contains("status") && i.contains("created_at")),
        "expected a compound identity in {indexes:?}"
    );
}

// ── Unique compound indexes ─────────────────────────────────────

/// A collection `members` with a unique compound index on `["org_id", "email"]`.
fn seed_members_unique() -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    db.collections().create("members").execute(&txn).unwrap();
    db.collection("members")
        .indexes()
        .create(["org_id", "email"], IndexOptions::unique())
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
    db
}

#[test]
fn unique_compound_allows_same_email_across_orgs() {
    let db = seed_members_unique();
    let txn = db.begin(false).unwrap();
    db.collection("members")
        .insert_one(doc! { "_id": "m1", "org_id": "acme", "email": "x@test.com" })
        .execute(&txn)
        .unwrap();
    // Same email, different org → allowed (the combination is distinct).
    db.collection("members")
        .insert_one(doc! { "_id": "m2", "org_id": "globex", "email": "x@test.com" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
}

#[test]
fn unique_compound_rejects_same_combination() {
    let db = seed_members_unique();
    let txn = db.begin(false).unwrap();
    db.collection("members")
        .insert_one(doc! { "_id": "m1", "org_id": "acme", "email": "x@test.com" })
        .execute(&txn)
        .unwrap();
    // Same org AND same email → the combination collides.
    let err = db
        .collection("members")
        .insert_one(doc! { "_id": "m2", "org_id": "acme", "email": "x@test.com" })
        .execute(&txn)
        .unwrap_err();
    assert!(
        matches!(err, slate_db::DbError::UniqueViolation { .. }),
        "expected DbError::UniqueViolation, got {err:?}"
    );
}

#[test]
fn unique_compound_backfill_detects_existing_duplicate() {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    db.collections().create("members").execute(&txn).unwrap();
    db.collection("members")
        .insert_many(vec![
            doc! { "_id": "m1", "org_id": "acme", "email": "x@test.com" },
            doc! { "_id": "m2", "org_id": "acme", "email": "x@test.com" },
        ])
        .execute(&txn)
        .unwrap();
    // Backfilling a unique compound index over duplicate combinations fails.
    let err = db
        .collection("members")
        .indexes()
        .create(["org_id", "email"], IndexOptions::unique())
        .execute(&txn)
        .unwrap_err();
    assert!(
        matches!(err, slate_db::DbError::UniqueViolation { .. }),
        "expected DbError::UniqueViolation, got {err:?}"
    );
}

fn eq(field: &str, value: Bson) -> bson::RawDocumentBuf {
    let mut doc = bson::Document::new();
    doc.insert(field.to_string(), value);
    bson::RawDocumentBuf::try_from(&doc).unwrap()
}
