//! Index intersection (Index Intersection RFC, Door A) at the db surface.
//!
//! An all-equality `AND` of indexed fields plans as a galloping `IndexIntersect`
//! (observable via `.explain`), and the intersection is **invisible to results**:
//! an indexed query returns exactly the same rows as the same query over an
//! unindexed collection (a full `Scan` + `Filter`). A non-equality part falls
//! back to today's `IndexMerge(And)`.

use slate_db::v2::IndexOptions;
use slate_db::{Database, DatabaseBuilder};
use slate_store::MemoryStore;

/// Seed `nba` with a skewed corpus. With `indexed`, single-field indexes on
/// `status` (half the rows — the large side), `user` (10 users — selective),
/// `region`, and a numeric `score` are created, so an all-equality `AND` plans
/// as `IndexIntersect` and an `Eq AND Range` falls back to `IndexMerge`.
fn seed(indexed: bool) -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    db.collections().create("nba").execute(&txn).unwrap();
    if indexed {
        for f in ["status", "user", "region", "score"] {
            db.collection("nba")
                .indexes()
                .create(f, IndexOptions::default())
                .execute(&txn)
                .unwrap();
        }
    }
    let docs: Vec<bson::Document> = (0..50)
        .map(|i| {
            bson::doc! {
                "_id": format!("rec-{i:02}"),
                "status": if i % 2 == 0 { "active" } else { "closed" },
                "user": format!("u{}", i % 10),
                "region": if i % 3 == 0 { "west" } else { "east" },
                "score": (i % 7) as i64,
            }
        })
        .collect();
    db.collection("nba")
        .insert_many(docs)
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
    db
}

/// Sorted `_id`s that `sql` (a `SELECT VALUE c._id`) selects.
fn ids(db: &Database<MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let mut got: Vec<String> = db
        .collection("nba")
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
    let plan = db.collection("nba").query(sql).explain(&txn).unwrap();
    txn.rollback().unwrap();
    plan
}

#[test]
fn all_equality_and_plans_as_index_intersect() {
    let plan = explain(
        &seed(true),
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.user = 'u4'",
    );
    assert!(
        plan.contains("IndexIntersect"),
        "expected an IndexIntersect: {plan}"
    );
    // Each equality part is rendered, and the intersection feeds a KeyLookup.
    assert!(
        plan.contains("nba.status = "),
        "missing status part: {plan}"
    );
    assert!(plan.contains("nba.user = "), "missing user part: {plan}");
    assert!(plan.contains("KeyLookup"), "expected a KeyLookup: {plan}");
    assert!(
        !plan.contains("IndexMerge"),
        "all-equality AND must not use IndexMerge: {plan}"
    );
}

#[test]
fn three_way_equality_and_plans_as_index_intersect() {
    let plan = explain(
        &seed(true),
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.user = 'u4' AND c.region = 'west'",
    );
    assert!(
        plan.contains("IndexIntersect"),
        "expected an IndexIntersect: {plan}"
    );
    // All three equality parts present under the one node.
    for part in ["nba.status = ", "nba.user = ", "nba.region = "] {
        assert!(plan.contains(part), "missing `{part}`: {plan}");
    }
}

#[test]
fn eq_and_range_falls_back_to_index_merge() {
    // A range part disqualifies the doc-id skip-merge → today's IndexMerge(And).
    let plan = explain(
        &seed(true),
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.score > 3",
    );
    assert!(
        plan.contains("IndexMerge AND"),
        "Eq+Range AND should keep IndexMerge(And): {plan}"
    );
    assert!(
        !plan.contains("IndexIntersect"),
        "a range part must not plan as IndexIntersect: {plan}"
    );
}

#[test]
fn index_intersect_is_invisible_to_results() {
    let indexed = seed(true);
    let plain = seed(false);
    for sql in [
        // 2-way skew: a whale's active rows.
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.user = 'u4'",
        // 3-way.
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.user = 'u4' AND c.region = 'west'",
        // Empty intersection (no active row for this user — u3 is on odd indices).
        "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.user = 'u3'",
        // Single survivor.
        "SELECT VALUE c._id FROM c WHERE c.user = 'u9' AND c.region = 'west' AND c.status = 'closed'",
    ] {
        assert_eq!(
            ids(&indexed, sql),
            ids(&plain, sql),
            "indexed (IndexIntersect) and full-scan disagree on `{sql}`"
        );
    }
}

#[test]
fn index_intersect_results_are_exact() {
    let db = seed(true);
    // status=active is even i; user=u4 is i in {4,14,24,34,44} (all even).
    assert_eq!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.user = 'u4'"
        ),
        vec!["rec-04", "rec-14", "rec-24", "rec-34", "rec-44"]
    );
    // u3 is on odd indices (3,13,…) → never active → empty.
    assert!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE c.status = 'active' AND c.user = 'u3'"
        )
        .is_empty()
    );
}
