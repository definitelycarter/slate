//! Regression: `x = null` must not be pushed to a sparse index.
//!
//! Indexes store only scalar non-null values, so an index `Eq(null)` matches
//! nothing in the index and (since the `Eq` atom is consumed, no recheck) returns
//! the wrong rows — the user saw `x = null` count the non-null *complement* once an
//! index existed. The pushdown must be declined so the predicate falls back to a
//! Filter; indexed results must equal unindexed (the index is invisible to results).

use bson::{Bson, doc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_store::MemoryStore;

/// `t` with `x` null in three docs and a real number in two, optionally indexed.
fn seed(indexed: bool) -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "t".into(),
        ..Default::default()
    })
    .unwrap();
    if indexed {
        txn.create_index(DEFAULT_CF, "t", "x").unwrap();
    }
    txn.insert_many(
        DEFAULT_CF,
        "t",
        vec![
            doc! { "_id": "a", "x": Bson::Null },
            doc! { "_id": "b", "x": Bson::Null },
            doc! { "_id": "c", "x": Bson::Null },
            doc! { "_id": "d", "x": 5 },
            doc! { "_id": "e", "x": 7 },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

/// The sorted `_id`s a `SELECT VALUE c._id` selects from `t`.
fn ids(db: &Database<MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let mut got: Vec<String> = txn
        .query(DEFAULT_CF, "t", sql)
        .unwrap()
        .iter_values::<String>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    got.sort();
    got
}

#[test]
fn eq_null_indexed_matches_unindexed() {
    let indexed = seed(true);
    let plain = seed(false);

    let eq_null = "SELECT VALUE c._id FROM c WHERE c.x = null";
    // Unindexed Filter is the source of truth: the three null docs.
    assert_eq!(ids(&plain, eq_null), vec!["a", "b", "c"]);
    // Indexed must match — not the non-null complement `["d", "e"]` the bug returned.
    assert_eq!(ids(&indexed, eq_null), vec!["a", "b", "c"]);

    // IS_NULL agrees, and a real value still uses the index correctly.
    assert_eq!(
        ids(&indexed, "SELECT VALUE c._id FROM c WHERE IS_NULL(c.x)"),
        vec!["a", "b", "c"]
    );
    assert_eq!(
        ids(&indexed, "SELECT VALUE c._id FROM c WHERE c.x = 5"),
        vec!["d"]
    );
}

#[test]
fn eq_null_plans_as_filter_not_index_scan() {
    let db = seed(true);
    let txn = db.begin(true).unwrap();
    let plan = txn
        .explain(
            DEFAULT_CF,
            "t",
            "SELECT VALUE c._id FROM c WHERE c.x = null",
        )
        .unwrap();
    assert!(
        plan.contains("Filter") && !plan.contains("IndexScan"),
        "`x = null` must plan as a Filter over a Scan, not an index scan; got:\n{plan}"
    );
    txn.rollback().unwrap();
}
