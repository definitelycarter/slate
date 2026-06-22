//! Increment A — `ARRAY_CONTAINS` / `_ANY` / `_ALL` reach the `.[]` multikey
//! index from SQL.
//!
//! The pushdown is *invisible to results*: an indexed query must return exactly
//! the same rows as a full scan. These pin that invariant, plus the two things
//! the index path could get wrong that a recheck can't repair — duplicate
//! result rows from a repeated array element, and cross-numeric-type matches.
//! The duplicate-element case is checked for both the SQL `ARRAY_CONTAINS` and
//! the Mongo `{tags.[]: v}` forms (the latter closing a latent `MultikeyEq` bug).

use bson::{Bson, doc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_query::FindOptions;
use slate_store::MemoryStore;

/// Seed `posts` with array-valued `tags`, optionally indexed on `tags.[]`.
/// `r5` has a duplicate `"db"` element — the dedup tripwire.
fn seed(indexed: bool) -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "posts".into(),
        ..Default::default()
    })
    .unwrap();
    if indexed {
        txn.create_index(DEFAULT_CF, "posts", "tags.[]").unwrap();
    }
    txn.insert_many(
        DEFAULT_CF,
        "posts",
        vec![
            doc! { "_id": "r1", "tags": ["rust", "db"] },
            doc! { "_id": "r2", "tags": ["go", "api"] },
            doc! { "_id": "r3", "tags": ["rust", "api"] },
            doc! { "_id": "r4", "tags": [] },
            doc! { "_id": "r5", "tags": ["db", "db"] },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

/// The sorted `_id`s that `sql` (a `SELECT VALUE c._id`) selects from `posts`.
fn ids(db: &Database<MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let mut got: Vec<String> = txn
        .query(DEFAULT_CF, "posts", sql)
        .unwrap()
        .iter_values::<String>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    got.sort();
    got
}

#[test]
fn array_contains_indexed_matches_full_scan() {
    let indexed = seed(true);
    let plain = seed(false);
    for sql in [
        "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS(c.tags, \"rust\")",
        "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS(c.tags, \"db\")",
        "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS(c.tags, \"missing\")",
        "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, \"go\", \"rust\")",
        "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, \"rust\", \"api\")",
        "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, \"rust\", \"db\")",
    ] {
        assert_eq!(
            ids(&indexed, sql),
            ids(&plain, sql),
            "indexed and full-scan disagree on `{sql}`"
        );
    }
}

#[test]
fn array_contains_results_are_exact() {
    // Spot-check the actual row sets, not just indexed-vs-scan agreement.
    let db = seed(true);
    assert_eq!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS(c.tags, \"rust\")"
        ),
        vec!["r1", "r3"]
    );
    assert_eq!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, \"go\", \"rust\")"
        ),
        vec!["r1", "r2", "r3"]
    );
    assert_eq!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, \"rust\", \"api\")"
        ),
        vec!["r3"]
    );
}

#[test]
fn array_contains_dedups_duplicate_elements_sql() {
    // `r5` has tags ["db","db"]: the `.[]` scan yields its id twice, so without
    // dedup the SQL `ARRAY_CONTAINS` would return r5 as two rows.
    let got = ids(
        &seed(true),
        "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS(c.tags, \"db\")",
    );
    assert_eq!(got, vec!["r1", "r5"], "r5 must appear exactly once");
}

#[test]
fn multikey_eq_dedups_duplicate_elements_mongo() {
    // The same lone-scan shape via the Mongo `{tags.[]: v}` form — this closes a
    // latent `MultikeyEq` duplicate-row bug (r5 was returned twice).
    let db = seed(true);
    let txn = db.begin(true).unwrap();
    let mut got: Vec<String> = txn
        .find(
            DEFAULT_CF,
            "posts",
            doc! { "tags.[]": "db" },
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .map(|r| r.unwrap().get_str("_id").unwrap().to_string())
        .collect();
    got.sort();
    assert_eq!(got, vec!["r1", "r5"], "r5 must appear exactly once");
}

/// Seed `nums` arrays with one element each, stored as distinct numeric types.
fn seed_numeric(indexed: bool) -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "posts".into(),
        ..Default::default()
    })
    .unwrap();
    if indexed {
        txn.create_index(DEFAULT_CF, "posts", "nums.[]").unwrap();
    }
    txn.insert_many(
        DEFAULT_CF,
        "posts",
        vec![
            doc! { "_id": "i32", "nums": [Bson::Int32(7)] },
            doc! { "_id": "i64", "nums": [Bson::Int64(7)] },
            doc! { "_id": "f64", "nums": [Bson::Double(7.0)] },
            doc! { "_id": "other", "nums": [Bson::Int32(8)] },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

#[test]
fn array_contains_numeric_matches_across_types() {
    // An Int64 needle `7` must sweep the Int32, Int64, and Double `7` elements —
    // the executor's numeric Eq is a full-field scan + coercing recheck, so the
    // multikey path can't miss a cross-type numeric, and matches a full scan.
    let sql = "SELECT VALUE c._id FROM c WHERE ARRAY_CONTAINS(c.nums, 7)";
    let nums = |db: &Database<MemoryStore>| {
        let txn = db.begin(true).unwrap();
        let mut got: Vec<String> = txn
            .query(DEFAULT_CF, "posts", sql)
            .unwrap()
            .iter_values::<String>()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        got.sort();
        got
    };
    assert_eq!(nums(&seed_numeric(true)), vec!["f64", "i32", "i64"]);
    assert_eq!(nums(&seed_numeric(true)), nums(&seed_numeric(false)));
}
