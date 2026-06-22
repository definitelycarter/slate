//! End-to-end across the write path and *both* read surfaces: insert documents,
//! then read them back through the MongoDB `find` API and the CosmosDB-style SQL
//! API, and assert the two surfaces agree on the same data.
//!
//! This pins the "one engine, two front-ends" design — `find` and SQL parse to
//! the same AST, lower with the same planner, and run on the same executor, so
//! for identical data they must return identical rows.

use bson::{doc, rawdoc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_query::FindOptions;
use slate_store::MemoryStore;

fn db() -> Database<MemoryStore> {
    DatabaseBuilder::new().open(MemoryStore::new()).unwrap()
}

#[test]
fn insert_then_find_and_sql_agree() {
    let db = db();

    // ── write: create a collection and insert documents ──
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "users".into(),
        ..Default::default()
    })
    .unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "users",
        vec![
            doc! { "_id": "u1", "name": "ada",   "age": 36 },
            doc! { "_id": "u2", "name": "alan",  "age": 41 },
            doc! { "_id": "u3", "name": "grace", "age": 29 },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // ── read 1: MongoDB `find_one` by _id returns the whole document ──
    let txn = db.begin(true).unwrap();
    let found = txn
        .find_one(DEFAULT_CF, "users", rawdoc! { "_id": "u1" })
        .unwrap()
        .expect("u1 should exist");
    assert_eq!(found.get_str("_id").unwrap(), "u1");
    assert_eq!(found.get_str("name").unwrap(), "ada");
    txn.rollback().unwrap();

    // ── read 2: MongoDB `find` with a filter (age > 30) ──
    let txn = db.begin(true).unwrap();
    let mut find_names: Vec<String> = txn
        .find(
            DEFAULT_CF,
            "users",
            rawdoc! { "age": { "$gt": 30 } },
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .map(|r| r.unwrap().get_str("name").unwrap().to_string())
        .collect();
    find_names.sort();
    txn.rollback().unwrap();

    // ── read 3: the SQL equivalent of the same filter ──
    let txn = db.begin(true).unwrap();
    let mut sql_names: Vec<String> = txn
        .query(
            DEFAULT_CF,
            "users",
            "SELECT VALUE c.name FROM c WHERE c.age > 30",
        )
        .unwrap()
        .iter_values::<String>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    sql_names.sort();
    txn.rollback().unwrap();

    // both surfaces return the same rows, and the expected ones
    assert_eq!(find_names, vec!["ada".to_string(), "alan".to_string()]);
    assert_eq!(
        find_names, sql_names,
        "find and SQL disagree: find={find_names:?} sql={sql_names:?}"
    );
}
