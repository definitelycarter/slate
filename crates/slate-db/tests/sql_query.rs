//! `Transaction::query` — the CosmosDB-style SQL surface. SQL shares the v2
//! stack with `find` (same AST, planner, executor), so these also pin that the
//! two surfaces agree.

use bson::{Document, doc, rawdoc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_query::FindOptions;
use slate_store::MemoryStore;

fn seeded() -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "people".into(),
        ..Default::default()
    })
    .unwrap();
    // Index a string field — numeric filters stay non-indexed to sidestep the
    // documented mixed-numeric index-range gap (SQL int literals are Int64).
    txn.create_index(DEFAULT_CF, "people", "name").unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "people",
        vec![
            doc! { "_id": "1", "name": "ada", "age": 36, "tags": ["x", "y"] },
            doc! { "_id": "2", "name": "alan", "age": 41, "tags": ["y", "z"] },
            doc! { "_id": "3", "name": "grace", "age": 44, "tags": [] },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

fn strings(db: &Database<MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    // `SELECT VALUE c.name` yields bare strings → value iteration, not `iter`
    // (which expects documents).
    txn.query(DEFAULT_CF, "people", sql)
        .unwrap()
        .iter_values::<String>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn select_value_with_order_by() {
    let db = seeded();
    assert_eq!(
        strings(&db, "SELECT VALUE c.name FROM c ORDER BY c.age ASC"),
        vec!["ada", "alan", "grace"]
    );
}

#[test]
fn where_string_eq_uses_index() {
    // `name` is indexed; `c.name = "alan"` is sargable to an IndexScan.
    let db = seeded();
    assert_eq!(
        strings(&db, r#"SELECT VALUE c.name FROM c WHERE c.name = "alan""#),
        vec!["alan"]
    );
}

#[test]
fn where_numeric_range() {
    let db = seeded();
    assert_eq!(
        strings(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.age > 40 ORDER BY c.age ASC"
        ),
        vec!["alan", "grace"]
    );
}

#[test]
fn offset_limit() {
    let db = seeded();
    assert_eq!(
        strings(
            &db,
            "SELECT VALUE c.name FROM c ORDER BY c.age ASC OFFSET 1 LIMIT 1"
        ),
        vec!["alan"]
    );
}

#[test]
fn object_projection_deserializes() {
    let db = seeded();
    let txn = db.begin(true).unwrap();
    let docs: Vec<Document> = txn
        .query(
            DEFAULT_CF,
            "people",
            r#"SELECT VALUE { "n": c.name, "a": c.age } FROM c WHERE c.name = "ada""#,
        )
        .unwrap()
        .iter::<Document>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(docs, vec![doc! { "n": "ada", "a": 36 }]);
}

#[test]
fn join_unwinds_an_array() {
    // SELECT VALUE t FROM c JOIN t IN c.tags — a SQL-only capability (find has
    // no joins). ada has tags ["x", "y"].
    let db = seeded();
    assert_eq!(
        strings(
            &db,
            r#"SELECT VALUE t FROM c JOIN t IN c.tags WHERE c.name = "ada""#
        ),
        vec!["x", "y"]
    );
}

#[test]
fn sql_agrees_with_find() {
    // The same logical query through both surfaces yields identical rows —
    // they share the v2 planner/executor.
    let db = seeded();
    let txn = db.begin(true).unwrap();
    let via_sql: Vec<Document> = txn
        .query(
            DEFAULT_CF,
            "people",
            r#"SELECT VALUE c FROM c WHERE c.name = "alan""#,
        )
        .unwrap()
        .iter::<Document>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let via_find: Vec<Document> = txn
        .find(
            DEFAULT_CF,
            "people",
            rawdoc! { "name": "alan" },
            FindOptions::default(),
        )
        .unwrap()
        .iter::<Document>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(via_sql, via_find);
    assert_eq!(via_sql.len(), 1);
}

#[test]
fn invalid_sql_is_an_error() {
    let db = seeded();
    let txn = db.begin(true).unwrap();
    assert!(
        txn.query(DEFAULT_CF, "people", "SELECT VALUE FROM WHERE")
            .is_err()
    );
}

#[test]
fn count_via_drain() {
    let db = seeded();
    let txn = db.begin(true).unwrap();
    let n = txn
        .query(
            DEFAULT_CF,
            "people",
            "SELECT VALUE c FROM c WHERE c.age > 40",
        )
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(n, 2);
}
