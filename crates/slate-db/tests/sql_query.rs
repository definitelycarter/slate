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
            doc! { "_id": "1", "name": "ada", "age": 36, "tags": ["x", "y"], "address": { "city": "austin", "zip": "78701" } },
            doc! { "_id": "2", "name": "alan", "age": 41, "tags": ["y", "z"], "address": { "city": "denver", "zip": "80202" } },
            doc! { "_id": "3", "name": "grace", "age": 44, "tags": [], "address": { "city": "miami", "zip": "33101" } },
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

fn docs(db: &Database<MemoryStore>, sql: &str) -> Vec<Document> {
    let txn = db.begin(true).unwrap();
    txn.query(DEFAULT_CF, "people", sql)
        .unwrap()
        .iter::<Document>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn select_star_returns_whole_documents() {
    let db = seeded();
    let out = docs(&db, r#"SELECT * FROM c WHERE c.name = "ada""#);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get_str("name").unwrap(), "ada");
    assert!(out[0].get("_id").is_some()); // whole document, pk included
}

#[test]
fn tabular_projection_has_no_auto_pk() {
    // SELECT c.name, c.age -> { name, age } ONLY — no auto `_id` (Cosmos
    // semantics, unlike Mongo find which auto-includes the pk).
    let db = seeded();
    let out = docs(&db, r#"SELECT c.name, c.age FROM c WHERE c.name = "alan""#);
    assert_eq!(out, vec![doc! { "name": "alan", "age": 41 }]);
}

#[test]
fn select_member_returns_whole_subdocument() {
    // SELECT c.address -> { "address": <whole sub-doc> }, keyed by last segment,
    // untrimmed.
    let db = seeded();
    let out = docs(&db, r#"SELECT c.address FROM c WHERE c.name = "ada""#);
    assert_eq!(
        out,
        vec![doc! { "address": { "city": "austin", "zip": "78701" } }]
    );
}

#[test]
fn select_dotted_member_flattens_to_last_segment() {
    // SELECT c.address.city -> { "city": <scalar> } (flat key, not nested).
    let db = seeded();
    let out = docs(&db, r#"SELECT c.address.city FROM c WHERE c.name = "ada""#);
    assert_eq!(out, vec![doc! { "city": "austin" }]);
}

#[test]
fn as_alias_renames_key() {
    let db = seeded();
    let out = docs(&db, r#"SELECT c.name AS who FROM c WHERE c.name = "grace""#);
    assert_eq!(out, vec![doc! { "who": "grace" }]);
}

#[test]
fn select_explicit_pk_is_included() {
    // The consumer opts into the pk by selecting it.
    let db = seeded();
    let out = docs(&db, r#"SELECT c._id, c.name FROM c WHERE c.name = "ada""#);
    assert_eq!(out, vec![doc! { "_id": "1", "name": "ada" }]);
}

#[test]
fn duplicate_projected_key_is_an_error() {
    let db = seeded();
    let txn = db.begin(true).unwrap();
    // `c.name` and `c.age AS name` both want key "name".
    assert!(
        txn.query(DEFAULT_CF, "people", "SELECT c.name, c.age AS name FROM c")
            .is_err()
    );
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
