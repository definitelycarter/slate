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
    // A string index exercises the sargable string path; numeric cross-type
    // index behaviour is covered by `indexed_numeric_queries_are_correct_across_types`.
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
fn where_in_list() {
    // `name` is indexed, so `IN (…)` desugars to an OR of equalities and takes
    // the sargable IndexMerge(Or) path.
    let db = seeded();
    assert_eq!(
        strings(
            &db,
            r#"SELECT VALUE c.name FROM c WHERE c.name IN ("ada", "grace") ORDER BY c.age ASC"#
        ),
        vec!["ada", "grace"]
    );
}

#[test]
fn where_not_in_list() {
    let db = seeded();
    assert_eq!(
        strings(
            &db,
            r#"SELECT VALUE c.name FROM c WHERE c.name NOT IN ("ada") ORDER BY c.age ASC"#
        ),
        vec!["alan", "grace"]
    );
}

#[test]
fn where_between_is_inclusive() {
    let db = seeded();
    // Inclusive on both ends: 41 (alan) and 44 (grace) are both included.
    assert_eq!(
        strings(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.age BETWEEN 41 AND 44 ORDER BY c.age ASC"
        ),
        vec!["alan", "grace"]
    );
    // Inclusive lower bound picks up ada (36); the upper bound 40 excludes alan.
    assert_eq!(
        strings(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.age BETWEEN 36 AND 40 ORDER BY c.age ASC"
        ),
        vec!["ada"]
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
fn indexed_numeric_queries_are_correct_across_types() {
    // Regression: SQL int literals are i64; an *indexed* numeric field stores
    // values of varied types (Int32/Int64/Double). Index scans must compare
    // cross-type, not over/under-return (a typed range scan let age=36 match
    // `age > 40`).
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "n".into(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "n", "age").unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "n",
        vec![
            doc! { "_id": "a", "age": 36_i32 },   // Int32
            doc! { "_id": "b", "age": 41_i64 },   // Int64
            doc! { "_id": "c", "age": 44.0_f64 }, // Double
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let ids = |sql: &str| -> Vec<String> {
        let mut out: Vec<String> = txn
            .query(DEFAULT_CF, "n", sql)
            .unwrap()
            .iter_values::<String>()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        out.sort();
        out
    };
    assert_eq!(
        ids("SELECT VALUE c._id FROM c WHERE c.age > 40"),
        vec!["b", "c"]
    );
    assert_eq!(
        ids("SELECT VALUE c._id FROM c WHERE c.age < 42"),
        vec!["a", "b"]
    );
    assert_eq!(
        ids("SELECT VALUE c._id FROM c WHERE c.age >= 44"),
        vec!["c"]
    );
    assert_eq!(ids("SELECT VALUE c._id FROM c WHERE c.age = 41"), vec!["b"]);
    // a Double bound against Int32/Int64-stored values
    assert_eq!(
        ids("SELECT VALUE c._id FROM c WHERE c.age > 40.5"),
        vec!["b", "c"]
    );
}

#[test]
fn fuzz_indexed_numeric_matches_brute_force() {
    // Property test for the cross-type index fix. The v1↔v2 differential fuzz
    // can't cover this — v1 shares the numeric-range over-return bug, so the two
    // engines would diverge — so this checks v2 against a brute-force reference
    // (the evaluator promotes numerics to f64, so the reference does too).
    use bson::Bson;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    for seed in 0..150u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
        let txn = db.begin(false).unwrap();
        txn.create_collection(&CollectionConfig {
            name: "f".into(),
            ..Default::default()
        })
        .unwrap();
        txn.create_index(DEFAULT_CF, "f", "age").unwrap();

        // Each doc's indexed `age` is a random numeric of a random BSON type.
        let mut expected: Vec<(String, f64)> = Vec::new();
        let mut docs = Vec::new();
        for i in 0..8 {
            let id = format!("d{i}");
            let n: i32 = rng.gen_range(-50..50);
            let (age, as_f64) = match rng.gen_range(0..3) {
                0 => (Bson::Int32(n), n as f64),
                1 => (Bson::Int64(n as i64), n as f64),
                _ => (Bson::Double(n as f64 + 0.5), n as f64 + 0.5),
            };
            docs.push(doc! { "_id": id.clone(), "age": age });
            expected.push((id, as_f64));
        }
        txn.insert_many(DEFAULT_CF, "f", docs)
            .unwrap()
            .drain()
            .unwrap();
        txn.commit().unwrap();

        // A random comparison against an integer bound (SQL int literal → i64).
        let bound: i32 = rng.gen_range(-50..50);
        let bf = bound as f64;
        let (op, pred): (&str, fn(f64, f64) -> bool) = match rng.gen_range(0..5) {
            0 => (">", |a, b| a > b),
            1 => (">=", |a, b| a >= b),
            2 => ("<", |a, b| a < b),
            3 => ("<=", |a, b| a <= b),
            _ => ("=", |a, b| a == b),
        };
        let sql = format!("SELECT VALUE c._id FROM c WHERE c.age {op} {bound}");

        let txn = db.begin(true).unwrap();
        let mut got: Vec<String> = txn
            .query(DEFAULT_CF, "f", &sql)
            .unwrap()
            .iter_values::<String>()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        got.sort();
        let mut want: Vec<String> = expected
            .iter()
            .filter(|(_, a)| pred(*a, bf))
            .map(|(id, _)| id.clone())
            .collect();
        want.sort();
        assert_eq!(got, want, "seed={seed} sql=`{sql}`");
    }
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
