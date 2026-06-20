//! End-to-end parameterized SQL (`Transaction::query_with_params`) over real
//! in-memory data. Mirrors `find.rs`: seed records, run queries, assert on
//! results.
//!
//! These pin that `@name` placeholders bind supplied values across every clause
//! (WHERE, projection, ORDER BY, JOIN ... IN), coerce numerics like literals,
//! and — crucially — that a value is *data*, never re-entering the parser.

mod common;
use common::*;

use bson::{Document, doc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database};
use slate_store::MemoryStore;

// ── Helpers ─────────────────────────────────────────────────────

/// Run a parameterized query and collect the bare values as strings.
fn names(db: &Database<MemoryStore>, sql: &str, params: Document) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    txn.query_with_params(DEFAULT_CF, COLLECTION, sql, params)
        .unwrap()
        .iter_values::<String>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// Run a parameterized query and collect whole documents.
fn docs(db: &Database<MemoryStore>, sql: &str, params: Document) -> Vec<Document> {
    let txn = db.begin(true).unwrap();
    txn.query_with_params(DEFAULT_CF, COLLECTION, sql, params)
        .unwrap()
        .iter::<Document>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// Run a parameterized query and return the row count (drain).
fn count(db: &Database<MemoryStore>, sql: &str, params: Document) -> u64 {
    let txn = db.begin(true).unwrap();
    txn.query_with_params(DEFAULT_CF, COLLECTION, sql, params)
        .unwrap()
        .drain()
        .unwrap()
}

/// The same five accounts `common::seed_records` inserts, for the indexed-seed
/// variant (which needs the index created before insertion).
fn account_docs() -> Vec<Document> {
    vec![
        doc! { "_id": "acct-1", "name": "Acme Corp", "revenue": 50000.0, "status": "active", "active": true },
        doc! { "_id": "acct-2", "name": "Globex", "revenue": 80000.0, "status": "snoozed", "active": true },
        doc! { "_id": "acct-3", "name": "Initech", "revenue": 12000.0, "status": "rejected", "active": false },
        doc! { "_id": "acct-4", "name": "Umbrella", "revenue": 95000.0, "status": "active", "active": true },
        doc! { "_id": "acct-5", "name": "Stark Industries", "revenue": 200000.0, "status": "active", "active": false },
    ]
}

// ── WHERE-clause parameters ─────────────────────────────────────

#[test]
fn param_eq_string() {
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.status = @status ORDER BY c.name ASC",
            doc! { "status": "active" },
        ),
        vec!["Acme Corp", "Stark Industries", "Umbrella"]
    );
}

#[test]
fn param_gt_numeric_coerces_like_a_literal() {
    // The field is a Double; the parameter is an Int32. Comparison coerces by
    // value, exactly as a literal would.
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.revenue > @min ORDER BY c.name ASC",
            doc! { "min": 80000 },
        ),
        vec!["Stark Industries", "Umbrella"]
    );
}

#[test]
fn param_range_two_parameters() {
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.revenue >= @lo AND c.revenue <= @hi ORDER BY c.name ASC",
            doc! { "lo": 50000.0, "hi": 95000.0 },
        ),
        vec!["Acme Corp", "Globex", "Umbrella"]
    );
}

#[test]
fn param_boolean() {
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.active = @flag ORDER BY c.name ASC",
            doc! { "flag": true },
        ),
        vec!["Acme Corp", "Globex", "Umbrella"]
    );
}

#[test]
fn param_in_list() {
    // Parameters inside a desugared `IN (...)` (an OR of equalities).
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.status IN (@a, @b) ORDER BY c.name ASC",
            doc! { "a": "active", "b": "rejected" },
        ),
        vec!["Acme Corp", "Initech", "Stark Industries", "Umbrella"]
    );
}

#[test]
fn param_between_is_inclusive() {
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.revenue BETWEEN @lo AND @hi ORDER BY c.name ASC",
            doc! { "lo": 12000.0, "hi": 80000.0 },
        ),
        vec!["Acme Corp", "Globex", "Initech"]
    );
}

#[test]
fn param_reused_in_two_places() {
    // The same `@v` resolves to one value wherever it appears.
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.status = @v OR c.name = @v ORDER BY c.name ASC",
            doc! { "v": "active" },
        ),
        vec!["Acme Corp", "Stark Industries", "Umbrella"]
    );
}

#[test]
fn multiple_parameters_of_distinct_types() {
    // string + bool + double in one predicate: active status, not the `active`
    // flag, revenue over 100k → only Stark.
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.status = @s AND c.active = @flag AND c.revenue > @min",
            doc! { "s": "active", "flag": false, "min": 100000.0 },
        ),
        vec!["Stark Industries"]
    );
}

// ── Projection / ORDER BY / JOIN parameters ─────────────────────

#[test]
fn param_in_projection_literal() {
    // A parameter projected directly, plus one in the filter.
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE @label FROM c WHERE c._id = @id",
            doc! { "label": "matched", "id": "acct-1" },
        ),
        vec!["matched"]
    );
}

#[test]
fn param_in_projection_expression() {
    // Parameter inside an IIF in the projection — exercises the Project node.
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            r#"SELECT VALUE IIF(c.revenue > @cut, "big", "small") FROM c ORDER BY c.revenue ASC"#,
            doc! { "cut": 80000.0 },
        ),
        vec!["small", "small", "small", "big", "big"]
    );
}

#[test]
fn param_in_order_by_expression() {
    // `ORDER BY c.revenue * @sign` with sign = -1 sorts descending — exercises
    // the Sort node's parameter threading.
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c ORDER BY c.revenue * @sign ASC",
            doc! { "sign": -1 },
        ),
        vec![
            "Stark Industries",
            "Umbrella",
            "Globex",
            "Acme Corp",
            "Initech"
        ]
    );
}

#[test]
fn param_array_in_join_unwind() {
    // `JOIN t IN @tags` unwinds a parameter array — exercises the Unwind node.
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            "SELECT VALUE t FROM c JOIN t IN @tags WHERE c._id = @id",
            doc! { "tags": ["x", "y", "z"], "id": "acct-1" },
        ),
        vec!["x", "y", "z"]
    );
}

// ── Output shapes with parameters ───────────────────────────────

#[test]
fn select_star_with_param() {
    let (db, _dir) = temp_db();
    seed_records(&db);
    let out = docs(
        &db,
        "SELECT * FROM c WHERE c.status = @s",
        doc! { "s": "rejected" },
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get_str("name").unwrap(), "Initech");
    assert!(out[0].get("_id").is_some());
}

#[test]
fn tabular_projection_with_param() {
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        docs(
            &db,
            "SELECT c.name, c.revenue FROM c WHERE c.revenue > @min",
            doc! { "min": 95000.0 },
        ),
        vec![doc! { "name": "Stark Industries", "revenue": 200000.0 }]
    );
}

#[test]
fn count_with_params_via_drain() {
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        count(
            &db,
            "SELECT VALUE c FROM c WHERE c.revenue > @min",
            doc! { "min": 80000.0 },
        ),
        2
    );
}

// ── Edge cases ──────────────────────────────────────────────────

#[test]
fn missing_param_is_an_error() {
    // A referenced `@missing` with no supplied value is rejected (matching
    // Cosmos) — it catches a misspelled or forgotten name rather than silently
    // returning nothing.
    let (db, _dir) = temp_db();
    seed_records(&db);
    let txn = db.begin(true).unwrap();
    assert!(
        txn.query_with_params(
            DEFAULT_CF,
            COLLECTION,
            "SELECT VALUE c.name FROM c WHERE c.revenue > @missing",
            doc! {},
        )
        .is_err()
    );
}

#[test]
fn partially_supplied_params_error_on_the_missing_one() {
    // `@lo` is supplied but `@hi` is not → error.
    let (db, _dir) = temp_db();
    seed_records(&db);
    let txn = db.begin(true).unwrap();
    assert!(
        txn.query_with_params(
            DEFAULT_CF,
            COLLECTION,
            "SELECT VALUE c.name FROM c WHERE c.revenue > @lo AND c.revenue < @hi",
            doc! { "lo": 1000.0 },
        )
        .is_err()
    );
}

#[test]
fn no_params_api_rejects_a_referenced_parameter() {
    // The plain `query` API supplies no parameters, so any `@name` is unsupplied.
    let (db, _dir) = temp_db();
    seed_records(&db);
    let txn = db.begin(true).unwrap();
    assert!(
        txn.query(
            DEFAULT_CF,
            COLLECTION,
            "SELECT VALUE c.name FROM c WHERE c.revenue > @min",
        )
        .is_err()
    );
}

#[test]
fn unused_params_are_ignored() {
    // Supplying parameters a query never references is harmless.
    let (db, _dir) = temp_db();
    seed_records(&db);
    assert_eq!(
        names(
            &db,
            r#"SELECT VALUE c.name FROM c WHERE c.status = "rejected""#,
            doc! { "unused": 1, "also_unused": "x" },
        ),
        vec!["Initech"]
    );
}

#[test]
fn param_alongside_sargable_index() {
    // `status` is indexed, so the literal `status = "active"` is sargable; the
    // `revenue > @min` parameter rides along as a residual filter on the
    // narrowed candidate set, and the result stays correct.
    let (db, _dir) = temp_db();
    {
        let txn = db.begin(false).unwrap();
        txn.create_collection(&CollectionConfig {
            name: COLLECTION.into(),
            ..Default::default()
        })
        .unwrap();
        txn.create_index(DEFAULT_CF, COLLECTION, "status").unwrap();
        txn.insert_many(DEFAULT_CF, COLLECTION, account_docs())
            .unwrap()
            .drain()
            .unwrap();
        txn.commit().unwrap();
    }
    assert_eq!(
        names(
            &db,
            r#"SELECT VALUE c.name FROM c WHERE c.status = "active" AND c.revenue > @min ORDER BY c.name ASC"#,
            doc! { "min": 80000.0 },
        ),
        vec!["Stark Industries", "Umbrella"]
    );
}

// ── Injection safety (the whole point of parameters) ────────────

#[test]
fn param_value_is_data_not_sql() {
    // A value carrying SQL metacharacters is compared as an opaque string: it
    // matches nothing (no account is literally named this), and the data is
    // untouched afterwards. The value never re-enters the parser.
    let (db, _dir) = temp_db();
    seed_records(&db);
    let malicious = r#"'); DROP TABLE accounts; --"#;
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.name = @n",
            doc! { "n": malicious },
        ),
        Vec::<String>::new()
    );
    // Everything is still there: the "statement" was pure data.
    assert_eq!(
        count(&db, "SELECT VALUE c FROM c", doc! {}),
        5,
        "all rows intact — the parameter was data, not SQL"
    );
}

#[test]
fn param_matches_value_with_special_characters_literally() {
    // A parameter value with characters that are significant in SQL/regex still
    // matches a document that genuinely holds that exact string.
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    {
        let txn = db.begin(false).unwrap();
        txn.insert_many(
            DEFAULT_CF,
            COLLECTION,
            vec![doc! { "_id": "x1", "name": "a%b_c'd" }],
        )
        .unwrap()
        .drain()
        .unwrap();
        txn.commit().unwrap();
    }
    assert_eq!(
        names(
            &db,
            "SELECT VALUE c.name FROM c WHERE c.name = @n",
            doc! { "n": "a%b_c'd" },
        ),
        vec!["a%b_c'd"]
    );
}
