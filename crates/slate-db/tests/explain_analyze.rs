//! `Transaction::explain_analyze` — run a query and render its plan annotated
//! with per-node *actuals* (`rows=` emitted, `examined=` rows in). These pin
//! that the counts reflect what actually flowed through each operator, and that
//! the annotated tree still has the same shape `explain` prints.

use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_store::MemoryStore;

fn seeded() -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "people".into(),
        ..Default::default()
    })
    .unwrap();
    // `name` indexed, `age` not — so the two predicate shapes differ.
    txn.create_index(DEFAULT_CF, "people", "name").unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "people",
        vec![
            bson::doc! { "_id": "1", "name": "ada", "age": 36 },
            bson::doc! { "_id": "2", "name": "alan", "age": 41 },
            bson::doc! { "_id": "3", "name": "grace", "age": 44 },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

fn analyze(db: &Database<MemoryStore>, sql: &str) -> String {
    let txn = db.begin(true).unwrap();
    let out = txn.explain_analyze(DEFAULT_CF, "people", sql).unwrap();
    txn.rollback().unwrap();
    out
}

#[test]
fn full_scan_reports_all_rows_examined_and_emitted() {
    let rendered = analyze(&seeded(), "SELECT VALUE c.name FROM c");
    // Every node line carries a row count.
    for line in rendered.lines() {
        assert!(
            line.contains("rows="),
            "every node should report rows=: {line}"
        );
    }
    // The source scan emits all 3 documents; the root projection emits 3 too.
    assert!(
        rendered.contains("rows=3"),
        "expected rows=3 somewhere: {rendered}"
    );
    let root = rendered.lines().next().unwrap();
    assert!(root.contains("rows=3"), "root line: {root}");
}

#[test]
fn filter_examines_more_than_it_emits() {
    // `age` is unindexed → a residual Filter over a full Scan. The filter keeps
    // 1 of the 3 rows it examines.
    let rendered = analyze(&seeded(), "SELECT VALUE c.name FROM c WHERE c.age = 41");
    let filter = rendered
        .lines()
        .find(|l| l.trim_start().starts_with("Filter"))
        .expect("expected a Filter line");
    assert!(
        filter.contains("rows=1") && filter.contains("examined=3"),
        "filter line: {filter}"
    );
}

#[test]
fn indexed_equality_seeks_instead_of_scanning() {
    // `name` is indexed → an IndexScan seek + KeyLookup; only the matching entry
    // is examined, not the whole collection.
    let rendered = analyze(
        &seeded(),
        "SELECT VALUE c.name FROM c WHERE c.name = \"grace\"",
    );
    assert!(
        rendered.contains("IndexScan"),
        "expected IndexScan: {rendered}"
    );
    let index_scan = rendered
        .lines()
        .find(|l| l.trim_start().starts_with("IndexScan"))
        .expect("IndexScan line");
    // Exactly one entry matches `grace`.
    assert!(
        index_scan.contains("rows=1"),
        "index scan line: {index_scan}"
    );
}

#[test]
fn annotated_tree_keeps_the_same_shape_as_explain() {
    let db = seeded();
    let sql = "SELECT VALUE c.name FROM c WHERE c.age = 41";

    let txn = db.begin(true).unwrap();
    let plain = txn.explain(DEFAULT_CF, "people", sql).unwrap();
    let annotated = txn.explain_analyze(DEFAULT_CF, "people", sql).unwrap();
    txn.rollback().unwrap();

    // Same number of node lines, same operator at each depth (the annotation is a
    // suffix, so each plain line is a prefix of the matching annotated line).
    let plain_lines: Vec<&str> = plain.lines().collect();
    let annotated_lines: Vec<&str> = annotated.lines().collect();
    assert_eq!(
        plain_lines.len(),
        annotated_lines.len(),
        "line counts differ:\n{plain}\n---\n{annotated}"
    );
    for (p, a) in plain_lines.iter().zip(annotated_lines.iter()) {
        assert!(
            a.starts_with(p),
            "annotated line should extend the plain line:\n  plain:     {p}\n  annotated: {a}"
        );
    }
}

#[test]
fn parameterized_query_is_rejected_like_query() {
    let db = seeded();
    let txn = db.begin(true).unwrap();
    let err = txn
        .explain_analyze(
            DEFAULT_CF,
            "people",
            "SELECT VALUE c.name FROM c WHERE c.age = @min",
        )
        .unwrap_err();
    assert!(err.to_string().contains("@min"), "error: {err}");
}
