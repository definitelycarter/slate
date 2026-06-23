//! Increments B & C — string predicates reach a scalar string index from SQL:
//! `STRINGEQUALS(x, lit)` → an `Eq` seek (C), and `STARTSWITH(x, "pre")` /
//! `LIKE 'pre%'` → a prefix range (B).
//!
//! The pushdown is *invisible to results*: an indexed query must return exactly
//! the same rows as a full scan. These pin that invariant against the corpus's
//! tripwires — a capitalised value (case sensitivity), values in a prefix
//! relationship (`alpha` ⊂ `alphabet`), and a non-string `name` (which the
//! string predicates never match, and the retained recheck must drop).

use bson::{Bson, doc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_store::MemoryStore;

/// Seed `things` with a scalar `name` field, optionally indexed on `name`.
/// `alpha`/`alphabet`/`alpine` exercise prefix relationships; `Alpha` is the
/// case-sensitivity tripwire; `numname` carries a non-string `name`.
fn seed(indexed: bool) -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "things".into(),
        ..Default::default()
    })
    .unwrap();
    if indexed {
        txn.create_index(DEFAULT_CF, "things", "name").unwrap();
    }
    txn.insert_many(
        DEFAULT_CF,
        "things",
        vec![
            doc! { "_id": "alpha", "name": "alpha" },
            doc! { "_id": "alphabet", "name": "alphabet" },
            doc! { "_id": "alpine", "name": "alpine" },
            doc! { "_id": "beta", "name": "beta" },
            doc! { "_id": "cap", "name": "Alpha" },
            doc! { "_id": "numname", "name": Bson::Int32(42) },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

/// The sorted `_id`s that `sql` (a `SELECT VALUE c._id`) selects from `things`.
fn ids(db: &Database<MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let mut got: Vec<String> = txn
        .query(DEFAULT_CF, "things", sql)
        .unwrap()
        .iter_values::<String>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    got.sort();
    got
}

fn explain(db: &Database<MemoryStore>, sql: &str) -> String {
    let txn = db.begin(true).unwrap();
    let plan = txn.explain(DEFAULT_CF, "things", sql).unwrap();
    txn.rollback().unwrap();
    plan
}

// ── Increment C: STRINGEQUALS → Eq ──────────────────────────────

#[test]
fn string_equals_indexed_matches_full_scan() {
    let indexed = seed(true);
    let plain = seed(false);
    for sql in [
        "SELECT VALUE c._id FROM c WHERE STRINGEQUALS(c.name, \"alpha\")",
        "SELECT VALUE c._id FROM c WHERE STRINGEQUALS(c.name, \"Alpha\")",
        "SELECT VALUE c._id FROM c WHERE STRINGEQUALS(c.name, \"alphabet\")",
        "SELECT VALUE c._id FROM c WHERE STRINGEQUALS(c.name, \"missing\")",
        // 3-arg case-insensitive form: not pushed down, but must still be correct.
        "SELECT VALUE c._id FROM c WHERE STRINGEQUALS(c.name, \"ALPHA\", true)",
    ] {
        assert_eq!(
            ids(&indexed, sql),
            ids(&plain, sql),
            "indexed and full-scan disagree on `{sql}`"
        );
    }
}

#[test]
fn string_equals_results_are_exact() {
    let db = seed(true);
    // Exact, case-sensitive: `alpha` only, never `Alpha` or the `alpha`-prefixed
    // `alphabet`/`alpine`.
    assert_eq!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE STRINGEQUALS(c.name, \"alpha\")"
        ),
        vec!["alpha"]
    );
    // The non-string `name` is never a STRINGEQUALS match.
    assert!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE STRINGEQUALS(c.name, \"42\")"
        )
        .is_empty()
    );
}

#[test]
fn string_equals_uses_index_scan() {
    let plan = explain(
        &seed(true),
        "SELECT VALUE c._id FROM c WHERE STRINGEQUALS(c.name, \"alpha\")",
    );
    assert!(plan.contains("IndexScan"), "expected an index scan: {plan}");
    assert!(plan.contains("KeyLookup"), "expected a key lookup: {plan}");
    assert!(
        plan.contains("things.name"),
        "index scan should name the indexed field: {plan}"
    );
}

#[test]
fn string_equals_3arg_stays_full_scan() {
    // The case-insensitive form can't ride a case-sensitive index.
    let plan = explain(
        &seed(true),
        "SELECT VALUE c._id FROM c WHERE STRINGEQUALS(c.name, \"alpha\", true)",
    );
    assert!(
        plan.contains("Scan") && !plan.contains("IndexScan"),
        "3-arg STRINGEQUALS should full-scan: {plan}"
    );
}
