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
    // Projecting only the pk (carried by the index entry) covers the query, so
    // the scan is `covering` and the document fetch is dropped (RFC Part B).
    assert!(
        plan.contains("covering"),
        "expected a covering scan: {plan}"
    );
    assert!(!plan.contains("KeyLookup"), "covered, no fetch: {plan}");
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

// ── Increment B: STARTSWITH / LIKE → prefix range ───────────────

#[test]
fn string_prefix_indexed_matches_full_scan() {
    let indexed = seed(true);
    let plain = seed(false);
    for sql in [
        // `alp` shares a prefix across alpha/alphabet/alpine but not Alpha/beta.
        "SELECT VALUE c._id FROM c WHERE STARTSWITH(c.name, \"alp\")",
        // `alpha` is itself a prefix of `alphabet` — the lower bound must include
        // the exact value, and `alpine` must fall outside the upper bound.
        "SELECT VALUE c._id FROM c WHERE STARTSWITH(c.name, \"alpha\")",
        "SELECT VALUE c._id FROM c WHERE STARTSWITH(c.name, \"Alp\")",
        "SELECT VALUE c._id FROM c WHERE STARTSWITH(c.name, \"beta\")",
        "SELECT VALUE c._id FROM c WHERE STARTSWITH(c.name, \"missing\")",
        "SELECT VALUE c._id FROM c WHERE c.name LIKE \"alp%\"",
        "SELECT VALUE c._id FROM c WHERE c.name LIKE \"al_ha\"",
        "SELECT VALUE c._id FROM c WHERE c.name LIKE \"% pine\"",
        "SELECT VALUE c._id FROM c WHERE REGEXMATCH(c.name, \"^alp\")",
    ] {
        assert_eq!(
            ids(&indexed, sql),
            ids(&plain, sql),
            "indexed and full-scan disagree on `{sql}`"
        );
    }
}

#[test]
fn string_prefix_results_are_exact() {
    let db = seed(true);
    // `alp`: the three lowercase `alp…`, never the capitalised `Alpha` or `beta`,
    // and never the non-string `name`.
    assert_eq!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE STARTSWITH(c.name, \"alp\")"
        ),
        vec!["alpha", "alphabet", "alpine"]
    );
    // `alpha` (exact value) is included via the inclusive lower bound; `alphabet`
    // shares the prefix; `alpine` is correctly excluded by the upper bound.
    assert_eq!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE STARTSWITH(c.name, \"alpha\")"
        ),
        vec!["alpha", "alphabet"]
    );
    // LIKE's wildcard tail past the prefix is applied by the recheck: `al_ha`
    // matches only the 5-char `alpha`, not the longer `alphabet`.
    assert_eq!(
        ids(&db, "SELECT VALUE c._id FROM c WHERE c.name LIKE \"al_ha\""),
        vec!["alpha"]
    );
}

#[test]
fn startswith_uses_index_scan() {
    let plan = explain(
        &seed(true),
        "SELECT VALUE c._id FROM c WHERE STARTSWITH(c.name, \"alp\")",
    );
    assert!(plan.contains("IndexScan"), "expected an index scan: {plan}");
    // Pk-only projection → covering scan; the retained STARTSWITH recheck still
    // runs as a Filter over the synthesized row (RFC Part B).
    assert!(
        plan.contains("covering"),
        "expected a covering scan: {plan}"
    );
    assert!(!plan.contains("KeyLookup"), "covered, no fetch: {plan}");
    assert!(
        plan.contains("things.name"),
        "index scan should name the indexed field: {plan}"
    );
    assert!(
        plan.contains("starts with"),
        "index scan should show a prefix bound: {plan}"
    );
}

#[test]
fn like_prefix_uses_index_scan() {
    let plan = explain(
        &seed(true),
        "SELECT VALUE c._id FROM c WHERE c.name LIKE \"alp%\"",
    );
    assert!(
        plan.contains("IndexScan") && plan.contains("things.name"),
        "LIKE prefix should plan as an index scan: {plan}"
    );
}

#[test]
fn startswith_3arg_stays_full_scan() {
    let plan = explain(
        &seed(true),
        "SELECT VALUE c._id FROM c WHERE STARTSWITH(c.name, \"alp\", true)",
    );
    assert!(
        plan.contains("Scan") && !plan.contains("IndexScan"),
        "3-arg STARTSWITH should full-scan: {plan}"
    );
}

#[test]
fn string_prefix_in_or_merges_and_matches_full_scan() {
    // A prefix predicate as a top-level OR branch must lower through the same
    // index path as Eq/Range — an `IndexMerge(Or)` over the string index — and
    // still agree with a full scan. (A scalar string scan yields one entry per
    // doc, so no dedup is needed, unlike a multikey scan.)
    let indexed = seed(true);
    let plain = seed(false);
    let sql = "SELECT VALUE c._id FROM c \
               WHERE STARTSWITH(c.name, \"alp\") OR STRINGEQUALS(c.name, \"beta\")";

    assert_eq!(
        ids(&indexed, sql),
        ids(&plain, sql),
        "indexed and full-scan disagree on the OR"
    );
    assert_eq!(
        ids(&indexed, sql),
        vec!["alpha", "alphabet", "alpine", "beta"]
    );

    let plan = explain(&indexed, sql);
    assert!(
        plan.contains("IndexMerge"),
        "an all-indexable OR should merge index scans, not full-scan: {plan}"
    );
    assert!(
        !plan.contains("\nScan") && !plan.contains("  Scan"),
        "the OR should not fall back to a collection scan: {plan}"
    );
}
