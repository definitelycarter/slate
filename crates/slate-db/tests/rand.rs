//! `RAND()` — the injected random source.
//!
//! Unlike the clock (a *static* per-transaction `$now`), `RAND()` draws a fresh
//! value per call from a source injected at `DatabaseBuilder::with_rand`. These
//! tests pin the determinism escape hatch (inject a fixed sequence and read it
//! back through `SELECT VALUE` and `WHERE`) and that the native default stays in
//! `[0, 1)`. `RAND()` is a deliberate non-Cosmos feature, so it is not part of
//! the parity corpus.

use std::sync::{Arc, Mutex};

use bson::doc;
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_store::MemoryStore;

const COLLECTION: &str = "nums";

/// A collection of `n` documents `{ _id, val }` with ascending string ids and
/// `val = i / 10` (so `val` spans `[0.0, 0.1, …]`).
fn seed(db: &Database<MemoryStore>, n: usize) {
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.into(),
        ..Default::default()
    })
    .unwrap();
    let docs: Vec<_> = (0..n)
        .map(|i| doc! { "_id": format!("{i:03}"), "val": i as f64 / 10.0 })
        .collect();
    txn.insert_many(DEFAULT_CF, COLLECTION, docs)
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();
}

/// Run `sql` and collect the result values as `f64`.
fn floats(db: &Database<MemoryStore>, sql: &str) -> Vec<f64> {
    let txn = db.begin(true).unwrap();
    txn.query(DEFAULT_CF, COLLECTION, sql)
        .unwrap()
        .iter_values::<f64>()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// A `with_rand` source that yields `seq` in order (cycling if drawn past the
/// end, so an unexpected extra evaluation can't panic the test).
fn fixed_sequence(seq: Vec<f64>) -> impl Fn() -> f64 + Send + Sync + 'static {
    let next = Arc::new(Mutex::new(0usize));
    move || {
        let mut i = next.lock().unwrap();
        let v = seq[*i % seq.len()];
        *i += 1;
        v
    }
}

#[test]
fn select_value_rand_reads_back_injected_sequence() {
    let seq = vec![0.25_f64, 0.5, 0.75];
    let db = DatabaseBuilder::new()
        .with_rand(fixed_sequence(seq.clone()))
        .open(MemoryStore::new())
        .unwrap();
    seed(&db, seq.len());

    // One `RAND()` evaluation per row, in scan order, drawing the sequence.
    assert_eq!(floats(&db, "SELECT VALUE RAND() FROM c"), seq);
}

#[test]
fn rand_is_a_fresh_draw_per_call() {
    // Two `RAND()` calls in one projected row are independent draws, so they get
    // consecutive sequence values rather than a single shared value (the way the
    // clock's `$now` is shared).
    let db = DatabaseBuilder::new()
        .with_rand(fixed_sequence(vec![0.1, 0.9]))
        .open(MemoryStore::new())
        .unwrap();
    seed(&db, 1);

    let out = floats(&db, "SELECT VALUE RAND() - RAND() FROM c");
    assert_eq!(out, vec![0.1 - 0.9]);
}

#[test]
fn rand_in_where_filters_rows() {
    // A constant source: `WHERE c.val > RAND()` keeps the rows above the draw.
    let db = DatabaseBuilder::new()
        .with_rand(|| 0.5)
        .open(MemoryStore::new())
        .unwrap();
    // vals: 0.0, 0.1, … 0.9 — those strictly above 0.5 are 0.6..=0.9.
    seed(&db, 10);

    let kept = floats(
        &db,
        "SELECT VALUE c.val FROM c WHERE c.val > RAND() ORDER BY c.val ASC",
    );
    assert_eq!(kept, vec![0.6, 0.7, 0.8, 0.9]);
}

#[test]
fn default_source_is_in_unit_interval() {
    // No injected source → the native seeded PRNG (runtime feature). Every draw
    // must land in [0, 1).
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    seed(&db, 50);

    let draws = floats(&db, "SELECT VALUE RAND() FROM c");
    assert_eq!(draws.len(), 50);
    for x in draws {
        assert!(
            (0.0..1.0).contains(&x),
            "RAND() returned {x}, outside [0, 1)"
        );
    }
}
