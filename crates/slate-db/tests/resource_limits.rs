//! Resource Limits & Safety Valves (RFC, A + B) — the query deadline and the
//! materialization cap, end to end through the public builder surface.
//!
//! ## Deterministic time
//!
//! The deadline reads the *injected* clock ([`DatabaseBuilder::with_clock`]).
//! [`advancing_clock`] returns a source whose consecutive reads differ by exactly
//! `step` ms (via `fetch_add`), so a test controls elapsed time without sleeping
//! and without depending on how many times `open`/`begin` happened to read the
//! clock first — only the *differences* between reads matter. The deadline's
//! start instant is read once when the cursor is built; each between-rows check is
//! one further read, `step` ms later. So with `start = S`:
//!   - the n=0 check reads `S + step`,
//!   - the next periodic check (1024 rows later) reads `S + 2*step`.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use bson::doc;
use slate_db::v2::IndexOptions;
use slate_db::{Database, DatabaseBuilder, DbError, SortDirection};
use slate_store::MemoryStore;

const C: &str = "items";

/// A clock whose consecutive reads differ by exactly `step` ms — see the module
/// note. Backed by an atomic so it is `Send + Sync` for `with_clock`.
fn advancing_clock(step: i64) -> impl Fn() -> i64 + Send + Sync + 'static {
    let now = Arc::new(AtomicI64::new(0));
    move || now.fetch_add(step, Ordering::Relaxed)
}

/// Open a memory database with the given clock and (optional) default deadline,
/// seeded with `n` trivial documents in collection `C`.
fn seeded(
    clock: impl Fn() -> i64 + Send + Sync + 'static,
    default_deadline: Option<Duration>,
    n: usize,
) -> Database<MemoryStore> {
    let mut builder = DatabaseBuilder::new().with_clock(clock);
    if let Some(d) = default_deadline {
        builder = builder.with_deadline(d);
    }
    let db = builder.open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    db.collections().create(C).execute(&txn).unwrap();
    let docs: Vec<_> = (0..n).map(|i| doc! { "_id": format!("{i:05}") }).collect();
    db.collection(C).insert_many(docs).execute(&txn).unwrap();
    txn.commit().unwrap();
    db
}

/// Drain a `find(all)` over `db`, returning the collected result.
fn drain_all(db: &Database<MemoryStore>) -> Result<Vec<bson::RawDocumentBuf>, DbError> {
    let txn = db.begin(true).unwrap();
    db.collection(C)
        .find(doc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect()
}

#[test]
fn tiny_default_deadline_trips_timeout() {
    // A 1 ms deadline against a clock that jumps 1000 ms per read: the first
    // between-rows check (start + 1000) is already past start + 1, so the scan
    // aborts with Timeout on its first row.
    let db = seeded(advancing_clock(1000), Some(Duration::from_millis(1)), 5);
    let result = drain_all(&db);
    assert!(
        matches!(result, Err(DbError::Timeout)),
        "expected Timeout, got {result:?}"
    );
}

#[test]
fn generous_default_deadline_does_not_trip() {
    // A very large deadline never trips for a small scan: all rows come through.
    let db = seeded(
        advancing_clock(1000),
        Some(Duration::from_secs(1_000_000)),
        5,
    );
    let rows = drain_all(&db).expect("a generous deadline must not trip");
    assert_eq!(rows.len(), 5);
}

#[test]
fn no_deadline_configured_never_trips() {
    // With no deadline at all (the default), the clock is irrelevant — every row
    // is returned regardless of how fast time advances.
    let db = seeded(advancing_clock(1_000_000), None, 5);
    let rows = drain_all(&db).expect("no deadline must never trip");
    assert_eq!(rows.len(), 5);
}

#[test]
fn per_query_deadline_overrides_generous_default() {
    // The database default is generous, but a tiny per-query override wins and
    // trips. (Demonstrates the FindBuilder `.deadline(..)` override path.)
    let db = seeded(
        advancing_clock(1000),
        Some(Duration::from_secs(1_000_000)),
        5,
    );
    let txn = db.begin(true).unwrap();
    let result: Result<Vec<_>, _> = db
        .collection(C)
        .find(doc! {})
        .deadline(Duration::from_millis(1))
        .iter_raw(&txn)
        .unwrap()
        .collect();
    assert!(
        matches!(result, Err(DbError::Timeout)),
        "per-query override should trip, got {result:?}"
    );
}

#[test]
fn deadline_is_cooperative_checked_between_rows() {
    // Cooperative, not preemptive: the check fires *between rows*, every 1024
    // rows. With start = S, step = 1000 ms, and a 1500 ms deadline (deadline_at =
    // S + 1500), the n=0 check reads S + 1000 (≤ deadline, passes) and the next
    // check, 1024 rows later, reads S + 2000 (> deadline, trips). So the first
    // 1024 rows stream out *before* the query aborts — it is not killed up front.
    let db = seeded(
        advancing_clock(1000),
        Some(Duration::from_millis(1500)),
        1500,
    );
    let txn = db.begin(true).unwrap();
    let iter = db.collection(C).find(doc! {}).iter_raw(&txn).unwrap();

    let mut rows_before_error = 0usize;
    let mut hit_timeout = false;
    for item in iter {
        match item {
            Ok(_) => rows_before_error += 1,
            Err(DbError::Timeout) => {
                hit_timeout = true;
                break;
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert!(hit_timeout, "expected the scan to abort with Timeout");
    // Rows flowed before the abort (cooperative); the abort lands on the periodic
    // check boundary (DEADLINE_CHECK_INTERVAL = 1024).
    assert_eq!(
        rows_before_error, 1024,
        "deadline must be checked between rows at the 1024-row boundary"
    );
}

// ── B — materialization cap ──────────────────────────────────────────────────
//
// A *blocking* node (Sort/IndexMerge/GroupBy) buffers its source eagerly when the
// cursor is built, so the cap can trip from `iter_raw(..)` itself; the lazy
// `Distinct` trips during iteration. `.and_then(collect)` captures the error from
// either point.

/// Open a memory db (optional database-wide cap) seeded with `n` docs
/// `{_id, v, age, g}`: `v` is distinct per doc (Sort/Distinct fixtures); `age` is
/// indexed and binary (the `IndexMerge(Or)` fixture); `g = i % 10` gives ten
/// GROUP BY groups.
fn cap_db(default_cap: Option<usize>, n: usize) -> Database<MemoryStore> {
    let mut builder = DatabaseBuilder::new();
    if let Some(cap) = default_cap {
        builder = builder.with_materialization_cap(cap);
    }
    let db = builder.open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    db.collections().create(C).execute(&txn).unwrap();
    db.collection(C)
        .indexes()
        .create("age", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    let docs: Vec<_> = (0..n)
        .map(|i| {
            doc! {
                "_id": format!("{i:05}"),
                "v": i as i64,
                "age": (i % 2) as i64,
                "g": (i % 10) as i64,
            }
        })
        .collect();
    db.collection(C).insert_many(docs).execute(&txn).unwrap();
    txn.commit().unwrap();
    db
}

#[test]
fn sort_trips_materialization_cap() {
    // 100 docs sorted by an unindexed field → the Sort node buffers all 100; a
    // per-query cap of 50 aborts with LimitExceeded.
    let db = cap_db(None, 100);
    let txn = db.begin(true).unwrap();
    let result = db
        .collection(C)
        .find(doc! {})
        .sort("v", SortDirection::Desc)
        .materialization_cap(50)
        .iter_raw(&txn)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>());
    assert!(
        matches!(result, Err(DbError::LimitExceeded(_))),
        "Sort over the cap should trip, got {result:?}"
    );
}

#[test]
fn sort_under_cap_passes() {
    // The same sort under a generous cap returns every row.
    let db = cap_db(None, 100);
    let txn = db.begin(true).unwrap();
    let rows: Vec<_> = db
        .collection(C)
        .find(doc! {})
        .sort("v", SortDirection::Desc)
        .materialization_cap(1000)
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows.len(), 100);
}

#[test]
fn distinct_trips_materialization_cap() {
    // 100 distinct `v` values → the distinct set grows to 100; cap 50 trips.
    let db = cap_db(None, 100);
    let txn = db.begin(true).unwrap();
    let result = db
        .collection(C)
        .find(doc! {})
        .distinct("v")
        .materialization_cap(50)
        .iter_raw(&txn)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>());
    assert!(
        matches!(result, Err(DbError::LimitExceeded(_))),
        "Distinct over the cap should trip, got {result:?}"
    );
}

#[test]
fn index_merge_trips_materialization_cap() {
    // `age = 0 OR age = 1` over the indexed field plans as IndexMerge(Or), which
    // buffers the id streams of both legs (~all 200 docs); cap 50 trips.
    let db = cap_db(None, 200);
    let txn = db.begin(true).unwrap();
    let result = db
        .collection(C)
        .query("SELECT * FROM c WHERE c.age = 0 OR c.age = 1")
        .materialization_cap(50)
        .iter_raw(&txn)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>());
    assert!(
        matches!(result, Err(DbError::LimitExceeded(_))),
        "IndexMerge over the cap should trip, got {result:?}"
    );
}

#[test]
fn group_by_trips_materialization_cap() {
    // GROUP BY consumes every source row (bounding both groups and per-group
    // accumulators); over 100 rows a cap of 50 trips.
    let db = cap_db(None, 100);
    let txn = db.begin(true).unwrap();
    let result = db
        .collection(C)
        .query("SELECT c.g, COUNT(1) AS n FROM c GROUP BY c.g")
        .materialization_cap(50)
        .iter_raw(&txn)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>());
    assert!(
        matches!(result, Err(DbError::LimitExceeded(_))),
        "GroupBy over the cap should trip, got {result:?}"
    );
}

#[test]
fn builder_default_materialization_cap_applies() {
    // A database-wide default cap (no per-query override) bounds a Sort.
    let db = cap_db(Some(50), 100);
    let txn = db.begin(true).unwrap();
    let result = db
        .collection(C)
        .find(doc! {})
        .sort("v", SortDirection::Desc)
        .iter_raw(&txn)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>());
    assert!(
        matches!(result, Err(DbError::LimitExceeded(_))),
        "the database-default cap should trip, got {result:?}"
    );
}
