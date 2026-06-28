//! The `Database::transact` retry helper (Transaction & Concurrency Contract
//! RFC, part 2). Exercises the begin → run → commit → retry-on-conflict loop
//! on both a serialize-writers backend (memory, never conflicts) and the
//! optimistic backend (RocksDB, where a concurrent writer forces a real
//! commit-time conflict the helper must absorb).

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bson::doc;
use slate_db::{DatabaseBuilder, DbError, RetryPolicy};
use slate_store::{MemoryStore, RocksStore};

// ── Memory backend (cannot conflict — body runs exactly once) ───────────────

#[test]
fn transact_commits_and_returns_value() {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    {
        let txn = db.begin(false).unwrap();
        db.collections().create("c").execute(&txn).unwrap();
        txn.commit().unwrap();
    }

    let calls = Cell::new(0u32);
    let id: String = db
        .transact(|txn| {
            calls.set(calls.get() + 1);
            db.collection("c")
                .insert_one(doc! { "_id": "a", "v": 1i64 })
                .execute(txn)?;
            Ok("a".to_string())
        })
        .unwrap();

    assert_eq!(id, "a", "transact returns the closure's value");
    assert_eq!(
        calls.get(),
        1,
        "a serialize-writers backend never conflicts → body runs once"
    );

    // The write committed.
    let txn = db.begin(true).unwrap();
    let doc = db
        .collection("c")
        .find(doc! { "_id": "a" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_i64("v").unwrap(), 1);
}

#[test]
fn transact_non_conflict_error_aborts_without_retry() {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    {
        let txn = db.begin(false).unwrap();
        db.collections().create("c").execute(&txn).unwrap();
        txn.commit().unwrap();
    }

    let calls = Cell::new(0u32);
    let result: Result<(), DbError> = db.transact(|txn| {
        calls.set(calls.get() + 1);
        // Stage a write, then fail with a non-conflict error.
        db.collection("c")
            .insert_one(doc! { "_id": "a" })
            .execute(txn)?;
        Err(DbError::InvalidQuery("boom".into()))
    });

    assert!(
        matches!(result, Err(DbError::InvalidQuery(_))),
        "a non-conflict error must propagate verbatim: {result:?}"
    );
    assert_eq!(calls.get(), 1, "a non-conflict error must not retry");

    // The aborted attempt rolled back — nothing persisted.
    let txn = db.begin(true).unwrap();
    let found = db
        .collection("c")
        .find(doc! { "_id": "a" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap();
    assert!(found.is_none(), "a rolled-back write must not persist");
}

// ── RocksDB backend (optimistic — real commit-time conflicts) ───────────────

/// Seed a single contended document `{_id: "x", n: 0}` in collection `c`.
fn seed_contended(db: &slate_db::Database<RocksStore>) {
    let txn = db.begin(false).unwrap();
    db.collections().create("c").execute(&txn).unwrap();
    db.collection("c")
        .insert_one(doc! { "_id": "x", "n": 0i64 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
}

/// Commit a competing write that moves `{_id: "x"}` to `n = 100` in its own
/// transaction — the concurrent writer that makes an in-flight transaction's
/// commit conflict.
fn competing_write(db: &slate_db::Database<RocksStore>) {
    let other = db.begin(false).unwrap();
    db.collection("c")
        .find(doc! { "_id": "x" })
        .update(doc! { "$set": { "n": 100i64 } })
        .one()
        .execute(&other)
        .unwrap();
    other.commit().unwrap();
}

#[test]
fn transact_retries_conflict_then_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new()
        .open(RocksStore::open(dir.path()).unwrap())
        .unwrap();
    seed_contended(&db);

    let attempts = Cell::new(0u32);
    let result: Result<(), DbError> = db.transact(|txn| {
        let attempt = attempts.get() + 1;
        attempts.set(attempt);

        // Stage this attempt's write first, so the optimistic transaction records
        // the contended key at its pre-conflict sequence (RocksDB tracks a key's
        // sequence at first access, not at `begin`).
        db.collection("c")
            .find(doc! { "_id": "x" })
            .update(doc! { "$set": { "n": 1i64 } })
            .one()
            .execute(txn)?;

        // On the first attempt only, a concurrent writer then commits a change to
        // the same document before this transaction commits — the optimistic
        // write-write race transact must absorb.
        if attempt == 1 {
            competing_write(&db);
        }
        Ok(())
    });

    assert!(
        result.is_ok(),
        "transact should succeed after one retry: {result:?}"
    );
    assert_eq!(
        attempts.get(),
        2,
        "body runs twice: one conflicting attempt, one committing attempt"
    );

    // The committing (second) attempt's write is the durable one.
    let txn = db.begin(true).unwrap();
    let doc = db
        .collection("c")
        .find(doc! { "_id": "x" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_i64("n").unwrap(), 1);
}

/// A conflict that never clears must exhaust the retry budget and then surface
/// as `DbError::Conflict` — not loop forever, not get swallowed. Driven by a
/// body that returns `DbError::Conflict` every run (a documented supported path:
/// `commit` *or* the body may report a conflict), which exercises the budget and
/// backoff deterministically without depending on a real backend producing N
/// conflicts in a row. The real commit-time conflict path is covered by
/// `transact_retries_conflict_then_succeeds`.
#[test]
fn transact_persistent_conflict_exhausts_budget_then_surfaces() {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();

    let attempts = Cell::new(0u32);
    // The backoff hook is `'static`, so it counts through a shared atomic rather
    // than borrowing a local — verifying it fires once before each retry.
    let backoffs = Arc::new(AtomicU32::new(0));
    let bo = Arc::clone(&backoffs);
    // 2 retries after the first attempt → 3 runs before giving up.
    let policy = RetryPolicy::new(2).with_backoff(move |_idx| {
        bo.fetch_add(1, Ordering::SeqCst);
    });

    let result: Result<(), DbError> = db.transact_with(&policy, |_txn| {
        attempts.set(attempts.get() + 1);
        Err(DbError::Conflict)
    });

    assert!(
        matches!(result, Err(DbError::Conflict)),
        "a persistent conflict must surface as DbError::Conflict, got {result:?}"
    );
    assert_eq!(
        attempts.get(),
        3,
        "initial attempt plus the 2-retry budget = 3 runs"
    );
    assert_eq!(
        backoffs.load(Ordering::SeqCst),
        2,
        "the backoff hook fires once before each of the 2 retries"
    );
}
