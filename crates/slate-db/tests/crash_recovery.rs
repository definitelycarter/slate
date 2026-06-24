//! Kill-during-commit → reopen → invariant-check harness — Thread B of the
//! Durability & Crash Safety RFC.
//!
//! This is the test that actually validates the crash recovery the engine
//! otherwise only *assumes*. A child process drives a write workload at
//! `Strict` durability against a persistent backend, recording each `Ok`-acked
//! commit to a fsync'd sidecar file. The parent `SIGKILL`s the child
//! mid-workload, reopens the database, and asserts:
//!
//! 1. **It opens.** A reopen after an abrupt kill must not fail.
//! 2. **Atomicity.** Every transaction the child *acked* (commit returned `Ok`
//!    and the ack was fsync'd before the kill) is wholly present on reopen — and
//!    no document beyond the ack frontier is required. A committed txn is present
//!    or absent as a whole, never half-applied.
//! 3. **Invariants hold.** [`Database::verify`] — the Thread-C oracle — reports
//!    a clean collection: record ↔ `i`-index ↔ `u`-slot ↔ TTL all consistent.
//!
//! ## Mechanism
//!
//! The worker logic lives in `#[ignore]`d tests (`crash_worker_rocks` /
//! `crash_worker_redb`) so the normal `cargo test` run skips them. The parent
//! re-execs this very test binary (`current_exe`) with `--exact --ignored` to
//! select the worker, passing the db path / ack-file path / backend via env
//! vars. `SIGKILL` (signal 9) is uncatchable, so the child cannot clean up — the
//! truest model of a process crash mid-commit.
//!
//! `MemoryStore` is out of scope (ephemeral; nothing survives a reopen). The
//! harness is Unix-only (it relies on `SIGKILL`); on other targets the parent
//! tests are skipped.

#![cfg(all(unix, feature = "runtime"))]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder, Durability};
use slate_store::{RedbStore, RocksStore, Store};

const COLLECTION: &str = "accounts";
const ENV_DB_PATH: &str = "SLATE_CRASH_DB_PATH";
const ENV_ACK_PATH: &str = "SLATE_CRASH_ACK_PATH";
const ENV_WORKER: &str = "SLATE_CRASH_IS_WORKER";

// ── Backend abstraction ────────────────────────────────────────

#[derive(Clone, Copy)]
enum Backend {
    Rocks,
    Redb,
}

impl Backend {
    /// The db file/dir path under a temp dir (rocks wants a dir, redb a file).
    fn db_path(self, base: &Path) -> PathBuf {
        match self {
            Backend::Rocks => base.join("db"),
            Backend::Redb => base.join("db.redb"),
        }
    }

    fn worker_test_name(self) -> &'static str {
        match self {
            Backend::Rocks => "crash_worker_rocks",
            Backend::Redb => "crash_worker_redb",
        }
    }
}

/// Open a database on the chosen backend with a default durability level.
/// Returns a boxed closure-free pair via an enum because the two `Database<S>`
/// types differ; we keep the open + workload monomorphic per backend instead.
fn open_rocks(path: &Path, durability: Durability) -> Database<RocksStore> {
    let store = RocksStore::open_with_durability(path, durability).unwrap();
    DatabaseBuilder::new().open(store).unwrap()
}

fn open_redb(path: &Path, durability: Durability) -> Database<RedbStore> {
    let store = RedbStore::open_with_durability(path, durability).unwrap();
    DatabaseBuilder::new().open(store).unwrap()
}

// ── Worker side ────────────────────────────────────────────────

/// Drive the workload: create the collection + indexes, then insert documents
/// one committed transaction at a time, fsync-acking each. Runs until killed.
///
/// Resumable: on an already-populated database (a second crash cycle) the schema
/// setup is idempotent and the seq counter resumes past the existing documents,
/// so re-running the worker never collides on `_id` or index existence.
fn run_worker<S: Store + Send + Sync + 'static>(
    db: &Database<S>,
    ack_path: &Path,
    start_seq: u64,
) -> ! {
    // Schema: a non-unique index on `bucket` and a unique index on `email`, so
    // the crash exercises `i`, `u`, and (via the auto TTL index) TTL structures.
    {
        let txn = db.begin(false).unwrap();
        txn.create_collection(&CollectionConfig {
            name: COLLECTION.into(),
            ..Default::default()
        })
        .unwrap();
        ignore_index_exists(txn.create_index(DEFAULT_CF, COLLECTION, "bucket"));
        ignore_index_exists(txn.create_unique_index(DEFAULT_CF, COLLECTION, "email"));
        txn.commit().unwrap();
    }

    let mut ack = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(ack_path)
        .unwrap();

    let mut seq: u64 = start_seq;
    loop {
        let txn = db.begin(false).unwrap();
        let doc = bson::doc! {
            "_id": format!("doc-{seq}"),
            "bucket": (seq % 8) as i64,
            "email": format!("user-{seq}@example.com"),
            "payload": "x".repeat(64),
        };
        txn.insert_one(DEFAULT_CF, COLLECTION, doc)
            .unwrap()
            .drain()
            .unwrap();
        txn.commit().unwrap();

        // Ack *after* the Strict commit returns Ok: this seq is durable. fsync
        // the ack so the parent's view of "acked" can't outrace the kill.
        writeln!(ack, "{seq}").unwrap();
        ack.flush().unwrap();
        ack.sync_all().unwrap();

        seq += 1;
    }
}

/// Swallow an `IndexExists` error (the schema setup is idempotent across crash
/// cycles); propagate anything else by panicking, as a worker should.
fn ignore_index_exists(result: Result<(), slate_db::DbError>) {
    match result {
        Ok(()) | Err(slate_db::DbError::IndexExists(_)) => {}
        Err(e) => panic!("worker schema setup failed: {e}"),
    }
}

/// The next seq to write: one past the highest `doc-{n}` already present, so a
/// resumed worker never collides on `_id`.
fn next_seq<S: Store + Send + Sync + 'static>(db: &Database<S>) -> u64 {
    let txn = db.begin(true).unwrap();
    // The collection may not exist yet on a first run.
    let count = txn
        .count(DEFAULT_CF, COLLECTION, bson::doc! {})
        .unwrap_or_default();
    txn.rollback().unwrap();
    count
}

// ── Worker entry points (selected by the parent via `--ignored`) ──────────

#[test]
#[ignore = "spawned as a crash-test worker subprocess by the parent harness"]
fn crash_worker_rocks() {
    if std::env::var(ENV_WORKER).is_err() {
        return; // Not actually a worker invocation; ignore.
    }
    let db_path = PathBuf::from(std::env::var(ENV_DB_PATH).unwrap());
    let ack_path = PathBuf::from(std::env::var(ENV_ACK_PATH).unwrap());
    let db = open_rocks(&db_path, Durability::Strict);
    let start = next_seq(&db);
    run_worker(&db, &ack_path, start);
}

#[test]
#[ignore = "spawned as a crash-test worker subprocess by the parent harness"]
fn crash_worker_redb() {
    if std::env::var(ENV_WORKER).is_err() {
        return;
    }
    let db_path = PathBuf::from(std::env::var(ENV_DB_PATH).unwrap());
    let ack_path = PathBuf::from(std::env::var(ENV_ACK_PATH).unwrap());
    let db = open_redb(&db_path, Durability::Strict);
    let start = next_seq(&db);
    run_worker(&db, &ack_path, start);
}

// ── Parent side ────────────────────────────────────────────────

/// Spawn a worker subprocess, let it ack at least `min_acks` commits, then
/// `SIGKILL` it. Returns the highest contiguous acked seq (the durability
/// frontier the reopen must satisfy).
fn spawn_kill(backend: Backend, db_path: &Path, ack_path: &Path, min_acks: u64) -> u64 {
    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(exe)
        .args([
            "--exact",
            "--ignored",
            "--nocapture",
            backend.worker_test_name(),
        ])
        .env(ENV_WORKER, "1")
        .env(ENV_DB_PATH, db_path)
        .env(ENV_ACK_PATH, ack_path)
        .spawn()
        .expect("spawn worker");

    // Wait until the worker has acked enough commits (or time out).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if last_ack(ack_path) >= Some(min_acks) {
            break;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("worker did not reach {min_acks} acks within the deadline");
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // Read the ack frontier, then kill *immediately* — the worker is mid-loop,
    // so the kill lands during or right after a commit (kill-during-commit).
    let frontier = last_ack(ack_path).unwrap();
    // SIGKILL: uncatchable, no cleanup — the truest crash model.
    unsafe {
        libc_kill(child.id() as i32, 9);
    }
    let _ = child.wait();
    frontier
}

/// The highest contiguous acked seq in the ack file, or `None` if empty. A
/// torn final line (kill mid-`writeln`) is dropped — only fully-flushed lines
/// count, which is exactly the durability frontier.
fn last_ack(ack_path: &Path) -> Option<u64> {
    let content = std::fs::read_to_string(ack_path).ok()?;
    let mut last = None;
    for line in content.lines() {
        if let Ok(n) = line.trim().parse::<u64>() {
            // Acks are written in order; track the max fully-parsed line.
            last = Some(last.map_or(n, |p: u64| p.max(n)));
        }
    }
    last
}

// Minimal FFI to `kill(2)` — avoids pulling the `libc` crate into dev-deps for
// a single call. `SIGKILL` (9) is the same on every Unix.
unsafe extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

/// Assert the reopened database satisfies atomicity (every acked doc present)
/// and the integrity invariants (`verify` is clean).
fn assert_recovered<S: Store + Send + Sync + 'static>(db: &Database<S>, frontier: u64) {
    let txn = db.begin(true).unwrap();
    // Atomicity: every acked commit must be wholly present on reopen.
    for seq in 0..=frontier {
        let id = format!("doc-{seq}");
        let found = txn
            .find_one(DEFAULT_CF, COLLECTION, bson::doc! { "_id": &id })
            .unwrap();
        assert!(
            found.is_some(),
            "acked doc {id} missing after crash+reopen (atomicity violation)"
        );
    }
    txn.rollback().unwrap();

    // Invariants: the Thread-C oracle must find no drift.
    let report = db.verify(DEFAULT_CF, COLLECTION).unwrap();
    assert!(
        report.ok(),
        "integrity drift after crash+reopen: {:?}",
        report.issues
    );
    assert!(report.records_checked > frontier);
}

#[test]
fn rocks_kill_during_commit_reopens_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Backend::Rocks;
    let db_path = backend.db_path(tmp.path());
    let ack_path = tmp.path().join("acks.log");

    let frontier = spawn_kill(backend, &db_path, &ack_path, 25);

    // Reopen and verify. A fresh open replays RocksDB's WAL.
    let db = open_rocks(&db_path, Durability::Strict);
    assert_recovered(&db, frontier);
}

#[test]
fn redb_kill_during_commit_reopens_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Backend::Redb;
    let db_path = backend.db_path(tmp.path());
    let ack_path = tmp.path().join("acks.log");

    let frontier = spawn_kill(backend, &db_path, &ack_path, 25);

    // Reopen and verify. redb recovers from its CoW B-tree's last durable root.
    let db = open_redb(&db_path, Durability::Strict);
    assert_recovered(&db, frontier);
}

/// A second kill/reopen *cycle* on the same database: recovery must be
/// idempotent, and the second crash must again leave a clean, atomic store.
#[test]
fn rocks_repeated_kill_cycles_stay_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Backend::Rocks;
    let db_path = backend.db_path(tmp.path());

    for cycle in 0..2 {
        let ack_path = tmp.path().join(format!("acks-{cycle}.log"));
        // The worker recreates the collection idempotently, so re-running it on
        // an existing db just continues to add documents.
        let frontier = spawn_kill(backend, &db_path, &ack_path, 15);
        let db = open_rocks(&db_path, Durability::Strict);
        assert_recovered(&db, frontier);
        // Drop the handle so the next cycle's worker can open exclusively.
        drop(db);
    }
}
