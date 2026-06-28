//! Transaction isolation at the slate-db boundary — that a transaction observes
//! a consistent snapshot taken at `begin` (snapshot isolation), as the
//! Transaction & Concurrency Contract documents for every backend. The
//! serialize-writers backends provide this by construction; the optimistic
//! backend (RocksDB) earns it by pinning a begin snapshot and threading it into
//! every read, so the repeatable-read assertion runs on RocksDB — where, before
//! this was wired, reads were read-committed and the re-read below would observe
//! the concurrent commit.

use bson::doc;
use slate_db::{Database, DatabaseBuilder, DatabaseTransaction};
use slate_store::RocksStore;

/// Read `c[id].n` through the given transaction.
fn read_n(db: &Database<RocksStore>, txn: &DatabaseTransaction<'_, RocksStore>, id: &str) -> i64 {
    db.collection("c")
        .find(doc! { "_id": id })
        .iter_raw(txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap()
        .get_i64("n")
        .unwrap()
}

#[test]
fn rocksdb_reads_observe_a_consistent_begin_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new()
        .open(RocksStore::open(dir.path()).unwrap())
        .unwrap();

    // Seed { _id: "x", n: 0 }.
    {
        let txn = db.begin(false).unwrap();
        db.collections().create("c").execute(&txn).unwrap();
        db.collection("c")
            .insert_one(doc! { "_id": "x", "n": 0i64 })
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();
    }

    // Open a read transaction and observe n = 0.
    let reader = db.begin(true).unwrap();
    assert_eq!(read_n(&db, &reader, "x"), 0);

    // A concurrent writer commits n = 1 while the reader is still open.
    {
        let w = db.begin(false).unwrap();
        db.collection("c")
            .find(doc! { "_id": "x" })
            .update(doc! { "$set": { "n": 1i64 } })
            .one()
            .execute(&w)
            .unwrap();
        w.commit().unwrap();
    }

    // The reader re-reads and STILL sees its begin snapshot (n = 0), not the
    // concurrent commit — the snapshot-isolation guarantee.
    assert_eq!(
        read_n(&db, &reader, "x"),
        0,
        "a repeatable read must not observe a concurrent commit"
    );

    // A fresh read transaction does see the new value — confirming the write
    // landed and only the in-flight transaction's begin snapshot is frozen.
    let after = db.begin(true).unwrap();
    assert_eq!(read_n(&db, &after, "x"), 1);
}
