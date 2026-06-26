//! Database-level durability + integrity surface (Threads A & C of the
//! Durability & Crash Safety RFC), exercised through the public `slate-db` API.

use slate_db::v2::IndexOptions;
use slate_db::{DEFAULT_CF, Database, DatabaseBuilder, Durability, IntegrityIssue};
use slate_store::{MemoryStore, RocksStore};

fn seed_accounts<S: slate_store::Store + Send + Sync + 'static>(db: &Database<S>) {
    let txn = db.begin(false).unwrap();
    db.collections().create("accounts").execute(&txn).unwrap();
    let accounts = db.collection("accounts");
    accounts
        .indexes()
        .create("bucket", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    accounts
        .indexes()
        .create("email", IndexOptions::unique())
        .execute(&txn)
        .unwrap();
    for i in 0..20 {
        let doc = bson::doc! {
            "_id": format!("a{i}"),
            "bucket": (i % 4) as i64,
            "email": format!("a{i}@x.com"),
        };
        accounts.insert_one(doc).execute(&txn).unwrap();
    }
    txn.commit().unwrap();
}

#[test]
fn verify_clean_after_normal_workload() {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    seed_accounts(&db);

    let report = db.verify(DEFAULT_CF, "accounts").unwrap();
    assert!(report.ok(), "issues: {:?}", report.issues);
    assert_eq!(report.records_checked, 20);
}

#[test]
fn verify_clean_after_updates_and_deletes() {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    seed_accounts(&db);

    // Mutate: change some buckets, move an email (frees + reclaims a u-slot),
    // delete a few documents.
    let txn = db.begin(false).unwrap();
    let accounts = db.collection("accounts");
    accounts
        .find(bson::doc! { "bucket": 0i64 })
        .update(bson::doc! { "$set": { "bucket": 9i64 } })
        .execute(&txn)
        .unwrap();
    accounts
        .find(bson::doc! { "_id": "a1" })
        .update(bson::doc! { "$set": { "email": "moved@x.com" } })
        .one()
        .execute(&txn)
        .unwrap();
    accounts
        .find(bson::doc! { "_id": "a2" })
        .delete()
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let report = db.verify(DEFAULT_CF, "accounts").unwrap();
    assert!(report.ok(), "issues: {:?}", report.issues);
}

#[test]
fn repair_restores_clean_after_repair() {
    // verify → repair → verify is clean even on an undamaged collection (repair
    // is idempotent: rebuilding from records reproduces the same entries).
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    seed_accounts(&db);

    db.repair(DEFAULT_CF, "accounts").unwrap();
    let report = db.verify(DEFAULT_CF, "accounts").unwrap();
    assert!(report.ok(), "post-repair issues: {:?}", report.issues);
    assert!(
        !report
            .issues
            .iter()
            .any(|i| matches!(i, IntegrityIssue::OrphanUniqueSlot { .. }))
    );
}

#[test]
fn builder_durability_default_round_trips_on_rocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let store = RocksStore::open(&path).unwrap();
        let db = DatabaseBuilder::new()
            .with_durability(Durability::Strict)
            .open(store)
            .unwrap();
        seed_accounts(&db);
        // A clean verify under the Strict default.
        assert!(db.verify(DEFAULT_CF, "accounts").unwrap().ok());
    }
    // Reopen: every Strict-committed document is present.
    let store = RocksStore::open(&path).unwrap();
    let db = DatabaseBuilder::new().open(store).unwrap();
    let txn = db.begin(true).unwrap();
    assert_eq!(
        db.collection("accounts")
            .find(bson::doc! {})
            .iter_raw(&txn)
            .unwrap()
            .count(),
        20
    );
    txn.rollback().unwrap();
    assert!(db.verify(DEFAULT_CF, "accounts").unwrap().ok());
}

#[test]
fn begin_with_overrides_builder_default_on_rocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = RocksStore::open(&path).unwrap();
    // Loosest builder default; a single transaction tightens to Strict.
    let db = DatabaseBuilder::new()
        .with_durability(Durability::Relaxed)
        .open(store)
        .unwrap();

    let txn = db.begin(false).unwrap();
    db.collections().create("ledger").execute(&txn).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_with(Durability::Strict).unwrap();
    db.collection("ledger")
        .insert_one(bson::doc! { "_id": "txn-1", "amount": 100i64 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let got = db
        .collection("ledger")
        .find(bson::doc! { "_id": "txn-1" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap();
    assert!(got.is_some());
}
