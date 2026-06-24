//! Database-level durability + integrity surface (Threads A & C of the
//! Durability & Crash Safety RFC), exercised through the public `slate-db` API.

use slate_db::{
    CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder, Durability, IntegrityIssue,
};
use slate_store::{MemoryStore, RocksStore};

fn seed_accounts<S: slate_store::Store + Send + Sync + 'static>(db: &Database<S>) {
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "accounts".into(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "accounts", "bucket").unwrap();
    txn.create_unique_index(DEFAULT_CF, "accounts", "email")
        .unwrap();
    for i in 0..20 {
        let doc = bson::doc! {
            "_id": format!("a{i}"),
            "bucket": (i % 4) as i64,
            "email": format!("a{i}@x.com"),
        };
        txn.insert_one(DEFAULT_CF, "accounts", doc)
            .unwrap()
            .drain()
            .unwrap();
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
    txn.update_many(
        DEFAULT_CF,
        "accounts",
        bson::doc! { "bucket": 0i64 },
        bson::doc! { "$set": { "bucket": 9i64 } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.update_one(
        DEFAULT_CF,
        "accounts",
        bson::doc! { "_id": "a1" },
        bson::doc! { "$set": { "email": "moved@x.com" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.delete_many(DEFAULT_CF, "accounts", bson::doc! { "_id": "a2" })
        .unwrap()
        .drain()
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
        txn.count(DEFAULT_CF, "accounts", bson::doc! {}).unwrap(),
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
    txn.create_collection(&CollectionConfig {
        name: "ledger".into(),
        ..Default::default()
    })
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin_with(Durability::Strict).unwrap();
    txn.insert_one(
        DEFAULT_CF,
        "ledger",
        bson::doc! { "_id": "txn-1", "amount": 100i64 },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let got = db
        .begin(true)
        .unwrap()
        .find_one(DEFAULT_CF, "ledger", bson::doc! { "_id": "txn-1" })
        .unwrap();
    assert!(got.is_some());
}
