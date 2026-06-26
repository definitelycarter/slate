//! Cross-backend logical export / import (the Logical Export / Import RFC's
//! headline payoff), exercised through the public `slate-db` API.
//!
//! Physical backup can only restore by the same backend; a logical dump is
//! backend-neutral. The flagship test here exports a populated **redb** database
//! and imports it into a fresh **RocksDB** one, asserting that both documents and
//! catalog survive — the migration path physical backup can't serve.

use bson::{Decimal128, doc, oid::ObjectId};
use slate_db::v2::IndexOptions;
use slate_db::{Database, DatabaseBuilder, ExportOptions, ImportOptions};
use slate_store::{RedbStore, RocksStore, Store};
use std::str::FromStr;

/// Populate a database with two collections, indexes (one unique), custom
/// pk/ttl paths, and BSON-specific types whose lossless survival is the point of
/// the BSON canonical format.
fn populate<S: Store + Send + Sync + 'static>(db: &Database<S>) {
    let txn = db.begin(false).unwrap();

    db.collections().create("users").execute(&txn).unwrap();
    let users = db.collection("users");
    users
        .indexes()
        .create("city", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    users
        .indexes()
        .create("email", IndexOptions::unique())
        .execute(&txn)
        .unwrap();
    users
        .insert_many(vec![
            doc! { "_id": "1", "name": "ada", "city": "London", "email": "ada@x.io",
            "joined": bson::DateTime::from_millis(1_700_000_000_000),
            "oid": ObjectId::from_str("64a9c0ffee0000000000beef").unwrap(),
            "balance": Decimal128::from_str("9999.99").unwrap() },
            doc! { "_id": "2", "name": "alan", "city": "London", "email": "alan@x.io" },
            doc! { "_id": "3", "name": "grace", "city": "York", "email": "grace@x.io" },
        ])
        .execute(&txn)
        .unwrap();

    db.collections()
        .create("events")
        .pk_path("key")
        .ttl_path("expires")
        .execute(&txn)
        .unwrap();
    db.collection("events")
        .insert_many(vec![
            doc! { "key": "e1", "kind": "click" },
            doc! { "key": "e2", "kind": "view" },
        ])
        .execute(&txn)
        .unwrap();

    txn.commit().unwrap();
}

/// Sorted-by-pk documents of one collection, for order-independent comparison.
fn docs<S: Store>(db: &Database<S>, collection: &str, pk: &str) -> Vec<bson::Document> {
    let txn = db.begin(true).unwrap();
    let mut out: Vec<bson::Document> = db
        .collection(collection)
        .find(doc! {})
        .iter::<bson::Document>(&txn)
        .unwrap()
        .map(|d| d.unwrap())
        .collect();
    txn.rollback().unwrap();
    out.sort_by(|a, b| {
        let ka = a.get(pk).and_then(|v| v.as_str()).unwrap_or("");
        let kb = b.get(pk).and_then(|v| v.as_str()).unwrap_or("");
        ka.cmp(kb)
    });
    out
}

#[test]
fn redb_to_rocksdb_round_trip() {
    let src_dir = tempfile::tempdir().unwrap();
    let dump_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();

    // Source: a populated redb database.
    let src = DatabaseBuilder::new()
        .open(RedbStore::open(&src_dir.path().join("src.redb")).unwrap())
        .unwrap();
    populate(&src);

    // Export to a backend-neutral logical dump.
    let report = src
        .export(dump_dir.path(), ExportOptions::default())
        .unwrap();
    assert_eq!(report.total_documents(), 5);

    // Destination: a fresh RocksDB database — a different backend entirely.
    let dst = DatabaseBuilder::new()
        .open(RocksStore::open(dst_dir.path()).unwrap())
        .unwrap();
    let imported = dst
        .import(dump_dir.path(), ImportOptions::default())
        .unwrap();
    assert_eq!(imported.total_documents(), 5);

    // Documents are identical across backends, BSON-specific types included.
    assert_eq!(docs(&src, "users", "_id"), docs(&dst, "users", "_id"));
    assert_eq!(docs(&src, "events", "key"), docs(&dst, "events", "key"));

    // Catalog is identical — the manifest carried the full definition.
    for (collection, _pk) in [("users", "_id"), ("events", "key")] {
        let src_txn = src.begin(true).unwrap();
        let s = src.collection(collection).schema(&src_txn).unwrap();
        let dst_txn = dst.begin(true).unwrap();
        let d = dst.collection(collection).schema(&dst_txn).unwrap();
        assert_eq!(s, d, "schema mismatch for {collection}");
    }

    // The rebuilt `city` index drives a query on the RocksDB side.
    let txn = dst.begin(true).unwrap();
    let names: Vec<String> = dst
        .collection("users")
        .query("SELECT VALUE c.name FROM c WHERE c.city = 'London' ORDER BY c.name")
        .iter::<String>(&txn)
        .unwrap()
        .map(|n| n.unwrap())
        .collect();
    txn.rollback().unwrap();
    assert_eq!(names, vec!["ada".to_string(), "alan".to_string()]);
}
