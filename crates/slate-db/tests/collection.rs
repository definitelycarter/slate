mod common;
use common::*;

use bson::{doc, rawdoc};
use slate_query::FindOptions;

// ── Collection tests ────────────────────────────────────────────

#[test]
fn list_collections() {
    let (db, _dir) = temp_db();
    create_collection(&db, "contacts");
    create_collection(&db, "accounts");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one("contacts", doc! { "_id": "c-1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one("accounts", doc! { "_id": "a-1", "name": "Acme" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let mut collections = txn.list_collections().unwrap();
    collections.sort();
    assert_eq!(collections, vec!["accounts", "contacts"]);
}

#[test]
fn drop_collection() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "a-1", "name": "Acme" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    txn.drop_collection(COLLECTION).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let result = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .and_then(|c| c.iter()?.collect::<Result<Vec<_>, _>>());
    assert!(matches!(
        result,
        Err(slate_db::DbError::CollectionNotFound(_))
    ));
    let collections = txn.list_collections().unwrap();
    assert!(!collections.contains(&COLLECTION.to_string()));
}

// ── Collection isolation ────────────────────────────────────────

#[test]
fn collection_isolation() {
    let (db, _dir) = temp_db();
    create_collection(&db, "contacts");
    create_collection(&db, "accounts");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one("contacts", doc! { "_id": "c-1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one("accounts", doc! { "_id": "a-1", "name": "Acme" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let contacts = txn
        .find("contacts", rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(contacts[0].get_str("name").unwrap(), "Alice");

    let accounts = txn
        .find("accounts", rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].get_str("name").unwrap(), "Acme");
}
