mod common;
use common::*;

use bson::{doc, rawdoc};
use slate_db::{CollectionConfig, DEFAULT_CF};
use slate_query::FindOptions;

// ── Collection tests ────────────────────────────────────────────

#[test]
fn list_collections() {
    let (db, _dir) = temp_db();
    create_collection(&db, "contacts");
    create_collection(&db, "accounts");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(DEFAULT_CF, "contacts", doc! { "_id": "c-1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(DEFAULT_CF, "accounts", doc! { "_id": "a-1", "name": "Acme" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let mut collections = txn.list_collections().unwrap();
    collections.sort_by(|a, b| a.1.cmp(&b.1));
    let names: Vec<&str> = collections.iter().map(|(_, n)| n.as_str()).collect();
    assert_eq!(names, vec!["accounts", "contacts"]);
}

#[test]
fn drop_collection() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(DEFAULT_CF, COLLECTION, doc! { "_id": "a-1", "name": "Acme" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    txn.drop_collection(DEFAULT_CF, COLLECTION).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let result = txn
        .find(DEFAULT_CF, COLLECTION, rawdoc! {}, FindOptions::default())
        .and_then(|c| c.iter()?.collect::<Result<Vec<_>, _>>());
    assert!(matches!(
        result,
        Err(slate_db::DbError::CollectionNotFound(_))
    ));
    let collections = txn.list_collections().unwrap();
    assert!(!collections.iter().any(|(_, n)| n == COLLECTION));
}

// ── Collection isolation ────────────────────────────────────────

#[test]
fn collection_isolation() {
    let (db, _dir) = temp_db();
    create_collection(&db, "contacts");
    create_collection(&db, "accounts");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(DEFAULT_CF, "contacts", doc! { "_id": "c-1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(DEFAULT_CF, "accounts", doc! { "_id": "a-1", "name": "Acme" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let contacts = txn
        .find(DEFAULT_CF, "contacts", rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(contacts[0].get_str("name").unwrap(), "Alice");

    let accounts = txn
        .find(DEFAULT_CF, "accounts", rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].get_str("name").unwrap(), "Acme");
}

// ── Function registration ───────────────────────────────────────

#[test]
fn register_triggers() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "users".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.register_trigger(DEFAULT_CF, "users", "audit", "print('audit')")
        .unwrap();
    txn.register_trigger(DEFAULT_CF, "users", "notify", "print('notify')")
        .unwrap();
    txn.commit().unwrap();

    // Verify the collection is usable.
    let mut txn = db.begin(false).unwrap();
    txn.insert_one(DEFAULT_CF, "users", doc! { "_id": "u1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();
}

#[test]
fn register_validators() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "users".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.register_validator(DEFAULT_CF, "users", "require_name", "assert(doc.name)")
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let collections = txn.list_collections().unwrap();
    assert!(collections.iter().any(|(_, n)| n == "users"));
}

#[test]
fn register_udfs() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "users".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.register_udf(DEFAULT_CF, "users", "full_name", "return first .. ' ' .. last")
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let collections = txn.list_collections().unwrap();
    assert!(collections.iter().any(|(_, n)| n == "users"));
}

#[test]
fn register_all_function_types_with_indexes() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "users".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "users", "email").unwrap();
    txn.register_trigger(DEFAULT_CF, "users", "audit", "print('audit')")
        .unwrap();
    txn.register_validator(DEFAULT_CF, "users", "check", "assert(doc.name)")
        .unwrap();
    txn.register_udf(DEFAULT_CF, "users", "full_name", "return first .. last")
        .unwrap();
    txn.commit().unwrap();

    // Verify collection works with all config together.
    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        DEFAULT_CF,
        "users",
        doc! { "_id": "u1", "name": "Alice", "email": "alice@test.com" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(DEFAULT_CF, "users", rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
}
