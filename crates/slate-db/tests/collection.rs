mod common;
use common::*;

use bson::{doc, rawdoc};
use slate_db::v2::{IndexOptions, UdfFunction, ValidatorFunction};
use slate_db::{ValidatorCtx, Verdict};

// ── Collection tests ────────────────────────────────────────────

#[test]
fn list_collections() {
    let (db, _dir) = temp_db();
    create_collection(&db, "contacts");
    create_collection(&db, "accounts");

    let txn = db.begin(false).unwrap();
    db.collection("contacts")
        .insert_one(doc! { "_id": "c-1", "name": "Alice" })
        .execute(&txn)
        .unwrap();
    db.collection("accounts")
        .insert_one(doc! { "_id": "a-1", "name": "Acme" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    // v2: no global list (collections().list() is cf-scoped)
    let mut collections = txn.list_collections().unwrap();
    collections.sort_by(|a, b| a.1.cmp(&b.1));
    let names: Vec<&str> = collections.iter().map(|(_, n)| n.as_str()).collect();
    assert_eq!(names, vec!["accounts", "contacts"]);
}

#[test]
fn drop_collection() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "a-1", "name": "Acme" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    db.collections().remove(COLLECTION).execute(&txn).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let result = db
        .collection(COLLECTION)
        .find(rawdoc! {})
        .iter_raw(&txn)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>());
    assert!(matches!(
        result,
        Err(slate_db::DbError::CollectionNotFound(_))
    ));
    // v2: no global list (collections().list() is cf-scoped)
    let collections = txn.list_collections().unwrap();
    assert!(!collections.iter().any(|(_, n)| n == COLLECTION));
}

// ── Collection isolation ────────────────────────────────────────

#[test]
fn collection_isolation() {
    let (db, _dir) = temp_db();
    create_collection(&db, "contacts");
    create_collection(&db, "accounts");

    let txn = db.begin(false).unwrap();
    db.collection("contacts")
        .insert_one(doc! { "_id": "c-1", "name": "Alice" })
        .execute(&txn)
        .unwrap();
    db.collection("accounts")
        .insert_one(doc! { "_id": "a-1", "name": "Acme" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let contacts = db
        .collection("contacts")
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(contacts[0].get_str("name").unwrap(), "Alice");

    let accounts = db
        .collection("accounts")
        .find(rawdoc! {})
        .iter_raw(&txn)
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
    let txn = db.begin(false).unwrap();
    db.collections().create("users").execute(&txn).unwrap();
    db.collection("users")
        .triggers()
        .create("audit", "print('audit')")
        .execute(&txn)
        .unwrap();
    db.collection("users")
        .triggers()
        .create("notify", "print('notify')")
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    // Verify the collection is usable.
    let txn = db.begin(false).unwrap();
    db.collection("users")
        .insert_one(doc! { "_id": "u1", "name": "Alice" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
}

#[test]
fn register_validators() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    db.collections().create("users").execute(&txn).unwrap();
    db.collection("users")
        .validators()
        .create(
            "require_name",
            ValidatorFunction::from_name("require_name_impl"),
        )
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    // v2: no global list (collections().list() is cf-scoped)
    let collections = txn.list_collections().unwrap();
    assert!(collections.iter().any(|(_, n)| n == "users"));
}

#[test]
fn register_udfs() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    db.collections().create("users").execute(&txn).unwrap();
    db.collection("users")
        .functions()
        .create("full_name", UdfFunction::from_name("full_name_impl"))
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    // v2: no global list (collections().list() is cf-scoped)
    let collections = txn.list_collections().unwrap();
    assert!(collections.iter().any(|(_, n)| n == "users"));
}

#[test]
fn register_all_function_types_with_indexes() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    db.collections().create("users").execute(&txn).unwrap();
    db.collection("users")
        .indexes()
        .create("email", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    db.collection("users")
        .triggers()
        .create("audit", "print('audit')")
        .execute(&txn)
        .unwrap();
    // Register the native validator the binding points at, so the later insert
    // resolves it — an unregistered (dangling) binding would abort the write.
    db.collection("users")
        .validators()
        .register("check_impl", |_: &ValidatorCtx<'_>| Ok(Verdict::Accept));
    db.collection("users")
        .validators()
        .create("check", ValidatorFunction::from_name("check_impl"))
        .execute(&txn)
        .unwrap();
    db.collection("users")
        .functions()
        .create("full_name", UdfFunction::from_name("full_name_impl"))
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    // Verify collection works with all config together.
    let txn = db.begin(false).unwrap();
    db.collection("users")
        .insert_one(doc! { "_id": "u1", "name": "Alice", "email": "alice@test.com" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = db
        .collection("users")
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
}
