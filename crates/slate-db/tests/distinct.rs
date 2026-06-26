mod common;
use common::*;

use bson::{Bson, RawBson, doc, rawdoc};
use slate_db::SortDirection;

fn to_bson_vec(raw: Vec<RawBson>) -> Vec<Bson> {
    raw.into_iter()
        .map(|r| Bson::try_from(r).unwrap())
        .collect()
}

// ── Distinct tests ──────────────────────────────────────────────

#[test]
fn distinct_scalar_field() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "status": "active" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "inactive" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "active" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("status")
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("active".into())));
    assert!(values.contains(&Bson::String("inactive".into())));
}

#[test]
fn distinct_nested_path() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "address": { "city": "Austin" } })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "address": { "city": "Denver" } })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "address": { "city": "Austin" } })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("address.city")
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("Austin".into())));
    assert!(values.contains(&Bson::String("Denver".into())));
}

#[test]
fn distinct_array_field() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "tags": ["rust", "db"] })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "tags": ["db", "perf"] })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("tags")
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(values.len(), 3);
    assert!(values.contains(&Bson::String("rust".into())));
    assert!(values.contains(&Bson::String("db".into())));
    assert!(values.contains(&Bson::String("perf".into())));
}

#[test]
fn distinct_with_filter() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "status": "active", "tier": "gold" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "inactive", "tier": "silver" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "active", "tier": "silver" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(eq_filter("status", Bson::String("active".into())))
            .distinct("tier")
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("gold".into())));
    assert!(values.contains(&Bson::String("silver".into())));
}

#[test]
fn distinct_with_sort_asc() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "status": "cherry" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "apple" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "banana" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("status")
            .sort(SortDirection::Asc)
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(
        values,
        vec![
            Bson::String("apple".into()),
            Bson::String("banana".into()),
            Bson::String("cherry".into()),
        ]
    );
}

#[test]
fn distinct_with_sort_desc() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "status": "cherry" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "apple" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "banana" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("status")
            .sort(SortDirection::Desc)
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(
        values,
        vec![
            Bson::String("cherry".into()),
            Bson::String("banana".into()),
            Bson::String("apple".into()),
        ]
    );
}

#[test]
fn distinct_missing_field() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "name": "alice" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "name": "bob" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("nonexistent")
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert!(values.is_empty());
}

#[test]
fn distinct_mixed_presence() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "status": "active" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "name": "bob" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "inactive" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("status")
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("active".into())));
    assert!(values.contains(&Bson::String("inactive".into())));
}

#[test]
fn distinct_array_of_sub_documents() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "triggers": [{ "type": "email" }, { "type": "sms" }] })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "triggers": [{ "type": "sms" }, { "type": "push" }] })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("triggers.type")
            .sort(SortDirection::Asc)
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(
        values,
        vec![
            Bson::String("email".into()),
            Bson::String("push".into()),
            Bson::String("sms".into()),
        ]
    );
}

#[test]
fn distinct_sub_document() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "address": { "city": "Austin", "state": "TX" } })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "address": { "city": "Denver", "state": "CO" } })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "address": { "city": "Austin", "state": "TX" } })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("address")
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::Document(doc! { "city": "Austin", "state": "TX" })));
    assert!(values.contains(&Bson::Document(doc! { "city": "Denver", "state": "CO" })));
}

#[test]
fn distinct_with_take() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "status": "cherry" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "apple" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "banana" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "date" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("status")
            .sort(SortDirection::Asc)
            .limit(2)
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(
        values,
        vec![Bson::String("apple".into()), Bson::String("banana".into()),]
    );
}

#[test]
fn distinct_with_skip_take() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "status": "cherry" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "apple" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "banana" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "date" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("status")
            .sort(SortDirection::Asc)
            .offset(1)
            .limit(2)
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(
        values,
        vec![Bson::String("banana".into()), Bson::String("cherry".into()),]
    );
}

#[test]
fn distinct_with_sort_and_limit() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "status": "cherry" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "apple" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "banana" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "date" })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "status": "elderberry" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    // Sort desc, skip 1, take 2 -> ["date", "cherry"]
    let values = to_bson_vec(
        coll.find(rawdoc! {})
            .distinct("status")
            .sort(SortDirection::Desc)
            .offset(1)
            .limit(2)
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    assert_eq!(
        values,
        vec![Bson::String("date".into()), Bson::String("cherry".into()),]
    );
}
