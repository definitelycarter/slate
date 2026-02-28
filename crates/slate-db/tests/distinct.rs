mod common;
use common::*;

use bson::{Bson, RawBson, doc, rawdoc};
use slate_db::CollectionConfig;
use slate_query::{DistinctOptions, SortDirection};

fn to_bson_vec(raw: RawBson) -> Vec<Bson> {
    match raw {
        RawBson::Array(arr) => arr
            .into_iter()
            .map(|r| Bson::try_from(r.unwrap()).unwrap())
            .collect(),
        _ => panic!("expected RawBson::Array"),
    }
}

// ── Distinct tests ──────────────────────────────────────────────

#[test]
fn distinct_scalar_field() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "inactive" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(COLLECTION, "status", rawdoc! {}, DistinctOptions::default())
            .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("active".into())));
    assert!(values.contains(&Bson::String("inactive".into())));
}

#[test]
fn distinct_nested_path() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "address": { "city": "Austin" } })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "address": { "city": "Denver" } })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "address": { "city": "Austin" } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "address.city",
            rawdoc! {},
            DistinctOptions::default(),
        )
        .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("Austin".into())));
    assert!(values.contains(&Bson::String("Denver".into())));
}

#[test]
fn distinct_array_field() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "tags": ["rust", "db"] })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "tags": ["db", "perf"] })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(COLLECTION, "tags", rawdoc! {}, DistinctOptions::default())
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active", "tier": "gold" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "inactive", "tier": "silver" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active", "tier": "silver" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "tier",
            eq_filter("status", Bson::String("active".into())),
            DistinctOptions::default(),
        )
        .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("gold".into())));
    assert!(values.contains(&Bson::String("silver".into())));
}

#[test]
fn distinct_with_sort_asc() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "status",
            rawdoc! {},
            DistinctOptions {
                sort: Some(SortDirection::Asc),
                ..Default::default()
            },
        )
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "status",
            rawdoc! {},
            DistinctOptions {
                sort: Some(SortDirection::Desc),
                ..Default::default()
            },
        )
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "name": "alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "name": "bob" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "nonexistent",
            rawdoc! {},
            DistinctOptions::default(),
        )
        .unwrap(),
    );
    assert!(values.is_empty());
}

#[test]
fn distinct_mixed_presence() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "name": "bob" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "inactive" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(COLLECTION, "status", rawdoc! {}, DistinctOptions::default())
            .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("active".into())));
    assert!(values.contains(&Bson::String("inactive".into())));
}

#[test]
fn distinct_array_of_sub_documents() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "triggers": [{ "type": "email" }, { "type": "sms" }] },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "triggers": [{ "type": "sms" }, { "type": "push" }] },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "triggers.type",
            rawdoc! {},
            DistinctOptions {
                sort: Some(SortDirection::Asc),
                ..Default::default()
            },
        )
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "address": { "city": "Austin", "state": "TX" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "address": { "city": "Denver", "state": "CO" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "address": { "city": "Austin", "state": "TX" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "address",
            rawdoc! {},
            DistinctOptions::default(),
        )
        .unwrap(),
    );
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::Document(doc! { "city": "Austin", "state": "TX" })));
    assert!(values.contains(&Bson::Document(doc! { "city": "Denver", "state": "CO" })));
}

#[test]
fn distinct_with_take() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "date" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "status",
            rawdoc! {},
            DistinctOptions {
                sort: Some(SortDirection::Asc),
                take: Some(2),
                ..Default::default()
            },
        )
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "date" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "status",
            rawdoc! {},
            DistinctOptions {
                sort: Some(SortDirection::Asc),
                skip: Some(1),
                take: Some(2),
            },
        )
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "date" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "elderberry" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    // Sort desc, skip 1, take 2 -> ["date", "cherry"]
    let values = to_bson_vec(
        txn.distinct(
            COLLECTION,
            "status",
            rawdoc! {},
            DistinctOptions {
                sort: Some(SortDirection::Desc),
                skip: Some(1),
                take: Some(2),
            },
        )
        .unwrap(),
    );
    assert_eq!(
        values,
        vec![Bson::String("date".into()), Bson::String("cherry".into()),]
    );
}
