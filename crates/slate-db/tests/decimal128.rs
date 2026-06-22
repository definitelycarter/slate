//! `Decimal128` (`$numberDecimal`) is readable through eval, not just stored.
//!
//! Regression for the bug where a stored decimal was invisible to per-field
//! evaluation: it projected as missing, never matched a comparison, and made
//! aggregates return nothing — even though `find` (raw passthrough) returned it
//! fine. slate evaluates numbers in one f64 tower, so a decimal participates by
//! its f64 value (comparisons/sorts exact for these magnitudes; SUM/AVG return a
//! double). The stored value stays a real `Decimal128` — `find` round-trips it.

use bson::{Bson, Decimal128, doc, rawdoc};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_query::FindOptions;
use slate_store::MemoryStore;

fn dec(s: &str) -> Decimal128 {
    s.parse().unwrap()
}

fn seeded() -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "items".into(),
        ..Default::default()
    })
    .unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "items",
        vec![
            doc! { "_id": "1", "price": dec("10.00") },
            doc! { "_id": "2", "price": dec("25.50") },
            doc! { "_id": "3", "price": dec("100.00") },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

fn ids(db: &Database<MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let out = txn
        .query(DEFAULT_CF, "items", sql)
        .unwrap()
        .iter_values::<String>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    txn.rollback().unwrap();
    out
}

fn one_f64(db: &Database<MemoryStore>, sql: &str) -> f64 {
    let txn = db.begin(true).unwrap();
    let v = txn
        .query(DEFAULT_CF, "items", sql)
        .unwrap()
        .iter_values::<f64>()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    txn.rollback().unwrap();
    v
}

fn one_bson(db: &Database<MemoryStore>, sql: &str) -> Bson {
    let txn = db.begin(true).unwrap();
    let v = txn
        .query(DEFAULT_CF, "items", sql)
        .unwrap()
        .iter_values::<Bson>()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    txn.rollback().unwrap();
    v
}

#[test]
fn projection_returns_the_decimal_not_undefined() {
    let db = seeded();
    // The headline regression: a bare projected decimal field used to vanish.
    assert_eq!(
        one_bson(&db, r#"SELECT VALUE c.price FROM c WHERE c._id = "2""#),
        Bson::Decimal128(dec("25.50"))
    );
}

#[test]
fn comparison_filters_on_decimal_value() {
    let db = seeded();
    // Used to return zero rows because the decimal read as undefined.
    assert_eq!(
        ids(&db, "SELECT VALUE c._id FROM c WHERE c.price > 20"),
        vec!["2", "3"]
    );
    assert_eq!(
        ids(
            &db,
            "SELECT VALUE c._id FROM c WHERE c.price > 20 AND c.price < 100"
        ),
        vec!["2"]
    );
    // Decimal compared against a double literal, too.
    assert_eq!(
        ids(&db, "SELECT VALUE c._id FROM c WHERE c.price >= 25.5"),
        vec!["2", "3"]
    );
}

#[test]
fn order_by_sorts_decimals() {
    let db = seeded();
    assert_eq!(
        ids(&db, "SELECT VALUE c._id FROM c ORDER BY c.price DESC"),
        vec!["3", "2", "1"]
    );
    assert_eq!(
        ids(&db, "SELECT VALUE c._id FROM c ORDER BY c.price"),
        vec!["1", "2", "3"]
    );
}

#[test]
fn aggregates_over_decimals() {
    let db = seeded();
    // SUM/AVG collapse to a documented double.
    assert!((one_f64(&db, "SELECT VALUE SUM(c.price) FROM c") - 135.5).abs() < 1e-9);
    assert!((one_f64(&db, "SELECT VALUE AVG(c.price) FROM c") - 135.5 / 3.0).abs() < 1e-9);
    // MIN/MAX preserve the winning value's original type (Decimal128).
    assert_eq!(
        one_bson(&db, "SELECT VALUE MIN(c.price) FROM c"),
        Bson::Decimal128(dec("10.00"))
    );
    assert_eq!(
        one_bson(&db, "SELECT VALUE MAX(c.price) FROM c"),
        Bson::Decimal128(dec("100.00"))
    );
}

#[test]
fn find_still_round_trips_the_raw_decimal() {
    let db = seeded();
    let txn = db.begin(true).unwrap();
    // find returns the stored document untouched — the decimal is intact.
    let found = txn
        .find_one(DEFAULT_CF, "items", rawdoc! { "_id": "3" })
        .unwrap()
        .expect("doc 3");
    assert_eq!(
        found.get("price").unwrap(),
        Some(bson::raw::RawBsonRef::Decimal128(dec("100.00")))
    );
    // and the Mongo filter surface compares decimals just like SQL does.
    let n = txn
        .find(
            DEFAULT_CF,
            "items",
            rawdoc! { "price": { "$gt": 20 } },
            FindOptions::default(),
        )
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(n, 2);
    txn.rollback().unwrap();
}
