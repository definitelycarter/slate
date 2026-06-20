//! Differential tests: upsert (replace + merge) match between v1 and v2.

mod diff_common;

use bson::doc;
use diff_common::assert_same_upsert;

#[test]
fn upsert_inserts_new() {
    assert_same_upsert(vec![doc! { "_id": "9", "name": "kay", "age": 50 }], false);
}

#[test]
fn upsert_replaces_existing() {
    assert_same_upsert(vec![doc! { "_id": "2", "name": "bob" }], false);
}

#[test]
fn merge_into_existing() {
    assert_same_upsert(vec![doc! { "_id": "2", "city": "nyc" }], true);
}

#[test]
fn merge_inserts_new() {
    assert_same_upsert(vec![doc! { "_id": "9", "name": "kay" }], true);
}

#[test]
fn upsert_mixed_batch() {
    assert_same_upsert(
        vec![
            doc! { "_id": "1", "name": "ADA" },
            doc! { "_id": "9", "name": "new" },
        ],
        false,
    );
}
