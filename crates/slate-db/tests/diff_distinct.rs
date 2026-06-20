//! Differential tests: DISTINCT matches between v1 and v2.

mod diff_common;

use bson::doc;
use diff_common::assert_same_distinct;

#[test]
fn distinct_status() {
    assert_same_distinct("status", doc! {});
}

#[test]
fn distinct_age() {
    assert_same_distinct("age", doc! {});
}

#[test]
fn distinct_with_filter() {
    assert_same_distinct("status", doc! { "age": { "$gt": 40 } });
}

#[test]
fn distinct_name_all_unique() {
    assert_same_distinct("name", doc! {});
}
