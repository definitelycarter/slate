//! Differential tests: column projection matches between v1 and v2.

mod diff_common;

use bson::doc;
use diff_common::assert_same;
use slate_query::FindOptions;

fn columns(cols: &[&str]) -> FindOptions {
    FindOptions {
        columns: Some(cols.iter().map(|s| s.to_string()).collect()),
        ..Default::default()
    }
}

#[test]
fn single_column() {
    assert_same(doc! {}, columns(&["name"]));
}

#[test]
fn multiple_columns() {
    assert_same(doc! {}, columns(&["name", "age"]));
}

#[test]
fn projection_includes_pk_implicitly() {
    // Even without _id requested, v1 includes the pk — v2 must too.
    assert_same(doc! {}, columns(&["status"]));
}

#[test]
fn explicit_pk_column() {
    assert_same(doc! {}, columns(&["_id", "name"]));
}

#[test]
fn projection_with_filter() {
    assert_same(doc! { "age": { "$gt": 40 } }, columns(&["name"]));
}

#[test]
fn projection_of_missing_column() {
    // No document has `nope`; both should just yield {_id}.
    assert_same(doc! {}, columns(&["nope"]));
}
