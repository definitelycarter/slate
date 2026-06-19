//! Differential tests: array-membership equality (`{tags: "x"}`) matches
//! between v1 (implicit array-contains) and v2 (`ARRAY_CONTAINS`).

mod diff_common;

use bson::doc;
use diff_common::assert_same;
use slate_query::FindOptions;

#[test]
fn member_present_single() {
    // tags contains "computing" → alan, grace
    assert_same(doc! { "tags": "computing" }, FindOptions::default());
}

#[test]
fn member_present_one_doc() {
    assert_same(doc! { "tags": "math" }, FindOptions::default());
}

#[test]
fn member_absent() {
    assert_same(doc! { "tags": "nope" }, FindOptions::default());
}

#[test]
fn explicit_eq_member() {
    assert_same(doc! { "tags": { "$eq": "naval" } }, FindOptions::default());
}

#[test]
fn array_member_with_other_predicate() {
    assert_same(
        doc! { "$and": [ { "tags": "computing" }, { "status": "active" } ] },
        FindOptions::default(),
    );
}
