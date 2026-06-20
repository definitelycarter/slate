//! Differential tests: $regex predicates now match between v1 and v2
//! (v2 gained the `REGEXMATCH` function).

mod diff_common;

use bson::doc;
use diff_common::assert_same;
use slate_query::FindOptions;

#[test]
fn prefix_anchor() {
    assert_same(doc! { "name": { "$regex": "^a" } }, FindOptions::default());
}

#[test]
fn contains() {
    assert_same(doc! { "name": { "$regex": "an" } }, FindOptions::default());
}

#[test]
fn case_insensitive_option() {
    assert_same(
        doc! { "name": { "$regex": "ADA", "$options": "i" } },
        FindOptions::default(),
    );
}

#[test]
fn no_match() {
    assert_same(
        doc! { "name": { "$regex": "^zzz" } },
        FindOptions::default(),
    );
}

#[test]
fn on_status_field() {
    assert_same(
        doc! { "status": { "$regex": "active$" } },
        FindOptions::default(),
    );
}
