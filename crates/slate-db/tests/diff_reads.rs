//! Differential tests: read predicates produce identical results in v1 and v2.

mod diff_common;

use bson::doc;
use diff_common::assert_same;
use slate_query::FindOptions;

#[test]
fn scan_all() {
    assert_same(doc! {}, FindOptions::default());
}

#[test]
fn implicit_eq_unindexed() {
    assert_same(doc! { "name": "ada" }, FindOptions::default());
}

#[test]
fn implicit_eq_indexed() {
    // v1 uses the age index; v2 scans — results must still match.
    assert_same(doc! { "age": 41 }, FindOptions::default());
}

#[test]
fn explicit_eq() {
    assert_same(doc! { "age": { "$eq": 41 } }, FindOptions::default());
}

#[test]
fn range_gt() {
    assert_same(doc! { "age": { "$gt": 40 } }, FindOptions::default());
}

#[test]
fn range_between() {
    assert_same(
        doc! { "age": { "$gt": 36, "$lt": 44 } },
        FindOptions::default(),
    );
}

#[test]
fn and_predicate() {
    assert_same(
        doc! { "$and": [ { "status": "active" }, { "age": { "$gt": 40 } } ] },
        FindOptions::default(),
    );
}

#[test]
fn or_predicate() {
    assert_same(
        doc! { "$or": [ { "age": 36 }, { "age": 44 } ] },
        FindOptions::default(),
    );
}

#[test]
fn exists_true() {
    assert_same(doc! { "name": { "$exists": true } }, FindOptions::default());
}

#[test]
fn exists_false_on_missing_field() {
    assert_same(
        doc! { "missing": { "$exists": false } },
        FindOptions::default(),
    );
}

#[test]
fn no_match() {
    assert_same(doc! { "age": { "$gt": 1000 } }, FindOptions::default());
}
