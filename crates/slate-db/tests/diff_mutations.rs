//! Differential tests: delete / update / replace match between v1 and v2.

mod diff_common;

use bson::doc;
use diff_common::{assert_same_delete, assert_same_replace, assert_same_update};

#[test]
fn delete_by_id() {
    assert_same_delete(doc! { "_id": "2" });
}

#[test]
fn delete_by_predicate() {
    assert_same_delete(doc! { "age": { "$gt": 40 } });
}

#[test]
fn delete_none() {
    assert_same_delete(doc! { "age": { "$gt": 1000 } });
}

#[test]
fn update_set() {
    assert_same_update(
        doc! { "status": "active" },
        doc! { "$set": { "status": "archived" } },
    );
}

#[test]
fn update_inc() {
    assert_same_update(doc! { "age": { "$gt": 40 } }, doc! { "$inc": { "age": 1 } });
}

#[test]
fn update_unset() {
    assert_same_update(
        doc! { "status": "active" },
        doc! { "$unset": { "status": "" } },
    );
}

#[test]
fn replace_one_doc() {
    assert_same_replace(doc! { "_id": "2" }, doc! { "name": "bob", "age": 99 });
}
