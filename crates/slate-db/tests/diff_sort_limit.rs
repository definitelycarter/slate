//! Differential tests: ORDER BY + OFFSET/LIMIT match between v1 and v2.

mod diff_common;

use bson::doc;
use diff_common::assert_same_ordered;
use slate_query::{FindOptions, Sort, SortDirection};

fn sort(field: &str, dir: SortDirection) -> FindOptions {
    FindOptions {
        sort: vec![Sort {
            field: field.into(),
            direction: dir,
        }],
        ..Default::default()
    }
}

#[test]
fn sort_asc() {
    assert_same_ordered(doc! {}, sort("age", SortDirection::Asc));
}

#[test]
fn sort_desc() {
    assert_same_ordered(doc! {}, sort("age", SortDirection::Desc));
}

#[test]
fn sort_by_string() {
    assert_same_ordered(doc! {}, sort("name", SortDirection::Asc));
}

#[test]
fn sort_with_filter() {
    let mut o = sort("age", SortDirection::Desc);
    assert_same_ordered(doc! { "status": "active" }, o.clone());
    o.sort[0].direction = SortDirection::Asc;
    assert_same_ordered(doc! { "status": "active" }, o);
}

#[test]
fn sort_then_limit() {
    let mut o = sort("age", SortDirection::Asc);
    o.take = Some(2);
    assert_same_ordered(doc! {}, o);
}

#[test]
fn sort_skip_and_take() {
    let mut o = sort("age", SortDirection::Asc);
    o.skip = Some(1);
    o.take = Some(1);
    assert_same_ordered(doc! {}, o);
}

#[test]
fn multi_field_sort() {
    let o = FindOptions {
        sort: vec![
            Sort {
                field: "status".into(),
                direction: SortDirection::Asc,
            },
            Sort {
                field: "age".into(),
                direction: SortDirection::Desc,
            },
        ],
        ..Default::default()
    };
    assert_same_ordered(doc! {}, o);
}
