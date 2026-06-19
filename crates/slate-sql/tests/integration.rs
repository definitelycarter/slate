//! End-to-end tests exercising lex → parse → execute through the public API.

use bson::{Bson, bson};
use slate_sql::query;

fn catalog() -> Vec<Bson> {
    vec![
        bson!({ "id": 1, "name": "widget", "price": 9.5, "tags": ["sale", "new"] }),
        bson!({ "id": 2, "name": "gadget", "price": 20.0, "tags": ["new"] }),
        bson!({ "id": 3, "name": "gizmo", "price": 5.0, "tags": [] }),
    ]
}

#[test]
fn end_to_end_select_value() {
    let docs = vec![
        bson!({ "name": "ada", "age": 36 }),
        bson!({ "name": "alan", "age": 41 }),
    ];
    let out = query("SELECT VALUE c.name FROM c WHERE c.age > 40", &docs).unwrap();
    assert_eq!(out, vec![bson!("alan")]);
}

#[test]
fn filter_project_order() {
    let out = query(
        "SELECT VALUE c.name FROM c WHERE c.price < 10 ORDER BY c.price ASC",
        &catalog(),
    )
    .unwrap();
    assert_eq!(out, vec![bson!("gizmo"), bson!("widget")]);
}

#[test]
fn join_unwind_with_computed_object() {
    let out = query(
        r#"SELECT VALUE { "p": c.name, "t": t } FROM c JOIN t IN c.tags WHERE c.id = 1"#,
        &catalog(),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            bson!({ "p": "widget", "t": "sale" }),
            bson!({ "p": "widget", "t": "new" }),
        ]
    );
}

#[test]
fn arithmetic_and_functions() {
    let out = query(
        r#"SELECT VALUE { "name": UPPER(c.name), "withTax": c.price * 1.1 } FROM c WHERE c.id = 2"#,
        &catalog(),
    )
    .unwrap();
    assert_eq!(out, vec![bson!({ "name": "GADGET", "withTax": 22.0 })]);
}

#[test]
fn limit_offset() {
    let out = query(
        "SELECT VALUE c.id FROM c ORDER BY c.id DESC OFFSET 1 LIMIT 1",
        &catalog(),
    )
    .unwrap();
    // `id` is stored as Int32 in the document.
    assert_eq!(out, vec![bson!(2_i32)]);
}

#[test]
fn parse_error_surfaces() {
    assert!(query("SELECT c FROM c", &catalog()).is_err());
}
