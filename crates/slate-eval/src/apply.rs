//! Applying an `UPDATE`'s assignments to a document.
//!
//! Naive strategy: deserialize the document, evaluate each assignment's
//! right-hand [`Expression`](slate_ast::Expression) against the *original*
//! document (so assignments don't observe each other — matching the Mongo
//! operators, whose operands never reference sibling assignments), set or remove
//! the target path, and re-serialize. A value that evaluates to *undefined*
//! removes the field. Returns `None` when nothing changed, so the write node can
//! drop an untouched row.
//!
//! A future pass will edit the BSON bytes in place via `slate-rawbson` for the
//! simple-assignment cases instead of rebuilding the whole document.

use bson::raw::RawDocument;
use bson::{Bson, Document, RawDocumentBuf};
use slate_ast::Assignment;

use crate::error::{EvalError, Result};
use crate::eval::{Env, eval};
use crate::value::Value;

fn bson_err(e: impl std::fmt::Display) -> EvalError {
    EvalError {
        message: format!("update apply: {e}"),
    }
}

/// Apply `assignments` to `old`, evaluating each right-hand side against the
/// original document bound to `alias`. Returns the rebuilt document, or `None`
/// if it is byte-identical to `old`.
pub fn apply_assignments(
    old: &RawDocument,
    alias: &str,
    assignments: &[Assignment],
    params: &Document,
) -> Result<Option<RawDocumentBuf>> {
    let original: Document = bson::deserialize_from_slice(old.as_bytes()).map_err(bson_err)?;
    // One immutable copy for evaluating each RHS; the original is moved into the
    // mutable working copy. (The future in-place pass avoids both.)
    let mut doc = original.clone();
    let snapshot = Bson::Document(original);

    for assignment in assignments {
        let binds = [(alias, &snapshot)];
        let env = Env::new(&binds, params);
        match eval(&assignment.value, &env)? {
            Value::Undefined => remove_path(&mut doc, &assignment.path),
            Value::Defined(value) => set_path(&mut doc, &assignment.path, value),
        }
    }

    // Unchanged → `None` so the write node drops the row.
    if matches!(&snapshot, Bson::Document(orig) if *orig == doc) {
        return Ok(None);
    }
    Ok(Some(
        bson::serialize_to_raw_document_buf(&doc).map_err(bson_err)?,
    ))
}

/// Set `value` at a dotted `path`, creating intermediate documents as needed.
fn set_path(doc: &mut Document, path: &[String], value: Bson) {
    match path {
        [] => {}
        [key] => {
            doc.insert(key.clone(), value);
        }
        [key, rest @ ..] => {
            if !matches!(doc.get(key), Some(Bson::Document(_))) {
                doc.insert(key.clone(), Bson::Document(Document::new()));
            }
            if let Some(Bson::Document(child)) = doc.get_mut(key) {
                set_path(child, rest, value);
            }
        }
    }
}

/// Remove the leaf of a dotted `path` (a no-op if any segment is absent).
fn remove_path(doc: &mut Document, path: &[String]) {
    match path {
        [] => {}
        [key] => {
            doc.remove(key);
        }
        [key, rest @ ..] => {
            if let Some(Bson::Document(child)) = doc.get_mut(key) {
                remove_path(child, rest);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{doc, rawdoc};

    fn apply(old: bson::RawDocumentBuf, assignments: &[Assignment]) -> Option<Document> {
        apply_assignments(&old, "c", assignments, &Document::new())
            .unwrap()
            .map(|raw| bson::deserialize_from_slice(raw.as_bytes()).unwrap())
    }

    fn set(path: &str, value: Bson) -> Assignment {
        Assignment {
            path: path.split('.').map(String::from).collect(),
            value: slate_ast::Expression::Value(value),
        }
    }

    #[test]
    fn sets_and_removes_fields() {
        let old = rawdoc! { "_id": "1", "a": 1, "b": 2 };
        let got = apply(
            old,
            &[
                set("a", Bson::Int32(9)),
                Assignment {
                    path: vec!["b".into()],
                    value: slate_ast::Expression::Identifier("undefined".into()),
                },
            ],
        );
        assert_eq!(got, Some(doc! { "_id": "1", "a": 9 }));
    }

    #[test]
    fn inc_references_current_value() {
        // `n = c.n + 1`
        let old = rawdoc! { "_id": "1", "n": 41 };
        let inc = Assignment {
            path: vec!["n".into()],
            value: slate_ast::Expression::Binary {
                op: slate_ast::BinOp::Add,
                lhs: Box::new(slate_ast::Expression::Member {
                    base: Box::new(slate_ast::Expression::Identifier("c".into())),
                    field: "n".into(),
                }),
                rhs: Box::new(slate_ast::Expression::Value(Bson::Int32(1))),
            },
        };
        // `Int + Int` stays integer but normalizes to Int64 (Cosmos number model).
        assert_eq!(apply(old, &[inc]), Some(doc! { "_id": "1", "n": 42_i64 }));
    }

    #[test]
    fn nested_path_creates_intermediate() {
        let old = rawdoc! { "_id": "1" };
        assert_eq!(
            apply(old, &[set("a.b", Bson::Int32(5))]),
            Some(doc! { "_id": "1", "a": { "b": 5 } })
        );
    }

    #[test]
    fn unchanged_returns_none() {
        let old = rawdoc! { "_id": "1", "a": 1 };
        assert_eq!(apply(old, &[set("a", Bson::Int32(1))]), None);
    }
}
