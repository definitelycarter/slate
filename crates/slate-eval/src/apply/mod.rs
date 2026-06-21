//! Applying an `UPDATE`'s assignments to a document.
//!
//! [`apply_assignments`] first tries the [`inplace`] byte-edit fast path, which
//! evaluates each assignment's right-hand [`Expression`](slate_ast::Expression)
//! over the raw bytes and splices the result into the document for the simple
//! shapes (single-segment paths writing a scalar/undefined, plus `$push`/`$pop`
//! on the field's own array). Anything it can't handle — dotted paths, whole
//! document/array values, `$lpush`, a repeated target field, supplied query
//! params — falls back to [`rebuild`].
//!
//! [`rebuild`] is the naive strategy: deserialize the document, evaluate each
//! assignment's right-hand side against the *original* document (so assignments
//! don't observe each other — matching the Mongo operators, whose operands never
//! reference sibling assignments), set or remove the target path, and
//! re-serialize. A value that evaluates to *undefined* removes the field. Both
//! paths return `None` when nothing changed, so the write node can drop an
//! untouched row, and produce byte-identical output (pinned by
//! `inplace_matches_rebuild`).

mod inplace;

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
/// original document bound to `alias`. Returns the new document, or `None` if it
/// is byte-identical to `old`.
///
/// Tries the in-place byte path first and falls back to a full rebuild for the
/// shapes it doesn't handle; the two are semantically identical (and produce the
/// same bytes), so the choice is purely a performance one.
pub fn apply_assignments(
    old: &RawDocument,
    alias: &str,
    assignments: &[Assignment],
    params: &Document,
) -> Result<Option<RawDocumentBuf>> {
    match inplace::try_apply(old, alias, assignments, params)? {
        inplace::Outcome::Done(out) => Ok(out),
        inplace::Outcome::Fallback => rebuild(old, alias, assignments, params),
    }
}

/// The naive apply: deserialize, mutate the owned document, re-serialize.
fn rebuild(
    old: &RawDocument,
    alias: &str,
    assignments: &[Assignment],
    params: &Document,
) -> Result<Option<RawDocumentBuf>> {
    let original: Document = bson::deserialize_from_slice(old.as_bytes()).map_err(bson_err)?;
    // One immutable copy for evaluating each RHS; the original is moved into the
    // mutable working copy. (The in-place path avoids both.)
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
    use slate_ast::Expression;

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

    // ── In-place vs rebuild differential ──────────────────────────────────────
    //
    // The strongest guard: wherever the in-place path claims to handle a case,
    // its bytes must equal the rebuild's. Builders below mirror the exact
    // expression shapes `slate_query::translate` emits for each Mongo operator.

    fn field_ref(field: &str) -> Expression {
        Expression::Member {
            base: Box::new(Expression::Identifier("c".into())),
            field: field.into(),
        }
    }

    fn unset(field: &str) -> Assignment {
        Assignment {
            path: vec![field.into()],
            value: Expression::Identifier("undefined".into()),
        }
    }

    fn inc(field: &str, delta: Bson) -> Assignment {
        Assignment {
            path: vec![field.into()],
            value: Expression::Function {
                name: "inc".into(),
                args: vec![field_ref(field), Expression::Value(delta)],
            },
        }
    }

    fn array_fn(name: &str, field: &str, args_tail: Vec<Expression>) -> Assignment {
        let mut args = vec![field_ref(field)];
        args.extend(args_tail);
        Assignment {
            path: vec![field.into()],
            value: Expression::Function {
                name: name.into(),
                args,
            },
        }
    }

    fn push(field: &str, v: Bson) -> Assignment {
        array_fn("rpush", field, vec![Expression::Value(v)])
    }

    fn lpush(field: &str, v: Bson) -> Assignment {
        array_fn("lpush", field, vec![Expression::Value(v)])
    }

    fn pop(field: &str) -> Assignment {
        array_fn("pop", field, vec![])
    }

    /// `$rename: { from: to }` desugars to set-new-from-old then unset-old.
    fn rename(from: &str, to: &str) -> Vec<Assignment> {
        vec![
            Assignment {
                path: vec![to.into()],
                value: field_ref(from),
            },
            unset(from),
        ]
    }

    fn corpus_docs() -> Vec<bson::RawDocumentBuf> {
        let oid = bson::oid::ObjectId::from_bytes([7; 12]);
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        vec![
            rawdoc! {},
            rawdoc! { "_id": "1" },
            rawdoc! { "_id": "1", "a": 1_i32, "b": 2_i32 },
            rawdoc! { "_id": "1", "name": "Al", "n": 41_i64, "score": 1.5_f64 },
            rawdoc! { "_id": "1", "n": i32::MAX, "name": "x" },
            rawdoc! { "_id": "1", "tags": ["a", "b", "c"], "k": "v" },
            rawdoc! { "_id": "1", "tags": [1_i32, 2_i32], "n": 9_i32 },
            rawdoc! { "_id": "1", "tags": [] },
            rawdoc! { "_id": "1", "flag": true, "when": dt, "oid": oid },
            rawdoc! { "_id": "1", "a": { "b": { "c": 1_i32 } }, "z": "keep" },
        ]
    }

    fn corpus_updates() -> Vec<Vec<Assignment>> {
        vec![
            // $set scalars: same width, widen, retype, null, create.
            vec![set("a", Bson::Int32(7))],
            vec![set("a", Bson::Int64(7))],
            vec![set("a", Bson::Double(2.5))],
            vec![set("a", Bson::String("hello world".into()))],
            vec![set("name", Bson::String("Q".into()))],
            vec![set("a", Bson::Null)],
            vec![set("fresh", Bson::Int32(5))],
            vec![set("when", Bson::DateTime(bson::DateTime::from_millis(42)))],
            // $unset present / absent.
            vec![unset("a")],
            vec![unset("name")],
            vec![unset("missing")],
            // $inc: same type, widen, double, overflow, create.
            vec![inc("n", Bson::Int32(1))],
            vec![inc("n", Bson::Int64(1))],
            vec![inc("score", Bson::Double(0.5))],
            vec![inc("counter", Bson::Int32(3))],
            // $push: existing array, create, non-array → remove.
            vec![push("tags", Bson::Int32(99))],
            vec![push("tags", Bson::String("z".into()))],
            vec![push("fresh", Bson::Int32(1))],
            vec![push("name", Bson::Int32(1))],
            // $pop: array, empty, non-array → remove, missing → no-op.
            vec![pop("tags")],
            vec![pop("name")],
            vec![pop("missing")],
            // Fallbacks: dotted path, whole-doc value, $lpush, $rename, dup field.
            vec![set("a.b.c", Bson::Int32(2))],
            vec![set("a", Bson::Document(doc! { "x": 1_i32 }))],
            vec![lpush("tags", Bson::Int32(0))],
            rename("name", "label"),
            vec![set("a", Bson::Int32(2)), set("a", Bson::Int32(3))],
            // Multi-assignment.
            vec![
                inc("n", Bson::Int32(5)),
                set("name", Bson::String("done".into())),
                unset("a"),
            ],
        ]
    }

    #[test]
    fn inplace_matches_rebuild() {
        let params = Document::new();
        let mut handled = 0usize;
        for old in corpus_docs() {
            for update in corpus_updates() {
                let rebuilt = rebuild(&old, "c", &update, &params).unwrap();
                match inplace::try_apply(&old, "c", &update, &params).unwrap() {
                    inplace::Outcome::Done(got) => {
                        handled += 1;
                        let got_bytes = got.as_ref().map(|b| b.as_bytes());
                        let want_bytes = rebuilt.as_ref().map(|b| b.as_bytes());
                        assert_eq!(
                            got_bytes, want_bytes,
                            "in-place != rebuild for update {update:?} on {old:?}"
                        );
                    }
                    // Fallback: the rebuild is the sole path; nothing to compare.
                    inplace::Outcome::Fallback => {}
                }
            }
        }
        // Guard against the differential passing vacuously by always falling back.
        assert!(handled > 50, "expected many in-place cases, got {handled}");
    }

    #[test]
    fn expected_shapes_take_inplace_path() {
        let params = Document::new();
        let doc = rawdoc! { "_id": "1", "n": 1_i32, "name": "x", "tags": [1_i32, 2_i32] };
        let inplace = |a: Vec<Assignment>| {
            matches!(
                inplace::try_apply(&doc, "c", &a, &params).unwrap(),
                inplace::Outcome::Done(_)
            )
        };
        assert!(inplace(vec![set("n", Bson::Int32(9))]), "$set scalar");
        assert!(inplace(vec![set("fresh", Bson::Int32(9))]), "create field");
        assert!(inplace(vec![inc("n", Bson::Int32(1))]), "$inc");
        assert!(inplace(vec![unset("name")]), "$unset");
        assert!(inplace(vec![push("tags", Bson::Int32(3))]), "$push array");
        assert!(inplace(vec![pop("tags")]), "$pop array");
    }

    #[test]
    fn expected_shapes_fall_back() {
        let params = Document::new();
        let doc = rawdoc! { "_id": "1", "a": { "b": 1_i32 }, "tags": [1_i32], "name": "x" };
        let falls_back = |a: Vec<Assignment>| {
            matches!(
                inplace::try_apply(&doc, "c", &a, &params).unwrap(),
                inplace::Outcome::Fallback
            )
        };
        assert!(falls_back(vec![set("a.b", Bson::Int32(2))]), "dotted path");
        assert!(
            falls_back(vec![set("a", Bson::Document(doc! { "x": 1_i32 }))]),
            "whole-doc value"
        );
        assert!(falls_back(vec![lpush("tags", Bson::Int32(0))]), "$lpush");
        assert!(
            falls_back(vec![set("a", Bson::Int32(1)), set("a", Bson::Int32(2))]),
            "duplicate field"
        );
    }
}
