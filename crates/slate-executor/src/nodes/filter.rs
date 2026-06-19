//! The `Filter` node — `WHERE <predicate>` (and residual/recheck/HAVING/ON).
//!
//! Evaluates a boolean predicate against the row environment using the shared
//! `slate-sql` evaluator, and passes the row through unchanged when it holds.
//! Rows where the predicate is false *or* undefined are dropped (the 3-valued
//! rule).

use bson::{Bson, RawBson};
use slate_sql::Value;
use slate_sql::ast::ScalarExpr;

use super::env;
use crate::{ExecError, ValueIter};

/// Wrap `source`, keeping only rows where `predicate` evaluates to `true`.
pub(crate) fn execute<'a>(predicate: ScalarExpr, source: ValueIter<'a>) -> ValueIter<'a> {
    Box::new(
        source.filter_map(move |item| match keep_row(item, &predicate) {
            Ok(Some(value)) => Some(Ok(Some(value))), // kept
            Ok(None) => None,                         // dropped
            Err(e) => Some(Err(e)),                   // surface the error
        }),
    )
}

/// Returns `Ok(Some(row))` to keep the original row unchanged, `Ok(None)` to
/// drop it, or `Err`.
fn keep_row(
    item: Result<Option<RawBson>, ExecError>,
    predicate: &ScalarExpr,
) -> Result<Option<RawBson>, ExecError> {
    let Some(row) = item? else {
        return Ok(None);
    };

    let bindings = env::decode(&row)?;
    match env::eval_in(&bindings, predicate)? {
        Value::Defined(Bson::Boolean(true)) => Ok(Some(row)),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::project;
    use crate::nodes::test_support::{bind_c, pred, sv};
    use bson::{RawBson, rawdoc};

    fn people() -> Vec<RawBson> {
        vec![
            RawBson::Document(rawdoc! { "name": "ada", "age": 36 }),
            RawBson::Document(rawdoc! { "name": "alan", "age": 41 }),
            RawBson::Document(rawdoc! { "name": "grace", "age": 44 }),
        ]
    }

    /// Filtered rows are environment docs `{c: ...}`; unwrap back to the docs.
    fn filtered_docs(pred_src: &str, docs: Vec<RawBson>) -> Vec<RawBson> {
        // Project `c` to recover the bound document for comparison.
        collect(project::execute(
            sv("c"),
            execute(pred(pred_src), bind_c(docs)),
        ))
        .unwrap()
    }

    #[test]
    fn keeps_matching_rows_unchanged() {
        assert_eq!(
            filtered_docs("c.age > 40", people()),
            vec![
                RawBson::Document(rawdoc! { "name": "alan", "age": 41 }),
                RawBson::Document(rawdoc! { "name": "grace", "age": 44 }),
            ]
        );
    }

    #[test]
    fn false_predicate_drops_all() {
        assert!(filtered_docs("c.age > 100", people()).is_empty());
    }

    #[test]
    fn undefined_predicate_drops_row() {
        assert!(filtered_docs("c.missing > 5", people()).is_empty());
    }

    #[test]
    fn and_predicate() {
        assert_eq!(
            filtered_docs("c.age >= 41 AND c.name = \"grace\"", people()),
            vec![RawBson::Document(rawdoc! { "name": "grace", "age": 44 })]
        );
    }

    #[test]
    fn filter_then_project_composes() {
        // SELECT VALUE c.name FROM c WHERE c.age > 40
        let projected =
            project::execute(sv("c.name"), execute(pred("c.age > 40"), bind_c(people())));
        assert_eq!(
            collect(projected).unwrap(),
            vec![
                RawBson::String("alan".into()),
                RawBson::String("grace".into())
            ]
        );
    }
}
