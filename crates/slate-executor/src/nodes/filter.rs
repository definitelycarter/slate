//! The `Filter` node — `WHERE <predicate>` (and residual/recheck/HAVING/ON).
//!
//! Evaluates a boolean predicate against the row environment using the shared
//! `slate-eval` raw evaluator, and passes the row through unchanged when it
//! holds. Rows where the predicate is false *or* undefined are dropped (the
//! 3-valued rule).

use bson::RawBson;
use slate_ast::ScalarExpr;
use slate_eval::raweval;
use slate_planner::RowBinding;

use super::env;
use crate::{ExecError, ValueIter};

/// Wrap `source`, keeping only rows where `predicate` evaluates to `true`.
pub(crate) fn execute<'a>(
    predicate: ScalarExpr,
    binding: RowBinding,
    source: ValueIter<'a>,
) -> ValueIter<'a> {
    Box::new(
        source.filter_map(move |item| match keep_row(item, &binding, &predicate) {
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
    binding: &RowBinding,
    predicate: &ScalarExpr,
) -> Result<Option<RawBson>, ExecError> {
    let Some(row) = item? else {
        return Ok(None);
    };

    // Evaluate the predicate against bindings that borrow from `row`; the
    // borrow ends before we move `row` through. Only `Some(true)` keeps the
    // row (3-valued rule: false *or* undefined drops it).
    let keep = env::with_env(&row, binding, |renv| {
        Ok(raweval::eval(predicate, renv)?.as_bool() == Some(true))
    })?;

    if keep { Ok(Some(row)) } else { Ok(None) }
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::project;
    use crate::nodes::test_support::{bind_c, pred, sv};
    use bson::{RawBson, rawdoc};
    use slate_planner::RowBinding;

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
            RowBinding::Env,
            execute(pred(pred_src), RowBinding::Env, bind_c(docs)),
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
        let projected = project::execute(
            sv("c.name"),
            RowBinding::Env,
            execute(pred("c.age > 40"), RowBinding::Env, bind_c(people())),
        );
        assert_eq!(
            collect(projected).unwrap(),
            vec![
                RawBson::String("alan".into()),
                RawBson::String("grace".into())
            ]
        );
    }
}
