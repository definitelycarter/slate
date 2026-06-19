//! The `Project` node — `SELECT VALUE <expr>`.
//!
//! Evaluates the projection expression against the row environment using the
//! shared `slate-sql` evaluator, and emits the result. An undefined result
//! drops the row at the output boundary. For `find`, the expression is the
//! identity (`c`) and the bound document passes through.

use bson::RawBson;
use slate_planner::RowBinding;
use slate_sql::ast::ScalarExpr;
use slate_sql::raweval;

use super::env;
use crate::{ExecError, ValueIter};

/// Wrap `source`, evaluating `expr` against each row environment.
pub(crate) fn execute<'a>(
    expr: ScalarExpr,
    binding: RowBinding,
    source: ValueIter<'a>,
) -> ValueIter<'a> {
    Box::new(source.map(move |item| project_row(item, &binding, &expr)))
}

fn project_row(
    item: Result<Option<RawBson>, ExecError>,
    binding: &RowBinding,
    expr: &ScalarExpr,
) -> Result<Option<RawBson>, ExecError> {
    let Some(row) = item? else {
        return Ok(None);
    };

    // Identity fast path: `SELECT VALUE c` in single-binding mode projects the
    // whole bound row, so pass it straight through — no eval, no copy. This is
    // the `find` shape, and what brings the scan path to v1 parity.
    if let (RowBinding::Alias(alias), ScalarExpr::Identifier(name)) = (binding, expr)
        && alias == name
    {
        return Ok(Some(row));
    }

    // Otherwise evaluate against bindings borrowing from `row`. `into_raw`
    // copies the result out (a `Ref` is a single byte copy, not a decode +
    // re-encode), so the row's borrow can end here.
    env::with_env(&row, binding, |renv| {
        Ok(raweval::eval(expr, renv)?.into_raw()?)
    })
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::test_support::{bind_c, sv};
    use crate::nodes::values;
    use bson::{RawBson, rawdoc};
    use slate_planner::RowBinding;

    /// Project `expr_src` over a bound (`{c: doc}`) source of `docs`.
    fn project(expr_src: &str, docs: Vec<RawBson>) -> Vec<RawBson> {
        collect(execute(sv(expr_src), RowBinding::Env, bind_c(docs))).unwrap()
    }

    fn people() -> Vec<RawBson> {
        vec![
            RawBson::Document(rawdoc! { "name": "ada", "age": 36 }),
            RawBson::Document(rawdoc! { "name": "alan", "age": 41 }),
        ]
    }

    #[test]
    fn member_access() {
        assert_eq!(
            project("c.name", people()),
            vec![
                RawBson::String("ada".into()),
                RawBson::String("alan".into())
            ]
        );
    }

    #[test]
    fn identity_emits_whole_document() {
        assert_eq!(project("c", people()), people());
    }

    #[test]
    fn object_literal_with_arithmetic() {
        assert_eq!(
            project(r#"{ "n": c.name, "twice": c.age * 2 }"#, people()),
            vec![
                RawBson::Document(rawdoc! { "n": "ada", "twice": 72_i64 }),
                RawBson::Document(rawdoc! { "n": "alan", "twice": 82_i64 }),
            ]
        );
    }

    #[test]
    fn undefined_row_is_dropped() {
        assert!(project("c.missing", people()).is_empty());
    }

    #[test]
    fn alias_mode_identity_passes_bare_rows_through() {
        // Single-binding `SELECT VALUE c`: bare docs in, same docs out (no Bind).
        let out = collect(execute(
            sv("c"),
            RowBinding::Alias("c".into()),
            values::execute(people()),
        ))
        .unwrap();
        assert_eq!(out, people());
    }

    #[test]
    fn alias_mode_member_access_over_bare_rows() {
        // `SELECT VALUE c.name` over bare docs.
        let out = collect(execute(
            sv("c.name"),
            RowBinding::Alias("c".into()),
            values::execute(people()),
        ))
        .unwrap();
        assert_eq!(
            out,
            vec![
                RawBson::String("ada".into()),
                RawBson::String("alan".into())
            ]
        );
    }
}
