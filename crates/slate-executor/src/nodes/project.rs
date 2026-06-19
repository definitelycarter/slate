//! The `Project` node — `SELECT VALUE <expr>`.
//!
//! Evaluates the projection expression against the row environment using the
//! shared `slate-sql` evaluator, and emits the result. An undefined result
//! drops the row at the output boundary. For `find`, the expression is the
//! identity (`c`) and the bound document passes through.

use bson::RawBson;
use slate_sql::SqlError;
use slate_sql::Value;
use slate_sql::ast::ScalarExpr;

use super::env;
use crate::{ExecError, ValueIter};

/// Wrap `source`, evaluating `expr` against each row environment.
pub(crate) fn execute<'a>(expr: ScalarExpr, source: ValueIter<'a>) -> ValueIter<'a> {
    Box::new(source.map(move |item| project_row(item, &expr)))
}

fn project_row(
    item: Result<Option<RawBson>, ExecError>,
    expr: &ScalarExpr,
) -> Result<Option<RawBson>, ExecError> {
    let Some(row) = item? else {
        return Ok(None);
    };

    let bindings = env::decode(&row)?;
    match env::eval_in(&bindings, expr)? {
        Value::Defined(b) => {
            let raw = RawBson::try_from(b).map_err(|e| SqlError::Eval {
                message: format!("could not encode projected value: {e}"),
            })?;
            Ok(Some(raw))
        }
        Value::Undefined => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::test_support::{bind_c, sv};
    use bson::{RawBson, rawdoc};

    /// Project `expr_src` over a bound (`{c: doc}`) source of `docs`.
    fn project(expr_src: &str, docs: Vec<RawBson>) -> Vec<RawBson> {
        collect(execute(sv(expr_src), bind_c(docs))).unwrap()
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
}
