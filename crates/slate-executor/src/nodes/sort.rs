//! The `Sort` node — `ORDER BY <expr> [ASC|DESC], ...`.
//!
//! A *blocking* transform: it buffers the whole source, evaluates each row's
//! sort keys against the row environment with the shared evaluator,
//! sorts, then emits. Because it consumes the source eagerly (which can fail),
//! its `execute` is fallible. The value ordering is `slate_eval::eval::order_values`
//! — the one definition shared with the in-memory engine, so `ORDER BY` can't
//! drift.

use std::cmp::Ordering;

use bson::RawBson;
use slate_ast::{OrderByItem, SortDirection};
use slate_eval::Value;
use slate_eval::eval::order_values;
use slate_eval::raweval;
use slate_planner::RowBinding;

use super::env::with_row_env;
use crate::budget;
use crate::{ExecEnv, ExecError, ValueIter};

/// Buffer, sort by `keys` (evaluated against each row environment), emit.
pub(crate) fn execute<'a>(
    keys: Vec<OrderByItem>,
    binding: RowBinding,
    source: ValueIter<'a>,
    env: ExecEnv<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    let mut rows: Vec<(Vec<Value>, RawBson)> = Vec::new();
    for item in source {
        // Undefined upstream rows are dropped (they'd be dropped at the
        // output boundary anyway).
        let Some(row) = item? else { continue };
        let key_values = eval_keys(&row, &binding, &keys, &env)?;
        rows.push((key_values, row));
        // OOM guard: cap the buffered rows (Resource Limits RFC, B).
        budget::check_cap(rows.len(), env.materialization_cap, "Sort")?;
    }

    rows.sort_by(|a, b| order_cmp(&a.0, &b.0, &keys));

    Ok(Box::new(rows.into_iter().map(|(_, row)| Ok(Some(row)))))
}

/// Evaluate each sort key expression against one row environment.
fn eval_keys(
    row: &RawBson,
    binding: &RowBinding,
    keys: &[OrderByItem],
    env: &ExecEnv,
) -> Result<Vec<Value>, ExecError> {
    with_row_env(row, binding, env, |renv| {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            // Sort keys are buffered, so they materialize to owned `Value`; the
            // ordering itself is the shared `order_values`, so `ORDER BY` can't
            // drift.
            out.push(raweval::eval(&key.expr, renv)?.into_value()?);
        }
        Ok(out)
    })
}

/// Lexicographic comparison of two rows' key vectors, honoring each key's
/// direction.
fn order_cmp(a: &[Value], b: &[Value], keys: &[OrderByItem]) -> Ordering {
    for ((av, bv), key) in a.iter().zip(b).zip(keys) {
        let ord = order_values(av, bv);
        let ord = match key.direction {
            SortDirection::Asc => ord,
            SortDirection::Desc => ord.reverse(),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::project;
    use crate::nodes::test_support::{bind_c, sv};
    use bson::{RawBson, rawdoc};
    use slate_ast::OrderByItem;
    use slate_planner::RowBinding;

    /// Parse `ORDER BY <src>` out of a query.
    fn order_by(src: &str) -> Vec<OrderByItem> {
        let q = slate_sql::parse(&format!("SELECT VALUE c FROM c ORDER BY {src}")).unwrap();
        q.order_by
    }

    /// Sort `docs` (bound to `c`) and recover the documents via `SELECT VALUE c`.
    fn sorted(order_src: &str, docs: Vec<RawBson>) -> Vec<RawBson> {
        let sorted = execute(
            order_by(order_src),
            RowBinding::Env,
            bind_c(docs),
            crate::ExecEnv::new(),
        )
        .unwrap();
        collect(project::execute(
            sv("c"),
            RowBinding::Env,
            sorted,
            crate::ExecEnv::new(),
        ))
        .unwrap()
    }

    fn people() -> Vec<RawBson> {
        vec![
            RawBson::Document(rawdoc! { "name": "ada", "age": 36 }),
            RawBson::Document(rawdoc! { "name": "grace", "age": 44 }),
            RawBson::Document(rawdoc! { "name": "alan", "age": 41 }),
        ]
    }

    #[test]
    fn ascending_by_field() {
        assert_eq!(
            sorted("c.age ASC", people()),
            vec![
                RawBson::Document(rawdoc! { "name": "ada", "age": 36 }),
                RawBson::Document(rawdoc! { "name": "alan", "age": 41 }),
                RawBson::Document(rawdoc! { "name": "grace", "age": 44 }),
            ]
        );
    }

    #[test]
    fn descending_by_field() {
        assert_eq!(
            sorted("c.age DESC", people()),
            vec![
                RawBson::Document(rawdoc! { "name": "grace", "age": 44 }),
                RawBson::Document(rawdoc! { "name": "alan", "age": 41 }),
                RawBson::Document(rawdoc! { "name": "ada", "age": 36 }),
            ]
        );
    }

    #[test]
    fn by_expression() {
        // ORDER BY -age ASC == age DESC
        assert_eq!(
            sorted("-c.age ASC", people()),
            vec![
                RawBson::Document(rawdoc! { "name": "grace", "age": 44 }),
                RawBson::Document(rawdoc! { "name": "alan", "age": 41 }),
                RawBson::Document(rawdoc! { "name": "ada", "age": 36 }),
            ]
        );
    }

    #[test]
    fn multi_key() {
        let docs = vec![
            RawBson::Document(rawdoc! { "team": "b", "age": 30 }),
            RawBson::Document(rawdoc! { "team": "a", "age": 20 }),
            RawBson::Document(rawdoc! { "team": "a", "age": 40 }),
        ];
        assert_eq!(
            sorted("c.team ASC, c.age DESC", docs),
            vec![
                RawBson::Document(rawdoc! { "team": "a", "age": 40 }),
                RawBson::Document(rawdoc! { "team": "a", "age": 20 }),
                RawBson::Document(rawdoc! { "team": "b", "age": 30 }),
            ]
        );
    }
}
