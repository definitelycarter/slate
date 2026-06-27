//! The `Project` node — `SELECT VALUE <expr>`.
//!
//! Evaluates the projection expression against the row environment using the
//! shared `slate-eval` raw evaluator, and emits the result. An undefined result
//! drops the row at the output boundary. For `find`, the expression is the
//! identity (`c`) and the bound document passes through.

use bson::RawBson;
use slate_ast::Expression;
use slate_eval::raweval::{self, Compiled};
use slate_planner::RowBinding;

use super::env::{sole_alias, with_row_env};
use crate::{ExecEnv, ExecError, ValueIter};

/// Wrap `source`, evaluating `expr` against each row environment.
pub(crate) fn execute<'a>(
    expr: Expression,
    binding: RowBinding,
    source: ValueIter<'a>,
    env: ExecEnv<'a>,
) -> ValueIter<'a> {
    // Identity fast path: `SELECT VALUE c` in single-binding mode projects the
    // whole bound row, so pass it straight through — no eval, no copy. This is
    // the `find` shape, and what brings the scan path to v1 parity. Detect it
    // once here rather than per row.
    if let (RowBinding::Alias(alias), Expression::Identifier(name)) = (&binding, &expr)
        && alias == name
    {
        return Box::new(source);
    }

    // Otherwise compile the projection once; the per-row closure evaluates the
    // resolved form (see `raweval::compile`).
    let program = raweval::compile(&expr, sole_alias(&binding), env.udf_ctx());
    Box::new(source.map(move |item| project_row(item, &binding, &program, &env)))
}

fn project_row(
    item: Result<Option<RawBson>, ExecError>,
    binding: &RowBinding,
    program: &Compiled,
    env: &ExecEnv,
) -> Result<Option<RawBson>, ExecError> {
    let Some(row) = item? else {
        return Ok(None);
    };

    // Evaluate against bindings borrowing from `row`. `into_raw` copies the
    // result out (a `Ref` is a single byte copy, not a decode + re-encode), so
    // the row's borrow can end here.
    with_row_env(&row, binding, env, |renv| {
        Ok(raweval::eval_compiled(program, renv)?.into_raw()?)
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
    use std::collections::HashMap;
    use std::rc::Rc;

    /// One binding (`query -> native`) for the executor-level UDF tests.
    fn bound(query: &str, native: &str) -> Option<Rc<HashMap<String, String>>> {
        Some(Rc::new(HashMap::from([(
            query.to_string(),
            native.to_string(),
        )])))
    }

    /// Project `expr_src` over a bound (`{c: doc}`) source of `docs`.
    fn project(expr_src: &str, docs: Vec<RawBson>) -> Vec<RawBson> {
        collect(execute(
            sv(expr_src),
            RowBinding::Env,
            bind_c(docs),
            crate::ExecEnv::new(),
        ))
        .unwrap()
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
            crate::ExecEnv::new(),
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
            crate::ExecEnv::new(),
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

    #[test]
    fn udf_resolves_against_the_bag_and_runs_per_row() {
        use slate_eval::Value;
        use slate_udf::{UdfBag, UdfError};

        let bag = UdfBag::new();
        bag.register("double", |args: &[Value]| {
            let n = args
                .first()
                .and_then(Value::as_bson)
                .and_then(|b| match b {
                    bson::Bson::Int32(i) => Some(f64::from(*i)),
                    bson::Bson::Int64(i) => Some(*i as f64),
                    bson::Bson::Double(d) => Some(*d),
                    _ => None,
                })
                .ok_or_else(|| UdfError::InvalidArgument {
                    name: "double".to_string(),
                    message: "expected a number".to_string(),
                })?;
            Ok(Value::defined(n * 2.0))
        });

        // `SELECT VALUE udf.double(c.age)` — resolved once against the bag at
        // compile, then called per row.
        let out = collect(execute(
            sv("udf.double(c.age)"),
            RowBinding::Env,
            bind_c(people()),
            crate::ExecEnv::new()
                .with_udf(Some(&bag))
                .with_udf_bindings(bound("double", "double")),
        ))
        .unwrap();
        assert_eq!(out, vec![RawBson::Double(72.0), RawBson::Double(82.0)]);
    }

    #[test]
    fn unbound_udf_is_an_error() {
        use slate_udf::UdfBag;

        let bag = UdfBag::new();
        // No binding for `missing` → unbound, regardless of the bag.
        let err = collect(execute(
            sv("udf.missing(c.age)"),
            RowBinding::Env,
            bind_c(people()),
            crate::ExecEnv::new().with_udf(Some(&bag)),
        ))
        .unwrap_err();
        assert!(
            err.to_string().contains("not bound"),
            "expected an unbound-udf error, got: {err}"
        );
    }

    #[test]
    fn bound_but_unregistered_native_fn_is_an_error() {
        use slate_udf::UdfBag;

        let bag = UdfBag::new(); // empty — the bound native fn is missing
        let err = collect(execute(
            sv("udf.tax(c.age)"),
            RowBinding::Env,
            bind_c(people()),
            crate::ExecEnv::new()
                .with_udf(Some(&bag))
                .with_udf_bindings(bound("tax", "compute_tax")),
        ))
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not registered") && msg.contains("compute_tax"),
            "expected an unregistered-native-fn error, got: {msg}"
        );
    }

    #[test]
    fn panicking_udf_fails_the_query() {
        use slate_eval::Value;
        use slate_udf::{UdfBag, UdfError};

        let bag = UdfBag::new();
        bag.register("boom", |_: &[Value]| -> Result<Value, UdfError> {
            panic!("kaboom")
        });
        // The panic is caught at the call boundary and turned into an error
        // (the "thread panicked" line on stderr is expected).
        let err = collect(execute(
            sv("udf.boom(c.age)"),
            RowBinding::Env,
            bind_c(people()),
            crate::ExecEnv::new()
                .with_udf(Some(&bag))
                .with_udf_bindings(bound("boom", "boom")),
        ))
        .unwrap_err();
        assert!(
            err.to_string().contains("panicked"),
            "expected a panic-to-error, got: {err}"
        );
    }
}
