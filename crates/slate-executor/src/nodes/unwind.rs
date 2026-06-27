//! The `Unwind` node — `JOIN <alias> IN <array>` intra-document array unwind.
//!
//! For each row, evaluates `array` against the current environment and emits
//! one row per element, extending the environment with `{alias: element}`. A
//! non-array or undefined `array` yields no rows (inner-join semantics).

use bson::raw::{BindRawBsonRef, CStr, CString, RawBsonRef, RawDocumentBuf};
use bson::{Bson, RawBson};
use slate_ast::Expression;
use slate_eval::EvalError;
use slate_eval::raweval::{self, RawValue};

use super::env::{bindings_of, row_env};
use crate::{ExecEnv, ExecError, ValueIter};

/// Unwind `array` into the binding `alias` for each incoming environment row.
pub(crate) fn execute<'a>(
    alias: String,
    array: Expression,
    source: ValueIter<'a>,
    env: ExecEnv<'a>,
) -> ValueIter<'a> {
    // The unwind alias is stable for the whole stream, so validate it into a
    // `CString` once and append it by reference for every emitted row rather than
    // re-allocating it per element. A rejected alias (interior NUL — vanishingly
    // rare) aborts the stream with the same error.
    let alias_key = match CString::try_from(alias.as_str()) {
        Ok(key) => key,
        Err(e) => {
            let err = ExecError::Eval(EvalError {
                message: format!("invalid binding alias '{alias}': {e}"),
            });
            return Box::new(std::iter::once(Err(err)));
        }
    };
    Box::new(source.flat_map(move |item| {
        let rows: Box<dyn Iterator<Item = Result<Option<RawBson>, ExecError>>> = match item {
            Ok(Some(row)) => match expand(&row, &alias_key, &array, &env) {
                Ok(rows) => Box::new(rows.into_iter().map(|r| Ok(Some(r)))),
                Err(e) => Box::new(std::iter::once(Err(e))),
            },
            Ok(None) => Box::new(std::iter::empty()),
            Err(e) => Box::new(std::iter::once(Err(e))),
        };
        rows
    }))
}

/// Produce the extended environment rows for one input row. `alias_key` is the
/// unwind alias, validated once by the caller.
fn expand(
    row: &RawBson,
    alias_key: &CStr,
    array: &Expression,
    env: &ExecEnv,
) -> Result<Vec<RawBson>, ExecError> {
    let bindings = bindings_of(row)?;
    let renv = row_env(&bindings, env);

    // Validate the existing binding keys into `CString`s once for this row, so
    // each emitted element reuses them by reference instead of re-validating.
    let keyed: Vec<(CString, RawBsonRef)> = bindings
        .iter()
        .map(|(k, v)| Ok((cstring(k)?, *v)))
        .collect::<Result<_, ExecError>>()?;

    let mut out = Vec::new();
    // Each output row is `{<existing bindings>, alias: <element>}`, built by
    // appending raw refs — no whole-document decode. A non-array / undefined
    // `array` yields no rows (inner-join semantics).
    match raweval::eval(array, &renv)? {
        RawValue::Ref(RawBsonRef::Array(a)) => {
            for elem in a {
                let elem = elem.map_err(|e| EvalError {
                    message: format!("could not read array element: {e}"),
                })?;
                out.push(extend_env(&keyed, alias_key, elem));
            }
        }
        // Computed array (e.g. from an object/function): elements are owned.
        RawValue::Owned(Bson::Array(items)) => {
            for item in items {
                let raw = RawBson::try_from(item).map_err(|e| EvalError {
                    message: format!("could not encode unwound element: {e}"),
                })?;
                out.push(extend_env(&keyed, alias_key, raw));
            }
        }
        // A constructed array kept in raw form — an array literal (`[…]`) or a
        // function/subquery result.
        RawValue::OwnedRaw(RawBson::Array(arr)) => {
            for elem in &*arr {
                let elem = elem.map_err(|e| EvalError {
                    message: format!("could not read array element: {e}"),
                })?;
                out.push(extend_env(&keyed, alias_key, elem));
            }
        }
        _ => {}
    }
    Ok(out)
}

/// Build one extended environment document: the existing `keyed` bindings plus
/// `alias_key -> elem`. `elem` may be a borrowed `RawBsonRef` or an owned
/// `RawBson` (both bind into the buffer). Keys are pre-validated, so this cannot
/// fail.
fn extend_env(
    keyed: &[(CString, RawBsonRef<'_>)],
    alias_key: &CStr,
    elem: impl BindRawBsonRef,
) -> RawBson {
    let mut doc = RawDocumentBuf::new();
    for (k, v) in keyed {
        doc.append(k, *v);
    }
    doc.append(alias_key, elem);
    RawBson::Document(doc)
}

fn cstring(s: &str) -> Result<CString, ExecError> {
    CString::try_from(s).map_err(|e| {
        EvalError {
            message: format!("invalid binding alias '{s}': {e}"),
        }
        .into()
    })
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::test_support::sv;
    use crate::nodes::{bind, project, values};
    use bson::{RawBson, rawdoc};
    use slate_planner::RowBinding;

    fn person(name: &str, tags: &[&str]) -> RawBson {
        let tag_arr: Vec<_> = tags
            .iter()
            .map(|t| bson::Bson::String((*t).into()))
            .collect();
        let doc = bson::doc! { "name": name, "tags": tag_arr };
        RawBson::try_from(bson::Bson::Document(doc)).unwrap()
    }

    /// `SELECT VALUE { "who": c.name, "tag": t } FROM c JOIN t IN c.tags`
    fn join_project(docs: Vec<RawBson>) -> Vec<RawBson> {
        let bound = bind::execute("c".into(), values::execute(docs));
        let unwound = execute("t".into(), sv("c.tags"), bound, crate::ExecEnv::new());
        let projected = project::execute(
            sv(r#"{ "who": c.name, "tag": t }"#),
            RowBinding::Env,
            unwound,
            crate::ExecEnv::new(),
        );
        collect(projected).unwrap()
    }

    #[test]
    fn unwinds_array_into_rows() {
        let out = join_project(vec![person("ada", &["x", "y"])]);
        assert_eq!(
            out,
            vec![
                RawBson::Document(rawdoc! { "who": "ada", "tag": "x" }),
                RawBson::Document(rawdoc! { "who": "ada", "tag": "y" }),
            ]
        );
    }

    #[test]
    fn cross_product_across_documents() {
        let out = join_project(vec![person("ada", &["x"]), person("alan", &["y", "z"])]);
        assert_eq!(
            out,
            vec![
                RawBson::Document(rawdoc! { "who": "ada", "tag": "x" }),
                RawBson::Document(rawdoc! { "who": "alan", "tag": "y" }),
                RawBson::Document(rawdoc! { "who": "alan", "tag": "z" }),
            ]
        );
    }

    #[test]
    fn empty_array_drops_row() {
        assert!(join_project(vec![person("grace", &[])]).is_empty());
    }

    #[test]
    fn missing_array_drops_row() {
        // c has no `tags` field → undefined → inner join drops it.
        let doc = RawBson::Document(rawdoc! { "name": "noone" });
        assert!(join_project(vec![doc]).is_empty());
    }
}
