//! The `Unwind` node — `JOIN <alias> IN <array>` intra-document array unwind.
//!
//! For each row, evaluates `array` against the current environment and emits
//! one row per element, extending the environment with `{alias: element}`. A
//! non-array or undefined `array` yields no rows (inner-join semantics).

use bson::raw::{BindRawBsonRef, CString, RawBsonRef, RawDocumentBuf};
use bson::{Bson, RawBson};
use slate_ast::ScalarExpr;
use slate_eval::EvalError;
use slate_eval::raweval::{self, RawValue};

use super::env;
use crate::{ExecError, ValueIter};

/// Unwind `array` into the binding `alias` for each incoming environment row.
pub(crate) fn execute<'a>(
    alias: String,
    array: ScalarExpr,
    source: ValueIter<'a>,
    params: env::Params,
) -> ValueIter<'a> {
    Box::new(source.flat_map(move |item| {
        let rows: Box<dyn Iterator<Item = Result<Option<RawBson>, ExecError>>> = match item {
            Ok(Some(row)) => match expand(&row, &alias, &array, env::params_doc(&params)) {
                Ok(rows) => Box::new(rows.into_iter().map(|r| Ok(Some(r)))),
                Err(e) => Box::new(std::iter::once(Err(e))),
            },
            Ok(None) => Box::new(std::iter::empty()),
            Err(e) => Box::new(std::iter::once(Err(e))),
        };
        rows
    }))
}

/// Produce the extended environment rows for one input row.
fn expand(
    row: &RawBson,
    alias: &str,
    array: &ScalarExpr,
    params: Option<&bson::RawDocument>,
) -> Result<Vec<RawBson>, ExecError> {
    let bindings = env::bindings_of(row)?;
    let renv = env::raw_env(&bindings, params);

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
                out.push(extend_env(&bindings, alias, elem)?);
            }
        }
        // Computed array (e.g. from an object/function): elements are owned.
        RawValue::Owned(Bson::Array(items)) => {
            for item in items {
                let raw = RawBson::try_from(item).map_err(|e| EvalError {
                    message: format!("could not encode unwound element: {e}"),
                })?;
                out.push(extend_env(&bindings, alias, raw)?);
            }
        }
        // A constructed array kept in raw form — an array literal (`[…]`) or a
        // function/subquery result.
        RawValue::OwnedRaw(RawBson::Array(arr)) => {
            for elem in &*arr {
                let elem = elem.map_err(|e| EvalError {
                    message: format!("could not read array element: {e}"),
                })?;
                out.push(extend_env(&bindings, alias, elem)?);
            }
        }
        _ => {}
    }
    Ok(out)
}

/// Build one extended environment document: the existing `bindings` plus
/// `alias -> elem`. `elem` may be a borrowed `RawBsonRef` or an owned `RawBson`
/// (both bind into the buffer).
fn extend_env(
    bindings: &[(&str, RawBsonRef<'_>)],
    alias: &str,
    elem: impl BindRawBsonRef,
) -> Result<RawBson, ExecError> {
    let mut doc = RawDocumentBuf::new();
    for (k, v) in bindings {
        doc.append(cstring(k)?, *v);
    }
    doc.append(cstring(alias)?, elem);
    Ok(RawBson::Document(doc))
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
        let unwound = execute("t".into(), sv("c.tags"), bound, None);
        let projected = project::execute(
            sv(r#"{ "who": c.name, "tag": t }"#),
            RowBinding::Env,
            unwound,
            None,
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
