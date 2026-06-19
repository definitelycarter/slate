//! The `Unwind` node — `JOIN <alias> IN <array>` intra-document array unwind.
//!
//! For each row, evaluates `array` against the current environment and emits
//! one row per element, extending the environment with `{alias: element}`. A
//! non-array or undefined `array` yields no rows (inner-join semantics).

use bson::{Bson, RawBson};
use slate_sql::SqlError;
use slate_sql::Value;
use slate_sql::ast::ScalarExpr;

use super::env;
use crate::{ExecError, ValueIter};

/// Unwind `array` into the binding `alias` for each incoming environment row.
pub(crate) fn execute<'a>(
    alias: String,
    array: ScalarExpr,
    source: ValueIter<'a>,
) -> ValueIter<'a> {
    Box::new(source.flat_map(move |item| {
        let rows: Box<dyn Iterator<Item = Result<Option<RawBson>, ExecError>>> = match item {
            Ok(Some(row)) => match expand(&row, &alias, &array) {
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
fn expand(row: &RawBson, alias: &str, array: &ScalarExpr) -> Result<Vec<RawBson>, ExecError> {
    let bindings = env::decode(row)?;
    let items = match env::eval_in(&bindings, array)? {
        Value::Defined(Bson::Array(items)) => items,
        _ => return Ok(Vec::new()), // non-array / undefined → inner join drops the row
    };

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        // Each output row needs its own environment copy.
        let mut env = bindings.clone();
        env.insert(alias.to_string(), item);
        let raw = RawBson::try_from(Bson::Document(env)).map_err(|e| SqlError::Eval {
            message: format!("could not encode unwound row: {e}"),
        })?;
        out.push(raw);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::test_support::sv;
    use crate::nodes::{bind, project, values};
    use bson::{RawBson, rawdoc};

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
        let unwound = execute("t".into(), sv("c.tags"), bound);
        let projected = project::execute(sv(r#"{ "who": c.name, "tag": t }"#), unwound);
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
