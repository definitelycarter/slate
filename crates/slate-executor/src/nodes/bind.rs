//! The `Bind` node — attach the `FROM` alias to a bare-value source.
//!
//! Wraps each incoming value `v` as the row environment `{alias: v}`. This is
//! the single bridge from the value pipeline (`Scan`, `KeyLookup`, …) into the
//! binding-aware nodes, so those sources need not know the alias.

use bson::RawBson;
use bson::raw::{CString, RawDocumentBuf};
use slate_sql::SqlError;

use crate::ValueIter;

/// Wrap each value from `source` as `{alias: value}`.
pub(crate) fn execute<'a>(alias: String, source: ValueIter<'a>) -> ValueIter<'a> {
    Box::new(source.map(move |item| {
        let Some(value) = item? else {
            return Ok(None);
        };
        let key = CString::try_from(alias.as_str()).map_err(|e| SqlError::Eval {
            message: format!("invalid binding alias '{alias}': {e}"),
        })?;
        let mut doc = RawDocumentBuf::new();
        doc.append(&key, value);
        Ok(Some(RawBson::Document(doc)))
    }))
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::values;
    use bson::{RawBson, rawdoc};

    #[test]
    fn wraps_value_under_alias() {
        let out = collect(execute(
            "c".into(),
            values::execute(vec![RawBson::Document(rawdoc! { "name": "ada" })]),
        ))
        .unwrap();
        assert_eq!(
            out,
            vec![RawBson::Document(rawdoc! { "c": { "name": "ada" } })]
        );
    }

    #[test]
    fn wraps_scalar_under_alias() {
        let out = collect(execute(
            "x".into(),
            values::execute(vec![RawBson::Int32(7)]),
        ))
        .unwrap();
        assert_eq!(out, vec![RawBson::Document(rawdoc! { "x": 7 })]);
    }
}
