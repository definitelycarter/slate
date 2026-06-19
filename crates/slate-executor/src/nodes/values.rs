//! The `Values` literal source — streams caller-provided values in order.
//!
//! The general form of v1's document-only `Values`: elements are arbitrary raw
//! values, not necessarily documents.

use bson::RawBson;

use crate::ValueIter;

/// Stream the provided values in order, each as a defined value.
pub(crate) fn execute(values: Vec<RawBson>) -> ValueIter<'static> {
    Box::new(values.into_iter().map(|v| Ok(Some(v))))
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use bson::{RawBson, rawdoc};

    #[test]
    fn streams_in_order() {
        let out = collect(execute(vec![
            RawBson::Int32(1),
            RawBson::String("a".into()),
            RawBson::Boolean(true),
        ]))
        .unwrap();
        assert_eq!(
            out,
            vec![
                RawBson::Int32(1),
                RawBson::String("a".into()),
                RawBson::Boolean(true),
            ]
        );
    }

    #[test]
    fn empty_yields_nothing() {
        assert!(collect(execute(vec![])).unwrap().is_empty());
    }

    #[test]
    fn document_is_just_a_value() {
        // A "document" is the case where the flowing value happens to be a doc.
        let doc = RawBson::Document(rawdoc! { "name": "ada" });
        assert_eq!(collect(execute(vec![doc.clone()])).unwrap(), vec![doc]);
    }
}
