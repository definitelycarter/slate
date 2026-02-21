use std::collections::HashSet;

use bson::raw::{RawBson, RawBsonRef};

use crate::error::DbError;
use crate::executor::exec;
use crate::executor::{RawIter, RawValue};
use crate::executor_v2::field_tree::{FieldTree, walk};

pub(crate) fn execute<'a>(field: &'a str, source: RawIter<'a>) -> Result<RawIter<'a>, DbError> {
    let paths = vec![field.to_string()];
    let tree = FieldTree::from_paths(&paths);

    let mut seen = HashSet::new();
    let mut buf = bson::RawArrayBuf::new();

    for result in source {
        let opt_val = result?;
        let val = match opt_val {
            Some(v) => v,
            None => continue,
        };
        let raw = val
            .as_document()
            .ok_or_else(|| DbError::InvalidQuery("expected document".into()))?;

        walk(raw, &tree, |_path, raw_ref| {
            if !matches!(raw_ref, RawBsonRef::Null) {
                exec::try_insert(&mut seen, &mut buf, raw_ref);
            }
        });
    }

    Ok(Box::new(std::iter::once(Ok(Some(RawValue::Owned(
        RawBson::Array(buf),
    ))))))
}
