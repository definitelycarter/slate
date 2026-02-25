use std::collections::HashSet;

use bson::raw::{RawBson, RawBsonRef};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;
use crate::executor::field_tree::{FieldTree, walk};

pub(crate) fn execute<'a>(field: String, source: RawIter<'a>) -> Result<RawIter<'a>, DbError> {
    let paths = vec![field];
    let tree = FieldTree::from_paths(&paths);

    let mut seen = HashSet::new();
    let mut buf = bson::RawArrayBuf::new();

    for result in source {
        let opt_val = result?;
        let val = match opt_val {
            Some(v) => v,
            None => continue,
        };
        let raw = match &val {
            RawBson::Document(d) => d.as_ref(),
            _ => return Err(DbError::InvalidQuery("expected document".into())),
        };

        walk(raw, &tree, |_path, raw_ref| {
            if !matches!(raw_ref, RawBsonRef::Null) {
                exec::try_insert(&mut seen, &mut buf, raw_ref);
            }
        });
    }

    Ok(Box::new(std::iter::once(Ok(Some(RawBson::Array(buf))))))
}
