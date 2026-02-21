use std::collections::HashSet;

use bson::RawBson;
use slate_query::LogicalOp;

use crate::error::DbError;
use crate::executor::exec;
use crate::executor::{RawIter, RawValue};

fn extract_id_from_raw_value<'a>(v: &'a RawValue<'a>) -> Option<&'a str> {
    match v {
        RawValue::Borrowed(bson::raw::RawBsonRef::String(s)) => Some(s),
        RawValue::Owned(RawBson::String(s)) => Some(s.as_str()),
        _ => v
            .as_document()
            .and_then(|d| exec::raw_extract_id(d).ok().flatten()),
    }
}

fn extract_id(v: &RawBson) -> Option<&str> {
    match v {
        RawBson::String(s) => Some(s.as_str()),
        RawBson::Document(doc) => exec::raw_extract_id(doc).ok().flatten(),
        _ => None,
    }
}

pub(crate) fn execute<'a>(
    logical: &LogicalOp,
    left_source: RawIter<'a>,
    right_source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let merged = match logical {
        LogicalOp::Or => {
            let left: Vec<Option<RawBson>> = left_source
                .map(|r| r.map(|opt| opt.and_then(RawValue::into_raw_bson)))
                .collect::<Result<_, _>>()?;
            let right: Vec<Option<RawBson>> = right_source
                .map(|r| r.map(|opt| opt.and_then(RawValue::into_raw_bson)))
                .collect::<Result<_, _>>()?;

            let mut seen = HashSet::with_capacity(left.len() + right.len());
            let mut result = Vec::with_capacity(left.len() + right.len());
            for val in left.into_iter().chain(right) {
                if let Some(ref v) = val {
                    if let Some(id_str) = extract_id(v) {
                        if !seen.insert(id_str.to_string()) {
                            continue;
                        }
                    }
                }
                result.push(val);
            }
            result
        }
        LogicalOp::And => {
            // Build ID set from right side without converting to owned RawBson.
            let mut right_set = HashSet::new();
            for result in right_source {
                if let Some(val) = result? {
                    if let Some(id) = extract_id_from_raw_value(&val) {
                        right_set.insert(id.to_string());
                    }
                }
            }

            // Materialize left side, keeping only docs whose ID is in right_set.
            left_source
                .map(|r| r.map(|opt| opt.and_then(RawValue::into_raw_bson)))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|val| {
                    val.as_ref()
                        .and_then(extract_id)
                        .map(|id| right_set.contains(id))
                        .unwrap_or(false)
                })
                .collect()
        }
    };

    Ok(Box::new(
        merged.into_iter().map(|val| Ok(val.map(RawValue::Owned))),
    ))
}
