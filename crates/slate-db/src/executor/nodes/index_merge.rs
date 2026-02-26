use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;
use crate::expression::LogicalOp;
use bson::RawBson;
use slate_engine::BsonValue;
use std::collections::HashSet;

fn extract_id(v: &RawBson) -> Option<BsonValue<'static>> {
    match v {
        RawBson::Document(doc) => exec::extract_doc_id(doc)
            .ok()
            .flatten()
            .map(|id| id.into_owned()),
        other => BsonValue::from_raw_bson(other),
    }
}

pub(crate) fn execute<'a>(
    logical: LogicalOp,
    left_source: RawIter<'a>,
    right_source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let merged = match logical {
        LogicalOp::Or => {
            let left: Vec<Option<RawBson>> = left_source.collect::<Result<_, _>>()?;
            let right: Vec<Option<RawBson>> = right_source.collect::<Result<_, _>>()?;

            let mut seen = HashSet::with_capacity(left.len() + right.len());
            let mut result = Vec::with_capacity(left.len() + right.len());
            for val in left.into_iter().chain(right) {
                if let Some(ref v) = val {
                    if let Some(id) = extract_id(v) {
                        if !seen.insert(id) {
                            continue;
                        }
                    }
                }
                result.push(val);
            }
            result
        }
        LogicalOp::And => {
            // Build ID set from right side.
            let mut right_set = HashSet::new();
            for result in right_source {
                if let Some(val) = result? {
                    if let Some(id) = extract_id(&val) {
                        right_set.insert(id);
                    }
                }
            }

            // Keep only left docs whose ID is in right_set.
            left_source
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|val| {
                    val.as_ref()
                        .and_then(|v| extract_id(v))
                        .map(|id| right_set.contains(&id))
                        .unwrap_or(false)
                })
                .collect()
        }
    };

    Ok(Box::new(merged.into_iter().map(Ok)))
}
