use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;
use crate::expression::LogicalOp;
use bson::RawBson;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

fn id_hash(v: &RawBson, pk_path: &str) -> Option<u64> {
    match v {
        RawBson::Document(doc) => doc.get(pk_path).ok().flatten().map(exec::hash_raw),
        other => Some(hash_owned(other)),
    }
}

fn hash_owned(v: &RawBson) -> u64 {
    let mut h = std::hash::DefaultHasher::new();
    match v {
        RawBson::String(s) => {
            0u8.hash(&mut h);
            s.hash(&mut h);
        }
        RawBson::Int32(i) => {
            1u8.hash(&mut h);
            i.hash(&mut h);
        }
        RawBson::Int64(i) => {
            2u8.hash(&mut h);
            i.hash(&mut h);
        }
        RawBson::ObjectId(oid) => {
            6u8.hash(&mut h);
            oid.bytes().hash(&mut h);
        }
        _ => {
            255u8.hash(&mut h);
        }
    }
    h.finish()
}

pub(crate) fn execute<'a>(
    pk_path: &str,
    logical: LogicalOp,
    left_source: RawIter<'a>,
    right_source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let pk_path = pk_path.to_string();
    let merged = match logical {
        LogicalOp::Or => {
            let left: Vec<Option<RawBson>> = left_source.collect::<Result<_, _>>()?;
            let right: Vec<Option<RawBson>> = right_source.collect::<Result<_, _>>()?;

            let mut seen = HashSet::with_capacity(left.len() + right.len());
            let mut result = Vec::with_capacity(left.len() + right.len());
            for val in left.into_iter().chain(right) {
                if let Some(ref v) = val
                    && let Some(id) = id_hash(v, &pk_path)
                    && !seen.insert(id)
                {
                    continue;
                }
                result.push(val);
            }
            result
        }
        LogicalOp::And => {
            // Build ID set from right side.
            let mut right_set = HashSet::new();
            for result in right_source {
                if let Some(val) = result?
                    && let Some(id) = id_hash(&val, &pk_path)
                {
                    right_set.insert(id);
                }
            }

            // Keep only left docs whose ID is in right_set.
            left_source
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|val| {
                    val.as_ref()
                        .and_then(|v| id_hash(v, &pk_path))
                        .map(|id| right_set.contains(&id))
                        .unwrap_or(false)
                })
                .collect()
        }
    };

    Ok(Box::new(merged.into_iter().map(Ok)))
}
