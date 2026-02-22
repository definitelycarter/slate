use bson::RawBson;
use bson::raw::RawDocumentBuf;
use slate_query::SortDirection;
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    column: &'a str,
    value: Option<&bson::Bson>,
    direction: SortDirection,
    limit: Option<usize>,
    complete_groups: bool,
    covered: bool,
) -> Result<RawIter<'a>, DbError> {
    let prefix = match value {
        Some(v) => encoding::index_scan_prefix(column, v),
        None => encoding::index_scan_field_prefix(column),
    };

    // Pre-convert the query value once for covered projections.
    let raw_value = if covered {
        value.map(|v| RawBson::try_from(v.clone()).unwrap_or(RawBson::Null))
    } else {
        None
    };

    let mut iter = match direction {
        SortDirection::Asc => txn.scan_prefix(cf, &prefix)?,
        SortDirection::Desc => txn.scan_prefix_rev(cf, &prefix)?,
    };

    let mut count = 0usize;
    let mut boundary_prefix: Option<Vec<u8>> = None;
    let mut done = false;

    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }

        for result in iter.by_ref() {
            match result {
                Ok((key, stored_value)) => {
                    if let Some(n) = limit {
                        if count >= n {
                            if complete_groups {
                                let val_prefix = encoding::index_key_value_prefix(&key);
                                let changed = match (&boundary_prefix, val_prefix) {
                                    (Some(prev), Some(cur)) => prev.as_slice() != cur,
                                    _ => false,
                                };
                                if changed {
                                    done = true;
                                    return None;
                                }
                                if boundary_prefix.is_none() {
                                    boundary_prefix = val_prefix.map(|p| p.to_vec());
                                }
                            } else {
                                done = true;
                                return None;
                            }
                        }
                    }

                    match encoding::parse_index_key(&key) {
                        Some((_, record_id)) => {
                            count += 1;
                            if let Some(ref rv) = raw_value {
                                // Covered path: yield { _id, column: coerced_value }
                                let coerced = encoding::coerce_to_stored_type(rv, &stored_value);
                                let mut doc = RawDocumentBuf::new();
                                doc.append("_id", RawBson::String(record_id.to_string()));
                                doc.append(column, coerced);
                                return Some(Ok(Some(RawValue::Owned(RawBson::Document(doc)))));
                            } else {
                                // Standard path: yield bare ID string
                                return Some(Ok(Some(RawValue::Owned(RawBson::String(
                                    record_id.to_string(),
                                )))));
                            }
                        }
                        None => continue,
                    }
                }
                Err(e) => {
                    done = true;
                    return Some(Err(DbError::Store(e)));
                }
            }
        }

        done = true;
        None
    })))
}
