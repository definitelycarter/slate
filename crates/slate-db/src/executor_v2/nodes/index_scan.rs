use bson::RawBson;
use slate_query::SortDirection;
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    column: &str,
    value: Option<&bson::Bson>,
    direction: SortDirection,
    limit: Option<usize>,
    complete_groups: bool,
) -> Result<RawIter<'a>, DbError> {
    let prefix = match value {
        Some(v) => encoding::index_scan_prefix(column, v),
        None => encoding::index_scan_field_prefix(column),
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
                Ok((key, _stored_value)) => {
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
                            return Some(Ok(Some(RawValue::Owned(RawBson::String(
                                record_id.to_string(),
                            )))));
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
