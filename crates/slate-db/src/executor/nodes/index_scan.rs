use bson::RawBson;
use bson::raw::RawDocumentBuf;
use slate_query::SortDirection;
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::{RawIter, RawValue};
use crate::planner::Expression;

/// Pre-encoded range bounds for byte-level filtering inside the iterator.
struct RangeBounds {
    lower: Option<Vec<u8>>,
    lower_inclusive: bool,
    upper: Option<Vec<u8>>,
    upper_inclusive: bool,
}

/// Convert an owned `Bson` to `RawBson` for covered projection output.
fn bson_to_raw_bson(val: &bson::Bson) -> Option<RawBson> {
    Some(match val {
        bson::Bson::String(s) => RawBson::String(s.clone()),
        bson::Bson::Int32(i) => RawBson::Int32(*i),
        bson::Bson::Int64(i) => RawBson::Int64(*i),
        bson::Bson::Double(f) => RawBson::Double(*f),
        bson::Bson::Boolean(b) => RawBson::Boolean(*b),
        bson::Bson::DateTime(dt) => RawBson::DateTime(*dt),
        _ => return None,
    })
}

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    column: String,
    filter: Option<&Expression>,
    direction: SortDirection,
    limit: Option<usize>,
    complete_groups: bool,
    covered: bool,
    now_millis: i64,
) -> Result<RawIter<'a>, DbError> {
    // Eq uses a narrow prefix; everything else scans the whole column.
    let prefix = match filter {
        Some(Expression::Eq(_, v)) => encoding::index_scan_prefix(&column, v),
        _ => encoding::index_scan_field_prefix(&column),
    };

    // Build range bounds only when needed. Boxed so the None case adds only
    // 8 bytes to the closure (pointer) instead of 56 bytes (inlined struct),
    // keeping the Eq/None hot path cache-friendly.
    let range_bounds: Option<Box<RangeBounds>> = match filter {
        Some(Expression::Gt(_, v)) => Some(Box::new(RangeBounds {
            lower: Some(encoding::encode_value(v)),
            lower_inclusive: false,
            upper: None,
            upper_inclusive: false,
        })),
        Some(Expression::Gte(_, v)) => Some(Box::new(RangeBounds {
            lower: Some(encoding::encode_value(v)),
            lower_inclusive: true,
            upper: None,
            upper_inclusive: false,
        })),
        Some(Expression::Lt(_, v)) => Some(Box::new(RangeBounds {
            lower: None,
            lower_inclusive: false,
            upper: Some(encoding::encode_value(v)),
            upper_inclusive: false,
        })),
        Some(Expression::Lte(_, v)) => Some(Box::new(RangeBounds {
            lower: None,
            lower_inclusive: false,
            upper: Some(encoding::encode_value(v)),
            upper_inclusive: true,
        })),
        // Two-sided range: And([Gte/Gt(_, lo), Lt/Lte(_, hi)])
        Some(Expression::And(children)) if children.len() == 2 => {
            let mut lower = None;
            let mut lower_inclusive = false;
            let mut upper = None;
            let mut upper_inclusive = false;
            for child in children {
                match child {
                    Expression::Gt(_, v) => {
                        lower = Some(encoding::encode_value(v));
                        lower_inclusive = false;
                    }
                    Expression::Gte(_, v) => {
                        lower = Some(encoding::encode_value(v));
                        lower_inclusive = true;
                    }
                    Expression::Lt(_, v) => {
                        upper = Some(encoding::encode_value(v));
                        upper_inclusive = false;
                    }
                    Expression::Lte(_, v) => {
                        upper = Some(encoding::encode_value(v));
                        upper_inclusive = true;
                    }
                    _ => {}
                }
            }
            if lower.is_some() || upper.is_some() {
                Some(Box::new(RangeBounds {
                    lower,
                    lower_inclusive,
                    upper,
                    upper_inclusive,
                }))
            } else {
                None
            }
        }
        _ => None, // Eq and None — no range filtering
    };

    // Pre-convert the query value once for covered projections (Eq only).
    let raw_value = if covered {
        if let Some(Expression::Eq(_, v)) = filter {
            bson_to_raw_bson(v)
        } else {
            None
        }
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
                    // Range filtering — only entered when range_bounds is Some.
                    if let Some(ref rb) = range_bounds {
                        if let Some(val_bytes) = encoding::index_key_value_bytes(&key) {
                            match direction {
                                SortDirection::Asc => {
                                    if let Some(ref lb) = rb.lower {
                                        let cmp = val_bytes.cmp(lb.as_slice());
                                        if cmp == std::cmp::Ordering::Less
                                            || (cmp == std::cmp::Ordering::Equal
                                                && !rb.lower_inclusive)
                                        {
                                            continue;
                                        }
                                    }
                                    if let Some(ref ub) = rb.upper {
                                        let cmp = val_bytes.cmp(ub.as_slice());
                                        if cmp == std::cmp::Ordering::Greater
                                            || (cmp == std::cmp::Ordering::Equal
                                                && !rb.upper_inclusive)
                                        {
                                            done = true;
                                            return None;
                                        }
                                    }
                                }
                                SortDirection::Desc => {
                                    if let Some(ref ub) = rb.upper {
                                        let cmp = val_bytes.cmp(ub.as_slice());
                                        if cmp == std::cmp::Ordering::Greater
                                            || (cmp == std::cmp::Ordering::Equal
                                                && !rb.upper_inclusive)
                                        {
                                            continue;
                                        }
                                    }
                                    if let Some(ref lb) = rb.lower {
                                        let cmp = val_bytes.cmp(lb.as_slice());
                                        if cmp == std::cmp::Ordering::Less
                                            || (cmp == std::cmp::Ordering::Equal
                                                && !rb.lower_inclusive)
                                        {
                                            done = true;
                                            return None;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Limit + complete_groups logic (runs after range filtering).
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
                                // Covered path: O(1) TTL check from inline millis
                                // in the index entry value — no extra txn.get needed.
                                if encoding::is_index_entry_expired(&stored_value, now_millis) {
                                    continue;
                                }
                                let coerced = encoding::coerce_to_stored_type(rv, &stored_value);
                                let mut doc = RawDocumentBuf::new();
                                doc.append("_id", RawBson::String(record_id.to_string()));
                                doc.append(&column, coerced);
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
