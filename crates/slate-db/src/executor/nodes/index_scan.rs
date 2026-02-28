use bson::RawBson;
use bson::raw::RawDocumentBuf;
use slate_engine::{CollectionHandle, EngineTransaction, IndexRange};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::planner::plan::{IndexScanRange, ScanDirection};

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

/// Extract a numeric value as i64 from a RawBson for cross-type comparison.
fn raw_bson_as_i64(val: &RawBson) -> Option<i64> {
    match val {
        RawBson::Int32(n) => Some(*n as i64),
        RawBson::Int64(n) => Some(*n),
        _ => None,
    }
}

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    field: String,
    range: &IndexScanRange,
    direction: ScanDirection,
    limit: Option<usize>,
    covered: bool,
) -> Result<RawIter<'a>, DbError> {
    // For Eq with numeric types (Int32/Int64), use a full field scan with post-filter
    // to handle cross-type matching (e.g. query Int64(100) matching stored Int32(100)).
    let is_numeric_eq = matches!(
        range,
        IndexScanRange::Eq(bson::Bson::Int32(_) | bson::Bson::Int64(_))
    );

    let engine_range = match range {
        IndexScanRange::Full => IndexRange::Full,
        IndexScanRange::Eq(_) if is_numeric_eq => IndexRange::Full,
        IndexScanRange::Eq(v) => IndexRange::Eq(v),
        IndexScanRange::Range { lower, upper } => IndexRange::Range {
            lower: lower.as_ref().map(|(v, incl)| (v, *incl)),
            upper: upper.as_ref().map(|(v, incl)| (v, *incl)),
        },
    };

    let reverse = matches!(direction, ScanDirection::Reverse);

    // Pre-convert the query value once for covered projections (Eq only).
    let raw_value = if covered {
        if let IndexScanRange::Eq(v) = range {
            bson_to_raw_bson(v)
        } else {
            None
        }
    } else {
        None
    };

    // For numeric Eq, extract the query value as i64 for cross-type comparison.
    let numeric_eq_value: Option<i64> = if is_numeric_eq {
        match range {
            IndexScanRange::Eq(bson::Bson::Int32(n)) => Some(*n as i64),
            IndexScanRange::Eq(bson::Bson::Int64(n)) => Some(*n),
            _ => None,
        }
    } else {
        None
    };

    let field_cstr = bson::raw::CString::try_from(field.as_str())
        .map_err(|e| DbError::InvalidQuery(format!("invalid field name: {e}")))?;
    let mut iter = txn.scan_index(&handle, &field, engine_range, reverse)?;
    let mut count = 0usize;
    let mut done = false;

    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }

        for result in iter.by_ref() {
            match result {
                Ok(entry) => {
                    // Numeric Eq post-filter: compare as i64 to handle Int32/Int64 cross-matching
                    if let Some(query_val) = numeric_eq_value {
                        let entry_val = raw_bson_as_i64(&entry.value);
                        if entry_val != Some(query_val) {
                            continue;
                        }
                    }

                    // Limit logic
                    if let Some(n) = limit
                        && count >= n
                    {
                        done = true;
                        return None;
                    }

                    count += 1;

                    if let Some(ref rv) = raw_value {
                        // Build a minimal document with _id + the indexed field.
                        // Coerce the query value to the type stored in the index.
                        let coerced = coerce_to_stored_type(rv, &entry.value);
                        let mut doc = RawDocumentBuf::new();
                        doc.append(bson::cstr!("_id"), entry.doc_id);
                        doc.append(&field_cstr, coerced);
                        return Some(Ok(Some(RawBson::Document(doc))));
                    } else {
                        // Standard path: yield bare ID for ReadRecord
                        return Some(Ok(Some(entry.doc_id)));
                    }
                }
                Err(e) => {
                    done = true;
                    return Some(Err(DbError::from(e)));
                }
            }
        }

        done = true;
        None
    })))
}

/// Coerce a query value to match the stored type in the index.
///
/// If the query value (e.g. Int64) differs from the stored value type (e.g. Int32),
/// we coerce to avoid type mismatches in the covered output.
fn coerce_to_stored_type(query_val: &RawBson, stored_val: &RawBson) -> RawBson {
    match (stored_val, query_val) {
        // Stored Int32, query Int64 → coerce to Int32
        (RawBson::Int32(_), RawBson::Int64(n)) => RawBson::Int32(*n as i32),
        // Stored Int64, query Int32 → coerce to Int64
        (RawBson::Int64(_), RawBson::Int32(n)) => RawBson::Int64(*n as i64),
        _ => query_val.clone(),
    }
}
