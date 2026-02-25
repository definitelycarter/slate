use bson::RawBson;
use bson::raw::RawDocumentBuf;
use slate_engine::{BsonValue, CollectionHandle, EngineTransaction, IndexRange};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::planner::plan::{IndexScanRange, ScanDirection};

/// Convert a `bson::Bson` value to its encoded bytes via `BsonValue`.
fn bson_to_value_bytes(val: &bson::Bson) -> Option<Vec<u8>> {
    BsonValue::from_bson(val).map(|bv| bv.to_vec())
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

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    field: String,
    range: &IndexScanRange,
    direction: ScanDirection,
    limit: Option<usize>,
    covered: bool,
    now_millis: i64,
) -> Result<RawIter<'a>, DbError> {
    // Pre-encode range values for the engine's IndexRange.
    let eq_bytes = match range {
        IndexScanRange::Eq(v) => bson_to_value_bytes(v),
        _ => None,
    };
    let lower_bytes = match range {
        IndexScanRange::Range {
            lower: Some((v, _)),
            ..
        } => bson_to_value_bytes(v),
        _ => None,
    };
    let upper_bytes = match range {
        IndexScanRange::Range {
            upper: Some((v, _)),
            ..
        } => bson_to_value_bytes(v),
        _ => None,
    };

    // For Eq with numeric types (Int32/Int64), use a full field scan with post-filter
    // to handle cross-type matching (e.g. query Int64(100) matching stored Int32(100)).
    let is_numeric_eq = matches!(
        range,
        IndexScanRange::Eq(bson::Bson::Int32(_) | bson::Bson::Int64(_))
    );

    let engine_range = match range {
        IndexScanRange::Full => IndexRange::Full,
        IndexScanRange::Eq(_) if is_numeric_eq => IndexRange::Full,
        IndexScanRange::Eq(_) => match &eq_bytes {
            Some(b) => IndexRange::Eq(b),
            None => IndexRange::Full,
        },
        IndexScanRange::Range { lower, upper } => IndexRange::Range {
            lower: lower_bytes.as_deref(),
            lower_inclusive: lower.as_ref().map_or(false, |(_, incl)| *incl),
            upper: upper_bytes.as_deref(),
            upper_inclusive: upper.as_ref().map_or(false, |(_, incl)| *incl),
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

    let mut iter = txn.scan_index(&handle, &field, engine_range, reverse, now_millis)?;
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
                        let entry_val = decode_index_value_as_i64(&entry.value_bytes);
                        if entry_val != Some(query_val) {
                            continue;
                        }
                    }

                    // Limit logic
                    if let Some(n) = limit {
                        if count >= n {
                            done = true;
                            return None;
                        }
                    }

                    count += 1;

                    if let Some(ref rv) = raw_value {
                        // Build a minimal document with _id + the indexed field.
                        // Coerce the query value to the type stored in the index.
                        let coerced = coerce_to_stored_type(rv, &entry.metadata);
                        let id_str = bson_value_to_string(&entry.doc_id);
                        let mut doc = RawDocumentBuf::new();
                        doc.append("_id", RawBson::String(id_str));
                        doc.append(&field, coerced);
                        return Some(Ok(Some(RawBson::Document(doc))));
                    } else {
                        // Standard path: yield bare ID string for KeyLookup
                        let id_str = bson_value_to_string(&entry.doc_id);
                        return Some(Ok(Some(RawBson::String(id_str))));
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

/// Convert a `BsonValue` doc_id to a string representation.
fn bson_value_to_string(bv: &BsonValue<'_>) -> String {
    match bv.tag {
        0x02 => {
            // String type: bytes are UTF-8
            String::from_utf8_lossy(&bv.bytes).to_string()
        }
        0x07 => {
            // ObjectId: 12 bytes → hex
            bson::oid::ObjectId::from_bytes(bv.bytes[..12].try_into().unwrap_or([0u8; 12])).to_hex()
        }
        _ => format!("{:?}", bv),
    }
}

/// Decode index value_bytes (sortable-encoded) as i64 for cross-type comparison.
///
/// Handles both Int32 (tag 0x10, 4 bytes) and Int64 (tag 0x12, 8 bytes).
fn decode_index_value_as_i64(value_bytes: &[u8]) -> Option<i64> {
    if value_bytes.is_empty() {
        return None;
    }
    match value_bytes[0] {
        0x10 if value_bytes.len() == 5 => {
            let b: [u8; 4] = value_bytes[1..5].try_into().ok()?;
            let n = (u32::from_be_bytes(b) ^ 0x8000_0000) as i32;
            Some(n as i64)
        }
        0x12 if value_bytes.len() == 9 => {
            let b: [u8; 8] = value_bytes[1..9].try_into().ok()?;
            let n = (u64::from_be_bytes(b) ^ 0x8000_0000_0000_0000) as i64;
            Some(n)
        }
        _ => None,
    }
}

/// Coerce a query value to match the stored type in the index metadata.
///
/// The metadata's first byte is the BSON type tag of the stored value.
/// If the query value (e.g. Int64) differs from the stored type (e.g. Int32),
/// we coerce to avoid type mismatches in the covered output.
fn coerce_to_stored_type(query_val: &RawBson, metadata: &[u8]) -> RawBson {
    if metadata.is_empty() {
        return query_val.clone();
    }
    let stored_type = metadata[0];
    match (stored_type, query_val) {
        // Int64 query → Int32 stored
        (0x10, RawBson::Int64(n)) => RawBson::Int32(*n as i32),
        // Int32 query → Int64 stored
        (0x12, RawBson::Int32(n)) => RawBson::Int64(*n as i64),
        _ => query_val.clone(),
    }
}
