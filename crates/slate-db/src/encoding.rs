//! Key encoding for record storage.
//!
//! Data layout: `d:{record_id}` → serialized RecordValue (all columns in one value)
//! Index layout: `i:{column}\x00{value_bytes}\x00{record_id}` → [] (empty value)

const DATA_PREFIX: &[u8] = b"d:";
const SEP: u8 = 0x00;

/// Build a record key: `d:{record_id}`
pub fn record_key(record_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(DATA_PREFIX.len() + record_id.len());
    key.extend_from_slice(DATA_PREFIX);
    key.extend_from_slice(record_id.as_bytes());
    key
}

/// Parse a record key back into record_id.
///
/// Expected format: `d:{record_id}`
pub fn parse_record_key(key: &[u8]) -> Option<&str> {
    if !key.starts_with(DATA_PREFIX) {
        return None;
    }
    std::str::from_utf8(&key[DATA_PREFIX.len()..]).ok()
}

/// Build a prefix for scanning records by key prefix: `d:{prefix}`
pub fn data_scan_prefix(prefix: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(DATA_PREFIX.len() + prefix.len());
    key.extend_from_slice(DATA_PREFIX);
    key.extend_from_slice(prefix.as_bytes());
    key
}

// ── Index key encoding ──────────────────────────────────────────
//
// Layout: `i:{column}\x00{value_bytes}\x00{record_id}` → [] (empty value)
//
// - Sorted by column, then value, then record_id
// - Scanning `i:{column}\x00{value_bytes}\x00` gives all record IDs with that exact value
// - value_bytes = canonical byte encoding of the Bson value

const INDEX_PREFIX: &[u8] = b"i:";

/// Encode a Bson value into canonical bytes for index key ordering.
///
/// The encoding must sort correctly under lexicographic byte comparison.
pub fn encode_value(value: &bson::Bson) -> Vec<u8> {
    match value {
        bson::Bson::String(s) => s.as_bytes().to_vec(),
        bson::Bson::Int32(i) => {
            let wide = *i as i64;
            let unsigned = (wide as u64) ^ (1u64 << 63);
            unsigned.to_be_bytes().to_vec()
        }
        bson::Bson::Int64(i) => {
            let unsigned = (*i as u64) ^ (1u64 << 63);
            unsigned.to_be_bytes().to_vec()
        }
        bson::Bson::Double(f) => {
            let bits = f.to_bits();
            // IEEE 754: flip all bits if negative, else flip sign bit only
            let sortable = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            sortable.to_be_bytes().to_vec()
        }
        bson::Bson::Boolean(b) => vec![*b as u8],
        bson::Bson::DateTime(dt) => {
            let millis = dt.timestamp_millis();
            let unsigned = (millis as u64) ^ (1u64 << 63);
            unsigned.to_be_bytes().to_vec()
        }
        _ => vec![], // Array/Document/Null not indexable
    }
}

pub fn encode_raw_value(value: bson::raw::RawBsonRef) -> Vec<u8> {
    match value {
        bson::raw::RawBsonRef::String(s) => s.as_bytes().to_vec(),
        bson::raw::RawBsonRef::Int32(i) => {
            let wide = i as i64;
            let unsigned = (wide as u64) ^ (1u64 << 63);
            unsigned.to_be_bytes().to_vec()
        }
        bson::raw::RawBsonRef::Int64(i) => {
            let unsigned = (i as u64) ^ (1u64 << 63);
            unsigned.to_be_bytes().to_vec()
        }
        bson::raw::RawBsonRef::Double(f) => {
            let bits = f.to_bits();
            let sortable = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            sortable.to_be_bytes().to_vec()
        }
        bson::raw::RawBsonRef::Boolean(b) => vec![b as u8],
        bson::raw::RawBsonRef::DateTime(dt) => {
            let millis = dt.timestamp_millis();
            let unsigned = (millis as u64) ^ (1u64 << 63);
            unsigned.to_be_bytes().to_vec()
        }
        _ => vec![],
    }
}

/// Return the BSON element type byte for a `Bson` value.
pub fn bson_type_byte(value: &bson::Bson) -> [u8; 1] {
    [value.element_type() as u8]
}

/// Return the BSON element type byte for a `RawBsonRef` value.
pub fn raw_bson_type_byte(value: bson::raw::RawBsonRef) -> [u8; 1] {
    [value.element_type() as u8]
}

/// Coerce a `RawBson` value to match the BSON type stored in the index entry.
///
/// Index-covered projections carry the query's value (e.g. `Int64(100)`), but the
/// stored document may have used a different numeric type (e.g. `Int32(100)`).
/// The stored type byte (from the BSON spec) lets us reconstruct the correct type.
/// Falls back to cloning the query value if the stored value is empty (backward
/// compat with old index entries) or if no coercion is needed.
pub fn coerce_to_stored_type(
    query_val: &bson::raw::RawBson,
    stored_value: &[u8],
) -> bson::raw::RawBson {
    use bson::raw::RawBson;

    if stored_value.is_empty() {
        return query_val.clone();
    }
    let stored_type = stored_value[0];
    let query_type = query_val.element_type() as u8;
    if stored_type == query_type {
        return query_val.clone();
    }
    match (query_val, stored_type) {
        (RawBson::Int64(n), 0x10) => RawBson::Int32(*n as i32), // stored Int32
        (RawBson::Int32(n), 0x12) => RawBson::Int64(*n as i64), // stored Int64
        _ => query_val.clone(),
    }
}

pub fn raw_index_key(column: &str, value: bson::raw::RawBsonRef, record_id: &str) -> Vec<u8> {
    let value_bytes = encode_raw_value(value);
    let mut key = Vec::with_capacity(
        INDEX_PREFIX.len() + column.len() + 1 + value_bytes.len() + 1 + record_id.len(),
    );
    key.extend_from_slice(INDEX_PREFIX);
    key.extend_from_slice(column.as_bytes());
    key.push(SEP);
    key.extend_from_slice(&value_bytes);
    key.push(SEP);
    key.extend_from_slice(record_id.as_bytes());
    key
}

/// Build an index key: `i:{column}\x00{value_bytes}\x00{record_id}`
pub fn index_key(column: &str, value: &bson::Bson, record_id: &str) -> Vec<u8> {
    let value_bytes = encode_value(value);
    let mut key = Vec::with_capacity(
        INDEX_PREFIX.len() + column.len() + 1 + value_bytes.len() + 1 + record_id.len(),
    );
    key.extend_from_slice(INDEX_PREFIX);
    key.extend_from_slice(column.as_bytes());
    key.push(SEP);
    key.extend_from_slice(&value_bytes);
    key.push(SEP);
    key.extend_from_slice(record_id.as_bytes());
    key
}

/// Build a prefix to scan all record IDs for a column+value: `i:{column}\x00{value_bytes}\x00`
pub fn index_scan_prefix(column: &str, value: &bson::Bson) -> Vec<u8> {
    let value_bytes = encode_value(value);
    let mut key = Vec::with_capacity(INDEX_PREFIX.len() + column.len() + 1 + value_bytes.len() + 1);
    key.extend_from_slice(INDEX_PREFIX);
    key.extend_from_slice(column.as_bytes());
    key.push(SEP);
    key.extend_from_slice(&value_bytes);
    key.push(SEP);
    key
}

/// Build a prefix to scan all index entries for a given column: `i:{column}\x00`
pub fn index_scan_field_prefix(column: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(INDEX_PREFIX.len() + column.len() + 1);
    key.extend_from_slice(INDEX_PREFIX);
    key.extend_from_slice(column.as_bytes());
    key.push(SEP);
    key
}

/// Extract the value prefix from an index key: `i:{column}\x00{value_bytes}\x00`.
/// Two index keys with the same field value share this prefix.
/// Returns the byte slice up to and including the second separator.
pub fn index_key_value_prefix(key: &[u8]) -> Option<&[u8]> {
    if !key.starts_with(INDEX_PREFIX) {
        return None;
    }
    let rest = &key[INDEX_PREFIX.len()..];
    let first_sep = rest.iter().position(|&b| b == SEP)?;
    let after_first = &rest[first_sep + 1..];
    let second_sep = after_first.iter().rposition(|&b| b == SEP)?;
    // Include everything up to and including the second separator
    let prefix_len = INDEX_PREFIX.len() + first_sep + 1 + second_sep + 1;
    Some(&key[..prefix_len])
}

/// Parse an index key back into (column, record_id).
///
/// Expected format: `i:{column}\x00{value_bytes}\x00{record_id}`
/// We don't decode value_bytes — the caller already knows the value they scanned for.
pub fn parse_index_key(key: &[u8]) -> Option<(&str, &str)> {
    if !key.starts_with(INDEX_PREFIX) {
        return None;
    }
    let rest = &key[INDEX_PREFIX.len()..];

    // Find first \x00 — separates column from value_bytes
    let first_sep = rest.iter().position(|&b| b == SEP)?;
    let column = std::str::from_utf8(&rest[..first_sep]).ok()?;

    let after_first = &rest[first_sep + 1..];

    // Find last \x00 — separates value_bytes from record_id.
    // Must search from the right because binary value encodings can contain \x00.
    let second_sep = after_first.iter().rposition(|&b| b == SEP)?;
    let record_id = std::str::from_utf8(&after_first[second_sep + 1..]).ok()?;

    Some((column, record_id))
}

/// Encode a DateTime's millisecond timestamp into sortable bytes.
/// Same encoding as `encode_value` for `Bson::DateTime`.
pub fn encode_datetime_millis(millis: i64) -> [u8; 8] {
    let unsigned = (millis as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_key_roundtrip() {
        let key = record_key("bench:acct-123");
        let id = parse_record_key(&key).unwrap();
        assert_eq!(id, "bench:acct-123");
    }

    #[test]
    fn data_scan_prefix_matches_record_keys() {
        let prefix = data_scan_prefix("user1:bench:");
        let k1 = record_key("user1:bench:acct-1");
        let k2 = record_key("user2:bench:acct-1");
        assert!(k1.starts_with(&prefix));
        assert!(!k2.starts_with(&prefix));
    }

    #[test]
    fn index_key_roundtrip() {
        let key = index_key("status", &bson::Bson::String("active".into()), "ds1:rec-1");
        let (column, record_id) = parse_index_key(&key).unwrap();
        assert_eq!(column, "status");
        assert_eq!(record_id, "ds1:rec-1");
    }

    #[test]
    fn index_scan_prefix_matches_keys() {
        let prefix = index_scan_prefix("status", &bson::Bson::String("active".into()));
        let k1 = index_key("status", &bson::Bson::String("active".into()), "rec-1");
        let k2 = index_key("status", &bson::Bson::String("active".into()), "rec-2");
        let k3 = index_key("status", &bson::Bson::String("rejected".into()), "rec-1");
        let k4 = index_key("name", &bson::Bson::String("active".into()), "rec-1");
        assert!(k1.starts_with(&prefix));
        assert!(k2.starts_with(&prefix));
        assert!(!k3.starts_with(&prefix)); // different value
        assert!(!k4.starts_with(&prefix)); // different column
    }

    #[test]
    fn index_int_sort_order() {
        let k1 = index_key("score", &bson::Bson::Int64(10), "rec-1");
        let k2 = index_key("score", &bson::Bson::Int64(20), "rec-1");
        let k3 = index_key("score", &bson::Bson::Int64(-5), "rec-1");
        assert!(k3 < k1); // -5 < 10
        assert!(k1 < k2); // 10 < 20
    }

    #[test]
    fn bson_type_byte_values() {
        assert_eq!(bson_type_byte(&bson::Bson::Double(1.0)), [0x01]);
        assert_eq!(bson_type_byte(&bson::Bson::String("x".into())), [0x02]);
        assert_eq!(bson_type_byte(&bson::Bson::Boolean(true)), [0x08]);
        assert_eq!(
            bson_type_byte(&bson::Bson::DateTime(bson::DateTime::now())),
            [0x09]
        );
        assert_eq!(bson_type_byte(&bson::Bson::Int32(1)), [0x10]);
        assert_eq!(bson_type_byte(&bson::Bson::Int64(1)), [0x12]);
    }
}
