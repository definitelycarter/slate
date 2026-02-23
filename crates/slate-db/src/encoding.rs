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

/// Return the BSON element type byte for a `RawBsonRef` value.
pub fn raw_bson_ref_type_byte(value: bson::raw::RawBsonRef) -> [u8; 1] {
    [value.element_type() as u8]
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
pub fn index_scan_prefix(column: &str, value: bson::raw::RawBsonRef) -> Vec<u8> {
    let value_bytes = encode_raw_value(value);
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

/// Extract just the value bytes from an index key.
///
/// Given `i:{column}\x00{value_bytes}\x00{record_id}`, returns `{value_bytes}`.
pub fn index_key_value_bytes(key: &[u8]) -> Option<&[u8]> {
    if !key.starts_with(INDEX_PREFIX) {
        return None;
    }
    let rest = &key[INDEX_PREFIX.len()..];
    let first_sep = rest.iter().position(|&b| b == SEP)?;
    let after_first = &rest[first_sep + 1..];
    let second_sep = after_first.iter().rposition(|&b| b == SEP)?;
    Some(&after_first[..second_sep])
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

// ── Record value envelope ────────────────────────────────────────
//
// Every record value is stored with a 1-byte tag prefix:
//   0x00 → no TTL, BSON follows immediately          [0x00][BSON...]
//   0x01 → has TTL, 8-byte LE millis then BSON       [0x01][i64 LE][BSON...]
//
// The tag byte makes the format extensible: future versions can add
// new tags (e.g. 0x02 for TTL + created_at timestamp) without breaking
// existing data.

const ENVELOPE_TAG_NO_TTL: u8 = 0x00;
const ENVELOPE_TAG_TTL: u8 = 0x01;
const ENVELOPE_TTL_SIZE: usize = 8;

/// Wrap raw BSON bytes in a record envelope.
///
/// Scans the BSON payload for a `ttl` DateTime field. If found, prepends
/// `[0x01][8-byte LE millis]`. Otherwise prepends `[0x00]`.
/// The BSON payload is included verbatim (TTL stays duplicated in BSON).
pub fn encode_record(bson_bytes: &[u8]) -> Vec<u8> {
    match extract_ttl_millis(bson_bytes) {
        Some(millis) => {
            let mut buf = Vec::with_capacity(1 + ENVELOPE_TTL_SIZE + bson_bytes.len());
            buf.push(ENVELOPE_TAG_TTL);
            buf.extend_from_slice(&millis.to_le_bytes());
            buf.extend_from_slice(bson_bytes);
            buf
        }
        None => {
            let mut buf = Vec::with_capacity(1 + bson_bytes.len());
            buf.push(ENVELOPE_TAG_NO_TTL);
            buf.extend_from_slice(bson_bytes);
            buf
        }
    }
}

/// Decode a record envelope, returning the TTL (if present) and a slice
/// of the raw BSON payload.
pub fn decode_record(data: &[u8]) -> Result<(Option<i64>, &[u8]), crate::error::DbError> {
    if data.is_empty() {
        return Err(crate::error::DbError::InvalidQuery(
            "empty record envelope".into(),
        ));
    }
    match data[0] {
        ENVELOPE_TAG_NO_TTL => Ok((None, &data[1..])),
        ENVELOPE_TAG_TTL => {
            let header_len = 1 + ENVELOPE_TTL_SIZE;
            if data.len() < header_len {
                return Err(crate::error::DbError::InvalidQuery(
                    "truncated TTL envelope".into(),
                ));
            }
            let millis = i64::from_le_bytes(data[1..1 + ENVELOPE_TTL_SIZE].try_into().unwrap());
            Ok((Some(millis), &data[header_len..]))
        }
        tag => Err(crate::error::DbError::InvalidQuery(format!(
            "unknown record envelope tag: 0x{:02X}",
            tag
        ))),
    }
}

/// Return the byte offset where the BSON payload begins in an enveloped record.
/// Useful when you need to slice a `Cow<[u8]>` without an intermediate borrow.
#[inline]
pub fn record_bson_offset(data: &[u8]) -> usize {
    if !data.is_empty() && data[0] == ENVELOPE_TAG_TTL {
        1 + ENVELOPE_TTL_SIZE
    } else {
        1
    }
}

/// Fast O(1) TTL check on an enveloped record.
/// Returns `true` if the record has a TTL that is less than `now_millis`.
#[inline]
pub fn is_record_expired(data: &[u8], now_millis: i64) -> bool {
    if data.len() >= 1 + ENVELOPE_TTL_SIZE && data[0] == ENVELOPE_TAG_TTL {
        let millis = i64::from_le_bytes(data[1..1 + ENVELOPE_TTL_SIZE].try_into().unwrap());
        millis < now_millis
    } else {
        false
    }
}

// ── Index entry value encoding ──────────────────────────────────
//
// Index entry values store a BSON type byte, optionally followed by
// 8-byte LE TTL millis when the document has a `ttl` field:
//
//   No TTL:    [type_byte]                         (1 byte)
//   With TTL:  [type_byte][8-byte LE i64 millis]   (9 bytes)

const INDEX_VALUE_TTL_OFFSET: usize = 1;
const INDEX_VALUE_TTL_LEN: usize = 8;
const INDEX_VALUE_WITH_TTL_SIZE: usize = INDEX_VALUE_TTL_OFFSET + INDEX_VALUE_TTL_LEN;

/// Encode an index entry value: `[type_byte]` or `[type_byte][8-byte LE millis]`.
pub fn encode_index_value(type_byte: u8, ttl_millis: Option<i64>) -> Vec<u8> {
    match ttl_millis {
        Some(millis) => {
            let mut buf = Vec::with_capacity(INDEX_VALUE_WITH_TTL_SIZE);
            buf.push(type_byte);
            buf.extend_from_slice(&millis.to_le_bytes());
            buf
        }
        None => vec![type_byte],
    }
}

/// Fast O(1) TTL check on an index entry value.
/// Returns `true` if the value has inline TTL millis and millis < `now_millis`.
#[inline]
pub fn is_index_entry_expired(value: &[u8], now_millis: i64) -> bool {
    if value.len() >= INDEX_VALUE_WITH_TTL_SIZE {
        let millis = i64::from_le_bytes(
            value[INDEX_VALUE_TTL_OFFSET..INDEX_VALUE_WITH_TTL_SIZE]
                .try_into()
                .unwrap(),
        );
        millis < now_millis
    } else {
        false
    }
}

/// Extract TTL millis from a `RawDocument` by looking up the `ttl` field.
/// Returns `Some(millis)` if the field exists and is a DateTime.
pub fn extract_ttl_millis_from_raw(raw: &bson::raw::RawDocument) -> Option<i64> {
    match raw.get("ttl") {
        Ok(Some(bson::raw::RawBsonRef::DateTime(dt))) => Some(dt.timestamp_millis()),
        _ => None,
    }
}

/// Scan raw BSON bytes for a `ttl` DateTime field and return its millis.
fn extract_ttl_millis(bytes: &[u8]) -> Option<i64> {
    let mut pos = 4; // skip BSON document length
    while pos < bytes.len() {
        let type_byte = bytes[pos];
        if type_byte == 0x00 {
            break;
        }
        pos += 1;

        let name_start = pos;
        while pos < bytes.len() && bytes[pos] != 0x00 {
            pos += 1;
        }
        let name = &bytes[name_start..pos];
        pos += 1; // skip null terminator

        if name == b"ttl" && type_byte == 0x09 {
            if pos + 8 <= bytes.len() {
                return Some(i64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()));
            }
            return None;
        }

        match type_byte {
            0x01 => pos += 8,
            0x02 => {
                if pos + 4 > bytes.len() {
                    break;
                }
                let len = i32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4 + len;
            }
            0x03 | 0x04 => {
                if pos + 4 > bytes.len() {
                    break;
                }
                let len = i32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                pos += len;
            }
            0x05 => {
                if pos + 4 > bytes.len() {
                    break;
                }
                let len = i32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 5 + len;
            }
            0x07 => pos += 12,
            0x08 => pos += 1,
            0x09 => pos += 8,
            0x0A => {}
            0x10 => pos += 4,
            0x11 => pos += 8,
            0x12 => pos += 8,
            0x13 => pos += 16,
            _ => break,
        }
    }
    None
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
        let prefix = index_scan_prefix("status", bson::raw::RawBsonRef::String("active"));
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
    fn index_key_value_bytes_roundtrip() {
        // Int64
        let key = index_key("score", &bson::Bson::Int64(42), "rec-1");
        let val_bytes = index_key_value_bytes(&key).unwrap();
        assert_eq!(val_bytes, encode_value(&bson::Bson::Int64(42)));

        // String
        let key = index_key("status", &bson::Bson::String("active".into()), "rec-1");
        let val_bytes = index_key_value_bytes(&key).unwrap();
        assert_eq!(
            val_bytes,
            encode_value(&bson::Bson::String("active".into()))
        );

        // Double
        let key = index_key("rating", &bson::Bson::Double(3.14), "rec-1");
        let val_bytes = index_key_value_bytes(&key).unwrap();
        assert_eq!(val_bytes, encode_value(&bson::Bson::Double(3.14)));

        // DateTime
        let dt = bson::DateTime::from_millis(1700000000000);
        let key = index_key("created", &bson::Bson::DateTime(dt), "rec-1");
        let val_bytes = index_key_value_bytes(&key).unwrap();
        assert_eq!(val_bytes, encode_value(&bson::Bson::DateTime(dt)));
    }

    #[test]
    fn index_key_value_bytes_rejects_non_index() {
        let key = record_key("rec-1");
        assert!(index_key_value_bytes(&key).is_none());
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

    // ── Record envelope ─────────────────────────────────────────

    #[test]
    fn encode_decode_no_ttl() {
        let bson = bson::to_vec(&bson::doc! { "_id": "a", "name": "test" }).unwrap();
        let encoded = encode_record(&bson);
        assert_eq!(encoded[0], ENVELOPE_TAG_NO_TTL);
        let (ttl, bson_slice) = decode_record(&encoded).unwrap();
        assert!(ttl.is_none());
        assert_eq!(bson_slice, &bson[..]);
    }

    #[test]
    fn encode_decode_with_ttl() {
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let bson = bson::to_vec(&bson::doc! { "_id": "a", "ttl": dt }).unwrap();
        let encoded = encode_record(&bson);
        assert_eq!(encoded[0], ENVELOPE_TAG_TTL);
        let (ttl, bson_slice) = decode_record(&encoded).unwrap();
        assert_eq!(ttl, Some(1_700_000_000_000));
        assert_eq!(bson_slice, &bson[..]);
    }

    #[test]
    fn is_record_expired_with_ttl() {
        let dt = bson::DateTime::from_millis(1_000);
        let bson = bson::to_vec(&bson::doc! { "_id": "a", "ttl": dt }).unwrap();
        let encoded = encode_record(&bson);
        assert!(is_record_expired(&encoded, 2_000));
        assert!(!is_record_expired(&encoded, 500));
    }

    #[test]
    fn is_record_expired_no_ttl() {
        let bson = bson::to_vec(&bson::doc! { "_id": "a", "name": "test" }).unwrap();
        let encoded = encode_record(&bson);
        assert!(!is_record_expired(&encoded, i64::MAX));
    }

    #[test]
    fn decode_record_empty_errors() {
        assert!(decode_record(&[]).is_err());
    }

    #[test]
    fn decode_record_unknown_tag_errors() {
        assert!(decode_record(&[0xFF, 0x00]).is_err());
    }

    // ── Index entry value encoding ──────────────────────────────

    #[test]
    fn encode_index_value_no_ttl() {
        let val = encode_index_value(0x02, None);
        assert_eq!(val, vec![0x02]);
    }

    #[test]
    fn encode_index_value_with_ttl() {
        let millis = 1_700_000_000_000_i64;
        let val = encode_index_value(0x02, Some(millis));
        assert_eq!(val.len(), 9);
        assert_eq!(val[0], 0x02);
        assert_eq!(i64::from_le_bytes(val[1..9].try_into().unwrap()), millis);
    }

    #[test]
    fn is_index_entry_expired_with_ttl() {
        let val = encode_index_value(0x02, Some(1_000));
        assert!(is_index_entry_expired(&val, 2_000));
        assert!(!is_index_entry_expired(&val, 500));
    }

    #[test]
    fn is_index_entry_expired_no_ttl() {
        let val = encode_index_value(0x02, None);
        assert!(!is_index_entry_expired(&val, i64::MAX));
    }

    #[test]
    fn extract_ttl_millis_from_raw_doc() {
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let bson = bson::rawdoc! { "_id": "a", "ttl": dt, "status": "active" };
        assert_eq!(extract_ttl_millis_from_raw(&bson), Some(1_700_000_000_000));
    }

    #[test]
    fn extract_ttl_millis_from_raw_doc_no_ttl() {
        let bson = bson::rawdoc! { "_id": "a", "status": "active" };
        assert_eq!(extract_ttl_millis_from_raw(&bson), None);
    }
}
