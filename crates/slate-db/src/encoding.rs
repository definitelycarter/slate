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
// - value_bytes = canonical byte encoding of the Value

const INDEX_PREFIX: &[u8] = b"i:";

/// Encode a Value into canonical bytes for index key ordering.
///
/// The encoding must sort correctly under lexicographic byte comparison.
pub fn encode_value(value: &super::record::Value) -> Vec<u8> {
    use super::record::Value;
    match value {
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Int(i) => {
            // Flip the sign bit so signed integers sort correctly as unsigned bytes
            let unsigned = (*i as u64) ^ (1u64 << 63);
            unsigned.to_be_bytes().to_vec()
        }
        Value::Float(f) => {
            let bits = f.to_bits();
            // IEEE 754: flip all bits if negative, else flip sign bit only
            let sortable = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            sortable.to_be_bytes().to_vec()
        }
        Value::Bool(b) => vec![*b as u8],
        Value::Date(d) => {
            let unsigned = (*d as u64) ^ (1u64 << 63);
            unsigned.to_be_bytes().to_vec()
        }
        _ => vec![], // List/Map not indexable
    }
}

/// Build an index key: `i:{column}\x00{value_bytes}\x00{record_id}`
pub fn index_key(column: &str, value: &super::record::Value, record_id: &str) -> Vec<u8> {
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
pub fn index_scan_prefix(column: &str, value: &super::record::Value) -> Vec<u8> {
    let value_bytes = encode_value(value);
    let mut key = Vec::with_capacity(INDEX_PREFIX.len() + column.len() + 1 + value_bytes.len() + 1);
    key.extend_from_slice(INDEX_PREFIX);
    key.extend_from_slice(column.as_bytes());
    key.push(SEP);
    key.extend_from_slice(&value_bytes);
    key.push(SEP);
    key
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

    // Find second \x00 — separates value_bytes from record_id
    let second_sep = after_first.iter().position(|&b| b == SEP)?;
    let record_id = std::str::from_utf8(&after_first[second_sep + 1..]).ok()?;

    Some((column, record_id))
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
        use crate::record::Value;
        let key = index_key("status", &Value::String("active".into()), "ds1:rec-1");
        let (column, record_id) = parse_index_key(&key).unwrap();
        assert_eq!(column, "status");
        assert_eq!(record_id, "ds1:rec-1");
    }

    #[test]
    fn index_scan_prefix_matches_keys() {
        use crate::record::Value;
        let prefix = index_scan_prefix("status", &Value::String("active".into()));
        let k1 = index_key("status", &Value::String("active".into()), "rec-1");
        let k2 = index_key("status", &Value::String("active".into()), "rec-2");
        let k3 = index_key("status", &Value::String("rejected".into()), "rec-1");
        let k4 = index_key("name", &Value::String("active".into()), "rec-1");
        assert!(k1.starts_with(&prefix));
        assert!(k2.starts_with(&prefix));
        assert!(!k3.starts_with(&prefix)); // different value
        assert!(!k4.starts_with(&prefix)); // different column
    }

    #[test]
    fn index_int_sort_order() {
        use crate::record::Value;
        let k1 = index_key("score", &Value::Int(10), "rec-1");
        let k2 = index_key("score", &Value::Int(20), "rec-1");
        let k3 = index_key("score", &Value::Int(-5), "rec-1");
        assert!(k3 < k1); // -5 < 10
        assert!(k1 < k2); // 10 < 20
    }
}
