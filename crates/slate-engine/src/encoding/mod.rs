pub mod bson_value;
pub mod index_record;
pub mod key;
pub mod record;

pub use index_record::IndexRecord;
pub use key::{Key, KeyPrefix};
pub use record::Record;

// ── Raw BSON skip primitive ─────────────────────────────────────

/// Given a BSON type byte and the position where value bytes begin,
/// return the position immediately after the value. Returns `None`
/// if bytes are truncated or the type is unrecognised.
pub fn skip_bson_value(type_byte: u8, bytes: &[u8], pos: usize) -> Option<usize> {
    match type_byte {
        0x01 => Some(pos + 8), // Double
        0x02 => {
            // String: i32(len) + utf8 + nul
            if pos + 4 > bytes.len() {
                return None;
            }
            let len = i32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
            Some(pos + 4 + len)
        }
        0x03 | 0x04 => {
            // Document / Array (self-contained)
            if pos + 4 > bytes.len() {
                return None;
            }
            let len = i32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
            Some(pos + len)
        }
        0x05 => {
            // Binary: i32(len) + subtype + data
            if pos + 4 > bytes.len() {
                return None;
            }
            let len = i32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
            Some(pos + 5 + len)
        }
        0x07 => Some(pos + 12), // ObjectId
        0x08 => Some(pos + 1),  // Boolean
        0x09 => Some(pos + 8),  // DateTime (i64)
        0x0A => Some(pos),      // Null (0 bytes)
        0x10 => Some(pos + 4),  // Int32
        0x11 => Some(pos + 8),  // Timestamp
        0x12 => Some(pos + 8),  // Int64
        0x13 => Some(pos + 16), // Decimal128
        _ => None,
    }
}
