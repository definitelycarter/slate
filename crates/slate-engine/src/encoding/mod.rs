pub mod bson_value;

use bson::raw::RawDocumentBuf;

use crate::error::EngineError;

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

// ── Record ─────────────────────────────────────────────────────
//
// Layout:
//   [0x00][BSON...]                    — no TTL
//   [0x01][8-byte LE i64 millis][BSON...] — has TTL

const TAG_NO_TTL: u8 = 0x00;
const TAG_TTL: u8 = 0x01;
const TTL_SIZE: usize = 8;

/// A decoded record: optional TTL + raw BSON document.
pub struct Record {
    pub ttl_millis: Option<i64>,
    pub doc: RawDocumentBuf,
}

impl Record {
    /// Extract the TTL millis from a raw BSON document.
    pub fn ttl_millis(doc: &bson::raw::RawDocument) -> Option<i64> {
        extract_ttl_millis(doc.as_bytes())
    }

    /// Encode a document into the record wire format.
    ///
    /// Scans the BSON payload for a `ttl` DateTime field. If found,
    /// prepends the TTL header.
    pub fn encode(doc: &bson::raw::RawDocument) -> Vec<u8> {
        let bson_bytes = doc.as_bytes();
        match extract_ttl_millis(bson_bytes) {
            Some(millis) => {
                let mut buf = Vec::with_capacity(1 + TTL_SIZE + bson_bytes.len());
                buf.push(TAG_TTL);
                buf.extend_from_slice(&millis.to_le_bytes());
                buf.extend_from_slice(bson_bytes);
                buf
            }
            None => {
                let mut buf = Vec::with_capacity(1 + bson_bytes.len());
                buf.push(TAG_NO_TTL);
                buf.extend_from_slice(bson_bytes);
                buf
            }
        }
    }

    /// Decode a record from its wire format (borrows, copies BSON portion).
    pub fn decode(data: &[u8]) -> Result<Self, EngineError> {
        if data.is_empty() {
            return Err(EngineError::Encoding("empty record".into()));
        }
        let (ttl_millis, bson_bytes) = match data[0] {
            TAG_NO_TTL => (None, &data[1..]),
            TAG_TTL => {
                let header_len = 1 + TTL_SIZE;
                if data.len() < header_len {
                    return Err(EngineError::Encoding("truncated TTL header".into()));
                }
                let millis = i64::from_le_bytes(data[1..1 + TTL_SIZE].try_into().unwrap());
                (Some(millis), &data[header_len..])
            }
            tag => {
                return Err(EngineError::Encoding(format!(
                    "unknown record tag: 0x{tag:02X}"
                )));
            }
        };
        let doc = RawDocumentBuf::from_bytes(bson_bytes.to_vec())
            .map_err(|e| EngineError::Encoding(format!("invalid BSON: {e}")))?;
        Ok(Record { ttl_millis, doc })
    }

    /// Decode a record from owned bytes, reusing the allocation.
    pub fn decode_owned(data: Vec<u8>) -> Result<Self, EngineError> {
        if data.is_empty() {
            return Err(EngineError::Encoding("empty record".into()));
        }
        let (ttl_millis, bson_start) = match data[0] {
            TAG_NO_TTL => (None, 1),
            TAG_TTL => {
                let header_len = 1 + TTL_SIZE;
                if data.len() < header_len {
                    return Err(EngineError::Encoding("truncated TTL header".into()));
                }
                let millis = i64::from_le_bytes(data[1..1 + TTL_SIZE].try_into().unwrap());
                (Some(millis), header_len)
            }
            tag => {
                return Err(EngineError::Encoding(format!(
                    "unknown record tag: 0x{tag:02X}"
                )));
            }
        };
        let mut bson_bytes = data;
        bson_bytes.drain(..bson_start);
        let doc = RawDocumentBuf::from_bytes(bson_bytes)
            .map_err(|e| EngineError::Encoding(format!("invalid BSON: {e}")))?;
        Ok(Record { ttl_millis, doc })
    }

    /// O(1) TTL expiry check on raw encoded bytes without full decode.
    #[inline]
    pub fn is_expired(data: &[u8], now_millis: i64) -> bool {
        if data.len() >= 1 + TTL_SIZE && data[0] == TAG_TTL {
            let millis = i64::from_le_bytes(data[1..1 + TTL_SIZE].try_into().unwrap());
            millis < now_millis
        } else {
            false
        }
    }
}

// ── IndexMeta ──────────────────────────────────────────────────
//
// Layout:
//   [type_byte]                        — no TTL (1 byte)
//   [type_byte][8-byte LE i64 millis]  — has TTL (9 bytes)

const INDEX_META_TTL_OFFSET: usize = 1;
const INDEX_META_WITH_TTL_SIZE: usize = INDEX_META_TTL_OFFSET + TTL_SIZE;

/// Metadata stored as the value of an index entry.
pub struct IndexMeta {
    pub type_byte: u8,
    pub ttl_millis: Option<i64>,
}

impl IndexMeta {
    /// Encode index metadata to bytes.
    pub fn encode(&self) -> Vec<u8> {
        match self.ttl_millis {
            Some(millis) => {
                let mut buf = Vec::with_capacity(INDEX_META_WITH_TTL_SIZE);
                buf.push(self.type_byte);
                buf.extend_from_slice(&millis.to_le_bytes());
                buf
            }
            None => vec![self.type_byte],
        }
    }

    /// Decode index metadata from bytes.
    pub fn decode(data: &[u8]) -> Result<Self, EngineError> {
        if data.is_empty() {
            return Err(EngineError::Encoding("empty index metadata".into()));
        }
        let type_byte = data[0];
        let ttl_millis = if data.len() >= INDEX_META_WITH_TTL_SIZE {
            Some(i64::from_le_bytes(
                data[INDEX_META_TTL_OFFSET..INDEX_META_WITH_TTL_SIZE]
                    .try_into()
                    .unwrap(),
            ))
        } else {
            None
        };
        Ok(IndexMeta {
            type_byte,
            ttl_millis,
        })
    }

    /// O(1) TTL expiry check on raw encoded bytes without full decode.
    #[inline]
    pub fn is_expired(data: &[u8], now_millis: i64) -> bool {
        if data.len() >= INDEX_META_WITH_TTL_SIZE {
            let millis = i64::from_le_bytes(
                data[INDEX_META_TTL_OFFSET..INDEX_META_WITH_TTL_SIZE]
                    .try_into()
                    .unwrap(),
            );
            millis < now_millis
        } else {
            false
        }
    }
}

// ── TTL extraction ─────────────────────────────────────────────

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

        match skip_bson_value(type_byte, bytes, pos) {
            Some(next) => pos = next,
            None => break,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_no_ttl_roundtrip() {
        let doc = bson::rawdoc! { "_id": "a", "name": "test" };
        let encoded = Record::encode(&doc);
        assert_eq!(encoded[0], TAG_NO_TTL);
        let record = Record::decode(&encoded).unwrap();
        assert!(record.ttl_millis.is_none());
        assert_eq!(record.doc, doc);
    }

    #[test]
    fn record_with_ttl_roundtrip() {
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let doc = bson::rawdoc! { "_id": "a", "ttl": dt };
        let encoded = Record::encode(&doc);
        assert_eq!(encoded[0], TAG_TTL);
        let record = Record::decode(&encoded).unwrap();
        assert_eq!(record.ttl_millis, Some(1_700_000_000_000));
        assert_eq!(record.doc, doc);
    }

    #[test]
    fn record_expired() {
        let dt = bson::DateTime::from_millis(1_000);
        let doc = bson::rawdoc! { "_id": "a", "ttl": dt };
        let encoded = Record::encode(&doc);
        assert!(Record::is_expired(&encoded, 2_000));
        assert!(!Record::is_expired(&encoded, 500));
    }

    #[test]
    fn record_no_ttl_not_expired() {
        let doc = bson::rawdoc! { "_id": "a", "name": "test" };
        let encoded = Record::encode(&doc);
        assert!(!Record::is_expired(&encoded, i64::MAX));
    }

    #[test]
    fn record_decode_empty_errors() {
        assert!(Record::decode(&[]).is_err());
    }

    #[test]
    fn index_meta_no_ttl_roundtrip() {
        let meta = IndexMeta {
            type_byte: 0x02,
            ttl_millis: None,
        };
        let encoded = meta.encode();
        assert_eq!(encoded, vec![0x02]);
        let decoded = IndexMeta::decode(&encoded).unwrap();
        assert_eq!(decoded.type_byte, 0x02);
        assert!(decoded.ttl_millis.is_none());
    }

    #[test]
    fn index_meta_with_ttl_roundtrip() {
        let meta = IndexMeta {
            type_byte: 0x02,
            ttl_millis: Some(1_700_000_000_000),
        };
        let encoded = meta.encode();
        assert_eq!(encoded.len(), 9);
        let decoded = IndexMeta::decode(&encoded).unwrap();
        assert_eq!(decoded.type_byte, 0x02);
        assert_eq!(decoded.ttl_millis, Some(1_700_000_000_000));
    }

    #[test]
    fn index_meta_expired() {
        let meta = IndexMeta {
            type_byte: 0x02,
            ttl_millis: Some(1_000),
        };
        let encoded = meta.encode();
        assert!(IndexMeta::is_expired(&encoded, 2_000));
        assert!(!IndexMeta::is_expired(&encoded, 500));
    }

    #[test]
    fn index_meta_no_ttl_not_expired() {
        let meta = IndexMeta {
            type_byte: 0x02,
            ttl_millis: None,
        };
        let encoded = meta.encode();
        assert!(!IndexMeta::is_expired(&encoded, i64::MAX));
    }
}
