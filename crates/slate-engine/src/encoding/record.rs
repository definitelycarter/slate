use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};

use crate::error::{EncodingError, EngineError};

// ── Wire format ───────────────────────────────────────────────
//
// Layout:
//   [0x00][BSON...]                       — no TTL
//   [0x01][8-byte LE i64 millis][BSON...] — has TTL

const TAG_NO_TTL: u8 = 0x00;
const TAG_TTL: u8 = 0x01;
const TTL_SIZE: usize = 8;

// ── RecordEncoder ─────────────────────────────────────────────

enum TtlSource<'a> {
    None,
    Explicit(i64),
    FromPath(&'a str),
}

/// Builder for encoding a BSON document into a [`Record`].
pub struct RecordEncoder<'a> {
    ttl: TtlSource<'a>,
}

impl<'a> RecordEncoder<'a> {
    /// Set an explicit TTL in milliseconds.
    pub fn with_ttl(self, millis: i64) -> Self {
        RecordEncoder {
            ttl: TtlSource::Explicit(millis),
        }
    }

    /// Extract TTL from a DateTime field at the given path in the document.
    pub fn with_ttl_at_path(self, path: &'a str) -> Self {
        RecordEncoder {
            ttl: TtlSource::FromPath(path),
        }
    }

    /// Encode a BSON document into a Record.
    pub fn encode(self, doc: &RawDocument) -> Record {
        let ttl_millis = match self.ttl {
            TtlSource::None => None,
            TtlSource::Explicit(millis) => Some(millis),
            TtlSource::FromPath(path) => extract_datetime_millis(doc, path),
        };

        let bson_bytes = doc.as_bytes();
        match ttl_millis {
            Some(millis) => {
                let bson_start = 1 + TTL_SIZE;
                let mut bytes = Vec::with_capacity(bson_start + bson_bytes.len());
                bytes.push(TAG_TTL);
                bytes.extend_from_slice(&millis.to_le_bytes());
                bytes.extend_from_slice(bson_bytes);
                Record { bytes, bson_start }
            }
            None => {
                let mut bytes = Vec::with_capacity(1 + bson_bytes.len());
                bytes.push(TAG_NO_TTL);
                bytes.extend_from_slice(bson_bytes);
                Record {
                    bytes,
                    bson_start: 1,
                }
            }
        }
    }
}

// ── Record ────────────────────────────────────────────────────

/// A stored record: header + raw BSON document bytes.
///
/// Holds the raw storage bytes and lazily validates the BSON
/// portion on access via [`doc()`](Record::doc).
///
/// Construct via [`Record::encoder()`] for writes,
/// or [`Record::from_bytes()`] for reads.
pub struct Record {
    bytes: Vec<u8>,
    bson_start: usize,
}

impl Record {
    /// Create a [`RecordEncoder`] for building a new record.
    pub fn encoder() -> RecordEncoder<'static> {
        RecordEncoder {
            ttl: TtlSource::None,
        }
    }

    /// Wrap raw storage bytes as a Record.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, EngineError> {
        if bytes.is_empty() {
            return Err(EncodingError::MalformedRecord("empty record".into()).into());
        }
        let bson_start = match bytes[0] {
            TAG_NO_TTL => 1,
            TAG_TTL => {
                let header_len = 1 + TTL_SIZE;
                if bytes.len() < header_len {
                    return Err(
                        EncodingError::MalformedRecord("truncated TTL header".into()).into(),
                    );
                }
                header_len
            }
            tag => {
                return Err(
                    EncodingError::MalformedRecord(format!("unknown tag: 0x{tag:02X}")).into(),
                );
            }
        };
        Ok(Record { bytes, bson_start })
    }

    /// Raw storage bytes (header + BSON).
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consume self and return the raw storage bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Borrow the BSON document portion, validating on access.
    pub fn doc(&self) -> Result<&RawDocument, EngineError> {
        RawDocument::from_bytes(&self.bytes[self.bson_start..])
            .map_err(|e| EncodingError::Bson(e).into())
    }

    /// Read the TTL millis from the record header.
    pub fn ttl_millis(&self) -> Option<i64> {
        if self.bytes[0] == TAG_TTL && self.bytes.len() >= 1 + TTL_SIZE {
            Some(i64::from_le_bytes(
                self.bytes[1..1 + TTL_SIZE].try_into().unwrap(),
            ))
        } else {
            None
        }
    }

    /// Extract TTL millis from raw record bytes without full decode.
    pub fn ttl_millis_raw(data: &[u8]) -> Option<i64> {
        if data.len() > TTL_SIZE && data[0] == TAG_TTL {
            Some(i64::from_le_bytes(
                data[1..1 + TTL_SIZE].try_into().unwrap(),
            ))
        } else {
            None
        }
    }

    /// O(1) TTL expiry check on raw encoded bytes without full decode.
    #[inline]
    pub fn is_expired(data: &[u8], now_millis: i64) -> bool {
        matches!(Record::ttl_millis_raw(data), Some(millis) if millis < now_millis)
    }
}

impl TryFrom<Record> for RawDocumentBuf {
    type Error = EngineError;

    fn try_from(mut record: Record) -> Result<Self, Self::Error> {
        record.bytes.drain(..record.bson_start);
        RawDocumentBuf::from_bytes(record.bytes)
            .map_err(|e| EncodingError::Bson(e).into())
    }
}

/// Extract a DateTime field's millis from a raw BSON document.
fn extract_datetime_millis(doc: &RawDocument, field: &str) -> Option<i64> {
    match doc.get(field) {
        Ok(Some(RawBsonRef::DateTime(dt))) => Some(dt.timestamp_millis()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_ttl_roundtrip() {
        let doc = bson::rawdoc! { "_id": "a", "name": "test" };
        let record = Record::encoder().encode(&doc);
        assert_eq!(record.as_bytes()[0], TAG_NO_TTL);
        assert!(!Record::is_expired(record.as_bytes(), i64::MAX));
        assert_eq!(record.doc().unwrap(), doc.as_ref());
        let doc_buf = RawDocumentBuf::try_from(record).unwrap();
        assert_eq!(doc_buf, doc);
    }

    #[test]
    fn with_ttl_at_path_roundtrip() {
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let doc = bson::rawdoc! { "_id": "a", "ttl": dt };
        let record = Record::encoder().with_ttl_at_path("ttl").encode(&doc);
        assert_eq!(record.as_bytes()[0], TAG_TTL);
        assert_eq!(record.ttl_millis(), Some(1_700_000_000_000));
        assert!(Record::is_expired(record.as_bytes(), 1_800_000_000_000));
        assert!(!Record::is_expired(record.as_bytes(), 1_600_000_000_000));
        assert_eq!(record.doc().unwrap(), doc.as_ref());
    }

    #[test]
    fn with_explicit_ttl() {
        let doc = bson::rawdoc! { "_id": "a", "name": "test" };
        let record = Record::encoder().with_ttl(5_000).encode(&doc);
        assert_eq!(record.as_bytes()[0], TAG_TTL);
        assert_eq!(record.ttl_millis(), Some(5_000));
        assert!(Record::is_expired(record.as_bytes(), 6_000));
        assert!(!Record::is_expired(record.as_bytes(), 4_000));
    }

    #[test]
    fn with_ttl_at_path_missing_field() {
        let doc = bson::rawdoc! { "_id": "a", "name": "test" };
        let record = Record::encoder().with_ttl_at_path("ttl").encode(&doc);
        assert_eq!(record.as_bytes()[0], TAG_NO_TTL);
        assert_eq!(record.ttl_millis(), None);
    }

    #[test]
    fn with_ttl_at_path_custom_field() {
        let dt = bson::DateTime::from_millis(42_000);
        let doc = bson::rawdoc! { "_id": "a", "expires_at": dt };
        let record = Record::encoder().with_ttl_at_path("expires_at").encode(&doc);
        assert_eq!(record.ttl_millis(), Some(42_000));
    }

    #[test]
    fn from_bytes_roundtrip() {
        let doc = bson::rawdoc! { "_id": "a", "name": "test" };
        let original = Record::encoder().encode(&doc);
        let bytes = original.into_bytes();
        let restored = Record::from_bytes(bytes).unwrap();
        assert_eq!(restored.doc().unwrap(), doc.as_ref());
    }

    #[test]
    fn expired() {
        let dt = bson::DateTime::from_millis(1_000);
        let doc = bson::rawdoc! { "_id": "a", "ttl": dt };
        let record = Record::encoder().with_ttl_at_path("ttl").encode(&doc);
        assert!(Record::is_expired(record.as_bytes(), 2_000));
        assert!(!Record::is_expired(record.as_bytes(), 500));
    }

    #[test]
    fn no_ttl_not_expired() {
        let doc = bson::rawdoc! { "_id": "a", "name": "test" };
        let record = Record::encoder().encode(&doc);
        assert!(!Record::is_expired(record.as_bytes(), i64::MAX));
    }

    #[test]
    fn from_bytes_empty_errors() {
        assert!(Record::from_bytes(vec![]).is_err());
    }

    #[test]
    fn try_into_raw_document_buf() {
        let doc = bson::rawdoc! { "_id": "a", "name": "test" };
        let record = Record::encoder().encode(&doc);
        let doc_buf: RawDocumentBuf = record.try_into().unwrap();
        assert_eq!(doc_buf, doc);
    }
}
