use std::borrow::Cow;
use std::hash::{Hash, Hasher};

use bson::raw::{RawBsonRef, RawDocument};
use bson::spec::ElementType;

/// Length-prefixed header size: 1 type byte + 2 length bytes.
const LP_HEADER: usize = 3;

// ── Sortable encoding helpers ──────────────────────────────────
//
// For index keys, numeric values must be encoded so that byte-level
// lexicographic comparison matches numeric ordering.  The standard
// technique for signed integers: XOR the sign bit, then big-endian.
// For IEEE 754 doubles: if positive, flip sign bit; if negative, flip
// all bits.

#[inline]
fn encode_i32_sortable(n: i32) -> [u8; 4] {
    ((n as u32) ^ 0x8000_0000).to_be_bytes()
}

#[inline]
fn decode_i32_sortable(b: [u8; 4]) -> i32 {
    (u32::from_be_bytes(b) ^ 0x8000_0000) as i32
}

#[inline]
fn encode_i64_sortable(n: i64) -> [u8; 8] {
    ((n as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

#[inline]
fn decode_i64_sortable(b: [u8; 8]) -> i64 {
    (u64::from_be_bytes(b) ^ 0x8000_0000_0000_0000) as i64
}

#[inline]
fn encode_f64_sortable(f: f64) -> [u8; 8] {
    let bits = f.to_bits();
    let encoded = if (bits & 0x8000_0000_0000_0000) != 0 {
        !bits // negative: flip all bits
    } else {
        bits ^ 0x8000_0000_0000_0000 // positive: flip sign bit
    };
    encoded.to_be_bytes()
}

#[inline]
fn decode_f64_sortable(b: [u8; 8]) -> f64 {
    let encoded = u64::from_be_bytes(b);
    let bits = if (encoded & 0x8000_0000_0000_0000) != 0 {
        encoded ^ 0x8000_0000_0000_0000 // was positive
    } else {
        !encoded // was negative
    };
    f64::from_bits(bits)
}

/// A raw BSON value: type tag + value bytes.
///
/// This is the shared representation for doc_id and index values.
/// The `tag` is the BSON element type byte, and `bytes` contains
/// only the raw value data (not the tag).
///
/// Construction from `RawBsonRef`:
/// - `ObjectId`, `String` → zero-alloc (borrows from source)
/// - `Int32`, `Int64`, `Double`, `DateTime`, `Boolean` → small owned allocation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BsonValue<'a> {
    pub tag: ElementType,
    pub bytes: Cow<'a, [u8]>,
}

impl Hash for BsonValue<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.tag as u8).hash(state);
        self.bytes.hash(state);
    }
}

impl<'a> BsonValue<'a> {
    /// Create a `BsonValue` from a `RawBsonRef`.
    ///
    /// Returns `None` for types that aren't supported as field values
    /// (Document, Array, Null, Binary, etc.).
    pub fn from_raw_bson_ref(val: RawBsonRef<'a>) -> Option<Self> {
        match val {
            RawBsonRef::ObjectId(oid) => Some(BsonValue {
                tag: ElementType::ObjectId,
                bytes: Cow::Owned(oid.bytes().to_vec()),
            }),
            RawBsonRef::String(s) => Some(BsonValue {
                tag: ElementType::String,
                bytes: Cow::Borrowed(s.as_bytes()),
            }),
            RawBsonRef::Int32(n) => Some(BsonValue {
                tag: ElementType::Int32,
                bytes: Cow::Owned(encode_i32_sortable(n).to_vec()),
            }),
            RawBsonRef::Int64(n) => Some(BsonValue {
                tag: ElementType::Int64,
                bytes: Cow::Owned(encode_i64_sortable(n).to_vec()),
            }),
            RawBsonRef::Double(f) => Some(BsonValue {
                tag: ElementType::Double,
                bytes: Cow::Owned(encode_f64_sortable(f).to_vec()),
            }),
            RawBsonRef::DateTime(dt) => Some(BsonValue {
                tag: ElementType::DateTime,
                bytes: Cow::Owned(encode_i64_sortable(dt.timestamp_millis()).to_vec()),
            }),
            RawBsonRef::Boolean(b) => Some(BsonValue {
                tag: ElementType::Boolean,
                bytes: Cow::Owned(vec![b as u8]),
            }),
            _ => None,
        }
    }

    /// Create a `BsonValue` from an owned `bson::Bson`.
    ///
    /// Returns `None` for types that aren't supported (Document, Array, Null, etc.).
    pub fn from_bson(val: &bson::Bson) -> Option<BsonValue<'static>> {
        match val {
            bson::Bson::ObjectId(oid) => Some(BsonValue {
                tag: ElementType::ObjectId,
                bytes: Cow::Owned(oid.bytes().to_vec()),
            }),
            bson::Bson::String(s) => Some(BsonValue {
                tag: ElementType::String,
                bytes: Cow::Owned(s.as_bytes().to_vec()),
            }),
            bson::Bson::Int32(n) => Some(BsonValue {
                tag: ElementType::Int32,
                bytes: Cow::Owned(encode_i32_sortable(*n).to_vec()),
            }),
            bson::Bson::Int64(n) => Some(BsonValue {
                tag: ElementType::Int64,
                bytes: Cow::Owned(encode_i64_sortable(*n).to_vec()),
            }),
            bson::Bson::Double(f) => Some(BsonValue {
                tag: ElementType::Double,
                bytes: Cow::Owned(encode_f64_sortable(*f).to_vec()),
            }),
            bson::Bson::DateTime(dt) => Some(BsonValue {
                tag: ElementType::DateTime,
                bytes: Cow::Owned(encode_i64_sortable(dt.timestamp_millis()).to_vec()),
            }),
            bson::Bson::Boolean(b) => Some(BsonValue {
                tag: ElementType::Boolean,
                bytes: Cow::Owned(vec![*b as u8]),
            }),
            _ => None,
        }
    }

    /// Extract a field from a `RawDocument` as a `BsonValue`.
    ///
    /// Supports dotted paths for nested documents (e.g. `"address.city"`).
    /// For array multi-key fields (containing `[]`), use [`extract_all`].
    pub fn extract(doc: &'a RawDocument, field: &str) -> Option<Self> {
        if !field.contains('.') {
            return match doc.get(field) {
                Ok(Some(val)) => Self::from_raw_bson_ref(val),
                _ => None,
            };
        }

        // Walk dotted path through nested documents
        let mut segments = field.split('.');
        let first = segments.next()?;
        let mut current = match doc.get(first) {
            Ok(Some(val)) => val,
            _ => return None,
        };

        for seg in segments {
            match current {
                RawBsonRef::Document(d) => {
                    current = match d.get(seg) {
                        Ok(Some(val)) => val,
                        _ => return None,
                    };
                }
                _ => return None,
            }
        }

        Self::from_raw_bson_ref(current)
    }

    /// Construct from a known type tag and raw value bytes.
    pub fn from_parts(tag: ElementType, bytes: &'a [u8]) -> Self {
        BsonValue {
            tag,
            bytes: Cow::Borrowed(bytes),
        }
    }

    /// Construct from a raw `[tag][bytes]` slice.
    #[cfg(test)]
    pub fn from_slice(raw: &'a [u8]) -> Option<Self> {
        if raw.is_empty() {
            return None;
        }
        Some(BsonValue {
            tag: ElementType::from(raw[0])?,
            bytes: Cow::Borrowed(&raw[1..]),
        })
    }

    /// Returns `[tag][bytes]` as a new `Vec<u8>`.
    #[cfg(test)]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + self.bytes.len());
        v.push(self.tag as u8);
        v.extend_from_slice(&self.bytes);
        v
    }

    /// Write in length-prefixed form: `[tag][len_be16][bytes]`.
    ///
    /// Used for doc_id encoding in keys where the value sits adjacent
    /// to other variable-length data.
    pub fn write_length_prefixed(&self, buf: &mut Vec<u8>) {
        let len = self.bytes.len() as u16;
        buf.push(self.tag as u8);
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&self.bytes);
    }

    /// Parse a `BsonValue` from length-prefixed form at the start of `bytes`.
    ///
    /// Returns `(value, remaining)`.
    pub fn parse_length_prefixed(bytes: &'a [u8]) -> Option<(Self, &'a [u8])> {
        if bytes.len() < LP_HEADER {
            return None;
        }
        let tag = ElementType::from(bytes[0])?;
        let len = u16::from_be_bytes([bytes[1], bytes[2]]) as usize;
        let total = LP_HEADER + len;
        if bytes.len() < total {
            return None;
        }
        Some((
            BsonValue {
                tag,
                bytes: Cow::Borrowed(&bytes[LP_HEADER..total]),
            },
            &bytes[total..],
        ))
    }

    /// Convert back to a `RawBson`, decoding sortable-encoded values.
    pub fn to_raw_bson(&self) -> Option<bson::RawBson> {
        Some(match self.tag {
            ElementType::String => bson::RawBson::String(
                std::str::from_utf8(&self.bytes).ok()?.to_string(),
            ),
            ElementType::ObjectId if self.bytes.len() == 12 => {
                let oid = bson::oid::ObjectId::from_bytes(self.bytes[..12].try_into().ok()?);
                bson::RawBson::ObjectId(oid)
            }
            ElementType::Int32 if self.bytes.len() == 4 => {
                bson::RawBson::Int32(decode_i32_sortable(self.bytes[..4].try_into().ok()?))
            }
            ElementType::Int64 if self.bytes.len() == 8 => {
                bson::RawBson::Int64(decode_i64_sortable(self.bytes[..8].try_into().ok()?))
            }
            ElementType::Double if self.bytes.len() == 8 => {
                bson::RawBson::Double(decode_f64_sortable(self.bytes[..8].try_into().ok()?))
            }
            ElementType::DateTime if self.bytes.len() == 8 => {
                let millis = decode_i64_sortable(self.bytes[..8].try_into().ok()?);
                bson::RawBson::DateTime(bson::DateTime::from_millis(millis))
            }
            ElementType::Boolean => {
                bson::RawBson::Boolean(self.bytes.first().is_some_and(|&v| v != 0))
            }
            _ => return None,
        })
    }

    /// Convert to owned, producing a `BsonValue<'static>`.
    pub fn into_owned(self) -> BsonValue<'static> {
        BsonValue {
            tag: self.tag,
            bytes: Cow::Owned(self.bytes.into_owned()),
        }
    }
}

impl std::fmt::Display for BsonValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.tag {
            ElementType::String => {
                let s = std::str::from_utf8(&self.bytes).unwrap_or("<invalid utf8>");
                write!(f, "{s}")
            }
            ElementType::ObjectId => {
                for b in self.bytes.iter() {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
            ElementType::Int32 if self.bytes.len() == 4 => {
                let n = decode_i32_sortable(self.bytes[..4].try_into().unwrap());
                write!(f, "{n}")
            }
            ElementType::Int64 | ElementType::DateTime if self.bytes.len() == 8 => {
                let n = decode_i64_sortable(self.bytes[..8].try_into().unwrap());
                write!(f, "{n}")
            }
            ElementType::Double if self.bytes.len() == 8 => {
                let n = decode_f64_sortable(self.bytes[..8].try_into().unwrap());
                write!(f, "{n}")
            }
            ElementType::Boolean => {
                let b = self.bytes.first().is_some_and(|&v| v != 0);
                write!(f, "{b}")
            }
            _ => write!(f, "<bson 0x{:02x}>", self.tag as u8),
        }
    }
}

/// Error returned when a `RawBsonRef` cannot be converted to a `BsonValue`
/// (e.g. Null, Document, Array, Binary).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedBsonType;

impl std::fmt::Display for UnsupportedBsonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("unsupported BSON type for BsonValue")
    }
}

impl std::error::Error for UnsupportedBsonType {}

impl<'a> TryFrom<RawBsonRef<'a>> for BsonValue<'a> {
    type Error = UnsupportedBsonType;

    fn try_from(val: RawBsonRef<'a>) -> Result<Self, Self::Error> {
        Self::from_raw_bson_ref(val).ok_or(UnsupportedBsonType)
    }
}

// ── Multi-key extraction (free functions to avoid impl<'a> lifetime issues) ──

/// Extract all values for a field path, supporting multi-key (array) indexing.
///
/// The path may contain `[]` segments to traverse arrays:
/// - `"status"` → `[BsonValue("active")]` (single value)
/// - `"address.city"` → `[BsonValue("Austin")]` (nested document)
/// - `"tags.[]"` → `[BsonValue("rust"), BsonValue("db")]` (array elements)
/// - `"items.[].sku"` → `[BsonValue("A1"), BsonValue("B2")]` (nested array)
///
/// Returns owned values since we may need to traverse array elements.
pub fn extract_all(doc: &RawDocument, field: &str) -> Vec<BsonValue<'static>> {
    if !field.contains("[]") {
        return match BsonValue::extract(doc, field) {
            Some(v) => vec![v.into_owned()],
            None => vec![],
        };
    }

    let segments: Vec<&str> = field.split('.').collect();
    let mut results = Vec::new();
    collect_from_doc(doc, &segments, 0, &mut results);
    results
}

fn collect_from_doc(
    doc: &RawDocument,
    segments: &[&str],
    idx: usize,
    out: &mut Vec<BsonValue<'static>>,
) {
    if idx >= segments.len() {
        return;
    }
    let seg = segments[idx];
    if seg == "[]" {
        return;
    }
    if let Ok(Some(value)) = doc.get(seg) {
        collect_from_value(value, segments, idx + 1, out);
    }
}

fn collect_from_value(
    value: RawBsonRef<'_>,
    segments: &[&str],
    idx: usize,
    out: &mut Vec<BsonValue<'static>>,
) {
    if idx >= segments.len() {
        // Terminal: try to convert to BsonValue
        match value {
            RawBsonRef::Array(arr) => {
                for v in arr.into_iter().flatten() {
                    if let Some(bv) = BsonValue::from_raw_bson_ref(v) {
                        out.push(bv.into_owned());
                    }
                }
            }
            _ => {
                if let Some(bv) = BsonValue::from_raw_bson_ref(value) {
                    out.push(bv.into_owned());
                }
            }
        }
        return;
    }

    let seg = segments[idx];
    if seg == "[]" {
        // Traverse array elements
        if let RawBsonRef::Array(arr) = value {
            for v in arr.into_iter().flatten() {
                collect_from_value(v, segments, idx + 1, out);
            }
        }
    } else if let RawBsonRef::Document(d) = value {
        collect_from_doc(d, segments, idx, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::oid::ObjectId;

    #[test]
    fn object_id_roundtrip() {
        let oid = ObjectId::new();
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::ObjectId(oid)).unwrap();
        assert_eq!(val.tag, ElementType::ObjectId);
        assert_eq!(val.bytes.len(), 12);
        // to_vec → from_slice roundtrip
        let raw = val.to_vec();
        let parsed = BsonValue::from_slice(&raw).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn string_roundtrip() {
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::String("hello")).unwrap();
        assert_eq!(val.tag, ElementType::String);
        assert_eq!(&*val.bytes, b"hello");
        let raw = val.to_vec();
        let parsed = BsonValue::from_slice(&raw).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn int32_roundtrip() {
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::Int32(42)).unwrap();
        assert_eq!(val.tag, ElementType::Int32);
        assert_eq!(&*val.bytes, &encode_i32_sortable(42));
        let raw = val.to_vec();
        let parsed = BsonValue::from_slice(&raw).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn int64_roundtrip() {
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::Int64(1_000_000)).unwrap();
        assert_eq!(val.tag, ElementType::Int64);
        assert_eq!(&*val.bytes, &encode_i64_sortable(1_000_000));
    }

    #[test]
    fn double_roundtrip() {
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::Double(2.78)).unwrap();
        assert_eq!(val.tag, ElementType::Double);
        assert_eq!(&*val.bytes, &encode_f64_sortable(2.78));
    }

    #[test]
    fn datetime_roundtrip() {
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::DateTime(dt)).unwrap();
        assert_eq!(val.tag, ElementType::DateTime);
        assert_eq!(&*val.bytes, &encode_i64_sortable(1_700_000_000_000));
    }

    #[test]
    fn boolean_roundtrip() {
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::Boolean(true)).unwrap();
        assert_eq!(val.tag, ElementType::Boolean);
        assert_eq!(&*val.bytes, &[1]);

        let val = BsonValue::from_raw_bson_ref(RawBsonRef::Boolean(false)).unwrap();
        assert_eq!(&*val.bytes, &[0]);
    }

    #[test]
    fn unsupported_type_returns_none() {
        assert!(BsonValue::from_raw_bson_ref(RawBsonRef::Null).is_none());
    }

    #[test]
    fn length_prefixed_roundtrip() {
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::String("test")).unwrap();
        let mut buf = Vec::new();
        val.write_length_prefixed(&mut buf);
        // [0x02][0x00][0x04][t][e][s][t]
        assert_eq!(buf.len(), 3 + 4);
        assert_eq!(buf[0], 0x02);
        assert_eq!(u16::from_be_bytes([buf[1], buf[2]]), 4);

        let (parsed, rest) = BsonValue::parse_length_prefixed(&buf).unwrap();
        assert_eq!(parsed, val);
        assert!(rest.is_empty());
    }

    #[test]
    fn length_prefixed_with_trailing_data() {
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::Int32(99)).unwrap();
        let mut buf = Vec::new();
        val.write_length_prefixed(&mut buf);
        buf.extend_from_slice(b"trailing");

        let (parsed, rest) = BsonValue::parse_length_prefixed(&buf).unwrap();
        assert_eq!(parsed, val);
        assert_eq!(rest, b"trailing");
    }

    #[test]
    fn extract_from_document() {
        let doc = bson::rawdoc! { "_id": "abc", "age": 25 };
        let id = BsonValue::extract(&doc, "_id").unwrap();
        assert_eq!(id.tag, ElementType::String);
        assert_eq!(&*id.bytes, b"abc");

        let age = BsonValue::extract(&doc, "age").unwrap();
        assert_eq!(age.tag, ElementType::Int32);
        assert_eq!(&*age.bytes, &encode_i32_sortable(25));
    }

    #[test]
    fn extract_missing_field() {
        let doc = bson::rawdoc! { "_id": "abc" };
        assert!(BsonValue::extract(&doc, "missing").is_none());
    }

    #[test]
    fn into_owned_produces_static() {
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::String("borrowed")).unwrap();
        let owned: BsonValue<'static> = val.into_owned();
        assert_eq!(owned.tag, ElementType::String);
        assert_eq!(&*owned.bytes, b"borrowed");
    }

    #[test]
    fn from_slice_empty_returns_none() {
        assert!(BsonValue::from_slice(&[]).is_none());
    }

    #[test]
    fn object_id_is_fixed_size() {
        let oid = ObjectId::new();
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::ObjectId(oid)).unwrap();
        assert_eq!(val.bytes.len(), 12);
    }

    #[test]
    fn string_is_zero_alloc() {
        let val = BsonValue::from_raw_bson_ref(RawBsonRef::String("test")).unwrap();
        assert!(matches!(val.bytes, Cow::Borrowed(_)));
    }

    #[test]
    fn try_from_raw_bson_ref() {
        let val: BsonValue<'_> = RawBsonRef::String("hi").try_into().unwrap();
        assert_eq!(val.tag, ElementType::String);
        assert_eq!(&*val.bytes, b"hi");

        let val: BsonValue<'_> = RawBsonRef::Int32(7).try_into().unwrap();
        assert_eq!(val.tag, ElementType::Int32);

        let err = BsonValue::try_from(RawBsonRef::Null);
        assert!(err.is_err());
    }

    #[test]
    fn sortable_encoding_preserves_order() {
        // Integers: byte ordering must match numeric ordering
        let neg = BsonValue::from_raw_bson_ref(RawBsonRef::Int32(-10)).unwrap();
        let zero = BsonValue::from_raw_bson_ref(RawBsonRef::Int32(0)).unwrap();
        let pos = BsonValue::from_raw_bson_ref(RawBsonRef::Int32(42)).unwrap();
        assert!(neg.to_vec() < zero.to_vec());
        assert!(zero.to_vec() < pos.to_vec());

        // Int64
        let a = BsonValue::from_raw_bson_ref(RawBsonRef::Int64(100)).unwrap();
        let b = BsonValue::from_raw_bson_ref(RawBsonRef::Int64(200)).unwrap();
        assert!(a.to_vec() < b.to_vec());

        // DateTime (i64 millis)
        let dt1 = bson::DateTime::from_millis(1_000_000);
        let dt2 = bson::DateTime::from_millis(2_000_000);
        let v1 = BsonValue::from_raw_bson_ref(RawBsonRef::DateTime(dt1)).unwrap();
        let v2 = BsonValue::from_raw_bson_ref(RawBsonRef::DateTime(dt2)).unwrap();
        assert!(v1.to_vec() < v2.to_vec());

        // Double
        let d1 = BsonValue::from_raw_bson_ref(RawBsonRef::Double(-1.5)).unwrap();
        let d2 = BsonValue::from_raw_bson_ref(RawBsonRef::Double(0.0)).unwrap();
        let d3 = BsonValue::from_raw_bson_ref(RawBsonRef::Double(2.78)).unwrap();
        assert!(d1.to_vec() < d2.to_vec());
        assert!(d2.to_vec() < d3.to_vec());
    }

    #[test]
    fn sortable_decode_roundtrip() {
        // Verify encode/decode roundtrip for sortable encoding
        assert_eq!(decode_i32_sortable(encode_i32_sortable(42)), 42);
        assert_eq!(decode_i32_sortable(encode_i32_sortable(-42)), -42);
        assert_eq!(decode_i32_sortable(encode_i32_sortable(0)), 0);
        assert_eq!(decode_i32_sortable(encode_i32_sortable(i32::MIN)), i32::MIN);
        assert_eq!(decode_i32_sortable(encode_i32_sortable(i32::MAX)), i32::MAX);

        assert_eq!(
            decode_i64_sortable(encode_i64_sortable(1_000_000)),
            1_000_000
        );
        assert_eq!(
            decode_i64_sortable(encode_i64_sortable(-1_000_000)),
            -1_000_000
        );

        assert_eq!(decode_f64_sortable(encode_f64_sortable(2.78)), 2.78);
        assert_eq!(decode_f64_sortable(encode_f64_sortable(-2.78)), -2.78);
        assert_eq!(decode_f64_sortable(encode_f64_sortable(0.0)), 0.0);
    }
}
