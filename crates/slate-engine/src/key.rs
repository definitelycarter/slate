use std::borrow::Cow;

use crate::encoding::bson_value::BsonValue;

const COLLECTION_TAG: u8 = b'c';
const INDEX_CONFIG_TAG: u8 = b'x';
const RECORD_TAG: u8 = b'r';
const INDEX_TAG: u8 = b'i';
const INDEX_MAP_TAG: u8 = b'j';
const SEP: u8 = 0x00;

/// Find the trailing length-prefixed doc_id at the end of a byte slice.
///
/// Returns `(value_bytes, BsonValue)`.
fn split_trailing_doc_id(bytes: &[u8]) -> Option<(&[u8], BsonValue<'_>)> {
    // Length-prefixed header: 1 type byte + 2 length bytes.
    const LP_HEADER: usize = 3;
    if bytes.len() < LP_HEADER {
        return None;
    }
    // Scan backwards for a valid doc_id header.
    for start in (0..=bytes.len().saturating_sub(LP_HEADER)).rev() {
        let candidate = &bytes[start..];
        if let Some((bv, rest)) = BsonValue::parse_length_prefixed(candidate) {
            if rest.is_empty() {
                return Some((&bytes[..start], bv));
            }
        }
    }
    None
}

/// Structured key for engine storage operations.
///
/// - `Collection(name)` — collection metadata in `_sys_`
/// - `IndexConfig(collection, field)` — index metadata in `_sys_`
/// - `Index(collection, field, doc_id)` — value-first index entry (`i` tag)
/// - `IndexMap(collection, field, doc_id)` — record-first index entry (`j` tag, internal)
/// - `Record(collection, doc_id)` — document record addressing
///
/// `doc_id` is encoded as `[bson_type: 1][len: 2 BE][id_bytes]` in keys,
/// and stored as the full encoded block (type + length + bytes) in the enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key<'a> {
    Collection(Cow<'a, str>),
    IndexConfig(Cow<'a, str>, Cow<'a, str>),
    Index(Cow<'a, str>, Cow<'a, str>, BsonValue<'a>),
    IndexMap(Cow<'a, str>, Cow<'a, str>, BsonValue<'a>),
    Record(Cow<'a, str>, BsonValue<'a>),
}

impl<'a> Key<'a> {
    /// Encode a key to bytes.
    ///
    /// - `Collection`: `c\x00{name}`
    /// - `IndexConfig`: `x\x00{collection}\x00{field}`
    /// - `Record`: `r\x00{collection}\x00[doc_id_encoded]`
    /// - `Index` (no value): `i\x00{collection}\x00{field}\x00\x00[doc_id_encoded]`
    /// - `IndexMap` (no value): `j\x00{collection}\x00{field}\x00[doc_id_encoded]\x00`
    ///
    /// For `Index`/`IndexMap` keys with value bytes, use
    /// [`encode_index`](Key::encode_index) / [`encode_index_map`](Key::encode_index_map).
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Key::Collection(name) => {
                let mut buf = Vec::with_capacity(2 + name.len());
                buf.push(COLLECTION_TAG);
                buf.push(SEP);
                buf.extend_from_slice(name.as_bytes());
                buf
            }
            Key::IndexConfig(collection, field) => {
                let mut buf = Vec::with_capacity(2 + collection.len() + 1 + field.len());
                buf.push(INDEX_CONFIG_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(field.as_bytes());
                buf
            }
            Key::Record(collection, doc_id) => {
                let mut buf = Vec::with_capacity(2 + collection.len() + 1 + 3 + doc_id.bytes.len());
                buf.push(RECORD_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                doc_id.write_length_prefixed(&mut buf);
                buf
            }
            Key::Index(collection, field, doc_id) => {
                let mut buf = Vec::with_capacity(
                    2 + collection.len() + 1 + field.len() + 1 + 1 + 3 + doc_id.bytes.len(),
                );
                buf.push(INDEX_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(field.as_bytes());
                buf.push(SEP);
                // empty value bytes
                buf.push(SEP);
                doc_id.write_length_prefixed(&mut buf);
                buf
            }
            Key::IndexMap(collection, field, doc_id) => {
                let mut buf = Vec::with_capacity(
                    2 + collection.len() + 1 + field.len() + 1 + 3 + doc_id.bytes.len() + 1,
                );
                buf.push(INDEX_MAP_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(field.as_bytes());
                buf.push(SEP);
                doc_id.write_length_prefixed(&mut buf);
                buf.push(SEP);
                // empty value bytes
                buf
            }
        }
    }

    /// Encode an `Index` key with the given encoded value bytes.
    ///
    /// Layout: `i\x00{collection}\x00{field}\x00{value_bytes}[doc_id_encoded]`
    ///
    /// Note: no separator between value_bytes and doc_id — the doc_id is
    /// length-prefixed so we know exactly where it starts.
    ///
    /// Panics if `self` is not an `Index` variant.
    pub fn encode_index(&self, value_bytes: &[u8]) -> Vec<u8> {
        let Key::Index(collection, field, doc_id) = self else {
            panic!("encode_index called on non-Index key");
        };
        let mut buf = Vec::with_capacity(
            2 + collection.len() + 1 + field.len() + 1 + value_bytes.len() + 3 + doc_id.bytes.len(),
        );
        buf.push(INDEX_TAG);
        buf.push(SEP);
        buf.extend_from_slice(collection.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(field.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(value_bytes);
        doc_id.write_length_prefixed(&mut buf);
        buf
    }

    /// Encode an `IndexMap` key with the given encoded value bytes.
    ///
    /// Layout: `j\x00{collection}\x00{field}\x00[doc_id_encoded]{value_bytes}`
    ///
    /// Panics if `self` is not an `IndexMap` variant.
    pub fn encode_index_map(&self, value_bytes: &[u8]) -> Vec<u8> {
        let Key::IndexMap(collection, field, doc_id) = self else {
            panic!("encode_index_map called on non-IndexMap key");
        };
        let mut buf = Vec::with_capacity(
            2 + collection.len() + 1 + field.len() + 1 + 3 + doc_id.bytes.len() + value_bytes.len(),
        );
        buf.push(INDEX_MAP_TAG);
        buf.push(SEP);
        buf.extend_from_slice(collection.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(field.as_bytes());
        buf.push(SEP);
        doc_id.write_length_prefixed(&mut buf);
        buf.extend_from_slice(value_bytes);
        buf
    }

    /// Decode a key from its byte representation.
    ///
    /// Returns `None` if the bytes don't match any known key format.
    /// String fields (collection, field) borrow from the input; doc_id is
    /// owned because the length prefix must be stripped.
    pub fn decode(bytes: &'a [u8]) -> Option<Key<'a>> {
        if bytes.len() < 2 || bytes[1] != SEP {
            return None;
        }
        let tag = bytes[0];
        let rest = &bytes[2..];
        match tag {
            COLLECTION_TAG => {
                let name = std::str::from_utf8(rest).ok()?;
                Some(Key::Collection(Cow::Borrowed(name)))
            }
            RECORD_TAG => {
                // r\x00{collection}\x00[type][len][id_bytes]
                let sep = rest.iter().position(|&b| b == SEP)?;
                let collection = std::str::from_utf8(&rest[..sep]).ok()?;
                let (bv, _) = BsonValue::parse_length_prefixed(&rest[sep + 1..])?;
                Some(Key::Record(Cow::Borrowed(collection), bv))
            }
            INDEX_CONFIG_TAG => {
                let sep = rest.iter().position(|&b| b == SEP)?;
                let collection = std::str::from_utf8(&rest[..sep]).ok()?;
                let field = std::str::from_utf8(&rest[sep + 1..]).ok()?;
                Some(Key::IndexConfig(
                    Cow::Borrowed(collection),
                    Cow::Borrowed(field),
                ))
            }
            INDEX_TAG => {
                // i\x00{collection}\x00{field}\x00{value_bytes}[type][len][id_bytes]
                let first_sep = rest.iter().position(|&b| b == SEP)?;
                let collection = std::str::from_utf8(&rest[..first_sep]).ok()?;
                let after_collection = &rest[first_sep + 1..];
                let second_sep = after_collection.iter().position(|&b| b == SEP)?;
                let field = std::str::from_utf8(&after_collection[..second_sep]).ok()?;
                let after_field = &after_collection[second_sep + 1..];
                let (_value_bytes, bv) = split_trailing_doc_id(after_field)?;
                Some(Key::Index(
                    Cow::Borrowed(collection),
                    Cow::Borrowed(field),
                    bv,
                ))
            }
            INDEX_MAP_TAG => {
                // j\x00{collection}\x00{field}\x00[type][len][id_bytes]{value_bytes}
                let first_sep = rest.iter().position(|&b| b == SEP)?;
                let collection = std::str::from_utf8(&rest[..first_sep]).ok()?;
                let after_collection = &rest[first_sep + 1..];
                let second_sep = after_collection.iter().position(|&b| b == SEP)?;
                let field = std::str::from_utf8(&after_collection[..second_sep]).ok()?;
                let after_field = &after_collection[second_sep + 1..];
                let (bv, _value_bytes) = BsonValue::parse_length_prefixed(after_field)?;
                Some(Key::IndexMap(
                    Cow::Borrowed(collection),
                    Cow::Borrowed(field),
                    bv,
                ))
            }
            _ => None,
        }
    }

    /// Parse the value bytes and doc_id from raw index key bytes,
    /// given the byte offset where the field separator ends.
    ///
    /// The slice after `field_sep_offset` has layout:
    /// `{value_bytes}[type][len_be16][id_bytes]`
    ///
    /// Returns `(value_bytes, raw_doc_id)` where raw_doc_id is `[type][id_bytes]`.
    pub fn parse_index_tail<'b>(
        key_bytes: &'b [u8],
        field_sep_offset: usize,
    ) -> Option<(&'b [u8], BsonValue<'b>)> {
        let after_field = key_bytes.get(field_sep_offset..)?;
        let (value_bytes, bv) = split_trailing_doc_id(after_field)?;
        Some((value_bytes, bv))
    }
}

/// Structured prefix for scan operations.
///
/// Each variant represents a partial key used as a scan prefix.
///
/// - `Collection` — all collection metadata keys (`c\x00`)
/// - `IndexConfig(collection)` — index configs for a collection (`x\x00{collection}\x00`)
/// - `Record(collection)` — all document records in a collection (`r\x00{collection}\x00`)
/// - `IndexField(collection, field)` — index entries for a field (`i\x00{collection}\x00{field}\x00`)
/// - `IndexValue(collection, field, value)` — index entries for a specific value
/// - `IndexMapRecord(collection, field, doc_id)` — index map entries for a record
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPrefix<'a> {
    Collection,
    IndexConfig(Cow<'a, str>),
    Record(Cow<'a, str>),
    IndexField(Cow<'a, str>, Cow<'a, str>),
    IndexValue(Cow<'a, str>, Cow<'a, str>, &'a [u8]),
    IndexMapRecord(Cow<'a, str>, Cow<'a, str>, BsonValue<'a>),
}

impl<'a> KeyPrefix<'a> {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            KeyPrefix::Collection => vec![COLLECTION_TAG, SEP],
            KeyPrefix::IndexConfig(collection) => {
                let mut buf = Vec::with_capacity(2 + collection.len() + 1);
                buf.push(INDEX_CONFIG_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf
            }
            KeyPrefix::Record(collection) => {
                let mut buf = Vec::with_capacity(2 + collection.len() + 1);
                buf.push(RECORD_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf
            }
            KeyPrefix::IndexField(collection, field) => {
                let mut buf = Vec::with_capacity(2 + collection.len() + 1 + field.len() + 1);
                buf.push(INDEX_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(field.as_bytes());
                buf.push(SEP);
                buf
            }
            KeyPrefix::IndexValue(collection, field, value) => {
                let mut buf =
                    Vec::with_capacity(2 + collection.len() + 1 + field.len() + 1 + value.len());
                buf.push(INDEX_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(field.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(value);
                buf
            }
            KeyPrefix::IndexMapRecord(collection, field, doc_id) => {
                let mut buf = Vec::with_capacity(
                    2 + collection.len() + 1 + field.len() + 1 + 3 + doc_id.bytes.len(),
                );
                buf.push(INDEX_MAP_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(field.as_bytes());
                buf.push(SEP);
                doc_id.write_length_prefixed(&mut buf);
                buf
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a string BsonValue.
    fn str_id(s: &str) -> BsonValue<'_> {
        BsonValue {
            tag: 0x02,
            bytes: Cow::Borrowed(s.as_bytes()),
        }
    }

    /// Helper: build an ObjectId BsonValue.
    fn oid_id(bytes: &[u8; 12]) -> BsonValue<'_> {
        BsonValue {
            tag: 0x07,
            bytes: Cow::Borrowed(bytes.as_slice()),
        }
    }

    #[test]
    fn collection_key_roundtrip() {
        let key = Key::Collection(Cow::Borrowed("users"));
        let bytes = key.encode();
        assert_eq!(bytes, b"c\x00users");
        let decoded = Key::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn record_key_string_id_roundtrip() {
        let key = Key::Record(Cow::Borrowed("users"), str_id("doc-123"));
        let bytes = key.encode();
        let decoded = Key::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn record_key_objectid_roundtrip() {
        let oid = [0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11];
        let key = Key::Record(Cow::Borrowed("users"), oid_id(&oid));
        let bytes = key.encode();
        let decoded = Key::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn index_key_roundtrip() {
        let key = Key::Index(
            Cow::Borrowed("users"),
            Cow::Borrowed("email"),
            str_id("doc-123"),
        );
        let value_bytes = b"alice@example.com";
        let bytes = key.encode_index(value_bytes);
        let decoded = Key::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn index_key_with_binary_value() {
        let key = Key::Index(
            Cow::Borrowed("scores"),
            Cow::Borrowed("rank"),
            str_id("rec-1"),
        );
        // Encoded integer that may contain \x00 bytes
        let value_bytes: &[u8] = &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A];
        let bytes = key.encode_index(value_bytes);
        let decoded = Key::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn index_key_objectid_with_null_bytes() {
        // ObjectId containing \x00 bytes
        let oid = [0x00, 0x00, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x00];
        let key = Key::Index(
            Cow::Borrowed("users"),
            Cow::Borrowed("email"),
            oid_id(&oid),
        );
        let value_bytes = b"test@example.com";
        let bytes = key.encode_index(value_bytes);
        let decoded = Key::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn index_map_key_roundtrip() {
        let key = Key::IndexMap(
            Cow::Borrowed("users"),
            Cow::Borrowed("email"),
            str_id("doc-123"),
        );
        let value_bytes = b"alice@example.com";
        let bytes = key.encode_index_map(value_bytes);
        let decoded = Key::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn parse_index_tail_with_typed_id() {
        let key = Key::Index(
            Cow::Borrowed("users"),
            Cow::Borrowed("age"),
            str_id("doc-1"),
        );
        let value_bytes: &[u8] = &[0x80, 0x00, 0x00, 0x19]; // encoded 25
        let encoded = key.encode_index(value_bytes);

        let field_prefix =
            KeyPrefix::IndexField(Cow::Borrowed("users"), Cow::Borrowed("age")).encode();
        let (parsed_value, parsed_id) =
            Key::parse_index_tail(&encoded, field_prefix.len()).unwrap();
        assert_eq!(parsed_value, value_bytes);
        assert_eq!(parsed_id, str_id("doc-1"));
    }

    #[test]
    fn decode_invalid_tag() {
        assert!(Key::decode(b"z\x00stuff").is_none());
    }

    #[test]
    fn decode_too_short() {
        assert!(Key::decode(b"r").is_none());
        assert!(Key::decode(b"").is_none());
    }

}
