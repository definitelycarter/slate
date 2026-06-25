use std::borrow::Cow;

use crate::encoding::bson_value::BsonValue;
use crate::traits::FunctionKind;

const COLLECTION_TAG: u8 = b'c';
const INDEX_CONFIG_TAG: u8 = b'x';
const RECORD_TAG: u8 = b'r';
const INDEX_TAG: u8 = b'i';
const UNIQUE_INDEX_TAG: u8 = b'u';
const TRIGGER_TAG: u8 = b't';
const VALIDATOR_TAG: u8 = b'v';
const DERIVED_TAG: u8 = b'd';
const SEP: u8 = 0x00;

/// Separator between the component field names of a compound index's identity.
///
/// A compound index on `["status", "created_at"]` is identified by the single
/// "field" string `status\x01created_at`, so the whole compound key machinery
/// (prefix scans, `IndexField`, `drop_index`, purge) treats it as one opaque
/// field segment — only the value encode/decode and the planner understand the
/// internal structure. `\x01` is distinct from the `\x00` key separator and
/// cannot collide with a real field-name byte (field names are dotted
/// identifiers and `.[]`, never control characters).
pub(crate) const FIELD_SEP: u8 = 0x01;

// Prefix-isolation invariant: `FIELD_SEP > SEP`.
//
// A single-field index `f` and a compound index whose first component is `f`
// share the value prefix `i\0{collection}\0f`, but their key prefixes diverge at
// the next byte — the single index has `SEP` (0x00) there (`…\0f\0…`), the
// compound has `FIELD_SEP` (0x01) (`…\0f\x01g\0…`). Because `FIELD_SEP > SEP`,
// the compound's `IndexField` prefix sorts *after* the single's, so the two
// scan ranges never overlap and a `status` scan can't sweep in `status\x01…`
// entries (and vice versa). The single/compound prefix isolation test
// (`tests/kv.rs::single_and_compound_index_prefixes_do_not_leak`) exercises
// this at the engine level.
const _: () = assert!(
    FIELD_SEP > SEP,
    "FIELD_SEP must sort after SEP so single-field and compound index prefixes isolate"
);

/// Join component field names into one compound-index identity string.
///
/// A single-field index round-trips to its own name (`["age"]` → `"age"`), so
/// the existing single-field path is the `len == 1` case of the compound path.
pub fn join_index_fields(fields: &[String]) -> String {
    fields.join(std::str::from_utf8(&[FIELD_SEP]).unwrap_or("\u{1}"))
}

/// Split a compound-index identity string back into its component field names.
///
/// The inverse of [`join_index_fields`]. A plain (non-compound) identity yields
/// a single-element vector.
pub fn split_index_fields(field: &str) -> Vec<String> {
    field.split(FIELD_SEP as char).map(str::to_string).collect()
}

fn function_tag(kind: FunctionKind) -> u8 {
    match kind {
        FunctionKind::Trigger => TRIGGER_TAG,
        FunctionKind::Validator => VALIDATOR_TAG,
        FunctionKind::Udf => DERIVED_TAG,
    }
}

fn tag_to_function_kind(tag: u8) -> Option<FunctionKind> {
    match tag {
        TRIGGER_TAG => Some(FunctionKind::Trigger),
        VALIDATOR_TAG => Some(FunctionKind::Validator),
        DERIVED_TAG => Some(FunctionKind::Udf),
        _ => None,
    }
}

/// Parse just the collection and field from an `i`-tagged index key.
///
/// Validates the `i\x00` prefix and the two separators. Does **not** resolve the
/// value/doc_id boundary — that lives in the entry's metadata (the value-side
/// precomputed offsets), which a bare key does not carry. Returns
/// `(collection, field)`.
pub(crate) fn parse_index_collection_field(key: &[u8]) -> Option<(&str, &str)> {
    if key.len() < 2 || key[0] != INDEX_TAG || key[1] != SEP {
        return None;
    }
    let rest = &key[2..];
    let first_sep = rest.iter().position(|&b| b == SEP)?;
    let collection = std::str::from_utf8(&rest[..first_sep]).ok()?;
    let after_collection = &rest[first_sep + 1..];
    let second_sep = after_collection.iter().position(|&b| b == SEP)?;
    let field = std::str::from_utf8(&after_collection[..second_sep]).ok()?;
    Some((collection, field))
}

/// Width of one component's value-side end-offset in an index entry's metadata.
///
/// `u32` (not `u16`) so an indexed string longer than 64 KiB cannot silently
/// truncate the recorded offset and corrupt the value/doc_id boundary. The
/// offsets are the single source of truth for the value/doc_id boundary, replacing
/// the old key-side length suffixes — see [`Key::encode_index_key_into`] and
/// [`IndexEntry`](crate::IndexEntry).
pub(crate) const VALUE_OFFSET_WIDTH: usize = 4;

/// Structured key for engine storage operations.
///
/// - `Collection(cf, name)` — collection metadata in `_sys_`
/// - `IndexConfig(cf, collection, field)` — index metadata in `_sys_`
/// - `FunctionConfig(kind, cf, collection, name)` — function metadata in `_sys_`
/// - `Record(collection, doc_id)` — document record addressing
///
/// `doc_id` is encoded as `[bson_type: 1][len: 2 BE][id_bytes]` in keys,
/// and stored as the full encoded block (type + length + bytes) in the enum.
///
/// Index (`i` tag) entries are not a `Key` variant: their value/doc_id boundary
/// cannot be resolved from the key alone (it needs the metadata type byte), so
/// they are built with [`Key::encode_index_key`] and read via `IndexRecord` /
/// `IndexEntry`, which carry the metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key<'a> {
    Collection(Cow<'a, str>, Cow<'a, str>),
    IndexConfig(Cow<'a, str>, Cow<'a, str>, Cow<'a, str>),
    FunctionConfig(FunctionKind, Cow<'a, str>, Cow<'a, str>, Cow<'a, str>),
    Record(Cow<'a, str>, BsonValue<'a>),
}

impl<'a> Key<'a> {
    /// Encode a key to bytes.
    ///
    /// - `Collection`: `c\x00{cf}\x00{name}`
    /// - `IndexConfig`: `x\x00{cf}\x00{collection}\x00{field}`
    /// - `FunctionConfig`: `{tag}\x00{cf}\x00{collection}\x00{name}`
    /// - `Record`: `r\x00{collection}\x00[doc_id_encoded]`
    ///
    /// Index (`i`) keys are encoded separately via
    /// [`encode_index_key`](Key::encode_index_key).
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Key::Collection(cf, name) => {
                let mut buf = Vec::with_capacity(2 + cf.len() + 1 + name.len());
                buf.push(COLLECTION_TAG);
                buf.push(SEP);
                buf.extend_from_slice(cf.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(name.as_bytes());
                buf
            }
            Key::IndexConfig(cf, collection, field) => {
                let mut buf =
                    Vec::with_capacity(2 + cf.len() + 1 + collection.len() + 1 + field.len());
                buf.push(INDEX_CONFIG_TAG);
                buf.push(SEP);
                buf.extend_from_slice(cf.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(field.as_bytes());
                buf
            }
            Key::FunctionConfig(kind, cf, collection, name) => {
                let mut buf =
                    Vec::with_capacity(2 + cf.len() + 1 + collection.len() + 1 + name.len());
                buf.push(function_tag(*kind));
                buf.push(SEP);
                buf.extend_from_slice(cf.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(name.as_bytes());
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
        }
    }

    /// Encode an `Index` key from borrowed parts into `buf`.
    ///
    /// Takes the parts directly rather than a `Key::Index` enum, so it never
    /// clones the `doc_id` (building the enum would require an owned
    /// `BsonValue`). The caller supplies the buffer, which is cleared first;
    /// reuse one `buf` across a batch to amortize the allocation.
    ///
    /// Layout: `i\x00{collection}\x00{field}\x00{value_bytes}[doc_id_encoded]`.
    ///
    /// There is no separator (and no trailing length suffix) between value_bytes
    /// and doc_id: the value/doc_id boundary is recovered from the **value-side**
    /// precomputed offset in the entry's metadata (see [`IndexRecord::encode`]),
    /// not from the key, so the key carries only the sort-relevant bytes.
    pub fn encode_index_key_into(
        buf: &mut Vec<u8>,
        collection: &str,
        field: &str,
        value: &BsonValue<'_>,
        doc_id: &BsonValue<'_>,
    ) {
        let value_bytes: &[u8] = &value.bytes;
        buf.clear();
        buf.reserve(
            2 + collection.len() + 1 + field.len() + 1 + value_bytes.len() + 3 + doc_id.bytes.len(),
        );
        buf.push(INDEX_TAG);
        buf.push(SEP);
        buf.extend_from_slice(collection.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(field.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(value_bytes);
        doc_id.write_length_prefixed(buf);
    }

    /// Allocating convenience over
    /// [`encode_index_key_into`](Key::encode_index_key_into).
    pub fn encode_index_key(
        collection: &str,
        field: &str,
        value: &BsonValue<'_>,
        doc_id: &BsonValue<'_>,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        Self::encode_index_key_into(&mut buf, collection, field, value, doc_id);
        buf
    }

    /// Encode a *compound* index key from N component values into `buf`.
    ///
    /// Layout: `i\x00{collection}\x00{field}\x00{v1}{v2}…{vN}[doc_id_lp]`, where
    /// `field` is the joined compound identity (`f1\x01f2`) and the component
    /// value bytes are concatenated in field order. Per-component value/doc_id
    /// boundaries are recovered from the **value-side** precomputed offsets in
    /// the entry's metadata (see [`IndexRecord::encode_compound`]), so the key
    /// carries no trailing length suffixes.
    ///
    /// The seek-relevant prefix is just the concatenated value bytes: a
    /// leading-field equality seek is a plain byte prefix. With N == 1 this
    /// produces byte-identical output to [`encode_index_key_into`].
    pub fn encode_compound_index_key_into(
        buf: &mut Vec<u8>,
        collection: &str,
        field: &str,
        values: &[BsonValue<'_>],
        doc_id: &BsonValue<'_>,
    ) {
        buf.clear();
        buf.push(INDEX_TAG);
        buf.push(SEP);
        buf.extend_from_slice(collection.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(field.as_bytes());
        buf.push(SEP);
        for value in values {
            buf.extend_from_slice(&value.bytes);
        }
        doc_id.write_length_prefixed(buf);
    }

    /// Encode a record key from borrowed parts, avoiding `BsonValue` clone.
    ///
    /// Layout: `r\x00{collection}\x00[doc_id_encoded]`
    pub fn encode_record_key(collection: &str, doc_id: &BsonValue<'_>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + collection.len() + 1 + 3 + doc_id.bytes.len());
        buf.push(RECORD_TAG);
        buf.push(SEP);
        buf.extend_from_slice(collection.as_bytes());
        buf.push(SEP);
        doc_id.write_length_prefixed(&mut buf);
        buf
    }

    /// Encode a unique-index key: `u\x00{collection}\x00{field}\x00{value_bytes}`.
    ///
    /// Unlike [`encode_index_key`](Key::encode_index_key), the doc_id is **not**
    /// part of the key — a unique index holds at most one entry per value. The owning
    /// doc_id is stored in the entry's value instead. Because there is no
    /// doc_id suffix, the value bytes run to the end of the key, so embedded
    /// `\x00` bytes in the value are unambiguous.
    pub fn encode_unique_index(collection: &str, field: &str, value_bytes: &[u8]) -> Vec<u8> {
        let mut buf =
            Vec::with_capacity(2 + collection.len() + 1 + field.len() + 1 + value_bytes.len());
        buf.push(UNIQUE_INDEX_TAG);
        buf.push(SEP);
        buf.extend_from_slice(collection.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(field.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(value_bytes);
        buf
    }

    /// Decode a unique-index key into `(collection, field, value_bytes)`.
    ///
    /// Layout: `u\x00{collection}\x00{field}\x00{value_bytes}`.
    pub fn decode_unique_index(key: &'a [u8]) -> Option<(&'a str, &'a str, &'a [u8])> {
        if key.len() < 2 || key[0] != UNIQUE_INDEX_TAG || key[1] != SEP {
            return None;
        }
        let rest = &key[2..];
        let first_sep = rest.iter().position(|&b| b == SEP)?;
        let collection = std::str::from_utf8(&rest[..first_sep]).ok()?;
        let after_collection = &rest[first_sep + 1..];
        let second_sep = after_collection.iter().position(|&b| b == SEP)?;
        let field = std::str::from_utf8(&after_collection[..second_sep]).ok()?;
        let value_bytes = &after_collection[second_sep + 1..];
        Some((collection, field, value_bytes))
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
                // c\x00{cf}\x00{name}
                let sep = rest.iter().position(|&b| b == SEP)?;
                let cf = std::str::from_utf8(&rest[..sep]).ok()?;
                let name = std::str::from_utf8(&rest[sep + 1..]).ok()?;
                Some(Key::Collection(Cow::Borrowed(cf), Cow::Borrowed(name)))
            }
            RECORD_TAG => {
                // r\x00{collection}\x00[type][len][id_bytes]
                let sep = rest.iter().position(|&b| b == SEP)?;
                let collection = std::str::from_utf8(&rest[..sep]).ok()?;
                let (bv, _) = BsonValue::parse_length_prefixed(&rest[sep + 1..])?;
                Some(Key::Record(Cow::Borrowed(collection), bv))
            }
            INDEX_CONFIG_TAG => {
                // x\x00{cf}\x00{collection}\x00{field}
                let first_sep = rest.iter().position(|&b| b == SEP)?;
                let cf = std::str::from_utf8(&rest[..first_sep]).ok()?;
                let after_cf = &rest[first_sep + 1..];
                let second_sep = after_cf.iter().position(|&b| b == SEP)?;
                let collection = std::str::from_utf8(&after_cf[..second_sep]).ok()?;
                let field = std::str::from_utf8(&after_cf[second_sep + 1..]).ok()?;
                Some(Key::IndexConfig(
                    Cow::Borrowed(cf),
                    Cow::Borrowed(collection),
                    Cow::Borrowed(field),
                ))
            }
            INDEX_TAG => {
                // An `i` key's value/doc_id boundary cannot be resolved from the
                // key alone — it needs the entry's metadata type byte (fixed-width
                // length) or the trailing suffix it implies (variable-width). Decode
                // index entries through `IndexRecord::from_pair` / `IndexEntry`,
                // which carry that metadata. Bare `Key::decode` does not handle them.
                let _ = rest;
                None
            }
            other => {
                // {tag}\x00{cf}\x00{collection}\x00{name}
                let kind = tag_to_function_kind(other)?;
                let first_sep = rest.iter().position(|&b| b == SEP)?;
                let cf = std::str::from_utf8(&rest[..first_sep]).ok()?;
                let after_cf = &rest[first_sep + 1..];
                let second_sep = after_cf.iter().position(|&b| b == SEP)?;
                let collection = std::str::from_utf8(&after_cf[..second_sep]).ok()?;
                let name = std::str::from_utf8(&after_cf[second_sep + 1..]).ok()?;
                Some(Key::FunctionConfig(
                    kind,
                    Cow::Borrowed(cf),
                    Cow::Borrowed(collection),
                    Cow::Borrowed(name),
                ))
            }
        }
    }
}

/// Structured prefix for scan operations.
///
/// Each variant represents a partial key used as a scan prefix.
///
/// - `Collection` — all collection metadata keys (`c\x00`)
/// - `CollectionByCf(cf)` — collection metadata for a specific CF (`c\x00{cf}\x00`)
/// - `IndexConfig(cf, collection)` — index configs for a collection (`x\x00{cf}\x00{collection}\x00`)
/// - `FunctionConfig(kind, cf, collection)` — function configs (`{tag}\x00{cf}\x00{collection}\x00`)
/// - `Record(collection)` — all document records in a collection (`r\x00{collection}\x00`)
/// - `IndexField(collection, field)` — index entries for a field (`i\x00{collection}\x00{field}\x00`)
/// - `IndexValue(collection, field, value)` — index entries for a specific value
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPrefix<'a> {
    Collection,
    CollectionByCf(Cow<'a, str>),
    IndexConfig(Cow<'a, str>, Cow<'a, str>),
    FunctionConfig(FunctionKind, Cow<'a, str>, Cow<'a, str>),
    Record(Cow<'a, str>),
    IndexField(Cow<'a, str>, Cow<'a, str>),
    IndexValue(Cow<'a, str>, Cow<'a, str>, &'a [u8]),
    /// All unique-index entries for a field (`u\x00{collection}\x00{field}\x00`).
    UniqueIndexField(Cow<'a, str>, Cow<'a, str>),
}

impl<'a> KeyPrefix<'a> {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            KeyPrefix::Collection => vec![COLLECTION_TAG, SEP],
            KeyPrefix::CollectionByCf(cf) => {
                let mut buf = Vec::with_capacity(2 + cf.len() + 1);
                buf.push(COLLECTION_TAG);
                buf.push(SEP);
                buf.extend_from_slice(cf.as_bytes());
                buf.push(SEP);
                buf
            }
            KeyPrefix::IndexConfig(cf, collection) => {
                let mut buf = Vec::with_capacity(2 + cf.len() + 1 + collection.len() + 1);
                buf.push(INDEX_CONFIG_TAG);
                buf.push(SEP);
                buf.extend_from_slice(cf.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf
            }
            KeyPrefix::FunctionConfig(kind, cf, collection) => {
                let mut buf = Vec::with_capacity(2 + cf.len() + 1 + collection.len() + 1);
                buf.push(function_tag(*kind));
                buf.push(SEP);
                buf.extend_from_slice(cf.as_bytes());
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
            KeyPrefix::UniqueIndexField(collection, field) => {
                let mut buf = Vec::with_capacity(2 + collection.len() + 1 + field.len() + 1);
                buf.push(UNIQUE_INDEX_TAG);
                buf.push(SEP);
                buf.extend_from_slice(collection.as_bytes());
                buf.push(SEP);
                buf.extend_from_slice(field.as_bytes());
                buf.push(SEP);
                buf
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::spec::ElementType;

    /// Helper: build a string BsonValue.
    fn str_id(s: &str) -> BsonValue<'_> {
        BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(s.as_bytes()),
        }
    }

    /// Helper: build an ObjectId BsonValue.
    fn oid_id(bytes: &[u8; 12]) -> BsonValue<'_> {
        BsonValue {
            tag: ElementType::ObjectId,
            bytes: Cow::Borrowed(bytes.as_slice()),
        }
    }

    /// Helper: encode an index entry, then recover its `(value_bytes, doc_id)`.
    ///
    /// The value/doc_id boundary now lives in the entry's *metadata* (the
    /// value-side precomputed offsets), not in the key — so this helper takes the
    /// boundary from the known `value.bytes.len()` (which the production write side
    /// records into the metadata offsets) and confirms the doc_id parses straight
    /// after the value bytes, with NO trailing length suffix.
    fn roundtrip_index_value<'b>(
        collection: &str,
        field: &str,
        value: &BsonValue<'b>,
        doc_id: &BsonValue<'_>,
    ) -> (Vec<u8>, BsonValue<'static>) {
        let bytes = Key::encode_index_key(collection, field, value, doc_id);
        let (c, f) = parse_index_collection_field(&bytes).unwrap();
        assert_eq!(c, collection);
        assert_eq!(f, field);
        let value_start = 2 + collection.len() + 1 + field.len() + 1;
        let value_len = value.bytes.len();
        let recovered_value = bytes[value_start..value_start + value_len].to_vec();
        let (id, rest) =
            BsonValue::parse_length_prefixed(&bytes[value_start + value_len..]).unwrap();
        // No trailing suffix: the doc_id runs to the very end of the key.
        assert!(rest.is_empty(), "index key must have no trailing suffix");
        (recovered_value, id.into_owned())
    }

    #[test]
    fn collection_key_roundtrip() {
        let key = Key::Collection(Cow::Borrowed("default_cf"), Cow::Borrowed("users"));
        let bytes = key.encode();
        assert_eq!(bytes, b"c\x00default_cf\x00users");
        let decoded = Key::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn collection_key_different_cfs() {
        let k1 = Key::Collection(Cow::Borrowed("cf1"), Cow::Borrowed("accounts"));
        let k2 = Key::Collection(Cow::Borrowed("cf2"), Cow::Borrowed("accounts"));
        assert_ne!(k1.encode(), k2.encode());
        assert_eq!(Key::decode(&k1.encode()).unwrap(), k1);
        assert_eq!(Key::decode(&k2.encode()).unwrap(), k2);
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
        let oid = [
            0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11,
        ];
        let key = Key::Record(Cow::Borrowed("users"), oid_id(&oid));
        let bytes = key.encode();
        let decoded = Key::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn index_key_string_value_roundtrip() {
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"alice@example.com"),
        };
        let doc_id = str_id("doc-123");
        let (value_bytes, id) = roundtrip_index_value("users", "email", &value, &doc_id);
        assert_eq!(value_bytes, b"alice@example.com");
        assert_eq!(id, doc_id);
    }

    #[test]
    fn index_key_string_value_collides_with_docid_header() {
        // Value bytes that embed a valid length-prefixed header (`[0x02][0x00][0x00]`)
        // — the collision the old backward scan mis-split. The value-side offset
        // (in the metadata) keeps the boundary exact regardless of doc_id type, so
        // the helper recovers the full value bytes.
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"active\x02\x00\x00\x00"),
        };
        for doc_id in [
            str_id("rec-1"),
            oid_id(&[
                0x00, 0x00, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x00,
            ]),
        ] {
            let (value_bytes, id) = roundtrip_index_value("users", "status", &value, &doc_id);
            assert_eq!(value_bytes, b"active\x02\x00\x00\x00");
            assert_eq!(id, doc_id);
        }
    }

    #[test]
    fn index_key_fixed_width_value_has_no_suffix() {
        // Fixed-width (Int64) value: `{value(8)}{doc_id_lp}` with no trailing suffix.
        let value = BsonValue {
            tag: ElementType::Int64,
            bytes: Cow::Borrowed(&[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A]),
        };
        let doc_id = str_id("rec-1");
        let bytes = Key::encode_index_key("scores", "rank", &value, &doc_id);
        let value_start = 2 + "scores".len() + 1 + "rank".len() + 1;
        // 8 value bytes + length-prefixed doc_id ("rec-1" = 5) + no suffix.
        assert_eq!(bytes.len(), value_start + 8 + (3 + 5));

        let (recovered, id) = roundtrip_index_value("scores", "rank", &value, &doc_id);
        assert_eq!(recovered, value.bytes.as_ref());
        assert_eq!(id, doc_id);
    }

    #[test]
    fn index_key_variable_width_value_has_no_suffix() {
        // String value: with value-side offsets, the key no longer carries a
        // trailing length suffix — it ends right after the length-prefixed doc_id.
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"hello"),
        };
        let doc_id = str_id("rec-1");
        let bytes = Key::encode_index_key("k", "f", &value, &doc_id);
        let value_start = 2 + "k".len() + 1 + "f".len() + 1;
        // 5 value bytes + length-prefixed doc_id (5), no suffix.
        assert_eq!(bytes.len(), value_start + 5 + (3 + 5));

        let (recovered, id) = roundtrip_index_value("k", "f", &value, &doc_id);
        assert_eq!(recovered, b"hello");
        assert_eq!(id, doc_id);
    }

    #[test]
    fn decode_returns_none_for_index_key() {
        // `Key::decode` cannot resolve an `i` key's value boundary without the
        // metadata value-side offsets, so it does not handle index keys.
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"v"),
        };
        let bytes = Key::encode_index_key("users", "email", &value, &str_id("doc-1"));
        assert!(Key::decode(&bytes).is_none());
    }

    #[test]
    fn join_split_index_fields_round_trip() {
        let fields = vec!["status".to_string(), "created_at".to_string()];
        let identity = join_index_fields(&fields);
        assert_eq!(identity, "status\u{1}created_at");
        assert_eq!(split_index_fields(&identity), fields);
        // A single field joins to itself.
        assert_eq!(join_index_fields(&["age".to_string()]), "age");
        assert_eq!(split_index_fields("age"), vec!["age".to_string()]);
    }

    #[test]
    fn compound_key_round_trips_string_plus_numeric() {
        // status (var-width) + created_at (fixed-width Int64): the key is the
        // concatenated value bytes then the length-prefixed doc_id, with NO
        // trailing suffix (the per-component boundaries live in the metadata's
        // value-side offsets, validated by the IndexRecord/IndexEntry tests).
        let status = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"active"),
        };
        let created = BsonValue {
            tag: ElementType::Int64,
            bytes: Cow::Borrowed(&[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14]),
        };
        let doc_id = str_id("order-1");
        let values = vec![status.clone(), created.clone()];

        let mut key = Vec::new();
        Key::encode_compound_index_key_into(
            &mut key,
            "orders",
            "status\u{1}created_at",
            &values,
            &doc_id,
        );

        let field = "status\u{1}created_at";
        let value_start = 2 + "orders".len() + 1 + field.len() + 1;
        // status (6) + created (8) = 14 total value bytes, then the doc_id.
        let total_value_len = status.bytes.len() + created.bytes.len();
        assert_eq!(total_value_len, 14);
        assert_eq!(&key[value_start..value_start + 6], b"active");
        let (id, rest) =
            BsonValue::parse_length_prefixed(&key[value_start + total_value_len..]).unwrap();
        assert_eq!(id, doc_id);
        // No trailing suffix — the doc_id runs to the end of the key.
        assert!(rest.is_empty(), "compound index key must have no suffix");
    }

    #[test]
    fn compound_key_two_var_width_has_no_suffix() {
        // Two strings: the key concatenates both then the doc_id, with no trailing
        // length suffixes (boundaries are metadata-side now).
        let a = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"hello"),
        };
        let b = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"worldd"),
        };
        let doc_id = str_id("d1");
        let total_value_len = a.bytes.len() + b.bytes.len(); // 5 + 6 = 11
        let values = vec![a, b];
        let mut key = Vec::new();
        Key::encode_compound_index_key_into(&mut key, "k", "a\u{1}b", &values, &doc_id);

        let value_start = 2 + "k".len() + 1 + "a\u{1}b".len() + 1;
        // value bytes + length-prefixed doc_id ("d1" = 2), no suffix.
        assert_eq!(key.len(), value_start + total_value_len + (3 + 2));
        let (id, rest) =
            BsonValue::parse_length_prefixed(&key[value_start + total_value_len..]).unwrap();
        assert_eq!(id, doc_id);
        assert!(rest.is_empty(), "compound index key must have no suffix");
    }

    #[test]
    fn function_config_key_roundtrip() {
        for kind in [
            FunctionKind::Trigger,
            FunctionKind::Validator,
            FunctionKind::Udf,
        ] {
            let key = Key::FunctionConfig(
                kind,
                Cow::Borrowed("default_cf"),
                Cow::Borrowed("users"),
                Cow::Borrowed("audit_log"),
            );
            let bytes = key.encode();
            let decoded = Key::decode(&bytes).unwrap();
            assert_eq!(decoded, key);
        }
    }

    #[test]
    fn function_config_tag_bytes() {
        let trigger = Key::FunctionConfig(
            FunctionKind::Trigger,
            Cow::Borrowed("cf"),
            Cow::Borrowed("users"),
            Cow::Borrowed("f"),
        );
        assert_eq!(trigger.encode()[0], b't');

        let validator = Key::FunctionConfig(
            FunctionKind::Validator,
            Cow::Borrowed("cf"),
            Cow::Borrowed("users"),
            Cow::Borrowed("f"),
        );
        assert_eq!(validator.encode()[0], b'v');

        let computed = Key::FunctionConfig(
            FunctionKind::Udf,
            Cow::Borrowed("cf"),
            Cow::Borrowed("users"),
            Cow::Borrowed("f"),
        );
        assert_eq!(computed.encode()[0], b'd');
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
