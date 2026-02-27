//! Shared raw byte-level BSON primitives.
//!
//! Provides fast field lookup by scanning raw `&[u8]` BSON bytes directly,
//! avoiding the overhead of the `bson` crate's `RawDocument::get()` iterator
//! (which constructs `RawElement` objects, wraps every step in `Result`, and
//! calls `try_into()` to build `RawBsonRef`).
//!
//! Used by the mutation engine (`raw_mutation.rs`), filter evaluation, sort
//! key extraction, and projection.

use bson::raw::{RawArray, RawBsonRef, RawDocument};
use bson::spec::ElementType;
use slate_engine::skip_bson_value;

// ── RawField ────────────────────────────────────────────────────

/// A located field within raw BSON bytes.
///
/// Holds a reference to the document bytes and the field's position
/// metadata. Value parsing is deferred until `.value()` is called.
pub(crate) struct RawField<'a> {
    bytes: &'a [u8],
    element_type: ElementType,
    element_start: usize,
    value_start: usize,
    element_end: usize,
}

impl<'a> RawField<'a> {
    // ── Constructors ────────────────────────────────────────────

    /// Find a top-level field by name.
    pub fn get(bytes: &'a [u8], name: &str) -> Option<Self> {
        scan_field(bytes, 0, name).map(|loc| loc.bind(bytes))
    }

    /// Find a field by dot-path (e.g. `"address.city"` or `"a.b.c"`).
    pub fn get_path(bytes: &'a [u8], path: &str) -> Option<Self> {
        resolve_path(bytes, 0, path).map(|loc| loc.bind(bytes))
    }

    /// Resolve a dot-path and return the parsed value.
    ///
    /// Returns `None` if the path is missing or the value is Null.
    pub fn get_value(bytes: &'a [u8], path: &str) -> Option<RawBsonRef<'a>> {
        let field = Self::get_path(bytes, path)?;
        if field.is_null() {
            return None;
        }
        field.value()
    }

    // ── Accessors ───────────────────────────────────────────────

    /// The BSON element type.
    #[cfg(test)]
    pub fn element_type(&self) -> ElementType {
        self.element_type
    }

    /// Whether this field is Null.
    pub fn is_null(&self) -> bool {
        self.element_type == ElementType::Null
    }

    /// Parse the value bytes into a `RawBsonRef`.
    pub fn value(&self) -> Option<RawBsonRef<'a>> {
        match self.element_type {
            ElementType::Double => {
                let v = f64::from_le_bytes(
                    self.bytes[self.value_start..self.value_start + 8]
                        .try_into()
                        .ok()?,
                );
                Some(RawBsonRef::Double(v))
            }
            ElementType::String => {
                let len = i32::from_le_bytes(
                    self.bytes[self.value_start..self.value_start + 4]
                        .try_into()
                        .ok()?,
                ) as usize;
                let s = std::str::from_utf8(
                    &self.bytes[self.value_start + 4..self.value_start + 4 + len - 1],
                )
                .ok()?;
                Some(RawBsonRef::String(s))
            }
            ElementType::EmbeddedDocument => {
                let doc =
                    RawDocument::from_bytes(&self.bytes[self.value_start..self.element_end]).ok()?;
                Some(RawBsonRef::Document(doc))
            }
            ElementType::Array => {
                let doc =
                    RawDocument::from_bytes(&self.bytes[self.value_start..self.element_end]).ok()?;
                // SAFETY: RawArray is repr-transparent over RawDocument.
                // The bson crate's own RawArray::from_doc does this same pointer cast.
                let arr: &RawArray =
                    unsafe { &*(doc as *const RawDocument as *const RawArray) };
                Some(RawBsonRef::Array(arr))
            }
            ElementType::ObjectId => {
                let oid = bson::oid::ObjectId::from_bytes(
                    self.bytes[self.value_start..self.value_start + 12]
                        .try_into()
                        .ok()?,
                );
                Some(RawBsonRef::ObjectId(oid))
            }
            ElementType::Boolean => {
                Some(RawBsonRef::Boolean(self.bytes[self.value_start] != 0))
            }
            ElementType::DateTime => {
                let ms = i64::from_le_bytes(
                    self.bytes[self.value_start..self.value_start + 8]
                        .try_into()
                        .ok()?,
                );
                Some(RawBsonRef::DateTime(bson::DateTime::from_millis(ms)))
            }
            ElementType::Null => Some(RawBsonRef::Null),
            ElementType::Int32 => {
                let v = i32::from_le_bytes(
                    self.bytes[self.value_start..self.value_start + 4]
                        .try_into()
                        .ok()?,
                );
                Some(RawBsonRef::Int32(v))
            }
            ElementType::Int64 => {
                let v = i64::from_le_bytes(
                    self.bytes[self.value_start..self.value_start + 8]
                        .try_into()
                        .ok()?,
                );
                Some(RawBsonRef::Int64(v))
            }
            _ => None,
        }
    }

    // ── Byte-level access (for tests) ──────────────────────────

    /// The raw value bytes (`value_start..element_end`).
    #[cfg(test)]
    pub fn value_bytes(&self) -> &'a [u8] {
        &self.bytes[self.value_start..self.element_end]
    }

    /// Byte offset where the value bytes begin.
    #[cfg(test)]
    pub fn value_start(&self) -> usize {
        self.value_start
    }

    /// Byte offset immediately after the value bytes.
    #[cfg(test)]
    pub fn element_end(&self) -> usize {
        self.element_end
    }

    /// Extract a borrow-free location snapshot.
    ///
    /// Useful when you need the field's position and type but also need
    /// to mutate the underlying byte buffer (which requires dropping
    /// the `RawField` borrow first).
    pub fn loc(&self) -> RawFieldLoc {
        RawFieldLoc {
            element_type: self.element_type,
            element_start: self.element_start,
            value_start: self.value_start,
            element_end: self.element_end,
        }
    }
}

/// Borrow-free field location snapshot.
///
/// Contains the same position/type metadata as [`RawField`] but without
/// holding a reference to the document bytes. Used by the mutation engine
/// to capture a field's location then mutate the buffer.
#[derive(Clone, Copy)]
pub(crate) struct RawFieldLoc {
    element_type: ElementType,
    element_start: usize,
    value_start: usize,
    element_end: usize,
}

impl RawFieldLoc {
    pub fn element_type(&self) -> ElementType {
        self.element_type
    }

    pub fn element_start(&self) -> usize {
        self.element_start
    }

    pub fn value_start(&self) -> usize {
        self.value_start
    }

    pub fn element_end(&self) -> usize {
        self.element_end
    }
}

// ── Internal scanning primitives ────────────────────────────────
//
// These return a lightweight `FieldLoc` (no byte reference) to avoid
// carrying the lifetime through recursive dot-path descent. The final
// result is wrapped into a `RawField` via `FieldLoc::bind`.

/// Lightweight field location without a byte reference.
struct FieldLoc {
    element_type: ElementType,
    element_start: usize,
    value_start: usize,
    element_end: usize,
}

impl FieldLoc {
    fn bind(self, bytes: &[u8]) -> RawField<'_> {
        RawField {
            bytes,
            element_type: self.element_type,
            element_start: self.element_start,
            value_start: self.value_start,
            element_end: self.element_end,
        }
    }
}

/// Scan a document's raw bytes for a field by name, starting at `base`.
fn scan_field(bytes: &[u8], base: usize, field_name: &str) -> Option<FieldLoc> {
    let target = field_name.as_bytes();
    if base + 4 > bytes.len() {
        return None;
    }
    let doc_len = i32::from_le_bytes(bytes[base..base + 4].try_into().ok()?) as usize;
    let doc_end = base + doc_len;
    let mut pos = base + 4; // skip document length header

    while pos < doc_end {
        let type_byte = bytes[pos];
        if type_byte == 0x00 {
            break;
        }
        let element_type = ElementType::from(type_byte)?;
        let element_start = pos;
        pos += 1; // skip type byte

        // Read null-terminated field name
        let name_start = pos;
        while pos < doc_end && bytes[pos] != 0x00 {
            pos += 1;
        }
        if pos >= doc_end {
            return None;
        }
        let name = &bytes[name_start..pos];
        pos += 1; // skip null terminator

        let value_start = pos;
        let element_end = skip_bson_value(type_byte, bytes, pos)?;

        if name == target {
            return Some(FieldLoc {
                element_type,
                element_start,
                value_start,
                element_end,
            });
        }
        pos = element_end;
    }
    None
}

/// Resolve a dot-path (or flat field name) within a document at `base`.
fn resolve_path(bytes: &[u8], base: usize, path: &str) -> Option<FieldLoc> {
    if !path.contains('.') {
        return scan_field(bytes, base, path);
    }
    let (first, rest) = path.split_once('.')?;
    let loc = scan_field(bytes, base, first)?;
    if loc.element_type != ElementType::EmbeddedDocument {
        return None;
    }
    resolve_path(bytes, loc.value_start, rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;

    fn doc_bytes(doc: &bson::RawDocumentBuf) -> &[u8] {
        doc.as_bytes()
    }

    // ── RawField::get ─────────────────────────────────────────

    #[test]
    fn get_exists() {
        let doc = rawdoc! { "a": 1_i32, "b": "hello" };
        let field = RawField::get(doc_bytes(&doc), "b").unwrap();
        assert_eq!(field.element_type(), ElementType::String);
    }

    #[test]
    fn get_missing() {
        let doc = rawdoc! { "a": 1_i32 };
        assert!(RawField::get(doc_bytes(&doc), "z").is_none());
    }

    // ── RawField::get_path ────────────────────────────────────

    #[test]
    fn get_path_flat() {
        let doc = rawdoc! { "score": 42_i32 };
        let field = RawField::get_path(doc_bytes(&doc), "score").unwrap();
        assert_eq!(field.element_type(), ElementType::Int32);
    }

    #[test]
    fn get_path_one_level() {
        let doc = rawdoc! { "address": { "city": "Austin" } };
        let field = RawField::get_path(doc_bytes(&doc), "address.city").unwrap();
        assert_eq!(field.element_type(), ElementType::String);
    }

    #[test]
    fn get_path_two_levels() {
        let doc = rawdoc! { "a": { "b": { "c": 99_i32 } } };
        let field = RawField::get_path(doc_bytes(&doc), "a.b.c").unwrap();
        assert_eq!(field.element_type(), ElementType::Int32);
    }

    #[test]
    fn get_path_miss_at_top() {
        let doc = rawdoc! { "a": { "b": 1_i32 } };
        assert!(RawField::get_path(doc_bytes(&doc), "z.b").is_none());
    }

    #[test]
    fn get_path_miss_at_nested() {
        let doc = rawdoc! { "a": { "b": 1_i32 } };
        assert!(RawField::get_path(doc_bytes(&doc), "a.z").is_none());
    }

    #[test]
    fn get_path_non_document_intermediate() {
        let doc = rawdoc! { "a": "not a doc" };
        assert!(RawField::get_path(doc_bytes(&doc), "a.b").is_none());
    }

    // ── value() ───────────────────────────────────────────────

    #[test]
    fn value_i32() {
        let doc = rawdoc! { "n": 42_i32 };
        let field = RawField::get(doc_bytes(&doc), "n").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Int32(42)));
    }

    #[test]
    fn value_i64() {
        let doc = rawdoc! { "n": 123_i64 };
        let field = RawField::get(doc_bytes(&doc), "n").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Int64(123)));
    }

    #[test]
    fn value_double() {
        let doc = rawdoc! { "n": 2.78_f64 };
        let field = RawField::get(doc_bytes(&doc), "n").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Double(2.78)));
    }

    #[test]
    fn value_string() {
        let doc = rawdoc! { "s": "hello" };
        let field = RawField::get(doc_bytes(&doc), "s").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::String("hello")));
    }

    #[test]
    fn value_bool() {
        let doc = rawdoc! { "b": true };
        let field = RawField::get(doc_bytes(&doc), "b").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Boolean(true)));
    }

    #[test]
    fn value_null() {
        let doc = rawdoc! { "n": null };
        let field = RawField::get(doc_bytes(&doc), "n").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Null));
        assert!(field.is_null());
    }

    #[test]
    fn value_datetime() {
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let doc = rawdoc! { "t": dt };
        let field = RawField::get(doc_bytes(&doc), "t").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::DateTime(dt)));
    }

    #[test]
    fn value_document() {
        let doc = rawdoc! { "sub": { "x": 1_i32 } };
        let field = RawField::get(doc_bytes(&doc), "sub").unwrap();
        match field.value() {
            Some(RawBsonRef::Document(d)) => {
                assert_eq!(d.get("x").unwrap(), Some(RawBsonRef::Int32(1)));
            }
            _ => panic!("expected Document"),
        }
    }

    #[test]
    fn value_array() {
        let doc = rawdoc! { "arr": [1_i32, 2_i32, 3_i32] };
        let field = RawField::get(doc_bytes(&doc), "arr").unwrap();
        match field.value() {
            Some(RawBsonRef::Array(arr)) => {
                let items: Vec<_> = arr.into_iter().flatten().collect();
                assert_eq!(items.len(), 3);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn value_objectid() {
        let oid = bson::oid::ObjectId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let doc = rawdoc! { "oid": oid };
        let field = RawField::get(doc_bytes(&doc), "oid").unwrap();
        assert_eq!(field.element_type(), ElementType::ObjectId);
        assert_eq!(field.value(), Some(RawBsonRef::ObjectId(oid)));
    }

    // ── Sub-document scanning ─────────────────────────────────

    #[test]
    fn get_in_sub_document() {
        let doc = rawdoc! { "outer": { "inner": 42_i32 } };
        let bytes = doc_bytes(&doc);
        let outer = RawField::get(bytes, "outer").unwrap();
        assert_eq!(outer.element_type(), ElementType::EmbeddedDocument);
        // Scan within the sub-document using get on the sub-doc bytes
        let inner_bytes = outer.value_bytes();
        let inner = RawField::get(inner_bytes, "inner").unwrap();
        assert_eq!(inner.value(), Some(RawBsonRef::Int32(42)));
    }

    #[test]
    fn get_in_sub_document_miss() {
        let doc = rawdoc! { "outer": { "inner": 1_i32 } };
        let bytes = doc_bytes(&doc);
        let outer = RawField::get(bytes, "outer").unwrap();
        let inner_bytes = outer.value_bytes();
        assert!(RawField::get(inner_bytes, "missing").is_none());
    }

    #[test]
    fn get_in_multiple_fields() {
        let doc = rawdoc! { "sub": { "a": 1_i32, "b": "two", "c": true } };
        let bytes = doc_bytes(&doc);
        let sub = RawField::get(bytes, "sub").unwrap();
        let sub_bytes = sub.value_bytes();
        assert_eq!(
            RawField::get(sub_bytes, "a").unwrap().value(),
            Some(RawBsonRef::Int32(1))
        );
        assert_eq!(
            RawField::get(sub_bytes, "b").unwrap().value(),
            Some(RawBsonRef::String("two"))
        );
        assert_eq!(
            RawField::get(sub_bytes, "c").unwrap().value(),
            Some(RawBsonRef::Boolean(true))
        );
    }

    // ── skip_bson_value edge cases ────────────────────────────

    #[test]
    fn skip_binary() {
        let doc = rawdoc! { "bin": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0xDE, 0xAD, 0xBE, 0xEF] } };
        let field = RawField::get(doc_bytes(&doc), "bin").unwrap();
        assert_eq!(field.element_type(), ElementType::Binary);
        assert!(field.element_end() <= doc_bytes(&doc).len());
        assert!(field.element_end() > field.value_start());
    }

    #[test]
    fn skip_objectid() {
        let oid = bson::oid::ObjectId::from_bytes([0; 12]);
        let doc = rawdoc! { "oid": oid };
        let field = RawField::get(doc_bytes(&doc), "oid").unwrap();
        assert_eq!(field.element_type(), ElementType::ObjectId);
        assert_eq!(field.element_end() - field.value_start(), 12);
    }

    #[test]
    fn skip_timestamp() {
        let ts = bson::Timestamp {
            time: 12345,
            increment: 1,
        };
        let doc = rawdoc! { "ts": ts };
        let field = RawField::get(doc_bytes(&doc), "ts").unwrap();
        assert_eq!(field.element_type(), ElementType::Timestamp);
        assert_eq!(field.element_end() - field.value_start(), 8);
    }

    #[test]
    fn skip_decimal128() {
        let dec = bson::Decimal128::from_bytes([0u8; 16]);
        let doc = rawdoc! { "dec": dec };
        let field = RawField::get(doc_bytes(&doc), "dec").unwrap();
        assert_eq!(field.element_type(), ElementType::Decimal128);
        assert_eq!(field.element_end() - field.value_start(), 16);
    }

    #[test]
    fn skip_null() {
        let doc = rawdoc! { "n": null };
        let field = RawField::get(doc_bytes(&doc), "n").unwrap();
        assert_eq!(field.element_type(), ElementType::Null);
        assert_eq!(field.element_end(), field.value_start());
    }

    #[test]
    fn skip_unknown_type_returns_none() {
        assert!(skip_bson_value(0xFF, &[0; 32], 0).is_none());
        assert!(skip_bson_value(0x06, &[0; 32], 0).is_none()); // Undefined (deprecated)
    }

    #[test]
    fn skip_truncated_string() {
        assert!(skip_bson_value(0x02, &[0, 0], 0).is_none());
    }

    #[test]
    fn skip_truncated_document() {
        assert!(skip_bson_value(0x03, &[0, 0], 0).is_none());
    }

    #[test]
    fn skip_truncated_binary() {
        assert!(skip_bson_value(0x05, &[0, 0], 0).is_none());
    }

    // ── value() edge cases ────────────────────────────────────

    #[test]
    fn value_unknown_type_returns_none() {
        // Build a field with a fake unknown type by overriding element_type
        // We use Timestamp which our value() doesn't handle → returns None
        let doc = rawdoc! { "ts": bson::Timestamp { time: 1, increment: 1 } };
        let field = RawField::get(doc_bytes(&doc), "ts").unwrap();
        // Timestamp is a known ElementType that our value() doesn't convert
        assert!(field.value().is_none());
    }

    #[test]
    fn value_bool_false() {
        let doc = rawdoc! { "b": false };
        let field = RawField::get(doc_bytes(&doc), "b").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Boolean(false)));
    }

    #[test]
    fn value_negative_numbers() {
        let doc = rawdoc! { "i32": -42_i32, "i64": -999_i64, "f64": -2.78_f64 };
        let bytes = doc_bytes(&doc);

        let field = RawField::get(bytes, "i32").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Int32(-42)));

        let field = RawField::get(bytes, "i64").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Int64(-999)));

        let field = RawField::get(bytes, "f64").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Double(-2.78)));
    }

    #[test]
    fn value_empty_string() {
        let doc = rawdoc! { "s": "" };
        let field = RawField::get(doc_bytes(&doc), "s").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::String("")));
    }

    #[test]
    fn value_empty_document() {
        let doc = rawdoc! { "sub": {} };
        let field = RawField::get(doc_bytes(&doc), "sub").unwrap();
        match field.value() {
            Some(RawBsonRef::Document(d)) => {
                assert_eq!(d.iter().count(), 0);
            }
            _ => panic!("expected Document"),
        }
    }

    #[test]
    fn value_empty_array() {
        let doc = rawdoc! { "arr": [] };
        let field = RawField::get(doc_bytes(&doc), "arr").unwrap();
        match field.value() {
            Some(RawBsonRef::Array(arr)) => {
                assert_eq!(arr.into_iter().count(), 0);
            }
            _ => panic!("expected Array"),
        }
    }

    // ── Finding fields after various types ────────────────────

    #[test]
    fn get_after_binary() {
        let doc = rawdoc! {
            "bin": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![1, 2, 3] },
            "after": "found"
        };
        let field = RawField::get(doc_bytes(&doc), "after").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::String("found")));
    }

    #[test]
    fn get_after_timestamp() {
        let doc = rawdoc! {
            "ts": bson::Timestamp { time: 1, increment: 2 },
            "after": 99_i32
        };
        let field = RawField::get(doc_bytes(&doc), "after").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Int32(99)));
    }

    #[test]
    fn get_after_decimal128() {
        let doc = rawdoc! {
            "dec": bson::Decimal128::from_bytes([0u8; 16]),
            "after": true
        };
        let field = RawField::get(doc_bytes(&doc), "after").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Boolean(true)));
    }

    // ── get_value ─────────────────────────────────────────────

    #[test]
    fn get_value_simple() {
        let doc = rawdoc! { "status": "active", "count": 42_i32 };
        assert_eq!(
            RawField::get_value(doc_bytes(&doc), "status"),
            Some(RawBsonRef::String("active"))
        );
    }

    #[test]
    fn get_value_dotted() {
        let doc = rawdoc! { "address": { "city": "Austin" } };
        assert_eq!(
            RawField::get_value(doc_bytes(&doc), "address.city"),
            Some(RawBsonRef::String("Austin"))
        );
    }

    #[test]
    fn get_value_missing() {
        let doc = rawdoc! { "name": "test" };
        assert_eq!(RawField::get_value(doc_bytes(&doc), "missing"), None);
    }

    #[test]
    fn get_value_null() {
        let doc = rawdoc! { "status": null };
        assert_eq!(RawField::get_value(doc_bytes(&doc), "status"), None);
    }

    #[test]
    fn get_value_id() {
        let doc = rawdoc! { "_id": "abc123", "name": "test" };
        assert_eq!(
            RawField::get_value(doc_bytes(&doc), "_id"),
            Some(RawBsonRef::String("abc123"))
        );
    }

    // ── Cross-check: get_path + value matches RawDocument::get() ─

    #[test]
    fn cross_check_flat_fields() {
        let doc = rawdoc! { "a": 1_i32, "b": "hello", "c": true, "d": 2.78_f64 };
        let bytes = doc_bytes(&doc);
        for name in &["a", "b", "c", "d"] {
            let expected = doc.get(name).unwrap();
            let field = RawField::get_path(bytes, name).unwrap();
            assert_eq!(field.value(), expected, "mismatch for field '{}'", name);
        }
    }

    #[test]
    fn cross_check_nested() {
        let doc = rawdoc! {
            "address": { "city": "Austin", "zip": 78701_i32 },
            "stats": { "meta": { "tier": "gold" } }
        };
        let bytes = doc_bytes(&doc);
        for path in &["address.city", "address.zip", "stats.meta.tier"] {
            // Get expected via RawDocument chain
            let parts: Vec<&str> = path.split('.').collect();
            let expected = {
                let mut current: Option<RawBsonRef> = None;
                let mut d: &RawDocument = &doc;
                for (i, part) in parts.iter().enumerate() {
                    match d.get(part).unwrap() {
                        Some(RawBsonRef::Document(sub)) if i < parts.len() - 1 => {
                            d = sub;
                        }
                        other => {
                            current = other;
                            break;
                        }
                    }
                }
                current
            };

            let field = RawField::get_path(bytes, path).unwrap();
            assert_eq!(field.value(), expected, "mismatch for path '{}'", path);
        }
    }
}
