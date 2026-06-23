//! Fast raw byte-level BSON field access.
//!
//! Locates a field within raw `&[u8]` BSON bytes by scanning directly, avoiding
//! the overhead of the `bson` crate's `RawDocument::get()` iterator (which
//! constructs `RawElement` objects, wraps every step in `Result`, and validates
//! each key it skips as UTF-8). A tiny leaf crate so every layer that reads raw
//! documents — the storage engine, mutation, and expression evaluation — can
//! share one scanner without pulling in the engine.

use bson::raw::{RawArray, RawBsonRef, RawDocument};
use bson::spec::ElementType;

mod merge;
pub use merge::{RawMergeError, raw_merge};

// ── skip_bson_value ─────────────────────────────────────────────

/// Given a BSON type byte and the position where value bytes begin, return the
/// position immediately after the value. Returns `None` if bytes are truncated
/// or the type is unrecognised.
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

// ── RawField ────────────────────────────────────────────────────

/// A located field within raw BSON bytes.
///
/// Holds a reference to the document bytes and the field's position
/// metadata. Value parsing is deferred until `.value()` is called.
pub struct RawField<'a> {
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
                let doc = RawDocument::from_bytes(&self.bytes[self.value_start..self.element_end])
                    .ok()?;
                Some(RawBsonRef::Document(doc))
            }
            ElementType::Array => {
                let doc = RawDocument::from_bytes(&self.bytes[self.value_start..self.element_end])
                    .ok()?;
                // SAFETY: RawArray is repr-transparent over RawDocument.
                // The bson crate's own RawArray::from_doc does this same pointer cast.
                let arr: &RawArray = unsafe { &*(doc as *const RawDocument as *const RawArray) };
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
            ElementType::Boolean => Some(RawBsonRef::Boolean(self.bytes[self.value_start] != 0)),
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
            ElementType::Decimal128 => {
                let bytes: [u8; 16] = self.bytes[self.value_start..self.value_start + 16]
                    .try_into()
                    .ok()?;
                Some(RawBsonRef::Decimal128(bson::Decimal128::from_bytes(bytes)))
            }
            _ => None,
        }
    }

    // ── Byte-level access (for tests) ──────────────────────────

    /// The raw value bytes (`value_start..element_end`).
    pub fn value_bytes(&self) -> &'a [u8] {
        &self.bytes[self.value_start..self.element_end]
    }

    /// Byte offset where the value bytes begin.
    pub fn value_start(&self) -> usize {
        self.value_start
    }

    /// Byte offset immediately after the value bytes.
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
pub struct RawFieldLoc {
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

// ── Multikey path traversal ─────────────────────────────────────

/// Visit every value reachable by `path` within `doc`, invoking `f` for each.
///
/// `path` is dot-separated. A `[]` segment iterates the array at that position,
/// fanning out across its elements (e.g. `"tags.[]"`, `"items.[].sku"`). At the
/// terminal position a reached array is fanned out — each element is visited —
/// while any other value is visited directly. Missing fields, type mismatches,
/// and a leading `[]` yield nothing.
///
/// Values are borrowed from `doc`; the caller owns any mapping or collection.
/// This is the raw traversal behind the engine's multikey (array) index
/// extraction — it yields raw `RawBsonRef`s and leaves type filtering to the
/// caller's closure.
pub fn for_each_path_value(doc: &RawDocument, path: &str, f: &mut impl FnMut(RawBsonRef<'_>)) {
    let segments: Vec<&str> = path.split('.').collect();
    walk_doc(doc, &segments, 0, f);
}

fn walk_doc<F: FnMut(RawBsonRef<'_>)>(doc: &RawDocument, segments: &[&str], idx: usize, f: &mut F) {
    if idx >= segments.len() {
        return;
    }
    let seg = segments[idx];
    if seg == "[]" {
        return;
    }
    if let Ok(Some(value)) = doc.get(seg) {
        walk_value(value, segments, idx + 1, f);
    }
}

fn walk_value<F: FnMut(RawBsonRef<'_>)>(
    value: RawBsonRef<'_>,
    segments: &[&str],
    idx: usize,
    f: &mut F,
) {
    if idx >= segments.len() {
        // Terminal: fan out a reached array, otherwise visit the value directly.
        match value {
            RawBsonRef::Array(arr) => {
                for v in arr.into_iter().flatten() {
                    f(v);
                }
            }
            _ => f(value),
        }
        return;
    }

    let seg = segments[idx];
    if seg == "[]" {
        // Array marker: the current value must be the array to iterate.
        if let RawBsonRef::Array(arr) = value {
            for v in arr.into_iter().flatten() {
                walk_value(v, segments, idx + 1, f);
            }
        }
    } else if let RawBsonRef::Document(d) = value {
        // Plain segment with depth remaining: descend into the sub-document.
        walk_doc(d, segments, idx, f);
    }
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

    // ── for_each_path_value ───────────────────────────────────

    /// Collect integer values reached by `path` (both Int32 and Int64).
    fn ints(doc: &bson::RawDocumentBuf, path: &str) -> Vec<i64> {
        let mut out = Vec::new();
        for_each_path_value(doc, path, &mut |v| match v {
            RawBsonRef::Int32(n) => out.push(n as i64),
            RawBsonRef::Int64(n) => out.push(n),
            _ => {}
        });
        out
    }

    /// Collect string values reached by `path`.
    fn strs(doc: &bson::RawDocumentBuf, path: &str) -> Vec<String> {
        let mut out = Vec::new();
        for_each_path_value(doc, path, &mut |v| {
            if let RawBsonRef::String(s) = v {
                out.push(s.to_string());
            }
        });
        out
    }

    #[test]
    fn path_value_single_field() {
        let doc = rawdoc! { "a": 1_i32, "b": 2_i32 };
        assert_eq!(ints(&doc, "a"), vec![1]);
    }

    #[test]
    fn path_value_nested_document() {
        let doc = rawdoc! { "a": { "b": { "c": 99_i32 } } };
        assert_eq!(ints(&doc, "a.b.c"), vec![99]);
    }

    #[test]
    fn path_value_array_fan_out() {
        let doc = rawdoc! { "tags": ["x", "y", "z"] };
        assert_eq!(strs(&doc, "tags.[]"), vec!["x", "y", "z"]);
    }

    #[test]
    fn path_value_nested_array_of_documents() {
        let doc = rawdoc! { "items": [ { "sku": "A1" }, { "sku": "B2" } ] };
        assert_eq!(strs(&doc, "items.[].sku"), vec!["A1", "B2"]);
    }

    #[test]
    fn path_value_terminal_array_fans_out() {
        // `a.[].b` where each `b` is itself an array: the terminal array is
        // fanned out, so every leaf integer is visited.
        let doc = rawdoc! { "a": [ { "b": [1_i32, 2_i32] }, { "b": [3_i32] } ] };
        assert_eq!(ints(&doc, "a.[].b"), vec![1, 2, 3]);
    }

    #[test]
    fn path_value_missing_field_yields_nothing() {
        let doc = rawdoc! { "a": 1_i32 };
        assert!(ints(&doc, "x.[]").is_empty());
    }

    #[test]
    fn path_value_marker_on_non_array_yields_nothing() {
        let doc = rawdoc! { "a": 1_i32 };
        assert!(ints(&doc, "a.[]").is_empty());
    }

    #[test]
    fn path_value_leading_marker_yields_nothing() {
        let doc = rawdoc! { "a": 1_i32 };
        assert!(ints(&doc, "[]").is_empty());
    }

    #[test]
    fn path_value_plain_array_terminal_fans_out() {
        // A trailing plain segment that resolves to an array also fans out at the
        // terminal position (no `[]` marker required once traversal is underway).
        let doc = rawdoc! { "outer": [ { "vals": [7_i32, 8_i32] } ] };
        assert_eq!(ints(&doc, "outer.[].vals"), vec![7, 8]);
    }
}
