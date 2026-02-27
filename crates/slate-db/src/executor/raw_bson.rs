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
use slate_engine::skip_bson_value;

// ── Field location ──────────────────────────────────────────────

/// Location of a BSON element within a document's byte buffer.
pub(crate) struct FieldLocation {
    /// Offset of the type byte.
    pub element_start: usize,
    /// BSON element type tag.
    pub type_byte: u8,
    /// Offset where the value bytes begin.
    pub value_start: usize,
    /// Offset immediately after the value bytes.
    pub element_end: usize,
}

/// Walk a BSON document's raw bytes to find a top-level field by name.
pub(crate) fn find_field(bytes: &[u8], field_name: &str) -> Option<FieldLocation> {
    find_field_in(bytes, 0, field_name)
}

/// Find a field within a sub-document starting at byte offset `base`.
/// `base` is the offset of the sub-document's i32 length header.
pub(crate) fn find_field_in(bytes: &[u8], base: usize, field_name: &str) -> Option<FieldLocation> {
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
            return Some(FieldLocation {
                element_start,
                type_byte,
                value_start,
                element_end,
            });
        }
        pos = element_end;
    }
    None
}

// ── Dot-path resolution ─────────────────────────────────────────

/// Find a field by dot-path (e.g. `"address.city"` or `"a.b.c"`).
/// Walks into nested sub-documents at the byte level.
pub(crate) fn find_field_path(bytes: &[u8], path: &str) -> Option<FieldLocation> {
    if !path.contains('.') {
        return find_field(bytes, path);
    }
    let (first, rest) = path.split_once('.')?;
    let loc = find_field(bytes, first)?;
    // Must be a sub-document (0x03) to descend
    if loc.type_byte != 0x03 {
        return None;
    }
    find_field_path_in(bytes, loc.value_start, rest)
}

/// Recursive dot-path descent within a sub-document at `base`.
fn find_field_path_in(bytes: &[u8], base: usize, path: &str) -> Option<FieldLocation> {
    if !path.contains('.') {
        return find_field_in(bytes, base, path);
    }
    let (first, rest) = path.split_once('.')?;
    let loc = find_field_in(bytes, base, first)?;
    if loc.type_byte != 0x03 {
        return None;
    }
    find_field_path_in(bytes, loc.value_start, rest)
}

// ── Bridge: byte offset → RawBsonRef ────────────────────────────

/// Construct a `RawBsonRef` from a `FieldLocation` + document bytes.
///
/// This is the zero-cost bridge between raw byte lookup and typed comparison.
/// The field has already been located by `find_field` / `find_field_path`;
/// this just reads the value bytes and wraps them in the appropriate variant.
pub(crate) fn field_to_raw_bson_ref<'a>(
    bytes: &'a [u8],
    loc: &FieldLocation,
) -> Option<RawBsonRef<'a>> {
    match loc.type_byte {
        0x01 => {
            // Double
            let v = f64::from_le_bytes(
                bytes[loc.value_start..loc.value_start + 8]
                    .try_into()
                    .ok()?,
            );
            Some(RawBsonRef::Double(v))
        }
        0x02 => {
            // String
            let len = i32::from_le_bytes(
                bytes[loc.value_start..loc.value_start + 4]
                    .try_into()
                    .ok()?,
            ) as usize;
            let s = std::str::from_utf8(&bytes[loc.value_start + 4..loc.value_start + 4 + len - 1])
                .ok()?;
            Some(RawBsonRef::String(s))
        }
        0x03 => {
            // Document
            let doc = RawDocument::from_bytes(&bytes[loc.value_start..loc.element_end]).ok()?;
            Some(RawBsonRef::Document(doc))
        }
        0x04 => {
            // Array — RawArray is a newtype over RawDocument with identical layout
            let doc = RawDocument::from_bytes(&bytes[loc.value_start..loc.element_end]).ok()?;
            // SAFETY: RawArray is repr-transparent over RawDocument (single field `doc: RawDocument`).
            // The bson crate's own RawArray::from_doc does this same pointer cast.
            let arr: &RawArray = unsafe { &*(doc as *const RawDocument as *const RawArray) };
            Some(RawBsonRef::Array(arr))
        }
        0x07 => {
            // ObjectId
            let oid = bson::oid::ObjectId::from_bytes(
                bytes[loc.value_start..loc.value_start + 12]
                    .try_into()
                    .ok()?,
            );
            Some(RawBsonRef::ObjectId(oid))
        }
        0x08 => {
            // Boolean
            Some(RawBsonRef::Boolean(bytes[loc.value_start] != 0))
        }
        0x09 => {
            // DateTime
            let ms = i64::from_le_bytes(
                bytes[loc.value_start..loc.value_start + 8]
                    .try_into()
                    .ok()?,
            );
            Some(RawBsonRef::DateTime(bson::DateTime::from_millis(ms)))
        }
        0x0A => Some(RawBsonRef::Null), // Null
        0x10 => {
            // Int32
            let v = i32::from_le_bytes(
                bytes[loc.value_start..loc.value_start + 4]
                    .try_into()
                    .ok()?,
            );
            Some(RawBsonRef::Int32(v))
        }
        0x12 => {
            // Int64
            let v = i64::from_le_bytes(
                bytes[loc.value_start..loc.value_start + 8]
                    .try_into()
                    .ok()?,
            );
            Some(RawBsonRef::Int64(v))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;

    fn doc_bytes(doc: &bson::RawDocumentBuf) -> &[u8] {
        doc.as_bytes()
    }

    // ── find_field ──────────────────────────────────────────────

    #[test]
    fn find_field_exists() {
        let doc = rawdoc! { "a": 1_i32, "b": "hello" };
        let loc = find_field(doc_bytes(&doc), "b").unwrap();
        assert_eq!(loc.type_byte, 0x02); // String
    }

    #[test]
    fn find_field_missing() {
        let doc = rawdoc! { "a": 1_i32 };
        assert!(find_field(doc_bytes(&doc), "z").is_none());
    }

    // ── find_field_path ─────────────────────────────────────────

    #[test]
    fn find_field_path_flat() {
        let doc = rawdoc! { "score": 42_i32 };
        let loc = find_field_path(doc_bytes(&doc), "score").unwrap();
        assert_eq!(loc.type_byte, 0x10);
    }

    #[test]
    fn find_field_path_one_level() {
        let doc = rawdoc! { "address": { "city": "Austin" } };
        let loc = find_field_path(doc_bytes(&doc), "address.city").unwrap();
        assert_eq!(loc.type_byte, 0x02);
    }

    #[test]
    fn find_field_path_two_levels() {
        let doc = rawdoc! { "a": { "b": { "c": 99_i32 } } };
        let loc = find_field_path(doc_bytes(&doc), "a.b.c").unwrap();
        assert_eq!(loc.type_byte, 0x10);
    }

    #[test]
    fn find_field_path_miss_at_top() {
        let doc = rawdoc! { "a": { "b": 1_i32 } };
        assert!(find_field_path(doc_bytes(&doc), "z.b").is_none());
    }

    #[test]
    fn find_field_path_miss_at_nested() {
        let doc = rawdoc! { "a": { "b": 1_i32 } };
        assert!(find_field_path(doc_bytes(&doc), "a.z").is_none());
    }

    #[test]
    fn find_field_path_non_document_intermediate() {
        let doc = rawdoc! { "a": "not a doc" };
        assert!(find_field_path(doc_bytes(&doc), "a.b").is_none());
    }

    // ── field_to_raw_bson_ref ───────────────────────────────────

    #[test]
    fn bridge_i32() {
        let doc = rawdoc! { "n": 42_i32 };
        let loc = find_field(doc_bytes(&doc), "n").unwrap();
        let val = field_to_raw_bson_ref(doc_bytes(&doc), &loc).unwrap();
        assert_eq!(val, RawBsonRef::Int32(42));
    }

    #[test]
    fn bridge_i64() {
        let doc = rawdoc! { "n": 123_i64 };
        let loc = find_field(doc_bytes(&doc), "n").unwrap();
        let val = field_to_raw_bson_ref(doc_bytes(&doc), &loc).unwrap();
        assert_eq!(val, RawBsonRef::Int64(123));
    }

    #[test]
    fn bridge_double() {
        let doc = rawdoc! { "n": 3.14_f64 };
        let loc = find_field(doc_bytes(&doc), "n").unwrap();
        let val = field_to_raw_bson_ref(doc_bytes(&doc), &loc).unwrap();
        assert_eq!(val, RawBsonRef::Double(3.14));
    }

    #[test]
    fn bridge_string() {
        let doc = rawdoc! { "s": "hello" };
        let loc = find_field(doc_bytes(&doc), "s").unwrap();
        let val = field_to_raw_bson_ref(doc_bytes(&doc), &loc).unwrap();
        assert_eq!(val, RawBsonRef::String("hello"));
    }

    #[test]
    fn bridge_bool() {
        let doc = rawdoc! { "b": true };
        let loc = find_field(doc_bytes(&doc), "b").unwrap();
        let val = field_to_raw_bson_ref(doc_bytes(&doc), &loc).unwrap();
        assert_eq!(val, RawBsonRef::Boolean(true));
    }

    #[test]
    fn bridge_null() {
        let doc = rawdoc! { "n": null };
        let loc = find_field(doc_bytes(&doc), "n").unwrap();
        let val = field_to_raw_bson_ref(doc_bytes(&doc), &loc).unwrap();
        assert_eq!(val, RawBsonRef::Null);
    }

    #[test]
    fn bridge_datetime() {
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let doc = rawdoc! { "t": dt };
        let loc = find_field(doc_bytes(&doc), "t").unwrap();
        let val = field_to_raw_bson_ref(doc_bytes(&doc), &loc).unwrap();
        assert_eq!(val, RawBsonRef::DateTime(dt));
    }

    #[test]
    fn bridge_document() {
        let doc = rawdoc! { "sub": { "x": 1_i32 } };
        let loc = find_field(doc_bytes(&doc), "sub").unwrap();
        let val = field_to_raw_bson_ref(doc_bytes(&doc), &loc).unwrap();
        match val {
            RawBsonRef::Document(d) => {
                assert_eq!(d.get("x").unwrap(), Some(RawBsonRef::Int32(1)));
            }
            _ => panic!("expected Document"),
        }
    }

    #[test]
    fn bridge_array() {
        let doc = rawdoc! { "arr": [1_i32, 2_i32, 3_i32] };
        let loc = find_field(doc_bytes(&doc), "arr").unwrap();
        let val = field_to_raw_bson_ref(doc_bytes(&doc), &loc).unwrap();
        match val {
            RawBsonRef::Array(arr) => {
                let items: Vec<_> = arr.into_iter().flatten().collect();
                assert_eq!(items.len(), 3);
            }
            _ => panic!("expected Array"),
        }
    }

    // ── bridge: ObjectId ────────────────────────────────────────

    #[test]
    fn bridge_objectid() {
        let oid = bson::oid::ObjectId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let doc = rawdoc! { "oid": oid };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "oid").unwrap();
        assert_eq!(loc.type_byte, 0x07);
        let val = field_to_raw_bson_ref(bytes, &loc).unwrap();
        assert_eq!(val, RawBsonRef::ObjectId(oid));
    }

    // ── find_field_in: direct sub-document tests ────────────────

    #[test]
    fn find_field_in_sub_document() {
        let doc = rawdoc! { "outer": { "inner": 42_i32 } };
        let bytes = doc_bytes(&doc);
        // First find "outer" to get sub-document offset
        let outer = find_field(bytes, "outer").unwrap();
        assert_eq!(outer.type_byte, 0x03);
        // Now scan within the sub-document
        let inner = find_field_in(bytes, outer.value_start, "inner").unwrap();
        assert_eq!(inner.type_byte, 0x10);
        let val = field_to_raw_bson_ref(bytes, &inner).unwrap();
        assert_eq!(val, RawBsonRef::Int32(42));
    }

    #[test]
    fn find_field_in_sub_document_miss() {
        let doc = rawdoc! { "outer": { "inner": 1_i32 } };
        let bytes = doc_bytes(&doc);
        let outer = find_field(bytes, "outer").unwrap();
        assert!(find_field_in(bytes, outer.value_start, "missing").is_none());
    }

    #[test]
    fn find_field_in_multiple_fields() {
        let doc = rawdoc! { "sub": { "a": 1_i32, "b": "two", "c": true } };
        let bytes = doc_bytes(&doc);
        let sub = find_field(bytes, "sub").unwrap();
        let a = find_field_in(bytes, sub.value_start, "a").unwrap();
        let b = find_field_in(bytes, sub.value_start, "b").unwrap();
        let c = find_field_in(bytes, sub.value_start, "c").unwrap();
        assert_eq!(field_to_raw_bson_ref(bytes, &a), Some(RawBsonRef::Int32(1)));
        assert_eq!(
            field_to_raw_bson_ref(bytes, &b),
            Some(RawBsonRef::String("two"))
        );
        assert_eq!(
            field_to_raw_bson_ref(bytes, &c),
            Some(RawBsonRef::Boolean(true))
        );
    }

    // ── skip_bson_value edge cases ──────────────────────────────

    #[test]
    fn skip_bson_value_binary() {
        // Binary: i32(len) + subtype(1) + data(len)
        let doc = rawdoc! { "bin": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0xDE, 0xAD, 0xBE, 0xEF] } };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "bin").unwrap();
        assert_eq!(loc.type_byte, 0x05);
        // Verify skip lands correctly by checking element_end is in range
        assert!(loc.element_end <= bytes.len());
        assert!(loc.element_end > loc.value_start);
    }

    #[test]
    fn skip_bson_value_objectid() {
        let oid = bson::oid::ObjectId::from_bytes([0; 12]);
        let doc = rawdoc! { "oid": oid };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "oid").unwrap();
        assert_eq!(loc.type_byte, 0x07);
        // ObjectId is 12 bytes
        assert_eq!(loc.element_end - loc.value_start, 12);
    }

    #[test]
    fn skip_bson_value_timestamp() {
        let ts = bson::Timestamp {
            time: 12345,
            increment: 1,
        };
        let doc = rawdoc! { "ts": ts };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "ts").unwrap();
        assert_eq!(loc.type_byte, 0x11);
        // Timestamp is 8 bytes
        assert_eq!(loc.element_end - loc.value_start, 8);
    }

    #[test]
    fn skip_bson_value_decimal128() {
        let dec = bson::Decimal128::from_bytes([0u8; 16]);
        let doc = rawdoc! { "dec": dec };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "dec").unwrap();
        assert_eq!(loc.type_byte, 0x13);
        // Decimal128 is 16 bytes
        assert_eq!(loc.element_end - loc.value_start, 16);
    }

    #[test]
    fn skip_bson_value_null() {
        let doc = rawdoc! { "n": null };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "n").unwrap();
        assert_eq!(loc.type_byte, 0x0A);
        // Null is 0 bytes
        assert_eq!(loc.element_end, loc.value_start);
    }

    #[test]
    fn skip_bson_value_unknown_type_returns_none() {
        // Directly test that an unrecognised type byte returns None
        assert!(skip_bson_value(0xFF, &[0; 32], 0).is_none());
        assert!(skip_bson_value(0x06, &[0; 32], 0).is_none()); // Undefined (deprecated)
    }

    #[test]
    fn skip_bson_value_truncated_string() {
        // String needs at least 4 bytes for the length prefix
        assert!(skip_bson_value(0x02, &[0, 0], 0).is_none());
    }

    #[test]
    fn skip_bson_value_truncated_document() {
        // Document needs at least 4 bytes for the length prefix
        assert!(skip_bson_value(0x03, &[0, 0], 0).is_none());
    }

    #[test]
    fn skip_bson_value_truncated_binary() {
        // Binary needs at least 4 bytes for the length prefix
        assert!(skip_bson_value(0x05, &[0, 0], 0).is_none());
    }

    // ── field_to_raw_bson_ref edge cases ────────────────────────

    #[test]
    fn bridge_unknown_type_returns_none() {
        // Build a location with an unrecognised type byte
        let doc = rawdoc! { "n": 1_i32 };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "n").unwrap();
        let fake_loc = FieldLocation {
            element_start: loc.element_start,
            type_byte: 0xFF, // unknown
            value_start: loc.value_start,
            element_end: loc.element_end,
        };
        assert!(field_to_raw_bson_ref(bytes, &fake_loc).is_none());
    }

    #[test]
    fn bridge_bool_false() {
        let doc = rawdoc! { "b": false };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "b").unwrap();
        let val = field_to_raw_bson_ref(bytes, &loc).unwrap();
        assert_eq!(val, RawBsonRef::Boolean(false));
    }

    #[test]
    fn bridge_negative_numbers() {
        let doc = rawdoc! { "i32": -42_i32, "i64": -999_i64, "f64": -3.14_f64 };
        let bytes = doc_bytes(&doc);

        let loc = find_field(bytes, "i32").unwrap();
        assert_eq!(
            field_to_raw_bson_ref(bytes, &loc),
            Some(RawBsonRef::Int32(-42))
        );

        let loc = find_field(bytes, "i64").unwrap();
        assert_eq!(
            field_to_raw_bson_ref(bytes, &loc),
            Some(RawBsonRef::Int64(-999))
        );

        let loc = find_field(bytes, "f64").unwrap();
        assert_eq!(
            field_to_raw_bson_ref(bytes, &loc),
            Some(RawBsonRef::Double(-3.14))
        );
    }

    #[test]
    fn bridge_empty_string() {
        let doc = rawdoc! { "s": "" };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "s").unwrap();
        let val = field_to_raw_bson_ref(bytes, &loc).unwrap();
        assert_eq!(val, RawBsonRef::String(""));
    }

    #[test]
    fn bridge_empty_document() {
        let doc = rawdoc! { "sub": {} };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "sub").unwrap();
        let val = field_to_raw_bson_ref(bytes, &loc).unwrap();
        match val {
            RawBsonRef::Document(d) => {
                assert_eq!(d.iter().count(), 0);
            }
            _ => panic!("expected Document"),
        }
    }

    #[test]
    fn bridge_empty_array() {
        let doc = rawdoc! { "arr": [] };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "arr").unwrap();
        let val = field_to_raw_bson_ref(bytes, &loc).unwrap();
        match val {
            RawBsonRef::Array(arr) => {
                assert_eq!(arr.into_iter().count(), 0);
            }
            _ => panic!("expected Array"),
        }
    }

    // ── find_field: can find fields after various types ─────────

    #[test]
    fn find_field_after_binary() {
        // Ensure skip_bson_value correctly skips a binary value so we can find the next field
        let doc = rawdoc! {
            "bin": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![1, 2, 3] },
            "after": "found"
        };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "after").unwrap();
        assert_eq!(
            field_to_raw_bson_ref(bytes, &loc),
            Some(RawBsonRef::String("found"))
        );
    }

    #[test]
    fn find_field_after_timestamp() {
        let doc = rawdoc! {
            "ts": bson::Timestamp { time: 1, increment: 2 },
            "after": 99_i32
        };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "after").unwrap();
        assert_eq!(
            field_to_raw_bson_ref(bytes, &loc),
            Some(RawBsonRef::Int32(99))
        );
    }

    #[test]
    fn find_field_after_decimal128() {
        let doc = rawdoc! {
            "dec": bson::Decimal128::from_bytes([0u8; 16]),
            "after": true
        };
        let bytes = doc_bytes(&doc);
        let loc = find_field(bytes, "after").unwrap();
        assert_eq!(
            field_to_raw_bson_ref(bytes, &loc),
            Some(RawBsonRef::Boolean(true))
        );
    }

    // ── Cross-check: find_field_path + bridge matches RawDocument::get() ─

    #[test]
    fn cross_check_flat_fields() {
        let doc = rawdoc! { "a": 1_i32, "b": "hello", "c": true, "d": 3.14_f64 };
        let bytes = doc_bytes(&doc);
        for field in &["a", "b", "c", "d"] {
            let expected = doc.get(field).unwrap();
            let loc = find_field_path(bytes, field).unwrap();
            let got = field_to_raw_bson_ref(bytes, &loc);
            assert_eq!(got, expected, "mismatch for field '{}'", field);
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

            let loc = find_field_path(bytes, path).unwrap();
            let got = field_to_raw_bson_ref(bytes, &loc);
            assert_eq!(got, expected, "mismatch for path '{}'", path);
        }
    }
}
