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

// ── Malformed-input contract ─────────────────────────────────────
//
// The crate uses two doors (per the rawbson-robustness RFC):
//
//   * Door 2 (`Option`) on the *internal* scan primitives — `skip_bson_value`,
//     `RawField::get` / `get_path` / `get_value` / `value`. Truncation and
//     "field absent" both collapse to `None`. These signatures are consumed by
//     the engine/eval hot paths and stay `Option` to keep that path branch-thin.
//   * Door 3 (typed error) on the *public, raw-`&[u8]`* boundary — the `try_*`
//     entry points below distinguish `Truncated` / `BadLength` corruption from a
//     genuinely absent field, so corrupt-on-disk bytes surface a signal instead
//     of masquerading as a missing field.

/// Why reading a raw BSON byte buffer failed.
///
/// Returned only by the `try_*` boundary on [`RawField`] (door 3). The internal
/// `Option`-returning scanners collapse every one of these to `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawBsonError {
    /// The buffer ended before a value the length headers promised — a value
    /// runs past `bytes.len()`, a document header is short, or a field name has
    /// no terminating nul.
    Truncated,
    /// A length field is structurally invalid (negative, or a document length
    /// smaller than its own 4-byte header).
    BadLength,
    /// A type byte is not a BSON element type this scanner recognises.
    UnknownType(u8),
}

impl std::fmt::Display for RawBsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawBsonError::Truncated => write!(f, "raw BSON truncated"),
            RawBsonError::BadLength => write!(f, "raw BSON has an invalid length field"),
            RawBsonError::UnknownType(b) => {
                write!(f, "raw BSON has unrecognised element type 0x{b:02X}")
            }
        }
    }
}

impl std::error::Error for RawBsonError {}

// ── skip_bson_value ─────────────────────────────────────────────

/// Advance `pos` by `n` fixed-width bytes, returning `None` if that would land
/// past the end of `bytes`. The bounds check is what makes every fixed-width
/// `skip` arm total: a truncated buffer yields `None`, never an offset that a
/// later slice would panic on.
#[inline]
fn skip_fixed(bytes: &[u8], pos: usize, n: usize) -> Option<usize> {
    let end = pos.checked_add(n)?;
    (end <= bytes.len()).then_some(end)
}

/// Read a 4-byte little-endian i32 length at `pos`, returning `None` if the
/// header itself is truncated or the value is negative (lengths are never
/// negative in well-formed BSON).
#[inline]
fn read_len(bytes: &[u8], pos: usize) -> Option<usize> {
    let header_end = pos.checked_add(4)?;
    let header = bytes.get(pos..header_end)?;
    let raw = i32::from_le_bytes(header.try_into().ok()?);
    usize::try_from(raw).ok()
}

/// Given a BSON type byte and the position where value bytes begin, return the
/// position immediately after the value. Returns `None` if bytes are truncated,
/// a length field is malformed, or the type is unrecognised.
///
/// # Contract
///
/// This is the crate's *internal* scan primitive and uses door 2 (`Option`):
/// truncation and "absent" both collapse to `None`. Every returned `Some(end)`
/// is guaranteed in-bounds (`end <= bytes.len()`), so callers may slice
/// `value_start..end` without re-checking. Callers that need to distinguish
/// corruption from absence should use the typed-error boundary on [`RawField`]
/// ([`RawField::try_get`]).
pub fn skip_bson_value(type_byte: u8, bytes: &[u8], pos: usize) -> Option<usize> {
    match type_byte {
        0x01 => skip_fixed(bytes, pos, 8), // Double
        0x02 => {
            // String: i32(len) + utf8 + nul. Guard both the header *and* the
            // computed end — the header check alone leaves a slice that can
            // still run off the buffer.
            let len = read_len(bytes, pos)?;
            skip_fixed(bytes, pos + 4, len)
        }
        0x03 | 0x04 => {
            // Document / Array (self-contained): length covers itself.
            let len = read_len(bytes, pos)?;
            // `len` includes the 4-byte header; the end is `pos + len`.
            if len < 4 {
                return None;
            }
            skip_fixed(bytes, pos, len)
        }
        0x05 => {
            // Binary: i32(len) + subtype + data.
            let len = read_len(bytes, pos)?;
            // 4-byte header + 1 subtype byte + `len` data bytes.
            skip_fixed(bytes, pos + 4, 1 + len)
        }
        0x07 => skip_fixed(bytes, pos, 12), // ObjectId
        0x08 => skip_fixed(bytes, pos, 1),  // Boolean
        0x09 => skip_fixed(bytes, pos, 8),  // DateTime (i64)
        0x0A => Some(pos),                  // Null (0 bytes — pos is in-bounds by construction)
        0x10 => skip_fixed(bytes, pos, 4),  // Int32
        0x11 => skip_fixed(bytes, pos, 8),  // Timestamp
        0x12 => skip_fixed(bytes, pos, 8),  // Int64
        0x13 => skip_fixed(bytes, pos, 16), // Decimal128
        _ => None,
    }
}

/// Door-3 sibling of [`skip_bson_value`]: same arithmetic, but it distinguishes
/// the *reason* a value cannot be skipped — `Truncated` when the buffer ends
/// early, `BadLength` for a structurally invalid length, `UnknownType` for an
/// unrecognised type byte. Used by the public `try_*` boundary; the engine/eval
/// hot path stays on the `Option`-returning [`skip_bson_value`].
pub fn try_skip_bson_value(type_byte: u8, bytes: &[u8], pos: usize) -> Result<usize, RawBsonError> {
    /// Bounds-check `pos + n`, attributing failure to truncation.
    fn fixed(bytes: &[u8], pos: usize, n: usize) -> Result<usize, RawBsonError> {
        let end = pos.checked_add(n).ok_or(RawBsonError::BadLength)?;
        if end <= bytes.len() {
            Ok(end)
        } else {
            Err(RawBsonError::Truncated)
        }
    }
    /// Read a 4-byte length header, attributing failure to truncation/bad-length.
    fn len(bytes: &[u8], pos: usize) -> Result<usize, RawBsonError> {
        let header_end = pos.checked_add(4).ok_or(RawBsonError::BadLength)?;
        let header = bytes.get(pos..header_end).ok_or(RawBsonError::Truncated)?;
        let raw = i32::from_le_bytes(header.try_into().map_err(|_| RawBsonError::Truncated)?);
        usize::try_from(raw).map_err(|_| RawBsonError::BadLength)
    }

    match type_byte {
        0x01 => fixed(bytes, pos, 8), // Double
        0x02 => {
            let l = len(bytes, pos)?;
            fixed(bytes, pos + 4, l)
        }
        0x03 | 0x04 => {
            let l = len(bytes, pos)?;
            if l < 4 {
                return Err(RawBsonError::BadLength);
            }
            fixed(bytes, pos, l)
        }
        0x05 => {
            let l = len(bytes, pos)?;
            fixed(bytes, pos + 4, 1 + l)
        }
        0x07 => fixed(bytes, pos, 12), // ObjectId
        0x08 => fixed(bytes, pos, 1),  // Boolean
        0x09 => fixed(bytes, pos, 8),  // DateTime
        0x0A => Ok(pos),               // Null
        0x10 => fixed(bytes, pos, 4),  // Int32
        0x11 => fixed(bytes, pos, 8),  // Timestamp
        0x12 => fixed(bytes, pos, 8),  // Int64
        0x13 => fixed(bytes, pos, 16), // Decimal128
        other => Err(RawBsonError::UnknownType(other)),
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

    // ── Fallible boundary (door 3) ───────────────────────────────

    /// Find a top-level field by name, distinguishing corruption from absence.
    ///
    /// `Ok(Some(field))` — found. `Ok(None)` — genuinely absent (the whole
    /// document scanned cleanly without the name). `Err(_)` — the bytes are
    /// malformed (truncated value, bad length, unknown type, missing name nul).
    ///
    /// This is the door-3 entry point for raw, possibly-corrupt `&[u8]` (e.g. a
    /// torn write read back from storage). Callers holding a validated
    /// `RawDocument` can keep using the cheaper [`get`](Self::get).
    pub fn try_get(bytes: &'a [u8], name: &str) -> Result<Option<Self>, RawBsonError> {
        Ok(scan_field_checked(bytes, 0, name)?.map(|loc| loc.bind(bytes)))
    }

    /// Dot-path sibling of [`try_get`](Self::try_get).
    pub fn try_get_path(bytes: &'a [u8], path: &str) -> Result<Option<Self>, RawBsonError> {
        Ok(resolve_path_checked(bytes, 0, path)?.map(|loc| loc.bind(bytes)))
    }

    /// Parse the value bytes, distinguishing corruption from an unconvertible
    /// type.
    ///
    /// `Ok(Some(value))` — parsed. `Ok(None)` — a valid type this scanner does
    /// not convert (e.g. Timestamp). `Err(_)` — the value bytes are truncated.
    pub fn try_value(&self) -> Result<Option<RawBsonRef<'a>>, RawBsonError> {
        // A converting type that fails `value()` means the value bytes are
        // truncated; a non-converting type yields `Ok(None)`.
        match self.value() {
            Some(v) => Ok(Some(v)),
            None if value_is_converting(self.element_type) => Err(RawBsonError::Truncated),
            None => Ok(None),
        }
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

    /// The `N` fixed-width value bytes as an array, or `None` if truncated.
    ///
    /// Every fixed-width slice in [`value`](Self::value) goes through here, so a
    /// truncated buffer yields `None` rather than panicking. On bytes from a
    /// validated `RawDocument` this never fails.
    #[inline]
    fn fixed<const N: usize>(&self) -> Option<[u8; N]> {
        let end = self.value_start.checked_add(N)?;
        self.bytes.get(self.value_start..end)?.try_into().ok()
    }

    /// Parse the value bytes into a `RawBsonRef`.
    ///
    /// Returns `None` for a value `value()` does not convert (e.g. Timestamp),
    /// *and* for truncated/malformed value bytes — both collapse under door 2.
    /// Use [`try_value`](Self::try_value) to distinguish corruption from an
    /// unconvertible type at the public boundary.
    pub fn value(&self) -> Option<RawBsonRef<'a>> {
        match self.element_type {
            ElementType::Double => Some(RawBsonRef::Double(f64::from_le_bytes(self.fixed()?))),
            ElementType::String => {
                let len = i32::from_le_bytes(self.fixed::<4>()?);
                // A well-formed string length counts the trailing nul, so the
                // minimum is 1 (empty string). `len == 0` only occurs on
                // corruption; computing `len - 1` would underflow `usize`.
                let str_len = usize::try_from(len).ok()?.checked_sub(1)?;
                let body_start = self.value_start.checked_add(4)?;
                let body_end = body_start.checked_add(str_len)?;
                let s = std::str::from_utf8(self.bytes.get(body_start..body_end)?).ok()?;
                Some(RawBsonRef::String(s))
            }
            ElementType::EmbeddedDocument => {
                let doc =
                    RawDocument::from_bytes(self.bytes.get(self.value_start..self.element_end)?)
                        .ok()?;
                Some(RawBsonRef::Document(doc))
            }
            ElementType::Array => {
                let doc =
                    RawDocument::from_bytes(self.bytes.get(self.value_start..self.element_end)?)
                        .ok()?;
                // SAFETY: RawArray is repr-transparent over RawDocument.
                // The bson crate's own RawArray::from_doc does this same pointer cast.
                let arr: &RawArray = unsafe { &*(doc as *const RawDocument as *const RawArray) };
                Some(RawBsonRef::Array(arr))
            }
            ElementType::ObjectId => {
                let oid = bson::oid::ObjectId::from_bytes(self.fixed()?);
                Some(RawBsonRef::ObjectId(oid))
            }
            ElementType::Boolean => {
                Some(RawBsonRef::Boolean(*self.bytes.get(self.value_start)? != 0))
            }
            ElementType::DateTime => {
                let ms = i64::from_le_bytes(self.fixed()?);
                Some(RawBsonRef::DateTime(bson::DateTime::from_millis(ms)))
            }
            ElementType::Null => Some(RawBsonRef::Null),
            ElementType::Int32 => Some(RawBsonRef::Int32(i32::from_le_bytes(self.fixed()?))),
            ElementType::Int64 => Some(RawBsonRef::Int64(i64::from_le_bytes(self.fixed()?))),
            ElementType::Decimal128 => Some(RawBsonRef::Decimal128(bson::Decimal128::from_bytes(
                self.fixed()?,
            ))),
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

// ── Fallible scanning primitives (door 3) ───────────────────────
//
// Mirror `scan_field` / `resolve_path` but surface a `RawBsonError` when the
// bytes are malformed, reserving `Ok(None)` for a genuinely absent field. The
// happy path (`get`/`get_path`) stays on the `Option` versions above.

/// Read a 4-byte length header at `pos`, distinguishing truncation from a
/// negative length.
fn read_len_checked(bytes: &[u8], pos: usize) -> Result<usize, RawBsonError> {
    let header_end = pos.checked_add(4).ok_or(RawBsonError::BadLength)?;
    let header = bytes.get(pos..header_end).ok_or(RawBsonError::Truncated)?;
    let raw = i32::from_le_bytes(header.try_into().map_err(|_| RawBsonError::Truncated)?);
    usize::try_from(raw).map_err(|_| RawBsonError::BadLength)
}

/// Door-3 sibling of [`scan_field`]: `Ok(None)` = absent, `Err` = malformed.
fn scan_field_checked(
    bytes: &[u8],
    base: usize,
    field_name: &str,
) -> Result<Option<FieldLoc>, RawBsonError> {
    let target = field_name.as_bytes();
    let doc_len = read_len_checked(bytes, base)?;
    if doc_len < 5 {
        // A document is at least a 4-byte header + terminating 0x00.
        return Err(RawBsonError::BadLength);
    }
    let doc_end = base.checked_add(doc_len).ok_or(RawBsonError::BadLength)?;
    if doc_end > bytes.len() {
        return Err(RawBsonError::Truncated);
    }
    let mut pos = base + 4; // skip document length header

    while pos < doc_end {
        let type_byte = bytes[pos];
        if type_byte == 0x00 {
            break;
        }
        let element_type =
            ElementType::from(type_byte).ok_or(RawBsonError::UnknownType(type_byte))?;
        let element_start = pos;
        pos += 1; // skip type byte

        // Read null-terminated field name.
        let name_start = pos;
        while pos < doc_end && bytes[pos] != 0x00 {
            pos += 1;
        }
        if pos >= doc_end {
            return Err(RawBsonError::Truncated); // name never terminated
        }
        let name = &bytes[name_start..pos];
        pos += 1; // skip null terminator

        let value_start = pos;
        let element_end = try_skip_bson_value(type_byte, bytes, pos)?;

        if name == target {
            return Ok(Some(FieldLoc {
                element_type,
                element_start,
                value_start,
                element_end,
            }));
        }
        pos = element_end;
    }
    Ok(None)
}

/// Door-3 sibling of [`resolve_path`].
fn resolve_path_checked(
    bytes: &[u8],
    base: usize,
    path: &str,
) -> Result<Option<FieldLoc>, RawBsonError> {
    if !path.contains('.') {
        return scan_field_checked(bytes, base, path);
    }
    let Some((first, rest)) = path.split_once('.') else {
        return Ok(None);
    };
    let Some(loc) = scan_field_checked(bytes, base, first)? else {
        return Ok(None);
    };
    if loc.element_type != ElementType::EmbeddedDocument {
        return Ok(None);
    }
    resolve_path_checked(bytes, loc.value_start, rest)
}

/// Does [`RawField::value`] convert this element type to a `RawBsonRef`?
/// (Used by [`RawField::try_value`] to tell "truncated" from "unconvertible".)
fn value_is_converting(t: ElementType) -> bool {
    matches!(
        t,
        ElementType::Double
            | ElementType::String
            | ElementType::EmbeddedDocument
            | ElementType::Array
            | ElementType::ObjectId
            | ElementType::Boolean
            | ElementType::DateTime
            | ElementType::Null
            | ElementType::Int32
            | ElementType::Int64
            | ElementType::Decimal128
    )
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

    #[test]
    fn value_decimal128() {
        // Exercises the Decimal128 arm of value() (an llvm-cov gap before).
        let dec =
            bson::Decimal128::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let doc = rawdoc! { "dec": dec };
        let field = RawField::get(doc_bytes(&doc), "dec").unwrap();
        assert_eq!(field.value(), Some(RawBsonRef::Decimal128(dec)));
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

    // ── Per-type truncation (door 2: skip → None) ─────────────
    //
    // Each fixed-width arm must bounds-check: a buffer shorter than the value
    // width yields `None`, not an out-of-bounds offset. The pre-hardening bug
    // was `skip_bson_value(0x07, &[0; 5], 0) == Some(12)` — twelve, into five.

    #[test]
    fn skip_truncated_fixed_width_arms() {
        // (type_byte, width, name). Buffer is one byte short of the width.
        let cases: &[(u8, usize, &str)] = &[
            (0x01, 8, "Double"),
            (0x07, 12, "ObjectId"),
            (0x08, 1, "Boolean"),
            (0x09, 8, "DateTime"),
            (0x10, 4, "Int32"),
            (0x11, 8, "Timestamp"),
            (0x12, 8, "Int64"),
            (0x13, 16, "Decimal128"),
        ];
        for &(tb, width, name) in cases {
            let buf = vec![0u8; width - 1];
            assert_eq!(
                skip_bson_value(tb, &buf, 0),
                None,
                "{name}: short buffer must skip to None"
            );
            // Exactly the width fits.
            let full = vec![0u8; width];
            assert_eq!(
                skip_bson_value(tb, &full, 0),
                Some(width),
                "{name}: exact buffer must skip to {width}"
            );
        }
    }

    #[test]
    fn skip_objectid_into_short_buffer_is_none() {
        // The concrete regression from the RFC: ObjectId into 5 bytes.
        assert_eq!(skip_bson_value(0x07, &[0; 5], 0), None);
    }

    #[test]
    fn skip_string_result_overrun_is_none() {
        // Header fits (len = 100) but the body runs off the buffer. The
        // pre-hardening checked arm only guarded the header.
        let mut buf = Vec::new();
        buf.extend_from_slice(&100_i32.to_le_bytes());
        buf.extend_from_slice(b"short");
        assert_eq!(skip_bson_value(0x02, &buf, 0), None);
    }

    #[test]
    fn skip_document_length_under_header_is_none() {
        // A document length smaller than its own 4-byte header is malformed.
        let buf = 2_i32.to_le_bytes();
        assert_eq!(skip_bson_value(0x03, &buf, 0), None);
    }

    #[test]
    fn skip_negative_length_is_none() {
        let buf = (-1_i32).to_le_bytes();
        assert_eq!(skip_bson_value(0x02, &buf, 0), None);
    }

    // ── Per-type truncation (door 3: try_skip → typed error) ──

    #[test]
    fn try_skip_truncated_fixed_width_is_truncated() {
        for &(tb, width) in &[
            (0x01u8, 8usize),
            (0x07, 12),
            (0x09, 8),
            (0x10, 4),
            (0x12, 8),
            (0x13, 16),
        ] {
            let buf = vec![0u8; width - 1];
            assert_eq!(
                try_skip_bson_value(tb, &buf, 0),
                Err(RawBsonError::Truncated)
            );
        }
    }

    #[test]
    fn try_skip_unknown_type_is_unknown() {
        assert_eq!(
            try_skip_bson_value(0xFF, &[0; 8], 0),
            Err(RawBsonError::UnknownType(0xFF))
        );
    }

    #[test]
    fn try_skip_negative_length_is_bad_length() {
        let buf = (-1_i32).to_le_bytes();
        assert_eq!(
            try_skip_bson_value(0x02, &buf, 0),
            Err(RawBsonError::BadLength)
        );
    }

    #[test]
    fn try_skip_short_document_length_is_bad_length() {
        let buf = 2_i32.to_le_bytes();
        assert_eq!(
            try_skip_bson_value(0x03, &buf, 0),
            Err(RawBsonError::BadLength)
        );
    }

    #[test]
    fn try_skip_string_body_overrun_is_truncated() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100_i32.to_le_bytes());
        buf.extend_from_slice(b"short");
        assert_eq!(
            try_skip_bson_value(0x02, &buf, 0),
            Err(RawBsonError::Truncated)
        );
    }

    #[test]
    fn try_skip_valid_matches_option_skip() {
        // On well-formed bytes the two doors agree on the offset.
        let doc = rawdoc! { "n": 7_i32 };
        let bytes = doc_bytes(&doc);
        // Locate the i32 value position via the scanner.
        let field = RawField::get(bytes, "n").unwrap();
        let pos = field.value_start();
        assert_eq!(
            try_skip_bson_value(0x10, bytes, pos),
            Ok(skip_bson_value(0x10, bytes, pos).unwrap())
        );
    }

    // ── value() on truncated value bytes (door 2 + door 3) ────

    /// Construct a `RawField` pointing at `bytes[0..]` with a value that begins
    /// at offset 0 and claims to end at `element_end` (which may be out of
    /// bounds — exactly the corruption we're guarding).
    fn field_over(etype: ElementType, bytes: &[u8], element_end: usize) -> RawField<'_> {
        RawField {
            bytes,
            element_type: etype,
            element_start: 0,
            value_start: 0,
            element_end,
        }
    }

    #[test]
    fn value_truncated_fixed_width_is_none() {
        let cases: &[(ElementType, usize)] = &[
            (ElementType::Double, 8),
            (ElementType::ObjectId, 12),
            (ElementType::Boolean, 1),
            (ElementType::DateTime, 8),
            (ElementType::Int32, 4),
            (ElementType::Int64, 8),
            (ElementType::Decimal128, 16),
        ];
        for &(etype, width) in cases {
            let short = vec![0u8; width - 1];
            let field = field_over(etype, &short, width); // element_end past buffer
            assert_eq!(
                field.value(),
                None,
                "{etype:?} truncated value() must be None"
            );
            assert_eq!(
                field.try_value(),
                Err(RawBsonError::Truncated),
                "{etype:?} truncated try_value() must be Truncated"
            );
        }
    }

    #[test]
    fn value_string_len_zero_underflow_is_none() {
        // A String length of 0 is corrupt (the minimum is 1, the nul). The
        // pre-hardening code computed `len - 1`, underflowing usize and
        // panicking. It must now be `None` / `Truncated`.
        let buf = 0_i32.to_le_bytes(); // len header = 0, no body
        let field = field_over(ElementType::String, &buf, 4);
        assert_eq!(field.value(), None);
        assert_eq!(field.try_value(), Err(RawBsonError::Truncated));
    }

    #[test]
    fn value_string_body_truncated_is_none() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100_i32.to_le_bytes()); // claims 100 bytes
        buf.extend_from_slice(b"hi\0");
        let field = field_over(ElementType::String, &buf, buf.len());
        assert_eq!(field.value(), None);
    }

    #[test]
    fn try_value_non_converting_type_is_ok_none() {
        // Timestamp is valid but value() doesn't convert it → Ok(None), not Err.
        let doc = rawdoc! { "ts": bson::Timestamp { time: 1, increment: 2 } };
        let field = RawField::get(doc_bytes(&doc), "ts").unwrap();
        assert_eq!(field.try_value(), Ok(None));
    }

    #[test]
    fn try_value_valid_is_ok_some() {
        let doc = rawdoc! { "n": 5_i32 };
        let field = RawField::get(doc_bytes(&doc), "n").unwrap();
        assert_eq!(field.try_value(), Ok(Some(RawBsonRef::Int32(5))));
    }

    // ── try_get boundary: corruption vs. absence ──────────────

    #[test]
    fn try_get_found() {
        let doc = rawdoc! { "a": 1_i32, "b": "x" };
        // `RawField` is not Debug, so destructure rather than `.unwrap()` it.
        let Ok(Some(field)) = RawField::try_get(doc_bytes(&doc), "b") else {
            panic!("expected found");
        };
        assert_eq!(field.value(), Some(RawBsonRef::String("x")));
    }

    #[test]
    fn try_get_absent_is_ok_none() {
        let doc = rawdoc! { "a": 1_i32 };
        assert!(matches!(RawField::try_get(doc_bytes(&doc), "z"), Ok(None)));
    }

    #[test]
    fn try_get_truncated_document_is_err() {
        // A 4-byte length header claiming a doc longer than the buffer.
        let mut buf = Vec::new();
        buf.extend_from_slice(&999_i32.to_le_bytes());
        buf.push(0x10); // an Int32 element type byte, no body
        // `RawField` is byte-borrowing and intentionally not PartialEq/Debug,
        // so assert on the error directly rather than the whole Result.
        assert_eq!(
            RawField::try_get(&buf, "x").err().unwrap(),
            RawBsonError::Truncated
        );
    }

    #[test]
    fn try_get_short_header_is_err() {
        assert_eq!(
            RawField::try_get(&[0, 0], "x").err().unwrap(),
            RawBsonError::Truncated
        );
    }

    #[test]
    fn try_get_unterminated_name_is_err() {
        // Valid doc length, type byte, then a name with no nul before doc end.
        let mut body = Vec::new();
        body.push(0x10); // Int32
        body.extend_from_slice(b"name"); // no nul terminator
        body.extend_from_slice(&7_i32.to_le_bytes());
        let total = (4 + body.len() + 1) as i32; // header + body + trailing 0x00
        let mut buf = total.to_le_bytes().to_vec();
        buf.extend_from_slice(&body);
        buf.push(0x00);
        // The name scan never finds a nul before doc_end → Truncated.
        assert_eq!(
            RawField::try_get(&buf, "name").err().unwrap(),
            RawBsonError::Truncated
        );
    }

    #[test]
    fn try_get_path_nested_found() {
        let doc = rawdoc! { "a": { "b": 9_i32 } };
        let Ok(Some(field)) = RawField::try_get_path(doc_bytes(&doc), "a.b") else {
            panic!("expected found");
        };
        assert_eq!(field.value(), Some(RawBsonRef::Int32(9)));
    }

    #[test]
    fn try_get_path_non_doc_intermediate_is_ok_none() {
        let doc = rawdoc! { "a": 1_i32 };
        assert!(matches!(
            RawField::try_get_path(doc_bytes(&doc), "a.b"),
            Ok(None)
        ));
    }

    // ── Option-scanner malformed guards (door 2: scan → None) ──

    #[test]
    fn get_short_header_is_none() {
        // Fewer than 4 bytes can't even hold a document length header.
        assert!(RawField::get(&[0, 0], "x").is_none());
        assert!(RawField::get(&[], "x").is_none());
    }

    #[test]
    fn get_unterminated_name_is_none() {
        // Doc length OK, type byte present, but the field name never hits a nul
        // before doc_end → the name-scan guard returns None.
        let mut body = Vec::new();
        body.push(0x10); // Int32
        body.extend_from_slice(b"name"); // no nul terminator
        body.extend_from_slice(&7_i32.to_le_bytes());
        let total = (4 + body.len() + 1) as i32;
        let mut buf = total.to_le_bytes().to_vec();
        buf.extend_from_slice(&body);
        buf.push(0x00);
        assert!(RawField::get(&buf, "name").is_none());
    }

    #[test]
    fn get_truncated_value_mid_doc_is_none() {
        // A document whose declared length includes a value that runs past the
        // buffer: the scanner's `skip_bson_value` returns None and `get`
        // propagates it instead of panicking.
        let mut body = Vec::new();
        body.push(0x12); // Int64 (8-byte value)
        body.extend_from_slice(b"n\0");
        body.extend_from_slice(&[0u8; 3]); // only 3 of 8 value bytes
        let total = (4 + body.len() + 1) as i32;
        let mut buf = total.to_le_bytes().to_vec();
        buf.extend_from_slice(&body);
        buf.push(0x00);
        assert!(RawField::get(&buf, "n").is_none());
    }

    // ── RawBsonError Display ───────────────────────────────────

    #[test]
    fn raw_bson_error_display() {
        assert_eq!(RawBsonError::Truncated.to_string(), "raw BSON truncated");
        assert_eq!(
            RawBsonError::BadLength.to_string(),
            "raw BSON has an invalid length field"
        );
        assert_eq!(
            RawBsonError::UnknownType(0x06).to_string(),
            "raw BSON has unrecognised element type 0x06"
        );
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
