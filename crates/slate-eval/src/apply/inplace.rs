//! In-place BSON byte edits for the simple UPDATE assignment shapes.
//!
//! The naive [`super::rebuild`] deserializes the whole document into a
//! `bson::Document`, mutates it, and re-serializes — paying O(doc) work on
//! every update regardless of how little it touches. For the common shapes this
//! module edits the raw bytes directly instead: evaluate each assignment's
//! right-hand [`Expression`] against the *original* document with the zero-copy
//! [`crate::raweval`] (so only the referenced fields are read), then splice the
//! result into the byte buffer.
//!
//! What it handles in place (everything else returns [`Outcome::Fallback`] and
//! the caller rebuilds):
//! - **single-segment paths only** — a dotted path edits a nested document;
//! - **scalar / undefined results** — `$set` to a scalar, `$inc`, `$rename`'s
//!   set-from-field, and `$unset` (an undefined result removes the field),
//!   including a value that grows/shrinks/retypes the slot, and creating a
//!   missing field;
//! - **`$push` / `$pop` over the field's own array** — appended/dropped at the
//!   array tail without re-encoding the array.
//!
//! The byte technique mirrors the (now-removed) `slate-mutation` raw engine:
//! overwrite in place when the new value keeps the same BSON type and width,
//! otherwise splice/shift; removal shifts bytes left. Malformed bytes degrade to
//! a fallback or a no-op rather than panicking (stored documents are validated
//! on write, so this is defence-in-depth).

use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use bson::spec::ElementType;
use bson::{Bson, Document};
use slate_ast::{Assignment, Expression};
use slate_rawbson::{RawField, RawFieldLoc, skip_bson_value};

use crate::error::{EvalError, Result};
use crate::raweval::{self, RawEnv, RawValue};

/// The outcome of attempting the in-place fast path.
pub(crate) enum Outcome {
    /// Handled entirely in place. `None` means the document was unchanged (the
    /// edits net to identical bytes), matching the rebuild's "unchanged" result.
    Done(Option<RawDocumentBuf>),
    /// A shape the byte path doesn't handle; the caller should rebuild.
    Fallback,
}

/// Try to apply `assignments` to `old` via in-place byte edits, evaluating each
/// right-hand side against the original `old` (so assignments don't observe each
/// other — same snapshot semantics as the rebuild).
pub(crate) fn try_apply(
    old: &RawDocument,
    alias: &str,
    assignments: &[Assignment],
    params: &Document,
) -> Result<Outcome> {
    // The fast path evaluates the RHS through `raweval` with no params binding.
    // If the caller supplied query parameters, defer to the rebuild (which
    // threads them through the owned evaluator). UPDATE never passes params.
    if !params.is_empty() {
        return Ok(Outcome::Fallback);
    }

    // Eligibility pre-scan: every path must be a single segment, and no field
    // may be targeted twice. A duplicate target would let one assignment's edit
    // be observed by a later one through the working buffer, breaking the
    // "evaluate against the original" invariant the byte ops rely on (the
    // rebuild handles that correctly, so we hand it over).
    let mut seen: Vec<&str> = Vec::with_capacity(assignments.len());
    for a in assignments {
        let [field] = a.path.as_slice() else {
            return Ok(Outcome::Fallback);
        };
        if seen.contains(&field.as_str()) {
            return Ok(Outcome::Fallback);
        }
        seen.push(field);
    }

    let mut bytes = old.as_bytes().to_vec();
    let binds = [(alias, RawBsonRef::Document(old))];
    let env = RawEnv::new(&binds, None);

    for a in assignments {
        let field = &a.path[0];
        match recognize_array_op(&a.value, alias, field) {
            // Tier 2: append/drop at the array tail.
            Some(ArrayOp::Push(value_expr)) => {
                if !apply_push(&mut bytes, &env, field, value_expr)? {
                    return Ok(Outcome::Fallback);
                }
            }
            Some(ArrayOp::Pop) => apply_pop(&mut bytes, field),
            // Tier 1: a scalar (or undefined) result edits/removes the slot.
            None => {
                let val = raweval::eval(&a.value, &env)?;
                if val.is_undefined() {
                    remove_field(&mut bytes, field);
                } else if let Some((etype, value)) = encode_scalar(&val) {
                    set_field(&mut bytes, field, etype, &value);
                } else {
                    // A non-scalar result (whole document/array). Rebuild.
                    return Ok(Outcome::Fallback);
                }
            }
        }
    }

    // Identical bytes ⇒ identical document, since every edit re-encodes its
    // value exactly as `bson` would — so this matches the rebuild's decision to
    // return `None` (drop the unchanged row).
    if bytes.as_slice() == old.as_bytes() {
        return Ok(Outcome::Done(None));
    }
    let buf = RawDocumentBuf::from_bytes(bytes)
        .map_err(|e| bson_err(format!("in-place apply produced invalid BSON: {e}")))?;
    Ok(Outcome::Done(Some(buf)))
}

fn bson_err(message: String) -> EvalError {
    EvalError { message }
}

// ── Recognizing the array operators ─────────────────────────────────────────

/// A structural array operator over a field's *own* array.
enum ArrayOp<'a> {
    /// `rpush(c.field, <value_expr>)` — append `value_expr` to the tail.
    Push(&'a Expression),
    /// `pop(c.field)` — drop the last element.
    Pop,
}

/// Recognize `rpush(c.field, v)` / `pop(c.field)` where the array argument is
/// exactly the assignment's own target field (`alias.field`) — the only case
/// where a tail edit is valid. `$lpush` is deliberately *not* matched: a
/// prepend renumbers every element key, so it has no cheap in-place form and
/// falls through to the rebuild.
fn recognize_array_op<'a>(expr: &'a Expression, alias: &str, field: &str) -> Option<ArrayOp<'a>> {
    let Expression::Function { name, args } = expr else {
        return None;
    };
    let is_own_field = |e: &Expression| match e {
        Expression::Member { base, field: f } => {
            f == field && matches!(base.as_ref(), Expression::Identifier(id) if id == alias)
        }
        _ => false,
    };
    if name.eq_ignore_ascii_case("rpush") && args.len() == 2 && is_own_field(&args[0]) {
        return Some(ArrayOp::Push(&args[1]));
    }
    if name.eq_ignore_ascii_case("pop") && args.len() == 1 && is_own_field(&args[0]) {
        return Some(ArrayOp::Pop);
    }
    None
}

// ── Scalar encoding ─────────────────────────────────────────────────────────

/// Encode a [`RawValue`] as `(type, value_bytes)` if it is a scalar this path
/// can splice; `None` for documents/arrays/undefined (which fall back). Encodes
/// each type exactly as `bson` serializes it, so the result is byte-identical to
/// the rebuild.
fn encode_scalar(v: &RawValue) -> Option<(ElementType, Vec<u8>)> {
    match v {
        RawValue::Ref(r) => encode_raw_ref(*r),
        RawValue::OwnedRaw(rb) => encode_raw_ref(rb.as_raw_bson_ref()),
        RawValue::Owned(b) => encode_bson(b),
        RawValue::Undefined => None,
    }
}

fn encode_raw_ref(r: RawBsonRef) -> Option<(ElementType, Vec<u8>)> {
    Some(match r {
        RawBsonRef::Double(f) => (ElementType::Double, f.to_le_bytes().to_vec()),
        RawBsonRef::Int32(n) => (ElementType::Int32, n.to_le_bytes().to_vec()),
        RawBsonRef::Int64(n) => (ElementType::Int64, n.to_le_bytes().to_vec()),
        RawBsonRef::Boolean(b) => (ElementType::Boolean, vec![b as u8]),
        RawBsonRef::Null => (ElementType::Null, Vec::new()),
        RawBsonRef::String(s) => (ElementType::String, encode_string(s)),
        RawBsonRef::DateTime(dt) => (
            ElementType::DateTime,
            dt.timestamp_millis().to_le_bytes().to_vec(),
        ),
        RawBsonRef::ObjectId(oid) => (ElementType::ObjectId, oid.bytes().to_vec()),
        _ => return None,
    })
}

fn encode_bson(b: &Bson) -> Option<(ElementType, Vec<u8>)> {
    match b {
        Bson::Double(_)
        | Bson::Int32(_)
        | Bson::Int64(_)
        | Bson::Boolean(_)
        | Bson::Null
        | Bson::String(_)
        | Bson::DateTime(_)
        | Bson::ObjectId(_) => encode_raw_ref(raw_ref_of(b)?),
        _ => None,
    }
}

/// Borrow a scalar `Bson` as a `RawBsonRef` (so both encoders share one match).
fn raw_ref_of(b: &Bson) -> Option<RawBsonRef<'_>> {
    Some(match b {
        Bson::Double(f) => RawBsonRef::Double(*f),
        Bson::Int32(n) => RawBsonRef::Int32(*n),
        Bson::Int64(n) => RawBsonRef::Int64(*n),
        Bson::Boolean(b) => RawBsonRef::Boolean(*b),
        Bson::Null => RawBsonRef::Null,
        Bson::String(s) => RawBsonRef::String(s),
        Bson::DateTime(dt) => RawBsonRef::DateTime(*dt),
        Bson::ObjectId(oid) => RawBsonRef::ObjectId(*oid),
        _ => return None,
    })
}

/// BSON string value bytes: `i32(len incl. nul) + utf8 + nul`.
fn encode_string(s: &str) -> Vec<u8> {
    let len = (s.len() + 1) as i32;
    let mut buf = Vec::with_capacity(4 + s.len() + 1);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf.push(0x00);
    buf
}

// ── Byte-level field edits ──────────────────────────────────────────────────

/// Build a BSON element: `[type][name\0][value]`.
fn encode_element(field: &str, etype: ElementType, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + field.len() + 1 + value.len());
    buf.push(etype as u8);
    buf.extend_from_slice(field.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(value);
    buf
}

/// Overwrite the 4-byte document-length header with the current buffer length.
fn update_doc_length(bytes: &mut [u8]) {
    let len = bytes.len() as i32;
    bytes[0..4].copy_from_slice(&len.to_le_bytes());
}

fn locate(bytes: &[u8], field: &str) -> Option<RawFieldLoc> {
    RawField::get(bytes, field).map(|f| f.loc())
}

/// Read a little-endian `i32` at `at`, or `None` if out of bounds.
fn read_i32_le(bytes: &[u8], at: usize) -> Option<i32> {
    let slice = bytes.get(at..at + 4)?;
    Some(i32::from_le_bytes(<[u8; 4]>::try_from(slice).ok()?))
}

/// Set `field` to `(etype, value)`: overwrite in place when the slot keeps the
/// same type and width, otherwise splice the element; append if absent.
fn set_field(bytes: &mut Vec<u8>, field: &str, etype: ElementType, value: &[u8]) {
    match locate(bytes, field) {
        Some(loc) => {
            if loc.element_type() == etype && loc.element_end() - loc.value_start() == value.len() {
                bytes[loc.value_start()..loc.element_end()].copy_from_slice(value);
                return;
            }
            let elem = encode_element(field, etype, value);
            bytes.splice(loc.element_start()..loc.element_end(), elem);
            update_doc_length(bytes);
        }
        None => append_element(bytes, encode_element(field, etype, value)),
    }
}

/// Remove `field`, shifting the trailing bytes left. A no-op if absent.
fn remove_field(bytes: &mut Vec<u8>, field: &str) {
    if let Some(loc) = locate(bytes, field) {
        bytes.drain(loc.element_start()..loc.element_end());
        update_doc_length(bytes);
    }
}

/// Splice a fully-encoded element in before the document's trailing `0x00`.
fn append_element(bytes: &mut Vec<u8>, elem: Vec<u8>) {
    let at = bytes.len().saturating_sub(1);
    bytes.splice(at..at, elem);
    update_doc_length(bytes);
}

// ── Array operators ─────────────────────────────────────────────────────────

/// Apply `rpush(c.field, value_expr)`. Returns `Ok(false)` (fall back) only when
/// the element isn't a splice-able scalar; every other case is handled in place:
/// missing field → singleton array, array → tail append, non-array → remove the
/// field (mirroring `rpush`'s undefined-on-non-array result), undefined value →
/// no-op (`rpush` leaves the array unchanged).
fn apply_push(
    bytes: &mut Vec<u8>,
    env: &RawEnv,
    field: &str,
    value_expr: &Expression,
) -> Result<bool> {
    let elem = raweval::eval(value_expr, env)?;
    if elem.is_undefined() {
        return Ok(true);
    }
    let Some((etype, value)) = encode_scalar(&elem) else {
        return Ok(false);
    };
    match locate(bytes, field) {
        None => append_element(
            bytes,
            encode_element(field, ElementType::Array, &singleton(etype, &value)),
        ),
        Some(loc) if loc.element_type() == ElementType::Array => {
            if !array_append(bytes, loc, etype, &value) {
                return Ok(false);
            }
        }
        Some(_) => remove_field(bytes, field),
    }
    Ok(true)
}

/// Array body for a single element keyed `"0"`: `[i32 size]["0"\0 value][0x00]`.
fn singleton(etype: ElementType, value: &[u8]) -> Vec<u8> {
    let inner = encode_element("0", etype, value);
    let size = (4 + inner.len() + 1) as i32;
    let mut arr = Vec::with_capacity(size as usize);
    arr.extend_from_slice(&size.to_le_bytes());
    arr.extend_from_slice(&inner);
    arr.push(0x00);
    arr
}

/// Append `(etype, value)` to the array located at `loc`, keyed by its next
/// index. Returns `false` (fall back) if the array bytes don't parse.
fn array_append(bytes: &mut Vec<u8>, loc: RawFieldLoc, etype: ElementType, value: &[u8]) -> bool {
    let arr_start = loc.value_start();
    let Some(count) = array_len(bytes, arr_start) else {
        return false;
    };
    let Some(arr_size) = read_i32_le(bytes, arr_start) else {
        return false;
    };
    let arr_end = arr_start.saturating_add(arr_size as usize).min(bytes.len());
    let elem = encode_element(&count.to_string(), etype, value);
    let elem_len = elem.len();
    let insert = arr_end.saturating_sub(1); // before the array terminator
    bytes.splice(insert..insert, elem);
    let new_size = (arr_size as usize + elem_len) as i32;
    bytes[arr_start..arr_start + 4].copy_from_slice(&new_size.to_le_bytes());
    update_doc_length(bytes);
    true
}

/// Apply `pop(c.field)`: array → drop the last element (empty array unchanged),
/// non-array → remove the field (`pop` yields undefined), missing → no-op.
fn apply_pop(bytes: &mut Vec<u8>, field: &str) {
    match locate(bytes, field) {
        Some(loc) if loc.element_type() == ElementType::Array => drop_last(bytes, loc),
        Some(_) => remove_field(bytes, field),
        None => {}
    }
}

/// Drop the last element of the array at `loc`, shifting the array terminator
/// left. A no-op for an empty array or unparsable bytes.
fn drop_last(bytes: &mut Vec<u8>, loc: RawFieldLoc) {
    let arr_start = loc.value_start();
    let Some(arr_size) = read_i32_le(bytes, arr_start) else {
        return;
    };
    let arr_end = arr_start.saturating_add(arr_size as usize).min(bytes.len());
    let Some(last_start) = last_element_start(bytes, arr_start, arr_end) else {
        return;
    };
    let drain_end = arr_end.saturating_sub(1); // before the array terminator
    if last_start >= drain_end {
        return;
    }
    let removed = drain_end - last_start;
    bytes.drain(last_start..drain_end);
    let new_size = (arr_size as usize - removed) as i32;
    bytes[arr_start..arr_start + 4].copy_from_slice(&new_size.to_le_bytes());
    update_doc_length(bytes);
}

/// Count the elements of the array whose header begins at `arr_start`, or `None`
/// if the bytes don't parse.
fn array_len(bytes: &[u8], arr_start: usize) -> Option<usize> {
    let arr_size = read_i32_le(bytes, arr_start)? as usize;
    let arr_end = arr_start.saturating_add(arr_size).min(bytes.len());
    let mut count = 0;
    let mut pos = arr_start + 4;
    while pos < arr_end {
        let tb = bytes[pos];
        if tb == 0x00 {
            break;
        }
        pos = skip_element(bytes, pos, arr_end)?;
        count += 1;
    }
    Some(count)
}

/// The byte offset where the array's last element starts, or `None` if empty /
/// unparsable.
fn last_element_start(bytes: &[u8], arr_start: usize, arr_end: usize) -> Option<usize> {
    let mut last = None;
    let mut pos = arr_start + 4;
    while pos < arr_end {
        let tb = bytes[pos];
        if tb == 0x00 {
            break;
        }
        last = Some(pos);
        pos = skip_element(bytes, pos, arr_end)?;
    }
    last
}

/// Advance past one `[type][name\0][value]` element starting at `pos` (bounded
/// by `end`), or `None` if it runs off the end.
fn skip_element(bytes: &[u8], pos: usize, end: usize) -> Option<usize> {
    let tb = *bytes.get(pos)?;
    let mut p = pos + 1;
    while p < end && bytes[p] != 0x00 {
        p += 1;
    }
    p += 1; // name terminator
    skip_bson_value(tb, bytes, p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{Document, doc, rawdoc};

    fn to_doc(bytes: &[u8]) -> Document {
        bson::deserialize_from_slice(bytes).unwrap()
    }

    // ── set_field ───────────────────────────────────────────────────────────

    #[test]
    fn set_same_width_overwrites_in_place() {
        let raw = rawdoc! { "_id": "1", "score": 10_i32 };
        let mut bytes = raw.as_bytes().to_vec();
        let len = bytes.len();
        set_field(
            &mut bytes,
            "score",
            ElementType::Int32,
            &20_i32.to_le_bytes(),
        );
        assert_eq!(bytes.len(), len, "same width must not resize");
        assert_eq!(to_doc(&bytes).get_i32("score").unwrap(), 20);
    }

    #[test]
    fn set_growing_value_splices() {
        let raw = rawdoc! { "_id": "1", "a": 1_i32, "b": 2_i32 };
        let mut bytes = raw.as_bytes().to_vec();
        set_field(
            &mut bytes,
            "a",
            ElementType::String,
            &encode_string("hello"),
        );
        let got = to_doc(&bytes);
        assert_eq!(got.get_str("a").unwrap(), "hello");
        assert_eq!(got.get_i32("b").unwrap(), 2);
    }

    #[test]
    fn set_shrinking_value_splices() {
        let raw = rawdoc! { "_id": "1", "name": "Alexander", "b": 2_i32 };
        let mut bytes = raw.as_bytes().to_vec();
        set_field(
            &mut bytes,
            "name",
            ElementType::String,
            &encode_string("Al"),
        );
        let got = to_doc(&bytes);
        assert_eq!(got.get_str("name").unwrap(), "Al");
        assert_eq!(got.get_i32("b").unwrap(), 2);
    }

    #[test]
    fn set_missing_field_appends() {
        let raw = rawdoc! { "a": 1_i32 };
        let mut bytes = raw.as_bytes().to_vec();
        set_field(&mut bytes, "b", ElementType::Int32, &2_i32.to_le_bytes());
        let got = to_doc(&bytes);
        assert_eq!(got.get_i32("a").unwrap(), 1);
        assert_eq!(got.get_i32("b").unwrap(), 2);
    }

    // ── remove_field ──────────────────────────────────────────────────────────

    #[test]
    fn remove_existing_field() {
        let raw = rawdoc! { "a": 1_i32, "b": 2_i32 };
        let mut bytes = raw.as_bytes().to_vec();
        remove_field(&mut bytes, "a");
        let got = to_doc(&bytes);
        assert!(got.get("a").is_none());
        assert_eq!(got.get_i32("b").unwrap(), 2);
    }

    #[test]
    fn remove_missing_field_is_noop() {
        let raw = rawdoc! { "a": 1_i32 };
        let mut bytes = raw.as_bytes().to_vec();
        let before = bytes.clone();
        remove_field(&mut bytes, "z");
        assert_eq!(bytes, before);
    }

    // ── array append / pop ────────────────────────────────────────────────────

    #[test]
    fn append_to_existing_array() {
        let raw = rawdoc! { "tags": ["a", "b"] };
        let mut bytes = raw.as_bytes().to_vec();
        let loc = locate(&bytes, "tags").unwrap();
        assert!(array_append(
            &mut bytes,
            loc,
            ElementType::String,
            &encode_string("c")
        ));
        let got = to_doc(&bytes);
        let arr = got.get_array("tags").unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[2].as_str().unwrap(), "c");
    }

    #[test]
    fn drop_last_from_array() {
        let raw = rawdoc! { "tags": ["a", "b", "c"] };
        let mut bytes = raw.as_bytes().to_vec();
        let loc = locate(&bytes, "tags").unwrap();
        drop_last(&mut bytes, loc);
        let got = to_doc(&bytes);
        let arr = got.get_array("tags").unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[1].as_str().unwrap(), "b");
    }

    #[test]
    fn drop_last_empty_array_is_noop() {
        let raw = rawdoc! { "tags": [] };
        let mut bytes = raw.as_bytes().to_vec();
        let before = bytes.clone();
        let loc = locate(&bytes, "tags").unwrap();
        drop_last(&mut bytes, loc);
        assert_eq!(bytes, before);
    }

    #[test]
    fn singleton_array_round_trips() {
        let mut bytes = rawdoc! { "a": 1_i32 }.as_bytes().to_vec();
        append_element(
            &mut bytes,
            encode_element(
                "tags",
                ElementType::Array,
                &singleton(ElementType::Int32, &9_i32.to_le_bytes()),
            ),
        );
        let arr = to_doc(&bytes).get_array("tags").unwrap().clone();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_i32().unwrap(), 9);
    }

    // ── try_apply dispatch ────────────────────────────────────────────────────

    fn set(field: &str, value: Bson) -> Assignment {
        Assignment {
            path: field.split('.').map(String::from).collect(),
            value: Expression::Value(value),
        }
    }

    fn done(old: &RawDocument, assignments: &[Assignment]) -> Option<Document> {
        match try_apply(old, "c", assignments, &Document::new()).unwrap() {
            Outcome::Done(opt) => opt.map(|raw| to_doc(raw.as_bytes())),
            Outcome::Fallback => panic!("expected Done, got Fallback"),
        }
    }

    fn is_fallback(old: &RawDocument, assignments: &[Assignment]) -> bool {
        matches!(
            try_apply(old, "c", assignments, &Document::new()).unwrap(),
            Outcome::Fallback
        )
    }

    #[test]
    fn set_then_unchanged_returns_none() {
        let raw = rawdoc! { "_id": "1", "a": 1_i32 };
        assert_eq!(done(&raw, &[set("a", Bson::Int32(1))]), None);
    }

    #[test]
    fn set_changes_value() {
        let raw = rawdoc! { "_id": "1", "a": 1_i32 };
        assert_eq!(
            done(&raw, &[set("a", Bson::Int32(9))]),
            Some(doc! { "_id": "1", "a": 9_i32 })
        );
    }

    #[test]
    fn dotted_path_falls_back() {
        let raw = rawdoc! { "a": { "b": 1_i32 } };
        assert!(is_fallback(&raw, &[set("a.b", Bson::Int32(2))]));
    }

    #[test]
    fn whole_document_value_falls_back() {
        let raw = rawdoc! { "a": 1_i32 };
        assert!(is_fallback(
            &raw,
            &[set("a", Bson::Document(doc! { "x": 1_i32 }))]
        ));
    }

    #[test]
    fn duplicate_target_field_falls_back() {
        let raw = rawdoc! { "a": 1_i32 };
        assert!(is_fallback(
            &raw,
            &[set("a", Bson::Int32(2)), set("a", Bson::Int32(3))]
        ));
    }
}
