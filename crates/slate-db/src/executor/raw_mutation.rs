//! Raw byte-level BSON mutation engine.
//!
//! Applies mutations by directly manipulating the `Vec<u8>` backing a
//! `RawDocumentBuf` — splicing, overwriting, or draining bytes — without
//! deserializing to `bson::Document`. Falls back to the deserialization
//! path for cases it cannot handle (dot-paths, `$rename`, `$lpush`,
//! Document/Array `$set` values).

use bson::raw::RawDocument;
use bson::{Bson, RawDocumentBuf};
use slate_query::{Mutation, MutationOp};

use super::raw_bson::find_field;
use slate_engine::encoding::skip_bson_value;
use crate::error::DbError;

// ── Result type ─────────────────────────────────────────────────

pub(crate) enum RawMutationResult {
    /// Mutation applied; here is the new document.
    Applied(RawDocumentBuf),
    /// Nothing changed (all ops were no-ops).
    Unchanged,
    /// Cannot handle at the byte level; caller should fall back.
    Fallback,
}

/// Encode a scalar `Bson` value into its raw BSON bytes and type tag.
/// Returns `None` for Document/Array (triggers fallback).
fn encode_bson_value(value: &Bson) -> Option<(u8, Vec<u8>)> {
    match value {
        Bson::Int32(n) => Some((0x10, n.to_le_bytes().to_vec())),
        Bson::Int64(n) => Some((0x12, n.to_le_bytes().to_vec())),
        Bson::Double(f) => Some((0x01, f.to_le_bytes().to_vec())),
        Bson::Boolean(b) => Some((0x08, vec![*b as u8])),
        Bson::String(s) => {
            let len = (s.len() + 1) as i32;
            let mut buf = Vec::with_capacity(4 + s.len() + 1);
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00);
            Some((0x02, buf))
        }
        Bson::DateTime(dt) => Some((0x09, dt.timestamp_millis().to_le_bytes().to_vec())),
        Bson::Null => Some((0x0A, vec![])),
        Bson::ObjectId(oid) => Some((0x07, oid.bytes().to_vec())),
        _ => None,
    }
}

/// Build a complete BSON element: `[type_byte][field_name\0][value_bytes]`.
fn encode_element(field_name: &str, type_byte: u8, value_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + field_name.len() + 1 + value_bytes.len());
    buf.push(type_byte);
    buf.extend_from_slice(field_name.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(value_bytes);
    buf
}

/// Overwrite the 4-byte document length header with the current vec length.
fn update_doc_length(bytes: &mut Vec<u8>) {
    let len = bytes.len() as i32;
    bytes[0..4].copy_from_slice(&len.to_le_bytes());
}

// ── Operator implementations ────────────────────────────────────

/// `$set` on a flat field. Returns true if the document changed.
fn raw_set(bytes: &mut Vec<u8>, field: &str, value: &Bson) -> Option<bool> {
    let (new_type, new_val) = encode_bson_value(value)?;

    match find_field(bytes, field) {
        Some(loc) => {
            let old_val = &bytes[loc.value_start..loc.element_end];
            if loc.type_byte == new_type && old_val == new_val.as_slice() {
                return Some(false); // no-op
            }
            // Same type + same value length → overwrite in place
            if loc.type_byte == new_type && old_val.len() == new_val.len() {
                bytes[loc.value_start..loc.element_end].copy_from_slice(&new_val);
                return Some(true);
            }
            // Different size or type → splice the whole element
            let new_elem = encode_element(field, new_type, &new_val);
            bytes.splice(loc.element_start..loc.element_end, new_elem);
            update_doc_length(bytes);
            Some(true)
        }
        None => {
            // Field missing → append before trailing 0x00
            let new_elem = encode_element(field, new_type, &new_val);
            let insert_pos = bytes.len() - 1; // before terminator
            bytes.splice(insert_pos..insert_pos, new_elem);
            update_doc_length(bytes);
            Some(true)
        }
    }
}

/// `$unset` on a flat field. Returns true if the document changed.
fn raw_unset(bytes: &mut Vec<u8>, field: &str) -> bool {
    match find_field(bytes, field) {
        Some(loc) => {
            bytes.drain(loc.element_start..loc.element_end);
            update_doc_length(bytes);
            true
        }
        None => false,
    }
}

/// `$inc` on a flat field. Returns `Ok(true)` if changed, `Err` on type error.
/// Returns `Ok(None)` via the outer Option if encoding is unsupported (shouldn't happen).
fn raw_inc(bytes: &mut Vec<u8>, field: &str, amount: &Bson) -> Result<bool, DbError> {
    match find_field(bytes, field) {
        Some(loc) => {
            match (loc.type_byte, amount) {
                // i32 + i32 → i32 (or promote to i64 on overflow)
                (0x10, Bson::Int32(b)) => {
                    let a = i32::from_le_bytes(
                        bytes[loc.value_start..loc.value_start + 4]
                            .try_into()
                            .unwrap(),
                    );
                    match a.checked_add(*b) {
                        Some(sum) => {
                            bytes[loc.value_start..loc.value_start + 4]
                                .copy_from_slice(&sum.to_le_bytes());
                            Ok(true)
                        }
                        None => {
                            // Overflow → promote to i64, element grows
                            let sum = a as i64 + *b as i64;
                            let new_elem = encode_element(field, 0x12, &sum.to_le_bytes());
                            bytes.splice(loc.element_start..loc.element_end, new_elem);
                            update_doc_length(bytes);
                            Ok(true)
                        }
                    }
                }
                // i32 + i64 → i64
                (0x10, Bson::Int64(b)) => {
                    let a = i32::from_le_bytes(
                        bytes[loc.value_start..loc.value_start + 4]
                            .try_into()
                            .unwrap(),
                    ) as i64;
                    let sum = a + b;
                    let new_elem = encode_element(field, 0x12, &sum.to_le_bytes());
                    bytes.splice(loc.element_start..loc.element_end, new_elem);
                    update_doc_length(bytes);
                    Ok(true)
                }
                // i64 + i32 → i64
                (0x12, Bson::Int32(b)) => {
                    let a = i64::from_le_bytes(
                        bytes[loc.value_start..loc.value_start + 8]
                            .try_into()
                            .unwrap(),
                    );
                    let sum = a + *b as i64;
                    bytes[loc.value_start..loc.value_start + 8].copy_from_slice(&sum.to_le_bytes());
                    Ok(true)
                }
                // i64 + i64 → i64
                (0x12, Bson::Int64(b)) => {
                    let a = i64::from_le_bytes(
                        bytes[loc.value_start..loc.value_start + 8]
                            .try_into()
                            .unwrap(),
                    );
                    let sum = a + b;
                    bytes[loc.value_start..loc.value_start + 8].copy_from_slice(&sum.to_le_bytes());
                    Ok(true)
                }
                // f64 + f64 → f64
                (0x01, Bson::Double(b)) => {
                    let a = f64::from_le_bytes(
                        bytes[loc.value_start..loc.value_start + 8]
                            .try_into()
                            .unwrap(),
                    );
                    let sum = a + b;
                    bytes[loc.value_start..loc.value_start + 8].copy_from_slice(&sum.to_le_bytes());
                    Ok(true)
                }
                // i32 + f64 → f64
                (0x10, Bson::Double(b)) => {
                    let a = i32::from_le_bytes(
                        bytes[loc.value_start..loc.value_start + 4]
                            .try_into()
                            .unwrap(),
                    ) as f64;
                    let sum = a + b;
                    let new_elem = encode_element(field, 0x01, &sum.to_le_bytes());
                    bytes.splice(loc.element_start..loc.element_end, new_elem);
                    update_doc_length(bytes);
                    Ok(true)
                }
                // i64 + f64 → f64
                (0x12, Bson::Double(b)) => {
                    let a = i64::from_le_bytes(
                        bytes[loc.value_start..loc.value_start + 8]
                            .try_into()
                            .unwrap(),
                    ) as f64;
                    let sum = a + b;
                    // Type changes from i64(0x12, 8 bytes) to f64(0x01, 8 bytes) — same size!
                    bytes[loc.element_start] = 0x01; // change type byte
                    bytes[loc.value_start..loc.value_start + 8].copy_from_slice(&sum.to_le_bytes());
                    Ok(true)
                }
                // f64 + i32 → f64
                (0x01, Bson::Int32(b)) => {
                    let a = f64::from_le_bytes(
                        bytes[loc.value_start..loc.value_start + 8]
                            .try_into()
                            .unwrap(),
                    );
                    let sum = a + *b as f64;
                    bytes[loc.value_start..loc.value_start + 8].copy_from_slice(&sum.to_le_bytes());
                    Ok(true)
                }
                // f64 + i64 → f64
                (0x01, Bson::Int64(b)) => {
                    let a = f64::from_le_bytes(
                        bytes[loc.value_start..loc.value_start + 8]
                            .try_into()
                            .unwrap(),
                    );
                    let sum = a + *b as f64;
                    bytes[loc.value_start..loc.value_start + 8].copy_from_slice(&sum.to_le_bytes());
                    Ok(true)
                }
                _ => Err(DbError::InvalidQuery(format!(
                    "$inc: field '{field}' is not numeric"
                ))),
            }
        }
        None => {
            // Missing field → default to 0 + amount
            let (type_byte, val_bytes) = match amount {
                Bson::Int32(n) => (0x10_u8, n.to_le_bytes().to_vec()),
                Bson::Int64(n) => (0x12_u8, n.to_le_bytes().to_vec()),
                Bson::Double(f) => (0x01_u8, f.to_le_bytes().to_vec()),
                _ => {
                    return Err(DbError::InvalidQuery("$inc: amount is not numeric".into()));
                }
            };
            let new_elem = encode_element(field, type_byte, &val_bytes);
            let insert_pos = bytes.len() - 1;
            bytes.splice(insert_pos..insert_pos, new_elem);
            update_doc_length(bytes);
            Ok(true)
        }
    }
}

/// `$push` on a flat field — append value to array.
fn raw_push(bytes: &mut Vec<u8>, field: &str, value: &Bson) -> Result<Option<bool>, DbError> {
    let (val_type, val_bytes) = match encode_bson_value(value) {
        Some(v) => v,
        None => return Ok(None), // unsupported value type → fallback
    };

    match find_field(bytes, field) {
        Some(loc) => {
            if loc.type_byte != 0x04 {
                return Err(DbError::InvalidQuery(format!(
                    "$push: field '{field}' is not an array"
                )));
            }
            // Count existing elements to determine the next index key
            let arr_start = loc.value_start;
            let arr_size =
                i32::from_le_bytes(bytes[arr_start..arr_start + 4].try_into().unwrap()) as usize;
            let arr_end = arr_start + arr_size; // includes terminator

            let mut count = 0usize;
            let mut pos = arr_start + 4;
            while pos < arr_end {
                if bytes[pos] == 0x00 {
                    break;
                }
                let tb = bytes[pos];
                pos += 1;
                // Skip element name
                while pos < arr_end && bytes[pos] != 0x00 {
                    pos += 1;
                }
                pos += 1; // null terminator
                pos = match skip_bson_value(tb, bytes, pos) {
                    Some(p) => p,
                    None => return Ok(None), // malformed → fallback
                };
                count += 1;
            }

            // Build the new array element: [type][index_str\0][value]
            let index_str = count.to_string();
            let new_arr_elem = encode_element(&index_str, val_type, &val_bytes);

            // Insert before the array's trailing 0x00
            let insert_pos = arr_end - 1;
            let elem_len = new_arr_elem.len();
            bytes.splice(insert_pos..insert_pos, new_arr_elem);

            // Update array size header
            let new_arr_size = (arr_size + elem_len) as i32;
            bytes[arr_start..arr_start + 4].copy_from_slice(&new_arr_size.to_le_bytes());

            // Update document size header
            update_doc_length(bytes);
            Ok(Some(true))
        }
        None => {
            // Missing field → create a new single-element array
            // Array: [i32: size]["0" => value][0x00]
            let inner_elem = encode_element("0", val_type, &val_bytes);
            let arr_size = (4 + inner_elem.len() + 1) as i32; // header + elem + terminator
            let mut arr_bytes = Vec::with_capacity(arr_size as usize);
            arr_bytes.extend_from_slice(&arr_size.to_le_bytes());
            arr_bytes.extend_from_slice(&inner_elem);
            arr_bytes.push(0x00);

            let outer_elem = encode_element(field, 0x04, &arr_bytes);
            let insert_pos = bytes.len() - 1;
            bytes.splice(insert_pos..insert_pos, outer_elem);
            update_doc_length(bytes);
            Ok(Some(true))
        }
    }
}

/// `$pop` on a flat field — remove last element from array.
fn raw_pop(bytes: &mut Vec<u8>, field: &str) -> Result<bool, DbError> {
    let loc = match find_field(bytes, field) {
        Some(l) => l,
        None => return Ok(false),
    };

    if loc.type_byte != 0x04 {
        return Err(DbError::InvalidQuery(format!(
            "$pop: field '{field}' is not an array"
        )));
    }

    let arr_start = loc.value_start;
    let arr_size = i32::from_le_bytes(bytes[arr_start..arr_start + 4].try_into().unwrap()) as usize;

    // Walk array elements to find the last one
    let mut last_start: Option<usize> = None;
    let mut pos = arr_start + 4;
    let arr_end = arr_start + arr_size;
    while pos < arr_end {
        if bytes[pos] == 0x00 {
            break;
        }
        let elem_start = pos;
        let tb = bytes[pos];
        pos += 1;
        while pos < arr_end && bytes[pos] != 0x00 {
            pos += 1;
        }
        pos += 1;
        pos = match skip_bson_value(tb, bytes, pos) {
            Some(p) => p,
            None => return Ok(false),
        };
        last_start = Some(elem_start);
    }

    let last_start = match last_start {
        Some(s) => s,
        None => return Ok(false), // empty array
    };

    // Drain the last element (from last_start to just before the array terminator)
    let drain_end = arr_end - 1; // before 0x00 terminator
    let removed = drain_end - last_start;
    bytes.drain(last_start..drain_end);

    // Update array size header
    let new_arr_size = (arr_size - removed) as i32;
    bytes[arr_start..arr_start + 4].copy_from_slice(&new_arr_size.to_le_bytes());

    update_doc_length(bytes);
    Ok(true)
}

// ── Orchestrator ────────────────────────────────────────────────

/// Check whether a mutation op is eligible for the raw byte path.
fn op_eligible(fm: &slate_query::FieldMutation) -> bool {
    // Dot-paths require walking into nested sub-documents, each with its
    // own i32 length header. After splicing bytes in the innermost doc we'd
    // have to patch the length prefix of every ancestor back up to the root.
    // The deserialization fallback handles this correctly already, and flat
    // field mutations are the common case, so the complexity isn't worth it yet.
    if fm.field.contains('.') {
        return false;
    }
    match &fm.op {
        // $rename and $lpush are too complex at the byte level
        MutationOp::Rename(_) | MutationOp::LPush(_) => false,
        // $set with Document/Array values can't be encoded by encode_bson_value
        MutationOp::Set(val) => encode_bson_value(val).is_some(),
        // $push with non-scalar values
        MutationOp::Push(val) => encode_bson_value(val).is_some(),
        // Everything else is fine
        _ => true,
    }
}

/// Apply mutations at the raw byte level. Returns `Fallback` if any op
/// is ineligible, `Unchanged` if nothing changed, or `Applied(buf)`.
pub(crate) fn raw_apply_mutation(
    old_raw: &RawDocument,
    mutation: &Mutation,
) -> Result<RawMutationResult, DbError> {
    // Pre-scan: if any op needs fallback, bail out entirely
    if !mutation.ops.iter().all(op_eligible) {
        return Ok(RawMutationResult::Fallback);
    }

    let mut bytes = old_raw.as_bytes().to_vec();
    let mut changed = false;

    for fm in &mutation.ops {
        match &fm.op {
            MutationOp::Set(val) => {
                // raw_set returns None only if encode_bson_value fails,
                // which we already checked in pre-scan.
                if let Some(c) = raw_set(&mut bytes, &fm.field, val) {
                    changed |= c;
                }
            }
            MutationOp::Unset => {
                changed |= raw_unset(&mut bytes, &fm.field);
            }
            MutationOp::Inc(amount) => {
                changed |= raw_inc(&mut bytes, &fm.field, amount)?;
            }
            MutationOp::Push(val) => {
                if let Some(c) = raw_push(&mut bytes, &fm.field, val)? {
                    changed |= c;
                }
            }
            MutationOp::Pop => {
                changed |= raw_pop(&mut bytes, &fm.field)?;
            }
            // $rename and $lpush filtered out by pre-scan
            _ => return Ok(RawMutationResult::Fallback),
        }
    }

    if !changed {
        return Ok(RawMutationResult::Unchanged);
    }

    let buf = RawDocumentBuf::from_bytes(bytes)
        .map_err(|e| DbError::Serialization(format!("raw mutation produced invalid BSON: {e}")))?;
    Ok(RawMutationResult::Applied(buf))
}

/// Raw byte-level merge: apply each field from `update` as a `$set` on `old_raw`.
/// Uses in-place overwrites instead of rebuilding the entire document.
///
/// Returns `None` if nothing changed (all values identical).
pub(crate) fn raw_merge(
    old_raw: &RawDocument,
    update: &RawDocument,
) -> Result<Option<RawDocumentBuf>, DbError> {
    let mut bytes = old_raw.as_bytes().to_vec();
    let mut changed = false;

    for result in update.iter() {
        let (key, _) = result?;
        if key == "_id" {
            continue;
        }
        // Try to find a matching field and overwrite at the byte level.
        // We work with the raw value bytes directly — extract type + value
        // from the update doc and splice them into the target.
        match find_field(&bytes, key) {
            Some(loc) => {
                // Extract the raw value bytes from the update element
                let update_bytes = update.as_bytes();
                let update_loc = find_field(update_bytes, key).unwrap();
                let new_type = update_loc.type_byte;
                let new_val = &update_bytes[update_loc.value_start..update_loc.element_end];

                // Check if unchanged
                if loc.type_byte == new_type && bytes[loc.value_start..loc.element_end] == *new_val
                {
                    continue;
                }

                // Same type + same value length → overwrite in place
                if loc.type_byte == new_type && (loc.element_end - loc.value_start) == new_val.len()
                {
                    bytes[loc.value_start..loc.element_end].copy_from_slice(new_val);
                    changed = true;
                    continue;
                }

                // Different size or type → splice the whole element
                let new_elem = encode_element(key, new_type, new_val);
                bytes.splice(loc.element_start..loc.element_end, new_elem);
                update_doc_length(&mut bytes);
                changed = true;
            }
            None => {
                // Field missing → append before trailing 0x00
                let update_bytes = update.as_bytes();
                let update_loc = find_field(update_bytes, key).unwrap();
                let new_type = update_loc.type_byte;
                let new_val = &update_bytes[update_loc.value_start..update_loc.element_end];

                let new_elem = encode_element(key, new_type, new_val);
                let insert_pos = bytes.len() - 1;
                bytes.splice(insert_pos..insert_pos, new_elem);
                update_doc_length(&mut bytes);
                changed = true;
            }
        }
    }

    if !changed {
        return Ok(None);
    }

    let buf = RawDocumentBuf::from_bytes(bytes)
        .map_err(|e| DbError::Serialization(format!("raw merge produced invalid BSON: {e}")))?;
    Ok(Some(buf))
}

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{doc, rawdoc};

    fn make_raw(doc: &bson::Document) -> bson::RawDocumentBuf {
        let bytes = bson::to_vec(doc).unwrap();
        bson::RawDocumentBuf::from_bytes(bytes).unwrap()
    }

    fn to_doc(raw: &bson::RawDocumentBuf) -> bson::Document {
        bson::from_slice(raw.as_bytes()).unwrap()
    }

    // ── skip_bson_value ─────────────────────────────────────────

    #[test]
    fn skip_int32() {
        let raw = make_raw(&doc! { "a": 42_i32 });
        let loc = find_field(raw.as_bytes(), "a").unwrap();
        assert_eq!(loc.type_byte, 0x10);
        assert_eq!(loc.element_end - loc.value_start, 4);
    }

    #[test]
    fn skip_int64() {
        let raw = make_raw(&doc! { "a": 42_i64 });
        let loc = find_field(raw.as_bytes(), "a").unwrap();
        assert_eq!(loc.type_byte, 0x12);
        assert_eq!(loc.element_end - loc.value_start, 8);
    }

    #[test]
    fn skip_double() {
        let raw = make_raw(&doc! { "a": 3.14_f64 });
        let loc = find_field(raw.as_bytes(), "a").unwrap();
        assert_eq!(loc.type_byte, 0x01);
        assert_eq!(loc.element_end - loc.value_start, 8);
    }

    #[test]
    fn skip_string() {
        let raw = make_raw(&doc! { "a": "hello" });
        let loc = find_field(raw.as_bytes(), "a").unwrap();
        assert_eq!(loc.type_byte, 0x02);
        // String: 4 (len header) + 5 (chars) + 1 (nul) = 10
        assert_eq!(loc.element_end - loc.value_start, 10);
    }

    #[test]
    fn skip_boolean() {
        let raw = make_raw(&doc! { "a": true });
        let loc = find_field(raw.as_bytes(), "a").unwrap();
        assert_eq!(loc.type_byte, 0x08);
        assert_eq!(loc.element_end - loc.value_start, 1);
    }

    #[test]
    fn skip_document() {
        let raw = make_raw(&doc! { "a": { "x": 1 } });
        let loc = find_field(raw.as_bytes(), "a").unwrap();
        assert_eq!(loc.type_byte, 0x03);
    }

    #[test]
    fn skip_array() {
        let raw = make_raw(&doc! { "a": [1, 2, 3] });
        let loc = find_field(raw.as_bytes(), "a").unwrap();
        assert_eq!(loc.type_byte, 0x04);
    }

    // ── find_field ──────────────────────────────────────────────

    #[test]
    fn find_field_exists() {
        let raw = make_raw(&doc! { "x": 1, "y": 2 });
        assert!(find_field(raw.as_bytes(), "x").is_some());
        assert!(find_field(raw.as_bytes(), "y").is_some());
    }

    #[test]
    fn find_field_missing() {
        let raw = make_raw(&doc! { "x": 1 });
        assert!(find_field(raw.as_bytes(), "z").is_none());
    }

    // ── encode_bson_value ───────────────────────────────────────

    #[test]
    fn encode_round_trip_i32() {
        let (tb, bytes) = encode_bson_value(&Bson::Int32(42)).unwrap();
        assert_eq!(tb, 0x10);
        assert_eq!(i32::from_le_bytes(bytes.try_into().unwrap()), 42);
    }

    #[test]
    fn encode_round_trip_string() {
        let (tb, bytes) = encode_bson_value(&Bson::String("hi".into())).unwrap();
        assert_eq!(tb, 0x02);
        let len = i32::from_le_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(len, 3); // "hi" + nul
        assert_eq!(&bytes[4..6], b"hi");
        assert_eq!(bytes[6], 0x00);
    }

    #[test]
    fn encode_round_trip_objectid() {
        let oid = bson::oid::ObjectId::new();
        let (tb, bytes) = encode_bson_value(&Bson::ObjectId(oid)).unwrap();
        assert_eq!(tb, 0x07);
        assert_eq!(bytes.len(), 12);
        assert_eq!(&bytes, &oid.bytes());
    }

    #[test]
    fn encode_doc_returns_none() {
        assert!(encode_bson_value(&Bson::Document(doc! {})).is_none());
    }

    // ── $set ────────────────────────────────────────────────────

    #[test]
    fn set_same_type_same_size_in_place() {
        let raw = make_raw(&doc! { "_id": "r1", "score": 10_i32 });
        let mut bytes = raw.as_bytes().to_vec();
        let orig_len = bytes.len();
        let c = raw_set(&mut bytes, "score", &Bson::Int32(20)).unwrap();
        assert!(c);
        assert_eq!(bytes.len(), orig_len); // no realloc
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert_eq!(result.get_i32("score").unwrap(), 20);
    }

    #[test]
    fn set_no_change_is_noop() {
        let raw = make_raw(&doc! { "a": 10_i32 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_set(&mut bytes, "a", &Bson::Int32(10)).unwrap();
        assert!(!c);
    }

    #[test]
    fn set_different_size_string() {
        let raw = make_raw(&doc! { "name": "Alice" });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_set(&mut bytes, "name", &Bson::String("Bob".into())).unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert_eq!(result.get_str("name").unwrap(), "Bob");
    }

    #[test]
    fn set_different_type() {
        let raw = make_raw(&doc! { "val": 42_i32 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_set(&mut bytes, "val", &Bson::String("hello".into())).unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert_eq!(result.get_str("val").unwrap(), "hello");
    }

    #[test]
    fn set_append_missing_field() {
        let raw = make_raw(&doc! { "a": 1 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_set(&mut bytes, "b", &Bson::Int32(2)).unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert_eq!(result.get_i32("a").unwrap(), 1);
        assert_eq!(result.get_i32("b").unwrap(), 2);
    }

    // ── $unset ──────────────────────────────────────────────────

    #[test]
    fn unset_existing() {
        let raw = make_raw(&doc! { "a": 1, "b": 2 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_unset(&mut bytes, "a");
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert!(result.get("a").is_none());
        assert_eq!(result.get_i32("b").unwrap(), 2);
    }

    #[test]
    fn unset_missing_is_noop() {
        let raw = make_raw(&doc! { "a": 1 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_unset(&mut bytes, "z");
        assert!(!c);
    }

    // ── $inc ────────────────────────────────────────────────────

    #[test]
    fn inc_i32_in_place() {
        let raw = make_raw(&doc! { "count": 10_i32 });
        let mut bytes = raw.as_bytes().to_vec();
        let orig_len = bytes.len();
        let c = raw_inc(&mut bytes, "count", &Bson::Int32(5)).unwrap();
        assert!(c);
        assert_eq!(bytes.len(), orig_len); // in-place
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert_eq!(result.get_i32("count").unwrap(), 15);
    }

    #[test]
    fn inc_i64_in_place() {
        let raw = make_raw(&doc! { "count": 100_i64 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_inc(&mut bytes, "count", &Bson::Int64(50)).unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert_eq!(result.get_i64("count").unwrap(), 150);
    }

    #[test]
    fn inc_f64_in_place() {
        let raw = make_raw(&doc! { "score": 1.5_f64 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_inc(&mut bytes, "score", &Bson::Double(0.5)).unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert!((result.get_f64("score").unwrap() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn inc_i32_overflow_promotes_to_i64() {
        let raw = make_raw(&doc! { "n": i32::MAX });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_inc(&mut bytes, "n", &Bson::Int32(1)).unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert_eq!(result.get_i64("n").unwrap(), i32::MAX as i64 + 1);
    }

    #[test]
    fn inc_i32_by_i64_promotes() {
        let raw = make_raw(&doc! { "n": 10_i32 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_inc(&mut bytes, "n", &Bson::Int64(5)).unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert_eq!(result.get_i64("n").unwrap(), 15);
    }

    #[test]
    fn inc_missing_field_creates() {
        let raw = make_raw(&doc! { "a": 1 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_inc(&mut bytes, "counter", &Bson::Int32(10)).unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert_eq!(result.get_i32("counter").unwrap(), 10);
    }

    #[test]
    fn inc_non_numeric_errors() {
        let raw = make_raw(&doc! { "name": "Alice" });
        let mut bytes = raw.as_bytes().to_vec();
        let err = raw_inc(&mut bytes, "name", &Bson::Int32(1));
        assert!(err.is_err());
    }

    #[test]
    fn inc_i32_by_f64_promotes() {
        let raw = make_raw(&doc! { "n": 10_i32 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_inc(&mut bytes, "n", &Bson::Double(0.5)).unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        assert!((result.get_f64("n").unwrap() - 10.5).abs() < f64::EPSILON);
    }

    // ── $push ───────────────────────────────────────────────────

    #[test]
    fn push_to_existing_array() {
        let raw = make_raw(&doc! { "tags": ["a", "b"] });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_push(&mut bytes, "tags", &Bson::String("c".into()))
            .unwrap()
            .unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        let arr = result.get_array("tags").unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[2].as_str().unwrap(), "c");
    }

    #[test]
    fn push_creates_array() {
        let raw = make_raw(&doc! { "a": 1 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_push(&mut bytes, "tags", &Bson::String("first".into()))
            .unwrap()
            .unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        let arr = result.get_array("tags").unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_str().unwrap(), "first");
    }

    #[test]
    fn push_non_array_errors() {
        let raw = make_raw(&doc! { "name": "Alice" });
        let mut bytes = raw.as_bytes().to_vec();
        let err = raw_push(&mut bytes, "name", &Bson::Int32(1)).unwrap_err();
        assert!(err.to_string().contains("not an array"));
    }

    // ── $pop ────────────────────────────────────────────────────

    #[test]
    fn pop_from_array() {
        let raw = make_raw(&doc! { "tags": ["a", "b", "c"] });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_pop(&mut bytes, "tags").unwrap();
        assert!(c);
        let result: bson::Document = bson::from_slice(&bytes).unwrap();
        let arr = result.get_array("tags").unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0].as_str().unwrap(), "a");
        assert_eq!(arr[1].as_str().unwrap(), "b");
    }

    #[test]
    fn pop_empty_array_is_noop() {
        let raw = make_raw(&doc! { "tags": bson::Bson::Array(vec![]) });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_pop(&mut bytes, "tags").unwrap();
        assert!(!c);
    }

    #[test]
    fn pop_missing_field_is_noop() {
        let raw = make_raw(&doc! { "a": 1 });
        let mut bytes = raw.as_bytes().to_vec();
        let c = raw_pop(&mut bytes, "tags").unwrap();
        assert!(!c);
    }

    // ── Orchestrator ────────────────────────────────────────────

    #[test]
    fn orchestrator_inc_produces_valid_bson() {
        let raw = make_raw(&doc! { "_id": "r1", "score": 10_i32, "name": "Alice" });
        let mutation = slate_query::parse_mutation(&rawdoc! { "$inc": { "score": 5 } }).unwrap();
        match raw_apply_mutation(&raw, &mutation).unwrap() {
            RawMutationResult::Applied(buf) => {
                let result = to_doc(&buf);
                assert_eq!(result.get_str("_id").unwrap(), "r1");
                assert_eq!(result.get_i32("score").unwrap(), 15);
                assert_eq!(result.get_str("name").unwrap(), "Alice");
            }
            other => panic!(
                "expected Applied, got {:?}",
                match other {
                    RawMutationResult::Unchanged => "Unchanged",
                    RawMutationResult::Fallback => "Fallback",
                    _ => "Applied",
                }
            ),
        }
    }

    #[test]
    fn orchestrator_unset() {
        let raw = make_raw(&doc! { "_id": "r1", "a": 1, "b": 2 });
        let mutation = slate_query::parse_mutation(&rawdoc! { "$unset": { "a": "" } }).unwrap();
        match raw_apply_mutation(&raw, &mutation).unwrap() {
            RawMutationResult::Applied(buf) => {
                let result = to_doc(&buf);
                assert!(result.get("a").is_none());
                assert_eq!(result.get_i32("b").unwrap(), 2);
            }
            _ => panic!("expected Applied"),
        }
    }

    #[test]
    fn orchestrator_noop_returns_unchanged() {
        let raw = make_raw(&doc! { "a": 10_i32 });
        let mutation = slate_query::parse_mutation(&rawdoc! { "$set": { "a": 10 } }).unwrap();
        assert!(matches!(
            raw_apply_mutation(&raw, &mutation).unwrap(),
            RawMutationResult::Unchanged
        ));
    }

    #[test]
    fn orchestrator_dot_path_falls_back() {
        let raw = make_raw(&doc! { "a": { "b": 1 } });
        let mutation = slate_query::parse_mutation(&rawdoc! { "$set": { "a.b": 2 } }).unwrap();
        assert!(matches!(
            raw_apply_mutation(&raw, &mutation).unwrap(),
            RawMutationResult::Fallback
        ));
    }

    #[test]
    fn orchestrator_rename_falls_back() {
        let raw = make_raw(&doc! { "old": 1 });
        let mutation =
            slate_query::parse_mutation(&rawdoc! { "$rename": { "old": "new" } }).unwrap();
        assert!(matches!(
            raw_apply_mutation(&raw, &mutation).unwrap(),
            RawMutationResult::Fallback
        ));
    }

    #[test]
    fn orchestrator_lpush_falls_back() {
        let raw = make_raw(&doc! { "tags": ["a"] });
        let mutation = slate_query::parse_mutation(&rawdoc! { "$lpush": { "tags": "z" } }).unwrap();
        assert!(matches!(
            raw_apply_mutation(&raw, &mutation).unwrap(),
            RawMutationResult::Fallback
        ));
    }

    #[test]
    fn orchestrator_multiple_ops() {
        let raw = make_raw(&doc! { "_id": "r1", "score": 10_i32, "status": "active" });
        let mutation = slate_query::parse_mutation(
            &rawdoc! { "$inc": { "score": 5 }, "$set": { "status": "done" } },
        )
        .unwrap();
        match raw_apply_mutation(&raw, &mutation).unwrap() {
            RawMutationResult::Applied(buf) => {
                let result = to_doc(&buf);
                assert_eq!(result.get_i32("score").unwrap(), 15);
                assert_eq!(result.get_str("status").unwrap(), "done");
            }
            _ => panic!("expected Applied"),
        }
    }
}
