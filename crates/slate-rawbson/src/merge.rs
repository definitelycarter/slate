//! Raw byte-level document merge.
//!
//! Applies each field of an `update` document onto an `old` document as an
//! implicit `$set`, manipulating the backing bytes directly — overwriting in
//! place when the new value is the same size, splicing otherwise — without
//! deserializing to `bson::Document`.

use bson::RawDocumentBuf;
use bson::raw::RawDocument;
use bson::spec::ElementType;

use crate::{RawField, RawFieldLoc};

/// An error produced while merging raw BSON documents.
///
/// Raised only when the input or the merged output is malformed BSON.
#[derive(Debug, Clone, PartialEq)]
pub struct RawMergeError(pub String);

impl std::fmt::Display for RawMergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "raw merge error: {}", self.0)
    }
}

impl std::error::Error for RawMergeError {}

impl From<bson::error::Error> for RawMergeError {
    fn from(e: bson::error::Error) -> Self {
        RawMergeError(e.to_string())
    }
}

/// Build a complete BSON element: `[type_byte][field_name\0][value_bytes]`.
fn encode_element(field_name: &str, element_type: ElementType, value_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + field_name.len() + 1 + value_bytes.len());
    buf.push(element_type as u8);
    buf.extend_from_slice(field_name.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(value_bytes);
    buf
}

/// Overwrite the 4-byte document length header with the current vec length.
fn update_doc_length(bytes: &mut [u8]) {
    let len = bytes.len() as i32;
    bytes[0..4].copy_from_slice(&len.to_le_bytes());
}

/// Look up a field and return a borrow-free location snapshot.
fn locate(bytes: &[u8], field: &str) -> Option<RawFieldLoc> {
    RawField::get(bytes, field).map(|f| f.loc())
}

/// Raw byte-level merge: apply each field from `update` as a `$set` on `old_raw`.
/// Uses in-place overwrites instead of rebuilding the entire document.
///
/// The primary-key field (`pk_path`) is never overwritten. Returns `None` if
/// nothing changed (all values identical).
pub fn raw_merge(
    old_raw: &RawDocument,
    update: &RawDocument,
    pk_path: &str,
) -> Result<Option<RawDocumentBuf>, RawMergeError> {
    let mut bytes = old_raw.as_bytes().to_vec();
    let mut changed = false;

    for result in update.iter() {
        let (key, _) = result?;
        if key == pk_path {
            continue;
        }

        // Look up the field in the update document (borrow-free).
        let update_bytes = update.as_bytes();
        let Some(update_loc) = locate(update_bytes, key.as_str()) else {
            continue;
        };
        let new_type = update_loc.element_type();
        let new_val = &update_bytes[update_loc.value_start()..update_loc.element_end()];

        match locate(&bytes, key.as_str()) {
            Some(loc) => {
                let old_val = &bytes[loc.value_start()..loc.element_end()];

                // Check if unchanged
                if loc.element_type() == new_type && old_val == new_val {
                    continue;
                }

                // Same type + same value length → overwrite in place
                if loc.element_type() == new_type && old_val.len() == new_val.len() {
                    bytes[loc.value_start()..loc.element_end()].copy_from_slice(new_val);
                    changed = true;
                    continue;
                }

                // Different size or type → splice the whole element
                let new_elem = encode_element(key.as_str(), new_type, new_val);
                bytes.splice(loc.element_start()..loc.element_end(), new_elem);
                update_doc_length(&mut bytes);
                changed = true;
            }
            None => {
                // Field missing → append before trailing 0x00
                let new_elem = encode_element(key.as_str(), new_type, new_val);
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
        .map_err(|e| RawMergeError(format!("raw merge produced invalid BSON: {e}")))?;
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{Document, doc};

    fn make_raw(doc: &Document) -> RawDocumentBuf {
        let bytes = bson::serialize_to_vec(doc).unwrap();
        RawDocumentBuf::from_bytes(bytes).unwrap()
    }

    fn to_doc(raw: &RawDocumentBuf) -> Document {
        bson::deserialize_from_slice(raw.as_bytes()).unwrap()
    }

    #[test]
    fn overwrites_same_size_in_place() {
        let old = make_raw(&doc! { "_id": "r1", "score": 10_i32 });
        let update = make_raw(&doc! { "score": 20_i32 });
        let orig_len = old.as_bytes().len();
        let merged = raw_merge(&old, &update, "_id").unwrap().unwrap();
        assert_eq!(merged.as_bytes().len(), orig_len);
        let result = to_doc(&merged);
        assert_eq!(result.get_i32("score").unwrap(), 20);
        assert_eq!(result.get_str("_id").unwrap(), "r1");
    }

    #[test]
    fn splices_different_size_value() {
        let old = make_raw(&doc! { "_id": "r1", "name": "Alice" });
        let update = make_raw(&doc! { "name": "Bob" });
        let merged = raw_merge(&old, &update, "_id").unwrap().unwrap();
        let result = to_doc(&merged);
        assert_eq!(result.get_str("name").unwrap(), "Bob");
    }

    #[test]
    fn appends_missing_field() {
        let old = make_raw(&doc! { "_id": "r1", "a": 1_i32 });
        let update = make_raw(&doc! { "b": 2_i32 });
        let merged = raw_merge(&old, &update, "_id").unwrap().unwrap();
        let result = to_doc(&merged);
        assert_eq!(result.get_i32("a").unwrap(), 1);
        assert_eq!(result.get_i32("b").unwrap(), 2);
    }

    #[test]
    fn changes_type() {
        let old = make_raw(&doc! { "_id": "r1", "v": 42_i32 });
        let update = make_raw(&doc! { "v": "hello" });
        let merged = raw_merge(&old, &update, "_id").unwrap().unwrap();
        let result = to_doc(&merged);
        assert_eq!(result.get_str("v").unwrap(), "hello");
    }

    #[test]
    fn pk_is_never_overwritten() {
        let old = make_raw(&doc! { "_id": "r1", "a": 1_i32 });
        let update = make_raw(&doc! { "_id": "r2", "a": 2_i32 });
        let merged = raw_merge(&old, &update, "_id").unwrap().unwrap();
        let result = to_doc(&merged);
        assert_eq!(result.get_str("_id").unwrap(), "r1");
        assert_eq!(result.get_i32("a").unwrap(), 2);
    }

    #[test]
    fn unchanged_returns_none() {
        let old = make_raw(&doc! { "_id": "r1", "a": 1_i32 });
        let update = make_raw(&doc! { "a": 1_i32 });
        assert!(raw_merge(&old, &update, "_id").unwrap().is_none());
    }
}
