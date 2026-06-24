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
    use bson::{Bson, Document, doc};

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

    // ── Multi-field offset-shift coverage ─────────────────────
    //
    // A single splice shifts every later field's byte offset, so the next
    // `locate` must re-scan against the *rewritten* buffer. Single-field tests
    // can't reach this; these multi-field cases and the property test below do.

    #[test]
    fn splice_then_overwrite_re_scans_shifted_offsets() {
        // First update grows `a` (splice, shifts `b` and `c` right); the second
        // update overwrites `c` in place — which only works if its offset was
        // recomputed against the post-splice buffer.
        let old = make_raw(&doc! { "_id": "r1", "a": "x", "b": 2_i32, "c": 3_i32 });
        let update = make_raw(&doc! { "a": "a much longer string", "c": 99_i32 });
        let merged = raw_merge(&old, &update, "_id").unwrap().unwrap();
        let result = to_doc(&merged);
        assert_eq!(result.get_str("a").unwrap(), "a much longer string");
        assert_eq!(result.get_i32("b").unwrap(), 2);
        assert_eq!(result.get_i32("c").unwrap(), 99);
        assert_eq!(result.get_str("_id").unwrap(), "r1");
    }

    #[test]
    fn shrink_then_append_keeps_buffer_consistent() {
        let old = make_raw(&doc! { "_id": "r1", "a": "a long value here", "b": 1_i32 });
        let update = make_raw(&doc! { "a": "x", "z": 7_i32 });
        let merged = raw_merge(&old, &update, "_id").unwrap().unwrap();
        let result = to_doc(&merged);
        assert_eq!(result.get_str("a").unwrap(), "x");
        assert_eq!(result.get_i32("b").unwrap(), 1);
        assert_eq!(result.get_i32("z").unwrap(), 7);
    }

    /// SplitMix64 — dependency-free deterministic PRNG (repo fuzz idiom).
    struct SplitMix64(u64);
    impl SplitMix64 {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// A random scalar value spanning sizes that exercise in-place vs. splice.
    fn rand_value(rng: &mut SplitMix64) -> Bson {
        match rng.below(6) {
            0 => Bson::Int32(rng.next() as i32),
            1 => Bson::Int64(rng.next() as i64),
            2 => Bson::Boolean(rng.next() & 1 == 0),
            3 => {
                let n = rng.below(12) as usize;
                Bson::String("v".repeat(n))
            }
            4 => Bson::Double(rng.next() as f64),
            _ => Bson::Null,
        }
    }

    /// Build a random document over a small key pool (so `old`/`update` overlap).
    fn rand_doc(rng: &mut SplitMix64, with_id: bool) -> Document {
        const KEYS: &[&str] = &["a", "b", "c", "d", "e"];
        let mut d = Document::new();
        if with_id {
            d.insert("_id", "rec");
        }
        let n = rng.below(5) as usize;
        for _ in 0..n {
            let k = KEYS[rng.below(KEYS.len() as u64) as usize];
            d.insert(k, rand_value(rng));
        }
        d
    }

    /// Reference `$set`: apply each `update` field onto a clone of `old`,
    /// skipping the pk. This is the oracle `raw_merge` must match.
    fn reference_set(old: &Document, update: &Document, pk: &str) -> Document {
        let mut out = old.clone();
        for (k, v) in update {
            if k == pk {
                continue;
            }
            out.insert(k.clone(), v.clone());
        }
        out
    }

    #[test]
    fn raw_merge_matches_reference_set_property() {
        let mut rng = SplitMix64(0xC0FF_EE12_3456_789A);
        for _ in 0..20_000 {
            let old_doc = rand_doc(&mut rng, true);
            let update_has_id = rng.next() & 1 == 0;
            let update_doc = rand_doc(&mut rng, update_has_id);

            let old = make_raw(&old_doc);
            let update = make_raw(&update_doc);

            let merged = raw_merge(&old, &update, "_id").unwrap();
            let got = match merged {
                Some(buf) => to_doc(&buf),
                // `None` means "nothing changed" — the merged result equals old.
                None => old_doc.clone(),
            };

            let want = reference_set(&old_doc, &update_doc, "_id");

            // Compare as field sets (order may differ: appends land at the end).
            assert_eq!(
                got.len(),
                want.len(),
                "field count: got {got:?} want {want:?}"
            );
            for (k, v) in &want {
                assert_eq!(
                    got.get(k),
                    Some(v),
                    "field '{k}': got {:?} want {v:?}\nold={old_doc:?}\nupdate={update_doc:?}",
                    got.get(k)
                );
            }
        }
    }

    // ── Error-formatting cleanup (llvm-cov gaps) ──────────────

    #[test]
    fn raw_merge_error_display() {
        let e = RawMergeError("boom".to_string());
        assert_eq!(e.to_string(), "raw merge error: boom");
    }

    #[test]
    fn raw_merge_error_from_bson_error() {
        // Any bson error converts into a RawMergeError carrying its message.
        let bson_err = RawDocumentBuf::from_bytes(vec![0, 0]).unwrap_err();
        let merge_err: RawMergeError = bson_err.into();
        assert!(merge_err.to_string().starts_with("raw merge error:"));
    }
}
