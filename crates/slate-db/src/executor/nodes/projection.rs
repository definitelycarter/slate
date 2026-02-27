use std::collections::HashMap;

use bson::raw::{RawArrayBuf, RawBson, RawBsonRef};
use bson::{RawDocument, RawDocumentBuf};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::field_tree::FieldTree;
use slate_engine::skip_bson_value;

pub(crate) fn execute<'a>(
    columns: Option<Vec<String>>,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let tree = columns.as_ref().map(|cols| FieldTree::from_paths(cols));
    let flat = tree.as_ref().is_some_and(is_all_leaf);

    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let val = opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))?;

        // No columns specified: pass through as-is (_id already in doc)
        let tree = match tree {
            Some(ref t) => t,
            None => return Ok(Some(val)),
        };

        let raw = match &val {
            RawBson::Document(d) => d.as_ref(),
            _ => return Err(DbError::InvalidQuery("expected document".into())),
        };

        // Fast path: all flat fields → raw byte projection
        if flat {
            let projected = raw_project_flat(raw.as_bytes(), tree);
            let buf = RawDocumentBuf::from_bytes(projected)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
            return Ok(Some(RawBson::Document(buf)));
        }

        // Slow path: nested projection (dot-paths, arrays of documents)
        let mut buf = RawDocumentBuf::new();
        project_document(raw, tree, &mut buf)?;
        Ok(Some(RawBson::Document(buf)))
    })))
}

/// Check if every entry in the projection tree is a Leaf (no nested paths).
fn is_all_leaf(tree: &HashMap<String, FieldTree>) -> bool {
    tree.values().all(|node| matches!(node, FieldTree::Leaf(_)))
}

/// Project flat fields by raw byte scanning. Single pass over the document,
/// copying matching element byte ranges directly into the output buffer.
fn raw_project_flat(bytes: &[u8], tree: &HashMap<String, FieldTree>) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    out.extend_from_slice(&[0, 0, 0, 0]); // length placeholder

    let mut pos = 4;
    while pos < bytes.len() {
        let type_byte = bytes[pos];
        if type_byte == 0x00 {
            break;
        }
        let element_start = pos;
        pos += 1;

        // Read null-terminated field name
        let name_start = pos;
        while pos < bytes.len() && bytes[pos] != 0x00 {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        let name = &bytes[name_start..pos];
        pos += 1; // skip null terminator

        let element_end = match skip_bson_value(type_byte, bytes, pos) {
            Some(end) => end,
            None => break,
        };

        // Always include _id, plus any field in the projection tree
        if name == b"_id" || std::str::from_utf8(name).is_ok_and(|s| tree.contains_key(s)) {
            out.extend_from_slice(&bytes[element_start..element_end]);
        }
        pos = element_end;
    }

    out.push(0x00); // terminator
    let len = out.len() as i32;
    out[0..4].copy_from_slice(&len.to_le_bytes());
    out
}

fn project_document(
    src: &RawDocument,
    tree: &HashMap<String, FieldTree>,
    dest: &mut RawDocumentBuf,
) -> Result<(), DbError> {
    for result in src.iter() {
        let (key, raw_val) = result?;

        // Always include _id
        if key == "_id" {
            dest.append(bson::cstr!("_id"), raw_val);
            continue;
        }

        let node = match tree.get(key.as_str()) {
            Some(node) => node,
            None => continue,
        };

        match node {
            FieldTree::Leaf(_) => {
                dest.append(key, raw_val);
            }
            FieldTree::Branch(children) => match raw_val {
                RawBsonRef::Document(sub_doc) => {
                    let mut trimmed = RawDocumentBuf::new();
                    project_document(sub_doc, children, &mut trimmed)?;
                    dest.append(key, RawBson::Document(trimmed));
                }
                RawBsonRef::Array(arr) => {
                    let mut out = RawArrayBuf::new();
                    for elem in arr {
                        let elem = elem?;
                        match elem {
                            RawBsonRef::Document(elem_doc) => {
                                let mut trimmed = RawDocumentBuf::new();
                                project_document(elem_doc, children, &mut trimmed)?;
                                out.push(RawBson::Document(trimmed));
                            }
                            other => {
                                out.push(other.to_owned());
                            }
                        }
                    }
                    dest.append(key, RawBson::Array(out));
                }
                _ => {
                    dest.append(key, raw_val);
                }
            },
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;

    fn doc_bytes(doc: &bson::RawDocumentBuf) -> &[u8] {
        doc.as_bytes()
    }

    fn parse_projected(bytes: Vec<u8>) -> bson::RawDocumentBuf {
        RawDocumentBuf::from_bytes(bytes).expect("invalid projected BSON")
    }

    fn make_tree(fields: &[&str]) -> HashMap<String, FieldTree> {
        FieldTree::from_paths(&fields.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn raw_project_flat_basic() {
        let doc = rawdoc! { "_id": "abc", "name": "Alice", "age": 30_i32, "email": "a@b.c" };
        let tree = make_tree(&["name", "age"]);
        let result = parse_projected(raw_project_flat(doc_bytes(&doc), &tree));

        assert_eq!(result.get("_id").unwrap(), Some(RawBsonRef::String("abc")));
        assert_eq!(
            result.get("name").unwrap(),
            Some(RawBsonRef::String("Alice"))
        );
        assert_eq!(result.get("age").unwrap(), Some(RawBsonRef::Int32(30)));
        assert!(result.get("email").unwrap().is_none());
    }

    #[test]
    fn raw_project_flat_keeps_id_always() {
        let doc = rawdoc! { "_id": "x", "a": 1_i32, "b": 2_i32 };
        let tree = make_tree(&["a"]);
        let result = parse_projected(raw_project_flat(doc_bytes(&doc), &tree));

        assert_eq!(result.get("_id").unwrap(), Some(RawBsonRef::String("x")));
        assert_eq!(result.get("a").unwrap(), Some(RawBsonRef::Int32(1)));
        assert!(result.get("b").unwrap().is_none());
    }

    #[test]
    fn raw_project_flat_no_matching_fields() {
        let doc = rawdoc! { "_id": "x", "a": 1_i32 };
        let tree = make_tree(&["z"]);
        let result = parse_projected(raw_project_flat(doc_bytes(&doc), &tree));

        // Only _id should be in the result
        assert_eq!(result.get("_id").unwrap(), Some(RawBsonRef::String("x")));
        assert!(result.get("a").unwrap().is_none());
        assert!(result.get("z").unwrap().is_none());
    }

    #[test]
    fn raw_project_flat_all_fields() {
        let doc = rawdoc! { "_id": "x", "a": 1_i32, "b": "hello" };
        let tree = make_tree(&["a", "b"]);
        let result = parse_projected(raw_project_flat(doc_bytes(&doc), &tree));

        assert_eq!(result.get("_id").unwrap(), Some(RawBsonRef::String("x")));
        assert_eq!(result.get("a").unwrap(), Some(RawBsonRef::Int32(1)));
        assert_eq!(result.get("b").unwrap(), Some(RawBsonRef::String("hello")));
    }

    #[test]
    fn raw_project_flat_various_types() {
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let doc = rawdoc! {
            "_id": "x",
            "i32": 42_i32,
            "i64": 99_i64,
            "f64": 3.14_f64,
            "str": "hello",
            "bool": true,
            "dt": dt,
            "skip": "nope"
        };
        let tree = make_tree(&["i32", "i64", "f64", "str", "bool", "dt"]);
        let result = parse_projected(raw_project_flat(doc_bytes(&doc), &tree));

        assert_eq!(result.get("i32").unwrap(), Some(RawBsonRef::Int32(42)));
        assert_eq!(result.get("i64").unwrap(), Some(RawBsonRef::Int64(99)));
        assert_eq!(result.get("f64").unwrap(), Some(RawBsonRef::Double(3.14)));
        assert_eq!(
            result.get("str").unwrap(),
            Some(RawBsonRef::String("hello"))
        );
        assert_eq!(result.get("bool").unwrap(), Some(RawBsonRef::Boolean(true)));
        assert_eq!(result.get("dt").unwrap(), Some(RawBsonRef::DateTime(dt)));
        assert!(result.get("skip").unwrap().is_none());
    }

    #[test]
    fn raw_project_flat_produces_valid_bson() {
        // Verify the projected output can be fully iterated as valid BSON
        let doc = rawdoc! { "_id": "t", "a": 1_i32, "b": "two", "c": true, "d": 4.0_f64 };
        let tree = make_tree(&["a", "c"]);
        let result = parse_projected(raw_project_flat(doc_bytes(&doc), &tree));

        let fields: Vec<_> = result.iter().map(|r| r.unwrap().0.to_string()).collect();
        assert_eq!(fields, vec!["_id", "a", "c"]);
    }

    #[test]
    fn is_all_leaf_true() {
        let tree = make_tree(&["a", "b", "c"]);
        assert!(is_all_leaf(&tree));
    }

    #[test]
    fn is_all_leaf_false_with_dotpath() {
        let tree = FieldTree::from_paths(&["a".to_string(), "b.c".to_string()]);
        assert!(!is_all_leaf(&tree));
    }
}
