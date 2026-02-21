use std::collections::HashMap;

use bson::raw::{RawArrayBuf, RawBson, RawBsonRef};
use bson::{RawDocument, RawDocumentBuf};

use crate::error::DbError;
use crate::executor::field_tree::FieldTree;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a>(
    columns: &'a Option<Vec<String>>,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let tree = columns.as_ref().map(|cols| FieldTree::from_paths(cols));

    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let val = opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))?;

        // No columns specified: pass through as-is (_id already in doc)
        let tree = match tree {
            Some(ref t) => t,
            None => return Ok(Some(val)),
        };

        let raw = val
            .as_document()
            .ok_or_else(|| DbError::InvalidQuery("expected document".into()))?;

        let mut buf = RawDocumentBuf::new();
        project_document(raw, tree, &mut buf)?;
        Ok(Some(RawValue::Owned(RawBson::Document(buf))))
    })))
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
            dest.append_ref("_id", raw_val);
            continue;
        }

        let node = match tree.get(key) {
            Some(node) => node,
            None => continue,
        };

        match node {
            FieldTree::Leaf(_) => {
                dest.append_ref(key, raw_val);
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
                                out.push(other.to_raw_bson());
                            }
                        }
                    }
                    dest.append(key, RawBson::Array(out));
                }
                _ => {
                    dest.append_ref(key, raw_val);
                }
            },
        }
    }

    Ok(())
}
