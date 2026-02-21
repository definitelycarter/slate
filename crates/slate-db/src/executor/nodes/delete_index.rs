use bson::raw::RawBsonRef;
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;
use crate::executor::field_tree::{FieldTree, walk};

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    indexed_fields: &'a [String],
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let mut paths: Vec<String> = indexed_fields.to_vec();
    if !paths.iter().any(|p| p == "ttl") {
        paths.push("ttl".into());
    }
    let tree = FieldTree::from_paths(&paths);

    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        if let Some(ref val) = opt_val {
            if let Some(raw) = val.as_document() {
                if let Some(id_str) = exec::raw_extract_id(raw)? {
                    let mut err: Option<DbError> = None;
                    walk(raw, &tree, |path, value| {
                        if err.is_some() {
                            return;
                        }
                        if path == "ttl" {
                            if !matches!(value, RawBsonRef::DateTime(_)) {
                                return;
                            }
                        }
                        let idx_key = encoding::raw_index_key(path, value, id_str);
                        if let Err(e) = txn.delete(cf, &idx_key) {
                            err = Some(DbError::Store(e));
                        }
                    });
                    if let Some(e) = err {
                        return Err(e);
                    }
                }
            }
        }
        Ok(opt_val)
    })))
}
