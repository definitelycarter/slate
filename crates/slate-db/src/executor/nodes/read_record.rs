use bson::RawBson;
use bson::raw::RawDocumentBuf;
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::exec;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    // IndexScan/IndexMerge path: extract _id, fetch full record
    Ok(Box::new(source.filter_map(move |result| {
        let opt_val = match result {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        let val = match opt_val {
            Some(v) => v,
            None => return None,
        };
        // Accept bare String id (from IndexScan) or Document with _id
        let id = match &val {
            RawValue::Owned(RawBson::String(s)) => s.clone(),
            _ => match val.as_document() {
                Some(raw) => match exec::raw_extract_id(raw) {
                    Ok(Some(s)) => s.to_string(),
                    Ok(None) => return None,
                    Err(e) => return Some(Err(e)),
                },
                None => return None,
            },
        };
        let key = encoding::record_key(&id);
        match txn.get(cf, &key) {
            Ok(Some(bytes)) => {
                let raw = match RawDocumentBuf::from_bytes(bytes.into_owned()) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(DbError::from(e))),
                };
                Some(Ok(Some(RawValue::Owned(RawBson::Document(raw)))))
            }
            Ok(None) => None, // dangling index entry
            Err(e) => Some(Err(DbError::Store(e))),
        }
    })))
}
