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
    now_millis: i64,
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
        match exec::get_record_if_alive(txn, cf, &key, now_millis) {
            Ok(Some(bytes)) => {
                let (_, bson_slice) = match encoding::decode_record(&bytes) {
                    Ok(pair) => pair,
                    Err(e) => return Some(Err(e)),
                };
                let raw = match RawDocumentBuf::from_bytes(bson_slice.to_vec()) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(DbError::from(e))),
                };
                Some(Ok(Some(RawValue::Owned(RawBson::Document(raw)))))
            }
            Ok(None) => None, // missing or expired
            Err(e) => Some(Err(e)),
        }
    })))
}
