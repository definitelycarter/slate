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
    update: &'a RawDocumentBuf,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let old_raw = match &opt_val {
            Some(val) => match val.as_document() {
                Some(r) => r,
                None => return Ok(None),
            },
            None => return Ok(None),
        };

        let id_str = match exec::raw_extract_id(old_raw)? {
            Some(s) => s.to_string(),
            None => return Ok(None),
        };

        match exec::raw_merge_doc(old_raw, update)? {
            Some(merged) => {
                let key = encoding::record_key(&id_str);
                txn.put(cf, &key, &encoding::encode_record(merged.as_bytes()))?;
                Ok(Some(RawValue::Owned(RawBson::Document(merged))))
            }
            None => Ok(None),
        }
    })))
}
