use std::borrow::Cow;

use bson::RawBson;
use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    now_millis: i64,
) -> Result<RawIter<'a>, DbError> {
    let scan_prefix = encoding::data_scan_prefix("");
    let iter = txn.scan_prefix(cf, &scan_prefix)?;

    Ok(Box::new(iter.filter_map(move |result| match result {
        Ok((_key, value)) => {
            // O(1) TTL check on the envelope prefix
            if encoding::is_record_expired(&value, now_millis) {
                return None;
            }
            // Strip envelope prefix to get BSON payload.
            // We split the Cow directly to preserve lifetimes for the Borrowed path.
            let bson_offset = encoding::record_bson_offset(&value);
            let val = match value {
                Cow::Borrowed(b) => match RawDocument::from_bytes(&b[bson_offset..]) {
                    Ok(raw) => RawValue::Borrowed(RawBsonRef::Document(raw)),
                    Err(e) => return Some(Err(DbError::from(e))),
                },
                Cow::Owned(v) => match RawDocumentBuf::from_bytes(v[bson_offset..].to_vec()) {
                    Ok(raw) => RawValue::Owned(RawBson::Document(raw)),
                    Err(e) => return Some(Err(DbError::from(e))),
                },
            };
            Some(Ok(Some(val)))
        }
        Err(e) => Some(Err(DbError::Store(e))),
    })))
}
