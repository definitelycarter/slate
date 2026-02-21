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
) -> Result<RawIter<'a>, DbError> {
    let scan_prefix = encoding::data_scan_prefix("");
    let iter = txn.scan_prefix(cf, &scan_prefix)?;

    Ok(Box::new(iter.filter_map(|result| match result {
        Ok((_key, value)) => {
            let val = match value {
                Cow::Borrowed(b) => match RawDocument::from_bytes(b) {
                    Ok(raw) => RawValue::Borrowed(RawBsonRef::Document(raw)),
                    Err(e) => return Some(Err(DbError::from(e))),
                },
                Cow::Owned(v) => match RawDocumentBuf::from_bytes(v) {
                    Ok(raw) => RawValue::Owned(RawBson::Document(raw)),
                    Err(e) => return Some(Err(DbError::from(e))),
                },
            };
            Some(Ok(Some(val)))
        }
        Err(e) => Some(Err(DbError::Store(e))),
    })))
}
