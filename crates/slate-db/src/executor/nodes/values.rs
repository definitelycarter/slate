use bson::RawDocumentBuf;
use bson::raw::RawBson;

use crate::error::DbError;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a>(docs: Vec<RawDocumentBuf>) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(docs.into_iter().map(|raw| {
        Ok(Some(RawValue::Owned(RawBson::Document(raw))))
    })))
}
