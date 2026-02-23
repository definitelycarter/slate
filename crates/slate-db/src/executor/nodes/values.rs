use bson::RawDocumentBuf;
use bson::raw::RawBsonRef;

use crate::error::DbError;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a>(docs: &'a [RawDocumentBuf]) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(docs.iter().map(|raw| {
        Ok(Some(RawValue::Borrowed(RawBsonRef::Document(raw))))
    })))
}
