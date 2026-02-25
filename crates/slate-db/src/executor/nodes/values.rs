use bson::RawDocumentBuf;

use crate::error::DbError;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a>(docs: Vec<RawDocumentBuf>) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(docs.into_iter().map(|raw| {
        Ok(Some(RawValue::Owned(bson::RawBson::Document(raw))))
    })))
}
