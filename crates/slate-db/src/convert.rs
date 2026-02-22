use bson::raw::RawDocumentBuf;

use crate::error::DbError;

/// Trait for types that can be converted into a [`RawDocumentBuf`].
///
/// Implemented for [`bson::Document`], [`RawDocumentBuf`], `Vec<u8>`, and `&[u8]`.
pub trait IntoRawDocumentBuf {
    fn into_raw_document_buf(self) -> Result<RawDocumentBuf, DbError>;
}

impl IntoRawDocumentBuf for bson::Document {
    fn into_raw_document_buf(self) -> Result<RawDocumentBuf, DbError> {
        Ok(RawDocumentBuf::from_document(&self)?)
    }
}

impl IntoRawDocumentBuf for RawDocumentBuf {
    fn into_raw_document_buf(self) -> Result<RawDocumentBuf, DbError> {
        Ok(self)
    }
}

impl IntoRawDocumentBuf for Vec<u8> {
    fn into_raw_document_buf(self) -> Result<RawDocumentBuf, DbError> {
        Ok(RawDocumentBuf::from_bytes(self)?)
    }
}

impl IntoRawDocumentBuf for &[u8] {
    fn into_raw_document_buf(self) -> Result<RawDocumentBuf, DbError> {
        Ok(RawDocumentBuf::from_bytes(self.to_vec())?)
    }
}
