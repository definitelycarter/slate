use bson::Bson;

use crate::error::DbError;

/// Allowed Bson variants for storage.
/// Rejects: ObjectId, Regex, JavaScript, Binary, Timestamp, Symbol,
///          Decimal128, Undefined, MaxKey, MinKey, DbPointer.
pub fn validate_bson(value: &Bson) -> Result<(), DbError> {
    match value {
        Bson::String(_)
        | Bson::Int32(_)
        | Bson::Int64(_)
        | Bson::Double(_)
        | Bson::Boolean(_)
        | Bson::DateTime(_)
        | Bson::Null => Ok(()),
        Bson::Array(arr) => {
            for item in arr {
                validate_bson(item)?;
            }
            Ok(())
        }
        Bson::Document(doc) => {
            for (_k, v) in doc {
                validate_bson(v)?;
            }
            Ok(())
        }
        other => Err(DbError::InvalidBson(format!(
            "unsupported BSON type: {:?}",
            other.element_type()
        ))),
    }
}

/// Validate every field in a document.
pub fn validate_document(doc: &bson::Document) -> Result<(), DbError> {
    for (_k, v) in doc {
        validate_bson(v)?;
    }
    Ok(())
}
