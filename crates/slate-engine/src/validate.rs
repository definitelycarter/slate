use bson::raw::{RawBsonRef, RawDocument};

use crate::error::EngineError;

/// Validate that a raw BSON document is structurally sound.
///
/// Walks every element and recursively validates nested documents and arrays.
/// Does **not** deserialize into `bson::Document` — no heap allocations beyond
/// the iterator stack frames.
///
/// This catches malformed type tags, truncated values, invalid UTF-8 in keys
/// and string values, and incorrect length headers in nested structures.
pub fn validate_raw_document(doc: &RawDocument) -> Result<(), EngineError> {
    for result in doc.iter() {
        let (_, value) = result.map_err(|e| EngineError::InvalidDocument(e.to_string()))?;
        validate_value(value)?;
    }
    Ok(())
}

fn validate_value(value: RawBsonRef<'_>) -> Result<(), EngineError> {
    match value {
        RawBsonRef::Document(sub) => validate_raw_document(sub)?,
        RawBsonRef::Array(arr) => {
            for elem in arr {
                let v = elem.map_err(|e| EngineError::InvalidDocument(e.to_string()))?;
                validate_value(v)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bson::rawdoc;

    use super::*;

    #[test]
    fn valid_flat_document() {
        let doc = rawdoc! { "name": "Alice", "age": 30 };
        assert!(validate_raw_document(&doc).is_ok());
    }

    #[test]
    fn valid_nested_document() {
        let doc = rawdoc! {
            "user": { "name": "Alice", "address": { "city": "NYC" } }
        };
        assert!(validate_raw_document(&doc).is_ok());
    }

    #[test]
    fn valid_array() {
        let doc = rawdoc! { "tags": ["a", "b", "c"] };
        assert!(validate_raw_document(&doc).is_ok());
    }

    #[test]
    fn valid_array_of_docs() {
        let doc = rawdoc! {
            "items": [{ "name": "A" }, { "name": "B" }]
        };
        assert!(validate_raw_document(&doc).is_ok());
    }

    #[test]
    fn valid_empty_document() {
        let doc = rawdoc! {};
        assert!(validate_raw_document(&doc).is_ok());
    }

    #[test]
    fn invalid_truncated_string() {
        // Build a document where the outer length is self-consistent
        // but the string element claims a length longer than the actual data.
        //
        // Layout: [len:4][type:1][key "a\0":2][str_len:4][str data + nul:?][doc nul:1]
        // We claim str_len=99 but only provide 1 byte of string data.
        let bytes: Vec<u8> = vec![
            13, 0, 0, 0, // doc length = 13 (matches actual byte count)
            0x02, // type: string
            b'a', 0, // key: "a\0"
            99, 0, 0, 0, // string length: 99 (bogus)
            0, // one byte (supposed null terminator)
            0, // doc terminator
        ];
        // from_bytes only checks the outer length + terminator, not elements.
        let doc = bson::raw::RawDocument::from_bytes(&bytes).unwrap();
        assert!(validate_raw_document(doc).is_err());
    }
}
