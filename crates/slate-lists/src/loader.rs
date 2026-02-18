use std::collections::HashMap;

use crate::error::ListError;

/// Trait for loading data into a collection from an external source.
///
/// Consumers implement this trait to connect Slate to upstream systems.
/// The loader is called when data for a given key is missing or stale.
///
/// Returns an iterator of BSON documents. Each loader implementation
/// owns the conversion to BSON — a REST loader converts from JSON,
/// a gRPC loader gets typed fields from protobuf.
pub trait Loader: Send + Sync {
    fn load(
        &self,
        collection: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<Box<dyn Iterator<Item = Result<bson::Document, ListError>> + '_>, ListError>;
}

/// A no-op loader that returns an empty iterator. Use when data is pre-populated.
pub struct NoopLoader;

impl Loader for NoopLoader {
    fn load(
        &self,
        _collection: &str,
        _metadata: &HashMap<String, String>,
    ) -> Result<Box<dyn Iterator<Item = Result<bson::Document, ListError>> + '_>, ListError> {
        Ok(Box::new(std::iter::empty()))
    }
}
