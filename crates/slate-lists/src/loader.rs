use std::collections::HashMap;

use crate::error::ListError;

/// Trait for loading data into a collection from an external source.
///
/// Consumers implement this trait to connect Slate to upstream systems.
/// The loader is called when data for a given key is missing or stale.
///
/// Not implemented yet — will be backed by a gRPC client when wired up.
pub trait Loader: Send + Sync {
    fn load(
        &self,
        collection: &str,
        key: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<(), ListError>;
}

/// A no-op loader that does nothing. Use when data is pre-populated.
pub struct NoopLoader;

impl Loader for NoopLoader {
    fn load(
        &self,
        _collection: &str,
        _key: &str,
        _metadata: &HashMap<String, String>,
    ) -> Result<(), ListError> {
        Ok(())
    }
}
