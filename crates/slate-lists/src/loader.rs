use std::collections::HashMap;
use std::io::{BufRead, BufReader};

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

impl<L: Loader + ?Sized> Loader for Box<L> {
    fn load(
        &self,
        collection: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<Box<dyn Iterator<Item = Result<bson::Document, ListError>> + '_>, ListError> {
        (**self).load(collection, metadata)
    }
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

/// Streams NDJSON from an HTTP endpoint. Each line is parsed as a JSON object
/// and converted to a BSON document.
pub struct HttpLoader {
    url: String,
}

impl HttpLoader {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

impl Loader for HttpLoader {
    fn load(
        &self,
        _collection: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<Box<dyn Iterator<Item = Result<bson::Document, ListError>> + '_>, ListError> {
        let mut req = ureq::get(&self.url);
        for (key, value) in metadata {
            req = req.header(key, value);
        }

        let resp = req
            .call()
            .map_err(|e| ListError::Loader(format!("HTTP request failed: {e}")))?;

        let boxed: Box<dyn std::io::Read + Send + Sync> = Box::new(resp.into_body().into_reader());
        let reader = BufReader::new(boxed);

        Ok(Box::new(NdjsonIter { reader }))
    }
}

struct NdjsonIter {
    reader: BufReader<Box<dyn std::io::Read + Send + Sync>>,
}

impl Iterator for NdjsonIter {
    type Item = Result<bson::Document, ListError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        loop {
            line.clear();
            match self.reader.read_line(&mut line) {
                Ok(0) => return None,
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let json: serde_json::Value = match serde_json::from_str(trimmed) {
                        Ok(v) => v,
                        Err(e) => {
                            return Some(Err(ListError::Loader(format!("invalid JSON line: {e}"))));
                        }
                    };
                    return Some(
                        bson::to_document(&json)
                            .map_err(|e| ListError::Loader(format!("BSON conversion: {e}"))),
                    );
                }
                Err(e) => return Some(Err(ListError::Loader(format!("read error: {e}")))),
            }
        }
    }
}
