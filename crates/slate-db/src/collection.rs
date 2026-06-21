#[derive(Debug, Clone)]
pub struct CollectionConfig {
    pub name: String,
    pub cf: String,
    pub pk_path: String,
    pub ttl_path: String,
}

impl Default for CollectionConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            cf: slate_engine::DEFAULT_CF.to_string(),
            pk_path: "_id".to_string(),
            ttl_path: "ttl".to_string(),
        }
    }
}

/// A read-only snapshot of a collection's catalog metadata: its key paths and
/// the fields it indexes. Returned by
/// [`Transaction::collection_schema`](crate::DatabaseTransaction::collection_schema)
/// for introspection (`unique_indexes` is the subset of `indexes` that enforce
/// uniqueness).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionSchema {
    pub cf: String,
    pub name: String,
    pub pk_path: String,
    pub ttl_path: String,
    pub indexes: Vec<String>,
    pub unique_indexes: Vec<String>,
}
