#[derive(Debug, Clone)]
pub struct CollectionConfig {
    pub name: String,
    pub pk_path: String,
    pub ttl_path: String,
}

impl Default for CollectionConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            pk_path: "_id".to_string(),
            ttl_path: "ttl".to_string(),
        }
    }
}
