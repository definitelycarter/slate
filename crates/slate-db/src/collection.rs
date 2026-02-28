use serde::{Deserialize, Serialize};

fn default_pk() -> String {
    "_id".to_string()
}

fn default_ttl() -> String {
    "ttl".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig {
    pub name: String,
    #[serde(default)]
    pub indexes: Vec<String>,
    #[serde(default = "default_pk")]
    pub pk_path: String,
    #[serde(default = "default_ttl")]
    pub ttl_path: String,
}

impl Default for CollectionConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            indexes: vec![],
            pk_path: default_pk(),
            ttl_path: default_ttl(),
        }
    }
}
