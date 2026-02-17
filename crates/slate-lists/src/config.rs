use serde::{Deserialize, Serialize};
use slate_db::CollectionConfig;
use slate_query::FilterGroup;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListConfig {
    pub id: String,
    pub title: String,
    pub collection: String,
    #[serde(default)]
    pub indexes: Vec<String>,
    pub filters: Option<FilterGroup>,
    pub columns: Vec<Column>,
}

impl ListConfig {
    pub fn collection_config(&self) -> CollectionConfig {
        CollectionConfig {
            name: self.collection.clone(),
            indexes: self.indexes.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub field: String,
    pub header: String,
    pub width: u32,
    pub pinned: bool,
}
