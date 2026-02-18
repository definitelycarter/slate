use serde::{Deserialize, Serialize};
use slate_query::FilterGroup;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListConfig {
    pub id: String,
    pub title: String,
    pub collection: String,
    pub filters: Option<FilterGroup>,
    pub columns: Vec<Column>,
    #[serde(default)]
    pub loader: Option<LoaderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoaderConfig {
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub field: String,
    pub header: String,
    pub width: u32,
    pub pinned: bool,
}
