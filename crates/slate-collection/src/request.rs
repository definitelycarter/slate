use serde::{Deserialize, Serialize};
use slate_query::{FilterGroup, Sort, SortDirection};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryRequest {
    pub filters: Option<FilterGroup>,
    #[serde(default)]
    pub sort: Vec<Sort>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub records: Vec<bson::Document>,
    pub total: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DistinctRequest {
    pub field: String,
    pub filters: Option<FilterGroup>,
    pub sort: Option<SortDirection>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DistinctResponse {
    pub values: bson::RawBson,
}
