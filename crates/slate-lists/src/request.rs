use serde::{Deserialize, Serialize};
use slate_query::{FilterGroup, Sort};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListRequest {
    pub filters: Option<FilterGroup>,
    #[serde(default)]
    pub sort: Vec<Sort>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResponse {
    pub records: Vec<bson::Document>,
    pub total: u64,
}
