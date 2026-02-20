use serde::{Deserialize, Serialize};

use crate::filter::FilterGroup;
use crate::sort::{Sort, SortDirection};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Query {
    pub filter: Option<FilterGroup>,
    pub sort: Vec<Sort>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
    /// Column projection — if Some, only these columns are returned.
    /// If None, all columns are returned.
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DistinctQuery {
    pub field: String,
    pub filter: Option<FilterGroup>,
    pub sort: Option<SortDirection>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
}
