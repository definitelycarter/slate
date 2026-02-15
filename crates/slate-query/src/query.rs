use serde::{Deserialize, Serialize};

use crate::filter::FilterGroup;
use crate::sort::Sort;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Query {
    pub filter: Option<FilterGroup>,
    pub sort: Vec<Sort>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
}
