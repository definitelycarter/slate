use serde::Deserialize;

use crate::sort::{Sort, SortDirection};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct FindOptions {
    #[serde(default)]
    pub sort: Vec<Sort>,
    #[serde(default)]
    pub skip: Option<usize>,
    #[serde(default)]
    pub take: Option<usize>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default)]
pub struct DistinctOptions {
    pub sort: Option<SortDirection>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
}
