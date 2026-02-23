use crate::sort::{Sort, SortDirection};
use bson::RawDocumentBuf;

#[derive(Debug, Clone)]
pub struct Query {
    pub filter: Option<RawDocumentBuf>,
    pub sort: Vec<Sort>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
    /// Column projection — if Some, only these columns are returned.
    /// If None, all columns are returned.
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct DistinctQuery {
    pub field: String,
    pub filter: Option<RawDocumentBuf>,
    pub sort: Option<SortDirection>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
}
