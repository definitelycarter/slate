use slate_query::{Mutation, Sort, SortDirection};

/// Represents a database operation to be planned.
#[derive(Debug, Clone)]
pub enum Statement {
    /// Find documents matching a query (also used for count).
    Find {
        filter: Option<bson::RawDocumentBuf>,
        sort: Vec<Sort>,
        skip: Option<usize>,
        take: Option<usize>,
        columns: Option<Vec<String>>,
    },
    /// Return distinct values for a field.
    Distinct {
        field: String,
        filter: Option<bson::RawDocumentBuf>,
        sort: Option<SortDirection>,
        skip: Option<usize>,
        take: Option<usize>,
    },
    /// Update documents matching a filter (merge semantics).
    Update {
        filter: bson::RawDocumentBuf,
        mutation: Mutation,
        limit: Option<usize>,
    },
    /// Replace the first document matching a filter entirely.
    Replace {
        filter: bson::RawDocumentBuf,
        replacement: bson::RawDocumentBuf,
    },
    /// Delete documents matching a filter.
    Delete {
        filter: bson::RawDocumentBuf,
        limit: Option<usize>,
    },
    /// Insert documents from caller-supplied values.
    Insert { docs: Vec<bson::RawDocumentBuf> },
    /// Upsert (insert-or-replace) a batch of documents by `_id`.
    UpsertMany { docs: Vec<bson::RawDocumentBuf> },
    /// Merge (insert-or-patch) a batch of partial documents by `_id`.
    MergeMany { docs: Vec<bson::RawDocumentBuf> },
    /// Internal: purge expired documents.
    FlushExpired { filter: bson::RawDocumentBuf },
}
