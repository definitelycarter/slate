use slate_query::{Mutation, Sort, SortDirection};

use super::expression::{Expression, LogicalOp};

/// Controls the behavior of the Upsert node when a document already exists.
#[derive(Debug, Clone, PartialEq)]
pub enum UpsertMode {
    /// Full replacement — existing doc is discarded.
    Replace,
    /// Partial merge — update fields are merged into the existing doc.
    Merge,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    /// Full scan — yields all record IDs in a collection.
    Scan,

    /// Index scan — yields record IDs from an index.
    ///
    /// The optional `filter` is an `Expression` scoped to `column`:
    /// - `Eq(_, v)` → narrow prefix scan
    /// - `Gt/Gte/Lt/Lte` → one-sided range
    /// - `And([Gte(_, lo), Lt(_, hi)])` → two-sided range
    IndexScan {
        column: String,
        filter: Option<Expression>,
        direction: SortDirection,
        limit: Option<usize>,
        complete_groups: bool,
        covered: bool,
    },

    /// Combines ID sets from two child nodes using AND (intersect) or OR (union).
    IndexMerge {
        logical: LogicalOp,
        lhs: Box<PlanNode>,
        rhs: Box<PlanNode>,
    },

    /// Fetch raw records by ID.
    ReadRecord {
        input: Box<PlanNode>,
    },

    /// Evaluate predicate against documents, skip non-matching.
    Filter {
        predicate: Expression,
        input: Box<PlanNode>,
    },

    /// Materialize + sort.
    Sort {
        sorts: Vec<Sort>,
        input: Box<PlanNode>,
    },

    /// Skip / take.
    Limit {
        skip: usize,
        take: Option<usize>,
        input: Box<PlanNode>,
    },

    /// Project fields and inject `_id`.
    Projection {
        columns: Option<Vec<String>>,
        input: Box<PlanNode>,
    },

    /// Extract unique values from a field.
    Distinct {
        field: String,
        input: Box<PlanNode>,
    },

    // ── Mutation nodes ──────────────────────────────────────────
    DeleteIndex {
        indexed_fields: Vec<String>,
        input: Box<PlanNode>,
    },

    Delete {
        input: Box<PlanNode>,
    },

    Update {
        mutation: Mutation,
        input: Box<PlanNode>,
    },

    Replace {
        replacement: bson::RawDocumentBuf,
        input: Box<PlanNode>,
    },

    InsertIndex {
        indexed_fields: Vec<String>,
        input: Box<PlanNode>,
    },

    InsertRecord {
        input: Box<PlanNode>,
    },

    Upsert {
        mode: UpsertMode,
        indexed_fields: Vec<String>,
        input: Box<PlanNode>,
    },

    /// Caller-provided documents.
    Values {
        docs: Vec<bson::RawDocumentBuf>,
    },
}
