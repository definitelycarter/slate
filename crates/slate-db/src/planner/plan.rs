use bson::RawDocumentBuf;
use slate_engine::CollectionHandle;
use slate_query::{Mutation, Sort};

use crate::expression::{Expression, LogicalOp};

/// Top-level plan — distinguishes the operation kind.
#[derive(Clone)]
pub enum Plan<Cf: Clone> {
    Find(Node<Cf>),
    Insert {
        collection: CollectionHandle<Cf>,
        source: Node<Cf>,
    },
    Update {
        collection: CollectionHandle<Cf>,
        mutation: Mutation,
        source: Node<Cf>,
    },
    Replace {
        collection: CollectionHandle<Cf>,
        replacement: RawDocumentBuf,
        source: Node<Cf>,
    },
    Delete {
        collection: CollectionHandle<Cf>,
        source: Node<Cf>,
    },
    Merge {
        collection: CollectionHandle<Cf>,
        source: Node<Cf>,
    },
    Upsert {
        collection: CollectionHandle<Cf>,
        source: Node<Cf>,
    },
}

#[derive(Clone)]
pub enum Node<Cf: Clone> {
    /// Full collection scan — yields (_id, doc) pairs.
    Scan {
        collection: CollectionHandle<Cf>,
    },

    /// Index scan — yields doc IDs from an index on `field`.
    IndexScan {
        collection: CollectionHandle<Cf>,
        field: String,
        range: IndexScanRange,
        direction: ScanDirection,
        limit: Option<usize>,
        covered: bool,
    },

    /// Combines ID sets from two child nodes using AND or OR.
    IndexMerge {
        logical: LogicalOp,
        lhs: Box<Node<Cf>>,
        rhs: Box<Node<Cf>>,
    },

    /// Point read by _id — takes doc IDs from source, fetches full documents.
    KeyLookup {
        collection: CollectionHandle<Cf>,
        source: Box<Node<Cf>>,
    },

    /// Evaluate predicate, skip non-matching rows.
    Filter {
        predicate: Expression,
        source: Box<Node<Cf>>,
    },

    /// Select/rename fields.
    Projection {
        columns: Option<Vec<String>>,
        source: Box<Node<Cf>>,
    },

    /// Extract unique values from a field.
    Distinct {
        field: String,
        source: Box<Node<Cf>>,
    },

    /// Sort by one or more fields.
    Sort {
        sorts: Vec<Sort>,
        source: Box<Node<Cf>>,
    },

    /// Skip + take.
    Limit {
        skip: usize,
        take: Option<usize>,
        source: Box<Node<Cf>>,
    },

    /// Caller-provided documents.
    Values(Vec<RawDocumentBuf>),
}

/// Scan direction for index scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Reverse,
}

/// Describes how an IndexScan should be bounded.
#[derive(Clone)]
pub enum IndexScanRange {
    /// All entries for the field.
    Full,
    /// Exact value match.
    Eq(bson::Bson),
    /// Range with optional lower/upper bounds.
    Range {
        lower: Option<(bson::Bson, bool)>,
        upper: Option<(bson::Bson, bool)>,
    },
}
