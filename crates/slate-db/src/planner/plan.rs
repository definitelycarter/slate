use bson::RawDocumentBuf;
use slate_engine::CollectionHandle;
use slate_query::Sort;

use crate::hooks::ResolvedHook;
use crate::mutation::Mutation;

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
        hooks: Vec<ResolvedHook>,
        source: Node<Cf>,
    },
    Upsert {
        collection: CollectionHandle<Cf>,
        hooks: Vec<ResolvedHook>,
        source: Node<Cf>,
    },
    /// After-mutation trigger wrapper. Fires hooks on each document
    /// yielded by the inner plan.
    Trigger {
        cf: String,
        action: String,
        hooks: Vec<ResolvedHook>,
        plan: Box<Plan<Cf>>,
    },
}

#[derive(Clone)]
pub enum Node<Cf: Clone> {
    /// Full collection scan — yields (_id, doc) pairs.
    Scan { collection: CollectionHandle<Cf> },

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
        collection: CollectionHandle<Cf>,
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
        collection: CollectionHandle<Cf>,
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

    /// Before-mutation trigger tap. Fires hooks per document as a
    /// side effect, always passes the document through.
    Trigger {
        cf: String,
        action: String,
        hooks: Vec<ResolvedHook>,
        source: Box<Node<Cf>>,
    },

    /// Validation gate. Runs validators per document, errors if any
    /// fail. Passes the document through on success.
    Validate {
        validators: Vec<ResolvedHook>,
        source: Box<Node<Cf>>,
    },
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
