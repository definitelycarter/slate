//! The v2 plan IR.
//!
//! A [`Plan`] is a tree of [`Node`]s that `slate-executor` streams values
//! through. The streamed unit is `Option<RawBson>`: `Some` is a value, `None`
//! is *undefined* (the row is omitted at the output boundary). Kept
//! deliberately small for now — see the crate docs for the planned set.

use bson::{Bson, RawBson, RawDocumentBuf};
use slate_sql::ast::{OrderByItem, ScalarExpr};
use slate_vm::ResolvedHook;

/// A top-level plan: a read query, or a write whose `source` is a read-node
/// tree yielding the documents to write.
///
/// Mutations are applied lazily as the executor's result stream is consumed —
/// the affected documents flow out, so the caller must drain the stream (e.g.
/// `execute_collect`) for the writes to happen, exactly as v1 does.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// A read query producing a stream of values.
    Query(Node),

    /// Insert each document yielded by `source` (generating a pk when absent),
    /// failing on a duplicate key. Yields the inserted documents.
    Insert {
        collection: CollectionRef,
        source: Node,
    },

    /// Delete each document yielded by `source` (by its primary key). Yields
    /// the deleted documents.
    Delete {
        collection: CollectionRef,
        source: Node,
    },

    /// Apply `mutation` to each document yielded by `source` and write it back.
    /// Yields the mutated documents (unchanged documents are dropped).
    Update {
        collection: CollectionRef,
        mutation: slate_mutation::Mutation,
        source: Node,
    },

    /// Replace each document yielded by `source` with `replacement`, preserving
    /// the original primary key. Yields the new documents.
    Replace {
        collection: CollectionRef,
        replacement: RawDocumentBuf,
        source: Node,
    },

    /// After-mutation trigger wrapper: run `plan`, then fire `hooks` with
    /// `action` on each document it yields (passing the documents through).
    Trigger {
        cf: String,
        action: String,
        hooks: Vec<ResolvedHook>,
        plan: Box<Plan>,
    },

    /// Upsert each document yielded by `source`: insert if absent, else
    /// `Replace`/`Merge` the existing document. Fires `inserting`/`inserted`
    /// or `updating`/`updated` triggers depending on the per-document runtime
    /// outcome (which is why hooks stay internal here). Yields the written docs.
    Upsert {
        collection: CollectionRef,
        mode: UpsertMode,
        hooks: Vec<ResolvedHook>,
        source: Node,
    },
}

/// How an [`Plan::Upsert`] writes over an existing document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertMode {
    /// Overwrite the existing document entirely (preserving its primary key).
    Replace,
    /// Field-merge the new document into the existing one.
    Merge,
}

/// Identifies a collection by its `(cf, name)` pair — the canonical identity in
/// the engine catalog. Carried by value (two `String`s) so the IR stays free of
/// the engine's `Cf` handle generic; the executor resolves it to a live handle.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionRef {
    pub cf: String,
    pub collection: String,
}

/// Direction of an index scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Reverse,
}

/// How an [`Node::IndexMerge`] combines its two child ID streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOp {
    /// Intersection — IDs present in both children.
    And,
    /// Union — IDs present in either child (deduplicated).
    Or,
}

/// How an [`Node::IndexScan`] is bounded.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexScanRange {
    /// All entries for the field.
    Full,
    /// Exact value match.
    Eq(Bson),
    /// Range with optional inclusive/exclusive lower and upper bounds.
    Range {
        lower: Option<(Bson, bool)>,
        upper: Option<(Bson, bool)>,
    },
}

/// A node in the plan tree.
///
/// ## The row environment
///
/// Rows flowing into the binding-aware nodes ([`Node::Filter`],
/// [`Node::Project`], [`Node::Sort`], [`Node::Unwind`]) are *environment
/// documents*: a `RawBson::Document` whose top-level fields are the bound
/// aliases (`{c: <doc>, t: <elem>}`). [`Node::Bind`] is where the `FROM` alias
/// first attaches; [`Node::Unwind`] adds a binding per array element. So bare
/// identifiers (`c`, `t`) in expressions resolve against those fields.
///
/// Future work: the write path.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    /// Literal source — caller-provided raw values, streamed in order.
    ///
    /// The general form of v1's `Values(Vec<RawDocumentBuf>)`: the elements are
    /// arbitrary raw values, not necessarily documents.
    Values(Vec<RawBson>),

    /// Full scan of a collection — yields each live document as a value.
    /// A *source* node: the executor resolves `collection` to a handle and
    /// streams the collection's documents.
    Scan { collection: CollectionRef },

    /// Index scan — a *source* node that yields bare document IDs from an index
    /// on `field`. Pair with [`Node::KeyLookup`] to fetch the documents. (The
    /// covered-index optimization is a planner concern, deferred with
    /// sargability.)
    IndexScan {
        collection: CollectionRef,
        field: String,
        range: IndexScanRange,
        direction: ScanDirection,
        limit: Option<usize>,
    },

    /// Point read by ID — takes IDs (or documents, from which the pk is
    /// extracted) from `source` and fetches the full document for each.
    KeyLookup {
        collection: CollectionRef,
        source: Box<Node>,
    },

    /// Combine two child ID streams by set intersection (`And`) or union
    /// (`Or`), deduplicating by document identity. `collection` supplies the pk
    /// path used to identify documents (children are usually `IndexScan`s
    /// yielding bare IDs).
    IndexMerge {
        collection: CollectionRef,
        logical: LogicalOp,
        lhs: Box<Node>,
        rhs: Box<Node>,
    },

    /// Attach the `FROM` alias to a bare-value source, producing the row
    /// environment `{alias: value}`. The bridge from the value pipeline
    /// (`Scan`, `KeyLookup`, …) into the binding-aware nodes.
    Bind { alias: String, source: Box<Node> },

    /// `JOIN <alias> IN <array>` — intra-document array unwind. For each row,
    /// evaluate `array` against the current environment and emit one row per
    /// element, extending the environment with `{alias: element}`. Non-array or
    /// undefined `array` yields no rows (inner-join semantics).
    Unwind {
        alias: String,
        array: ScalarExpr,
        source: Box<Node>,
    },

    /// `SELECT VALUE <expr>` — evaluate `expr` against the row environment and
    /// emit its value. An undefined result omits the row. For `find`, the
    /// projection is the identity (`c`) and emits the bound document.
    Project { expr: ScalarExpr, source: Box<Node> },

    /// `WHERE <predicate>` — keep only rows where `predicate` evaluates to true.
    ///
    /// A generic predicate gate, not tied to `WHERE`: the planner also emits it
    /// as the residual after index pushdown, as a recheck above a lossy index
    /// merge, and (later) for `HAVING` and `JOIN ... ON`. Rows where the
    /// predicate is false *or* undefined are dropped (the 3-valued rule).
    Filter {
        predicate: ScalarExpr,
        source: Box<Node>,
    },

    /// `ORDER BY <expr> [ASC|DESC], ...` — reorder rows by the evaluated keys.
    /// A *blocking* transform: it consumes its source fully before emitting.
    Sort {
        keys: Vec<OrderByItem>,
        source: Box<Node>,
    },

    /// `OFFSET <skip> LIMIT <take>` — skip then take. `take` is unbounded when
    /// `None`.
    Limit {
        skip: usize,
        take: Option<usize>,
        source: Box<Node>,
    },

    /// `DISTINCT` — deduplicate the input stream by value identity, emitting the
    /// first occurrence of each distinct value. Composes with `Project`
    /// (`Project(c.city) → Distinct` yields distinct cities).
    Distinct { source: Box<Node> },

    /// Before-mutation trigger tap: fire `hooks` with `action` on each document
    /// as a side effect, passing the document through unchanged.
    Trigger {
        cf: String,
        action: String,
        hooks: Vec<ResolvedHook>,
        source: Box<Node>,
    },

    /// Validation gate: run `validators` on each document; error if any rejects.
    /// Passes the document through on success.
    Validate {
        validators: Vec<ResolvedHook>,
        source: Box<Node>,
    },
}
