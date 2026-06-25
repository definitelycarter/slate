//! The v2 plan IR.
//!
//! A [`Plan`] is a tree of [`Node`]s that `slate-executor` streams values
//! through. The streamed unit is `Option<RawBson>`: `Some` is a value, `None`
//! is *undefined* (the row is omitted at the output boundary). Kept
//! deliberately small for now — see the crate docs for the planned set.

use bson::{Bson, RawBson, RawDocumentBuf};
use slate_ast::{Expression, OrderByItem, SubqueryKind};
use slate_vm::ResolvedHook;

/// A top-level plan: a read query, or a write whose `source` is a read-node
/// tree yielding the documents to write.
///
/// Mutations are applied lazily as the executor's result stream is consumed —
/// the affected documents flow out, so the caller must drain the stream (e.g.
/// `execute_collect`) for the writes to happen.
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

    /// Apply `assignments` to each document yielded by `source` and write it
    /// back. Yields the mutated documents (unchanged documents are dropped).
    Update {
        collection: CollectionRef,
        assignments: Vec<slate_ast::Assignment>,
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

/// How a binding-aware node ([`Node::Filter`], [`Node::Project`],
/// [`Node::Sort`]) reads its input row.
///
/// This is the optimization that avoids the row-environment wrapper for the
/// common single-source query (`FROM c` with no `JOIN`): the source streams
/// bare documents and the node binds the whole row to one alias, with no
/// per-row allocation. When a query has joins, [`Node::Bind`]/[`Node::Unwind`]
/// build a real environment document and the node reads its fields instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowBinding {
    /// The row is the bare bound value; bind the whole row to this alias.
    Alias(String),
    /// The row is an environment document; its top-level fields are the
    /// bindings (the multi-binding, post-`JOIN` shape).
    Env,
}

pub use slate_ast::UpsertMode;

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

/// The distance/similarity metric a [`Node::VectorTopK`] measures by — Cosmos's
/// three. Carried in the IR (not [`slate_eval::VectorMetric`]) to keep the
/// planner free of the eval crate; the executor maps it across the boundary.
/// Each variant's *sense* (higher-is-closer vs lower-is-closer) decides which k
/// the top-k keeps and which `ORDER BY` direction the planner will seek for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMetric {
    /// Cosine similarity — higher is closer (`ORDER BY … DESC`).
    Cosine,
    /// Inner (dot) product — higher is closer (`ORDER BY … DESC`).
    DotProduct,
    /// Euclidean (L2) distance — lower is closer (`ORDER BY … ASC`).
    Euclidean,
}

impl VectorMetric {
    /// Whether a *larger* score means *nearer* — true for the similarity metrics,
    /// false for the `euclidean` distance. The recogniser only emits a
    /// `VectorTopK` when the `ORDER BY` direction matches this (DESC when true,
    /// ASC when false), so a mismatch falls back to the full `Sort`+`Limit`.
    pub fn higher_is_closer(self) -> bool {
        match self {
            VectorMetric::Cosine | VectorMetric::DotProduct => true,
            VectorMetric::Euclidean => false,
        }
    }
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
    /// All strings with a given (non-empty) prefix — `STARTSWITH(x, "pre")` /
    /// `LIKE 'pre%'`. Lowered to a byte-level half-open range `[pre, pre⁺)`,
    /// where `pre⁺` is the prefix with its final byte incremented. That upper
    /// bound may not be valid UTF-8, so it can't be expressed as a `Bson::String`
    /// `Range` bound — hence its own variant.
    StringPrefix(String),
}

/// How a [`Node::CompoundIndexScan`] is bounded — the leftmost-prefix model.
///
/// `eq_prefix` pins the leading components to exact values; `tail` optionally
/// constrains the next component. Components past the tail are unconstrained and
/// dropped from the index by the residual recheck the planner keeps.
#[derive(Debug, Clone, PartialEq)]
pub struct CompoundScanRange {
    /// Exact values for the leading components, in field order.
    pub eq_prefix: Vec<Bson>,
    /// The predicate on the component immediately after the equality prefix.
    pub tail: CompoundScanTail,
}

/// The trailing predicate of a [`CompoundScanRange`].
#[derive(Debug, Clone, PartialEq)]
pub enum CompoundScanTail {
    /// No further constraint — scan the whole equality-prefix group.
    Unbounded,
    /// Equality on the next component.
    Eq(Bson),
    /// Range on the next component.
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
    /// Literal source — caller-provided raw values, streamed in order. The
    /// elements are arbitrary raw values, not necessarily documents.
    Values(Vec<RawBson>),

    /// Full scan of a collection — yields each live document as a value.
    /// A *source* node: the executor resolves `collection` to a handle and
    /// streams the collection's documents.
    Scan { collection: CollectionRef },

    /// Index scan — a *source* node yielding document IDs from an index on
    /// `field`, or — when `covering` — synthesized documents served entirely
    /// from the index entries. A non-covering scan pairs with
    /// [`Node::KeyLookup`] to fetch the documents; a covering scan is a complete
    /// source and the planner omits the `KeyLookup` (RFC: Covering Index Scans,
    /// Part B).
    IndexScan {
        collection: CollectionRef,
        field: String,
        range: IndexScanRange,
        direction: ScanDirection,
        limit: Option<usize>,
        /// When `true`, yield a synthesized `{field: entry.value(), <pk>: id}`
        /// document per entry instead of a bare doc-id, so a query referencing
        /// only `field` and the primary key needs no `KeyLookup`. Set solely by
        /// the covering pass ([`crate::covering`]), which proves every
        /// referenced field is the scanned field or the pk before flipping it.
        covering: bool,
    },

    /// Compound index scan — a *source* node yielding bare document IDs from a
    /// multi-field index, bounded by the leftmost-prefix [`CompoundScanRange`].
    /// `field` is the joined compound identity (`f1\x01f2`). Pair with
    /// [`Node::KeyLookup`] to fetch the documents; the planner keeps a residual
    /// recheck since the byte seek is a conservative superset — unless the scan is
    /// **covering** (RFC: Covering Index Scans, Part B, phase 2).
    CompoundIndexScan {
        collection: CollectionRef,
        field: String,
        range: CompoundScanRange,
        direction: ScanDirection,
        limit: Option<usize>,
        /// When `Some(components)`, yield a synthesized document per entry built
        /// from each component's value placed at its dotted path (merging shared
        /// prefixes, e.g. `user.id` + `user.name` → `{user: {id, name}}`) plus the
        /// doc-id under the pk path — so a query referencing only these components
        /// and the pk needs no `KeyLookup`. `None` yields a bare doc-id for the
        /// paired `KeyLookup`. The component paths are carried explicitly because
        /// the node's `field` is the opaque joined identity (`f1\x01f2`), not the
        /// component names. Set solely by the covering pass ([`crate::covering`]),
        /// which proves every referenced field is a component or the pk before
        /// flipping it.
        covering: Option<Vec<String>>,
    },

    /// Flat-vector-index k-nearest-neighbour source — the physical form of
    /// `ORDER BY VECTORDISTANCE(c.<field>, <q>[, <m>]) … LIMIT k` when `<field>`
    /// has a matching flat vector index (the recogniser in [`crate::lower`]
    /// emits it in place of `Sort`+`Limit`). Scans the index's stored vectors,
    /// measures each against `query_vector` by `metric`, and yields the `k`
    /// nearest doc-ids in nearest-first order — paired with [`Node::KeyLookup`]
    /// to fetch the documents (a bare-id source, exactly like an `IndexScan`).
    ///
    /// `source`, when `Some`, is the **pre-filter**: a sub-plan yielding the
    /// candidate doc-ids a constraining `WHERE` admits, materialized into a set
    /// the top-k restricts to *before* selecting the k nearest. This is the
    /// correctness rule for filtered vector search — the filter must shrink the
    /// candidate set *before* the top-k, never after (a global top-k then
    /// filtered would under-return). `None` means no constraining `WHERE`: the
    /// top-k runs over the whole field.
    VectorTopK {
        collection: CollectionRef,
        /// The document field the vector index is on (the path inside the
        /// `VECTORDISTANCE` call, e.g. `embedding`).
        field: String,
        /// The query vector expression — evaluated *once* by the executor (it is
        /// row-independent: a literal array, an `@parameter`, etc.).
        query_vector: Expression,
        /// The metric to measure by — the index's declared metric (which the
        /// recogniser proved matches the call's, if one was given).
        metric: VectorMetric,
        /// How many nearest neighbours to keep (the `LIMIT`/`TOP`).
        k: usize,
        /// The pre-filter candidate-id source, or `None` for a whole-field scan.
        source: Option<Box<Node>>,
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
        array: Expression,
        source: Box<Node>,
    },

    /// `SELECT VALUE <expr>` — evaluate `expr` against the row environment and
    /// emit its value. An undefined result omits the row. For `find`, the
    /// projection is the identity (`c`) and emits the bound document — which,
    /// in [`RowBinding::Alias`] mode, passes the row through with no copy.
    Project {
        expr: Expression,
        binding: RowBinding,
        source: Box<Node>,
    },

    /// `WHERE <predicate>` — keep only rows where `predicate` evaluates to true.
    ///
    /// A generic predicate gate, not tied to `WHERE`: the planner also emits it
    /// as the residual after index pushdown, as a recheck above a lossy index
    /// merge, and (later) for `HAVING` and `JOIN ... ON`. Rows where the
    /// predicate is false *or* undefined are dropped (the 3-valued rule).
    Filter {
        predicate: Expression,
        binding: RowBinding,
        source: Box<Node>,
    },

    /// `ORDER BY <expr> [ASC|DESC], ...` — reorder rows by the evaluated keys.
    /// A *blocking* transform: it consumes its source fully before emitting.
    Sort {
        keys: Vec<OrderByItem>,
        binding: RowBinding,
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
    ///
    /// `flatten` selects the array semantics: Mongo's `distinct("tags")`
    /// flattens an array row one level (its elements are the distinct values),
    /// whereas SQL `SELECT DISTINCT` dedups whole rows — an array value is one
    /// value, matching Cosmos.
    Distinct { source: Box<Node>, flatten: bool },

    /// Aggregation — `GROUP BY` and/or aggregate functions in `SELECT`. A
    /// *blocking* transform: it buffers the source, groups rows by `group_keys`
    /// (empty = a single group over the whole input), folds each aggregate's
    /// accumulator per group, and emits one **environment** row per group of the
    /// form `{ $key0: …, $agg0: … }`. With no group keys and an empty input it
    /// still emits one row (so `COUNT` is `0`). The downstream `Project` shapes
    /// the output, referencing the `$keyN`/`$aggN` slots the lowering substituted
    /// in.
    Aggregate {
        group_keys: Vec<GroupKey>,
        aggregates: Vec<AggregateExpr>,
        binding: RowBinding,
        source: Box<Node>,
    },

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

    /// A correlated subquery (an `Apply`): for each row from `source`, run
    /// `subplan` — with that row fed in via [`Node::CurrentRow`] so the subplan's
    /// correlated fields resolve — reduce the subplan's rows by `kind`
    /// (scalar / exists / array), and emit the row extended with `{slot: value}`.
    /// Rows flow from `source`; `subplan` is a per-row subroutine, not a second
    /// input. Always produces an environment row, so downstream binds as `Env`.
    Subquery {
        slot: String,
        kind: SubqueryKind,
        subplan: Box<Node>,
        source: Box<Node>,
    },

    /// The single row supplied by the enclosing [`Node::Subquery`] for the
    /// current outer iteration — the leaf of a subplan, in place of a `Scan`.
    /// It carries the outer environment so correlated array sources resolve.
    CurrentRow,
}

/// One grouping key of an [`Node::Aggregate`]: `expr` is evaluated per input row
/// to form the group identity, and its value is bound to `slot` (e.g. `$key0`)
/// in the emitted environment row so the downstream `Project` can reference it.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupKey {
    pub slot: String,
    pub expr: Expression,
}

/// One aggregate of an [`Node::Aggregate`]: the recognized function name (e.g.
/// `"COUNT"`), its single argument expression (evaluated per row), and the
/// output `slot` (e.g. `$agg0`) its result binds to in the emitted row.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub func: String,
    pub arg: Expression,
    pub slot: String,
}
