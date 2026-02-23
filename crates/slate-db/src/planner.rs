use bson::raw::RawBsonRef;
use slate_query::{Mutation, Query, Sort, SortDirection};

use crate::error::DbError;
use crate::expression::{Expression, LogicalOp};

/// Represents a database operation to be planned.
#[derive(Debug, Clone)]
pub enum Statement {
    /// Find documents matching a query (also used for count).
    Find(Query),
    /// Return distinct values for a field.
    Distinct(slate_query::DistinctQuery),
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
    /// Internal: purge expired documents. Carries a pre-built filter doc.
    FlushExpired { filter: bson::RawDocumentBuf },
}

/// Controls the behavior of the Upsert node when a document already exists.
#[derive(Debug, Clone, PartialEq)]
pub enum UpsertMode {
    /// Full replacement — existing doc is discarded.
    Replace,
    /// Partial merge — update fields are merged into the existing doc.
    Merge,
}

/// A single bound for an index range scan.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IndexBound<'a> {
    pub value: RawBsonRef<'a>,
    pub inclusive: bool,
}

/// Controls how an `IndexScan` filters index entries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IndexFilter<'a> {
    /// Exact equality — uses narrow prefix scan `i:{column}\x00{value}\x00`.
    Eq(RawBsonRef<'a>),
    /// Exclusive lower bound — `column > value`.
    Gt(RawBsonRef<'a>),
    /// Inclusive lower bound — `column >= value`.
    Gte(RawBsonRef<'a>),
    /// Exclusive upper bound — `column < value`.
    Lt(RawBsonRef<'a>),
    /// Inclusive upper bound — `column <= value`.
    Lte(RawBsonRef<'a>),
    /// Both lower and upper bounds — `lower <[=] column <[=] upper`.
    Range {
        lower: IndexBound<'a>,
        upper: IndexBound<'a>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode<'a> {
    /// Full scan — yields all record IDs in a collection.
    Scan,

    /// Index scan — yields record IDs from an index.
    IndexScan {
        column: String,
        filter: Option<IndexFilter<'a>>,
        direction: SortDirection,
        limit: Option<usize>,
        complete_groups: bool,
        covered: bool,
    },

    /// Combines ID sets from two child nodes using AND (intersect) or OR (union).
    IndexMerge {
        logical: LogicalOp,
        lhs: Box<PlanNode<'a>>,
        rhs: Box<PlanNode<'a>>,
    },

    /// Fetch raw records by ID.
    ReadRecord {
        input: Box<PlanNode<'a>>,
    },

    /// Evaluate predicate against documents, skip non-matching.
    Filter {
        predicate: Expression<'a>,
        input: Box<PlanNode<'a>>,
    },

    /// Materialize + sort.
    Sort {
        sorts: Vec<Sort>,
        input: Box<PlanNode<'a>>,
    },

    /// Skip / take.
    Limit {
        skip: usize,
        take: Option<usize>,
        input: Box<PlanNode<'a>>,
    },

    /// Project fields and inject `_id`.
    Projection {
        columns: Option<Vec<String>>,
        input: Box<PlanNode<'a>>,
    },

    /// Extract unique values from a field.
    Distinct {
        field: String,
        input: Box<PlanNode<'a>>,
    },

    // ── Mutation nodes ──────────────────────────────────────────
    DeleteIndex {
        indexed_fields: &'a [String],
        input: Box<PlanNode<'a>>,
    },

    Delete {
        input: Box<PlanNode<'a>>,
    },

    Update {
        mutation: Mutation,
        input: Box<PlanNode<'a>>,
    },

    Replace {
        replacement: bson::RawDocumentBuf,
        input: Box<PlanNode<'a>>,
    },

    InsertIndex {
        indexed_fields: &'a [String],
        input: Box<PlanNode<'a>>,
    },

    InsertRecord {
        input: Box<PlanNode<'a>>,
    },

    Upsert {
        mode: UpsertMode,
        indexed_fields: &'a [String],
        input: Box<PlanNode<'a>>,
    },

    /// Caller-provided documents.
    Values {
        docs: Vec<bson::RawDocumentBuf>,
    },
}

/// Co-locates ownership of `Statement` and `indexed_fields` so that
/// `plan()` can borrow both from a single source.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub statement: Statement,
    pub indexed_fields: Vec<String>,
}

/// Build a query plan. The returned `PlanNode<'a>` borrows from the
/// `PreparedStatement` so that `Expression<'a>`, `IndexFilter<'a>`, and
/// mutation-node `indexed_fields` can all reference owned data without cloning.
pub fn plan<'a>(prepared: &'a PreparedStatement) -> Result<PlanNode<'a>, DbError> {
    let indexed_fields = &prepared.indexed_fields;
    match &prepared.statement {
        Statement::Find(query) => plan_find(indexed_fields, query),
        Statement::Distinct(query) => plan_distinct(indexed_fields, query),
        Statement::Update {
            filter,
            mutation,
            limit,
        } => {
            let expr = parse_raw_filter(filter)?;
            Ok(plan_update(indexed_fields, &expr, mutation.clone(), *limit))
        }
        Statement::Replace {
            filter,
            replacement,
        } => {
            let expr = parse_raw_filter(filter)?;
            Ok(plan_replace(indexed_fields, &expr, replacement.clone()))
        }
        Statement::Delete { filter, limit } => {
            let expr = parse_raw_filter(filter)?;
            Ok(plan_delete(indexed_fields, &expr, *limit))
        }
        Statement::Insert { docs } => Ok(plan_insert(indexed_fields, docs.clone())),
        Statement::UpsertMany { docs } => Ok(plan_upsert(
            indexed_fields,
            docs.clone(),
            UpsertMode::Replace,
        )),
        Statement::MergeMany { docs } => {
            Ok(plan_upsert(indexed_fields, docs.clone(), UpsertMode::Merge))
        }
        Statement::FlushExpired { filter } => {
            let expr = parse_raw_filter(filter)?;
            Ok(plan_delete(indexed_fields, &expr, None))
        }
    }
}

/// Parse a raw BSON filter document into an Expression.
fn parse_raw_filter<'a>(raw: &'a bson::RawDocumentBuf) -> Result<Expression<'a>, DbError> {
    crate::parse_filter::parse_filter(raw.as_bytes())
        .map_err(|e| DbError::InvalidQuery(e.to_string()))
}

/// Parse an optional raw BSON filter document into an Option<Expression>.
fn parse_optional_filter<'a>(
    raw: Option<&'a bson::RawDocumentBuf>,
) -> Result<Option<Expression<'a>>, DbError> {
    match raw {
        _ => Ok(None), // Some(r) => parse_raw_filter(r).map(Some),
                       // None => Ok(None),
    }
}

fn plan_find<'a>(indexed_fields: &[String], query: &'a Query) -> Result<PlanNode<'a>, DbError> {
    let expr = parse_optional_filter(query.filter.as_ref())?;

    // Step 1: Plan the filter — split into ID-tier node + residual document-tier predicate
    let (id_node, residual_filter) = match &expr {
        Some(e) => plan_filter(indexed_fields, e),
        None => (PlanNode::Scan, None),
    };

    // Capture before id_node is consumed — needed for indexed sort optimization in Step 4.
    let id_is_scan = matches!(id_node, PlanNode::Scan);
    let has_residual_filter = residual_filter.is_some();

    // Step 2: Check if the index covers the projection (skip ReadRecord entirely).
    // CoverProject applies when: IndexScan has an equality value, projection only
    // requests _id and/or the indexed column, and there's no residual filter.
    let no_residual = !has_residual_filter;
    // Covered projections: IndexScan yields { _id, column: value } directly,
    // skipping ReadRecord + Filter + Projection. TTL is checked inline from
    // millis stored in the index entry value — zero extra lookups.
    let covered = no_residual
        && matches!(
            &id_node,
            PlanNode::IndexScan {
                column,
                filter: Some(IndexFilter::Eq(_)),
                ..
            } if query.columns.as_ref().is_some_and(|cols|
                cols.iter().all(|c| c == "_id" || c == column)
            )
        );

    // Set covered flag on the IndexScan so it yields { _id, column: value } documents.
    let mut id_node = id_node;
    if covered {
        if let PlanNode::IndexScan {
            covered: ref mut c, ..
        } = id_node
        {
            *c = true;
        }
    }

    let node = if covered {
        // Index covers the projection — IndexScan yields finished docs.
        // Skip ReadRecord + Filter + Projection.
        id_node
    } else {
        // Standard path: optional ReadRecord → optional Filter.
        // Scan already yields full documents, so ReadRecord is only
        // needed for IndexScan/IndexMerge (which yield bare IDs).
        let node = if id_is_scan {
            id_node
        } else {
            PlanNode::ReadRecord {
                input: Box::new(id_node),
            }
        };

        match residual_filter {
            Some(expr) => PlanNode::Filter {
                predicate: expr,
                input: Box::new(node),
            },
            None => node,
        }
    };

    // Step 4: Sort
    //
    // Optimization: when sort[0] is indexed, has a Limit, and the ID tier is a
    // Scan (no value-filtered IndexScan), we replace Scan with an ordered IndexScan.
    //
    // Single-field sort: eliminate Sort entirely — index provides full ordering.
    //   Limit pushdown into IndexScan stops the walk early.
    // Multi-field sort: IndexScan with complete_groups=true provides primary ordering
    //   and finishes the last value group. Sort handles sub-sorting by remaining fields.
    let can_use_indexed_sort = !query.sort.is_empty()
        && query.take.is_some()
        && indexed_fields.contains(&query.sort[0].field)
        && id_is_scan;

    let node = if can_use_indexed_sort && query.sort.len() == 1 {
        // Single-field: limit pushdown when no filter, exact cutoff is fine.
        let index_limit = if !has_residual_filter {
            Some(query.skip.unwrap_or(0) + query.take.unwrap_or(0))
        } else {
            None
        };

        replace_scan_with_index_order(
            node,
            &query.sort[0].field,
            query.sort[0].direction,
            index_limit,
            false, // no sub-sort needed
        )
    } else if can_use_indexed_sort {
        // Multi-field: push limit into IndexScan with complete_groups=true.
        // IndexScan reads skip+take entries then finishes the last value group.
        // Sort handles sub-sorting by sorts[1..] on the reduced record set.
        let index_limit = if !has_residual_filter {
            Some(query.skip.unwrap_or(0) + query.take.unwrap_or(0))
        } else {
            None
        };
        let node = replace_scan_with_index_order(
            node,
            &query.sort[0].field,
            query.sort[0].direction,
            index_limit,
            true, // finish last value group for correct sub-sorting
        );
        PlanNode::Sort {
            sorts: query.sort.clone(),
            input: Box::new(node),
        }
    } else if !query.sort.is_empty() {
        PlanNode::Sort {
            sorts: query.sort.clone(),
            input: Box::new(node),
        }
    } else {
        node
    };

    // Step 5: Limit
    let node = if query.skip.is_some() || query.take.is_some() {
        PlanNode::Limit {
            skip: query.skip.unwrap_or(0),
            take: query.take,
            input: Box::new(node),
        }
    } else {
        node
    };

    // Step 6: Projection — skip when CoverProject already handles it
    Ok(if covered {
        node
    } else {
        PlanNode::Projection {
            columns: query.columns.clone(),
            input: Box::new(node),
        }
    })
}

/// Build a plan for a distinct query, reusing the filter planning logic.
///
/// Plan shape:
/// - No sort: `Distinct → Projection([field]) → Filter → ReadRecord → Scan`
/// - With sort: `Limit → Sort → Distinct → Projection([field]) → ...`
///
/// Projection extracts the distinct field; Distinct deduplicates;
/// Sort/Limit are standard pipeline nodes on top.
fn plan_distinct<'a>(
    indexed_fields: &[String],
    query: &'a slate_query::DistinctQuery,
) -> Result<PlanNode<'a>, DbError> {
    let expr = parse_optional_filter(query.filter.as_ref())?;

    // Step 1: Plan the filter — same as find
    let (id_node, residual_filter) = match &expr {
        Some(e) => plan_filter(indexed_fields, e),
        None => (PlanNode::Scan, None),
    };

    // Step 2: ReadRecord (only for index paths — Scan already yields documents)
    let node = if matches!(id_node, PlanNode::Scan) {
        id_node
    } else {
        PlanNode::ReadRecord {
            input: Box::new(id_node),
        }
    };

    // Step 3: Wrap with residual filter if any
    let node = match residual_filter {
        Some(expr) => PlanNode::Filter {
            predicate: expr,
            input: Box::new(node),
        },
        None => node,
    };

    // Step 4: Projection — extract the distinct field
    let node = PlanNode::Projection {
        columns: Some(vec![query.field.clone()]),
        input: Box::new(node),
    };

    // Step 5: Distinct — dedup
    let node = PlanNode::Distinct {
        field: query.field.clone(),
        input: Box::new(node),
    };

    // Step 6: Sort (if requested)
    let node = match query.sort {
        Some(dir) => PlanNode::Sort {
            sorts: vec![slate_query::Sort {
                field: query.field.clone(),
                direction: dir,
            }],
            input: Box::new(node),
        },
        None => node,
    };

    // Step 7: Limit (if requested)
    let node = match (query.skip, query.take) {
        (None, None) => node,
        (skip, take) => PlanNode::Limit {
            skip: skip.unwrap_or(0),
            take,
            input: Box::new(node),
        },
    };

    Ok(node)
}

/// Replace the Scan node inside a plan subtree with an ordered IndexScan.
/// Handles both `ReadRecord(Scan)` and `Filter(ReadRecord(Scan))`.
fn replace_scan_with_index_order<'a>(
    node: PlanNode<'a>,
    sort_field: &str,
    direction: SortDirection,
    limit: Option<usize>,
    complete_groups: bool,
) -> PlanNode<'a> {
    match node {
        PlanNode::Scan => PlanNode::ReadRecord {
            input: Box::new(PlanNode::IndexScan {
                column: sort_field.to_string(),
                filter: None,
                direction,
                limit,
                complete_groups,
                covered: false,
            }),
        },
        PlanNode::Filter { predicate, input } => PlanNode::Filter {
            predicate,
            input: Box::new(replace_scan_with_index_order(
                *input,
                sort_field,
                direction,
                limit,
                complete_groups,
            )),
        },
        other => other,
    }
}

// ── Expression-based filter planning ────────────────────────────
//
// These functions split an Expression tree into an ID-tier node
// (IndexScan / IndexMerge) and a residual Expression for document-tier
// filtering. The logic mirrors the old FilterGroup-based planner but
// pattern-matches on Expression variants directly.

/// Plan a filter expression, returning an ID-tier node and optional residual.
fn plan_filter<'a>(
    indexed_fields: &[String],
    expr: &Expression<'a>,
) -> (PlanNode<'a>, Option<Expression<'a>>) {
    match expr {
        Expression::And(children) => plan_and_children(indexed_fields, children, expr),
        Expression::Or(children) => plan_or_children(indexed_fields, children, expr),
        // Single condition at top level — wrap in a one-element "and" for uniform handling
        other => plan_single_condition(indexed_fields, other, expr),
    }
}

/// Plan a single (non-And, non-Or) expression at the top level.
fn plan_single_condition<'a>(
    indexed_fields: &[String],
    expr: &Expression<'a>,
    original: &Expression<'a>,
) -> (PlanNode<'a>, Option<Expression<'a>>) {
    // Try to push into index
    if let Some(node) = try_index_condition(indexed_fields, expr) {
        (node, None)
    } else {
        (PlanNode::Scan, Some(original.clone()))
    }
}

/// Try to convert a single condition expression into an IndexScan node.
/// Returns None if the condition can't be served by an index.
fn try_index_condition<'a>(
    indexed_fields: &[String],
    expr: &Expression<'a>,
) -> Option<PlanNode<'a>> {
    let (field, index_filter) = match expr {
        Expression::Eq(f, val) => (*f, IndexFilter::Eq(*val)),
        Expression::Gt(f, val) => (*f, IndexFilter::Gt(*val)),
        Expression::Gte(f, val) => (*f, IndexFilter::Gte(*val)),
        Expression::Lt(f, val) => (*f, IndexFilter::Lt(*val)),
        Expression::Lte(f, val) => (*f, IndexFilter::Lte(*val)),
        _ => return None,
    };
    if !indexed_fields.iter().any(|f| f == field) {
        return None;
    }
    Some(PlanNode::IndexScan {
        column: field.to_string(),
        filter: Some(index_filter),
        direction: SortDirection::Asc,
        limit: None,
        complete_groups: false,
        covered: false,
    })
}

/// Plan an AND group of children.
///
/// Strategy: iterate indexed_fields in priority order, pick the best child
/// to push into the ID tier. Non-selected children become residual.
fn plan_and_children<'a>(
    indexed_fields: &[String],
    children: &[Expression<'a>],
    original: &Expression<'a>,
) -> (PlanNode<'a>, Option<Expression<'a>>) {
    let best = find_best_and_child(indexed_fields, children);

    match best {
        Some((id_node, consumed_indices)) => {
            // Build residual: all children except the consumed ones
            let remaining: Vec<Expression> = children
                .iter()
                .enumerate()
                .filter(|(i, _)| !consumed_indices.contains(i))
                .map(|(_, c)| c.clone())
                .collect();

            let residual = if remaining.is_empty() {
                None
            } else if remaining.len() == 1 {
                Some(remaining.into_iter().next().unwrap())
            } else {
                Some(Expression::And(remaining))
            };

            (id_node, residual)
        }
        None => {
            // No indexed condition found — full scan, entire expression is residual
            (PlanNode::Scan, Some(original.clone()))
        }
    }
}

/// Find the best AND child to push into the ID tier.
///
/// Iterates indexed_fields in priority order. For each field, checks:
/// 1. Is there a direct Eq condition on this field? → IndexScan
/// 2. Is there a fully-indexable OR sub-group that uses this field? → IndexMerge(Or)
/// 3. Are there range conditions (Gt/Gte/Lt/Lte) on this field? → IndexScan with range
///
/// Returns the ID-tier node and the indices of consumed children.
fn find_best_and_child<'a>(
    indexed_fields: &[String],
    children: &[Expression<'a>],
) -> Option<(PlanNode<'a>, Vec<usize>)> {
    // Priority pass 1: Eq conditions (most selective)
    for field in indexed_fields {
        for (i, child) in children.iter().enumerate() {
            if let Expression::Eq(f, val) = child {
                if *f == field.as_str() {
                    let node = PlanNode::IndexScan {
                        column: f.to_string(),
                        filter: Some(IndexFilter::Eq(*val)),
                        direction: SortDirection::Asc,
                        limit: None,
                        complete_groups: false,
                        covered: false,
                    };
                    return Some((node, vec![i]));
                }
            }
        }
    }

    // Priority pass 2: fully-indexable OR sub-groups
    // The OR child stays in the residual (empty consumed list) because each
    // IndexScan branch may over-fetch — the full OR must be rechecked.
    for (_i, child) in children.iter().enumerate() {
        if let Expression::Or(or_children) = child {
            if let Some(id_node) = try_or_index_merge(indexed_fields, or_children) {
                return Some((id_node, vec![]));
            }
        }
    }

    // Priority pass 3: range conditions (Gt/Gte/Lt/Lte) on indexed fields
    for field in indexed_fields {
        let mut lower: Option<(usize, IndexFilter<'a>)> = None;
        let mut upper: Option<(usize, IndexFilter<'a>)> = None;

        for (i, child) in children.iter().enumerate() {
            match child {
                Expression::Gt(f, val) if *f == field.as_str() => {
                    lower = Some((i, IndexFilter::Gt(*val)));
                }
                Expression::Gte(f, val) if *f == field.as_str() => {
                    lower = Some((i, IndexFilter::Gte(*val)));
                }
                Expression::Lt(f, val) if *f == field.as_str() => {
                    upper = Some((i, IndexFilter::Lt(*val)));
                }
                Expression::Lte(f, val) if *f == field.as_str() => {
                    upper = Some((i, IndexFilter::Lte(*val)));
                }
                _ => {}
            }
        }

        if lower.is_some() || upper.is_some() {
            let (index_filter, consumed) = match (lower, upper) {
                (Some((li, _)), Some((ui, _))) => {
                    // Both bounds → Range
                    let lower_bound = match &children[li] {
                        Expression::Gte(_, val) => IndexBound {
                            value: *val,
                            inclusive: true,
                        },
                        Expression::Gt(_, val) => IndexBound {
                            value: *val,
                            inclusive: false,
                        },
                        _ => unreachable!(),
                    };
                    let upper_bound = match &children[ui] {
                        Expression::Lte(_, val) => IndexBound {
                            value: *val,
                            inclusive: true,
                        },
                        Expression::Lt(_, val) => IndexBound {
                            value: *val,
                            inclusive: false,
                        },
                        _ => unreachable!(),
                    };
                    (
                        IndexFilter::Range {
                            lower: lower_bound,
                            upper: upper_bound,
                        },
                        vec![li, ui],
                    )
                }
                (Some((li, lf)), None) => (lf, vec![li]),
                (None, Some((ui, uf))) => (uf, vec![ui]),
                (None, None) => unreachable!(),
            };

            let node = PlanNode::IndexScan {
                column: field.to_string(),
                filter: Some(index_filter),
                direction: SortDirection::Asc,
                limit: None,
                complete_groups: false,
                covered: false,
            };
            return Some((node, consumed));
        }
    }

    None
}

/// Plan an OR group of children.
///
/// Strategy: for each child, try to produce an IndexScan (or recurse for nested groups).
/// If every child produces an ID-tier node, combine with IndexMerge(Or).
/// If any child has zero indexed conditions, fall back to Scan with full predicate.
///
/// The full original OR expression always becomes the residual filter (recheck),
/// because each IndexScan branch may over-fetch.
fn plan_or_children<'a>(
    indexed_fields: &[String],
    children: &[Expression<'a>],
    original: &Expression<'a>,
) -> (PlanNode<'a>, Option<Expression<'a>>) {
    match try_or_index_merge(indexed_fields, children) {
        Some(id_node) => {
            // All branches indexed — use IndexMerge(Or), full expression is residual recheck
            (id_node, Some(original.clone()))
        }
        None => {
            // Can't fully index the OR — fall back to Scan
            (PlanNode::Scan, Some(original.clone()))
        }
    }
}

/// Try to build an IndexMerge(Or) from OR children.
///
/// Returns Some(id_node) if every child can produce an ID-tier node.
/// Returns None if any child has zero indexed conditions.
fn try_or_index_merge<'a>(
    indexed_fields: &[String],
    children: &[Expression<'a>],
) -> Option<PlanNode<'a>> {
    let mut id_nodes: Vec<PlanNode<'a>> = Vec::new();

    for child in children {
        match child {
            Expression::Eq(field, val) if indexed_fields.iter().any(|f| f.as_str() == *field) => {
                id_nodes.push(PlanNode::IndexScan {
                    column: field.to_string(),
                    filter: Some(IndexFilter::Eq(*val)),
                    direction: SortDirection::Asc,
                    limit: None,
                    complete_groups: false,
                    covered: false,
                });
            }
            Expression::And(sub_children) => {
                // Recurse: try to get an ID-tier node from the nested AND
                let (id_node, _residual) = plan_and_children(indexed_fields, sub_children, child);
                if matches!(id_node, PlanNode::Scan) {
                    return None;
                }
                id_nodes.push(id_node);
            }
            Expression::Or(sub_children) => {
                // Nested OR — recurse
                match try_or_index_merge(indexed_fields, sub_children) {
                    Some(node) => id_nodes.push(node),
                    None => return None,
                }
            }
            _ => {
                // Non-indexed condition in OR — can't use indexes for this OR
                return None;
            }
        }
    }

    if id_nodes.is_empty() {
        return None;
    }

    if id_nodes.len() == 1 {
        return Some(id_nodes.into_iter().next().unwrap());
    }

    // Fold into a binary tree of IndexMerge(Or)
    let mut iter = id_nodes.into_iter();
    let mut result = iter.next().unwrap();
    for node in iter {
        result = PlanNode::IndexMerge {
            logical: LogicalOp::Or,
            lhs: Box::new(result),
            rhs: Box::new(node),
        };
    }

    Some(result)
}

// ── Mutation plan builders ──────────────────────────────────────
//
// Each builder composes a pipeline of mutation nodes on top of a
// filtered read source. The read source is built from the same
// filter planning logic used by `plan()`.

/// Build the read source for a mutation: ReadRecord → Filter → Scan/IndexScan.
/// Optionally wraps with Limit for `_one` variants.
fn build_filtered_source<'a>(
    indexed_fields: &[String],
    filter: &Expression<'a>,
    take: Option<usize>,
) -> PlanNode<'a> {
    let (id_node, residual_filter) = plan_filter(indexed_fields, filter);

    // Scan already yields full documents — only wrap in ReadRecord for index paths.
    let node = if matches!(id_node, PlanNode::Scan) {
        id_node
    } else {
        PlanNode::ReadRecord {
            input: Box::new(id_node),
        }
    };

    let node = match residual_filter {
        Some(expr) => PlanNode::Filter {
            predicate: expr,
            input: Box::new(node),
        },
        None => node,
    };

    match take {
        Some(n) => PlanNode::Limit {
            skip: 0,
            take: Some(n),
            input: Box::new(node),
        },
        None => node,
    }
}

/// Plan a delete operation.
///
/// Pipeline: `Delete → DeleteIndex → [Limit] → Filter → ReadRecord → Scan/IndexScan`
fn plan_delete<'a>(
    indexed_fields: &'a [String],
    filter: &Expression<'a>,
    take: Option<usize>,
) -> PlanNode<'a> {
    let source = build_filtered_source(indexed_fields, filter, take);

    let node = PlanNode::DeleteIndex {
        indexed_fields,
        input: Box::new(source),
    };

    PlanNode::Delete {
        input: Box::new(node),
    }
}

/// Plan an update (merge) operation.
///
/// Pipeline: `InsertIndex → Update → DeleteIndex → [Limit] → Filter → ReadRecord → Scan/IndexScan`
fn plan_update<'a>(
    indexed_fields: &'a [String],
    filter: &Expression<'a>,
    mutation: Mutation,
    take: Option<usize>,
) -> PlanNode<'a> {
    let source = build_filtered_source(indexed_fields, filter, take);

    let node = PlanNode::DeleteIndex {
        indexed_fields,
        input: Box::new(source),
    };

    let node = PlanNode::Update {
        mutation,
        input: Box::new(node),
    };

    PlanNode::InsertIndex {
        indexed_fields,
        input: Box::new(node),
    }
}

/// Plan a replace operation.
///
/// Pipeline: `InsertIndex → Replace → DeleteIndex → [Limit(1)] → Filter → ReadRecord → Scan/IndexScan`
fn plan_replace<'a>(
    indexed_fields: &'a [String],
    filter: &Expression<'a>,
    replacement: bson::RawDocumentBuf,
) -> PlanNode<'a> {
    let source = build_filtered_source(indexed_fields, filter, Some(1));

    let node = PlanNode::DeleteIndex {
        indexed_fields,
        input: Box::new(source),
    };

    let node = PlanNode::Replace {
        replacement,
        input: Box::new(node),
    };

    PlanNode::InsertIndex {
        indexed_fields,
        input: Box::new(node),
    }
}

/// Plan an insert operation.
///
/// Pipeline: `InsertIndex → InsertRecord → Values`
fn plan_insert<'a>(indexed_fields: &'a [String], docs: Vec<bson::RawDocumentBuf>) -> PlanNode<'a> {
    let node = PlanNode::Values { docs };

    let node = PlanNode::InsertRecord {
        input: Box::new(node),
    };

    PlanNode::InsertIndex {
        indexed_fields,
        input: Box::new(node),
    }
}

/// Plan an upsert operation (insert-or-replace / insert-or-merge).
///
/// Pipeline: `InsertIndex → Upsert(mode, indexed_fields) → Values`
///
/// The Upsert node handles old-doc lookup and old-index cleanup internally,
/// so no separate DeleteIndex node is needed.
fn plan_upsert<'a>(
    indexed_fields: &'a [String],
    docs: Vec<bson::RawDocumentBuf>,
    mode: UpsertMode,
) -> PlanNode<'a> {
    let node = PlanNode::Values { docs };

    let node = PlanNode::Upsert {
        mode,
        indexed_fields,
        input: Box::new(node),
    };

    PlanNode::InsertIndex {
        indexed_fields,
        input: Box::new(node),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{Bson, raw::RawBsonRef, rawdoc};

    fn empty_query() -> Query {
        Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        }
    }

    fn prep(indexed_fields: Vec<String>, statement: Statement) -> PreparedStatement {
        PreparedStatement {
            statement,
            indexed_fields,
        }
    }

    fn eq_raw(field: &str, value: Bson) -> bson::RawDocumentBuf {
        let mut doc = bson::Document::new();
        doc.insert(field.to_string(), value);
        bson::RawDocumentBuf::from_document(&doc).unwrap()
    }

    /// Unwrap the outermost Projection node (always present in plan output).
    /// Returns (columns, inner_node).
    fn unwrap_projection(node: PlanNode) -> (Option<Vec<String>>, PlanNode) {
        match node {
            PlanNode::Projection { columns, input } => (columns, *input),
            other => panic!("expected Projection at top, got {:?}", other),
        }
    }

    /// Helper: check if the residual predicate contains a field condition.
    fn residual_has_field(expr: &Expression, expected_field: &str) -> bool {
        match expr {
            Expression::And(children) => children
                .iter()
                .any(|c| residual_has_field(c, expected_field)),
            Expression::Eq(f, _)
            | Expression::Gt(f, _)
            | Expression::Gte(f, _)
            | Expression::Lt(f, _)
            | Expression::Lte(f, _)
            | Expression::Regex(f, _)
            | Expression::Exists(f, _) => *f == expected_field,
            Expression::Or(_) => false,
        }
    }

    /// Helper: check if the residual is an Or expression.
    fn residual_is_or(expr: &Expression) -> bool {
        matches!(expr, Expression::Or(_))
    }

    // ── Basic plan tests ────────────────────────────────────────

    #[test]
    fn plan_no_filter() {
        let stmt = Statement::Find(empty_query());
        let prepared = prep(vec![], stmt);
        let p = plan(&prepared).unwrap();
        let (cols, inner) = unwrap_projection(p);
        assert_eq!(cols, None);
        assert!(matches!(inner, PlanNode::Scan));
    }

    #[test]
    fn plan_with_filter_no_index() {
        let q = Query {
            filter: Some(eq_raw("status", Bson::String("active".into()))),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(vec![], stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(*input, PlanNode::Scan));
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_with_indexed_eq_filter() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(eq_raw("status", Bson::String("active".into()))),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        // ReadRecord(IndexScan) — no residual filter since only condition is indexed
        match inner {
            PlanNode::ReadRecord { input, .. } => {
                assert_eq!(
                    *input,
                    PlanNode::IndexScan {
                        column: "status".into(),
                        filter: Some(IndexFilter::Eq(RawBsonRef::String("active"))),
                        direction: SortDirection::Asc,
                        limit: None,
                        complete_groups: false,
                        covered: false,
                    }
                );
            }
            _ => panic!("expected ReadRecord, got {:?}", inner),
        }
    }

    #[test]
    fn plan_indexed_eq_plus_residual() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "status": "active", "score": { "$gt": 50_i64 } }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        // Filter(score > 50, ReadRecord(IndexScan(status)))
        match inner {
            PlanNode::Filter { predicate, input } => {
                assert!(residual_has_field(&predicate, "score"));
                match *input {
                    PlanNode::ReadRecord { input, .. } => {
                        assert!(matches!(*input, PlanNode::IndexScan { .. }));
                    }
                    _ => panic!("expected ReadRecord"),
                }
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_with_sort() {
        let q = Query {
            sort: vec![Sort {
                field: "name".into(),
                direction: SortDirection::Asc,
            }],
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(vec![], stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Sort { input, .. } => {
                assert!(matches!(*input, PlanNode::Scan));
            }
            _ => panic!("expected Sort, got {:?}", inner),
        }
    }

    #[test]
    fn plan_with_filter_and_sort() {
        let q = Query {
            filter: Some(eq_raw("status", Bson::String("active".into()))),
            sort: vec![Sort {
                field: "name".into(),
                direction: SortDirection::Asc,
            }],
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(vec![], stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Sort { input, .. } => {
                assert!(matches!(*input, PlanNode::Filter { .. }));
            }
            _ => panic!("expected Sort, got {:?}", inner),
        }
    }

    #[test]
    fn plan_with_skip_take() {
        let q = Query {
            skip: Some(10),
            take: Some(5),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(vec![], stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { skip, take, input } => {
                assert_eq!(skip, 10);
                assert_eq!(take, Some(5));
                assert!(matches!(*input, PlanNode::Scan));
            }
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    #[test]
    fn plan_with_projection() {
        let q = Query {
            columns: Some(vec!["name".into(), "status".into()]),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(vec![], stmt);
        let p = plan(&prepared).unwrap();
        let (cols, inner) = unwrap_projection(p);
        assert_eq!(cols, Some(vec!["name".to_string(), "status".to_string()]));
        assert!(matches!(inner, PlanNode::Scan));
    }

    #[test]
    fn plan_full_query() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "status": "active", "score": { "$gt": 50_i64 } }),
            sort: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            skip: Some(10),
            take: Some(5),
            columns: Some(vec!["name".into(), "score".into()]),
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        // Projection(Limit(Sort(Filter(ReadRecord(IndexScan)))))
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::Sort { input, .. } => match *input {
                    PlanNode::Filter { input, .. } => match *input {
                        PlanNode::ReadRecord { input, .. } => {
                            assert!(matches!(*input, PlanNode::IndexScan { .. }));
                        }
                        _ => panic!("expected ReadRecord"),
                    },
                    _ => panic!("expected Filter"),
                },
                _ => panic!("expected Sort"),
            },
            _ => panic!("expected Limit"),
        }
    }

    // ── AND priority selection ───────────────────────────────────

    #[test]
    fn plan_and_priority_selection() {
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "status": "active", "user_id": "abc" }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        // Filter(status = "active", ReadRecord(IndexScan(user_id = "abc")))
        match inner {
            PlanNode::Filter { predicate, input } => {
                assert!(residual_has_field(&predicate, "status"));
                match *input {
                    PlanNode::ReadRecord { input, .. } => match *input {
                        PlanNode::IndexScan { column, .. } => {
                            assert_eq!(column, "user_id");
                        }
                        _ => panic!("expected IndexScan"),
                    },
                    _ => panic!("expected ReadRecord"),
                }
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    // ── OR with indexes ─────────────────────────────────────────

    #[test]
    fn plan_or_both_indexed() {
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "$or": [{ "user_id": "abc" }, { "status": "active" }] }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        // Filter(recheck, ReadRecord(IndexMerge(Or, IndexScan, IndexScan)))
        match inner {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => match *input {
                    PlanNode::IndexMerge { logical, lhs, rhs } => {
                        assert_eq!(logical, LogicalOp::Or);
                        assert!(matches!(*lhs, PlanNode::IndexScan { .. }));
                        assert!(matches!(*rhs, PlanNode::IndexScan { .. }));
                    }
                    _ => panic!("expected IndexMerge"),
                },
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_or_one_not_indexed() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "$or": [{ "status": "active" }, { "name": "test" }] }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(*input, PlanNode::Scan));
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_or_same_field() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "$or": [{ "status": "active" }, { "status": "archived" }] }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => match *input {
                    PlanNode::IndexMerge { logical, .. } => {
                        assert_eq!(logical, LogicalOp::Or);
                    }
                    _ => panic!("expected IndexMerge"),
                },
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_or_three_values() {
        let indexed = vec!["user_id".to_string()];
        let q = Query {
            filter: Some(
                rawdoc! { "$or": [{ "user_id": 1_i64 }, { "user_id": 2_i64 }, { "user_id": 3_i64 }] },
            ),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => match *input {
                    PlanNode::IndexMerge { logical, lhs, rhs } => {
                        assert_eq!(logical, LogicalOp::Or);
                        assert!(matches!(*lhs, PlanNode::IndexMerge { .. }));
                        assert!(matches!(*rhs, PlanNode::IndexScan { .. }));
                    }
                    _ => panic!("expected IndexMerge"),
                },
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    // ── Nested AND/OR ───────────────────────────────────────────

    #[test]
    fn plan_and_with_nested_or_indexed() {
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(
                rawdoc! { "user_id": "abc", "$or": [{ "status": "active" }, { "status": "archived" }] },
            ),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { predicate, input } => {
                assert!(residual_is_or(&predicate));
                match *input {
                    PlanNode::ReadRecord { input, .. } => match *input {
                        PlanNode::IndexScan { column, .. } => {
                            assert_eq!(column, "user_id");
                        }
                        _ => panic!("expected IndexScan"),
                    },
                    _ => panic!("expected ReadRecord"),
                }
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_or_with_nested_ands() {
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "$or": [
                { "$and": [{ "user_id": "abc" }, { "status": "active" }] },
                { "$and": [{ "user_id": "xyz" }, { "status": "pending" }] }
            ] }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => match *input {
                    PlanNode::IndexMerge { logical, lhs, rhs } => {
                        assert_eq!(logical, LogicalOp::Or);
                        match *lhs {
                            PlanNode::IndexScan { column, .. } => {
                                assert_eq!(column, "user_id");
                            }
                            _ => panic!("expected IndexScan for lhs"),
                        }
                        match *rhs {
                            PlanNode::IndexScan { column, .. } => {
                                assert_eq!(column, "user_id");
                            }
                            _ => panic!("expected IndexScan for rhs"),
                        }
                    }
                    _ => panic!("expected IndexMerge"),
                },
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_or_partial_index_per_branch() {
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "$or": [
                { "user_id": "abc", "score": { "$gt": 50_i64 } },
                { "status": "active" }
            ] }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => match *input {
                    PlanNode::IndexMerge { logical, lhs, rhs } => {
                        assert_eq!(logical, LogicalOp::Or);
                        match *lhs {
                            PlanNode::IndexScan { column, .. } => {
                                assert_eq!(column, "user_id");
                            }
                            _ => panic!("expected IndexScan for lhs"),
                        }
                        match *rhs {
                            PlanNode::IndexScan { column, .. } => {
                                assert_eq!(column, "status");
                            }
                            _ => panic!("expected IndexScan for rhs"),
                        }
                    }
                    _ => panic!("expected IndexMerge"),
                },
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_or_unindexed_branch_fallback() {
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "$or": [
                { "$or": [{ "user_id": "abc" }, { "status": "active" }] },
                { "$or": [{ "count": { "$gt": 5_i64 } }, { "name": "foo" }] }
            ] }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(*input, PlanNode::Scan));
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_fully_unindexed() {
        let q = Query {
            filter: Some(rawdoc! { "score": { "$gt": 50_i64 } }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(vec![], stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(*input, PlanNode::Scan));
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_and_selects_or_subgroup_when_no_direct_eq() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(
                rawdoc! { "$or": [{ "status": "active" }, { "status": "archived" }], "score": { "$gt": 50_i64 } },
            ),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { predicate, input } => {
                // Both OR (recheck) and score condition in residual
                match &predicate {
                    Expression::And(children) => {
                        assert_eq!(children.len(), 2);
                    }
                    _ => panic!("expected And in residual"),
                }
                match *input {
                    PlanNode::ReadRecord { input, .. } => {
                        assert!(matches!(*input, PlanNode::IndexMerge { .. }));
                    }
                    _ => panic!("expected ReadRecord"),
                }
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    // ── Indexed sort optimization ───────────────────────────────

    #[test]
    fn plan_indexed_sort_with_limit_eliminates_sort() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            sort: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            take: Some(5),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::ReadRecord { input } => match *input {
                    PlanNode::IndexScan {
                        column,
                        filter,
                        direction,
                        limit,
                        ..
                    } => {
                        assert_eq!(column, "score");
                        assert_eq!(filter, None);
                        assert_eq!(direction, SortDirection::Desc);
                        assert_eq!(limit, Some(5));
                    }
                    _ => panic!("expected IndexScan"),
                },
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    #[test]
    fn plan_indexed_sort_asc_with_limit() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            sort: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Asc,
            }],
            take: Some(10),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::ReadRecord { input } => match *input {
                    PlanNode::IndexScan {
                        direction,
                        filter,
                        limit,
                        ..
                    } => {
                        assert_eq!(direction, SortDirection::Asc);
                        assert_eq!(filter, None);
                        assert_eq!(limit, Some(10));
                    }
                    _ => panic!("expected IndexScan"),
                },
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    #[test]
    fn plan_indexed_sort_with_filter_and_limit() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "name": { "$gt": "a" } }),
            sort: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            take: Some(5),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::Filter { input, .. } => match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan {
                            column,
                            filter,
                            direction,
                            limit,
                            ..
                        } => {
                            assert_eq!(column, "score");
                            assert_eq!(filter, None);
                            assert_eq!(direction, SortDirection::Desc);
                            assert_eq!(limit, None);
                        }
                        _ => panic!("expected IndexScan"),
                    },
                    _ => panic!("expected ReadRecord"),
                },
                _ => panic!("expected Filter"),
            },
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    #[test]
    fn plan_indexed_sort_no_limit_keeps_sort() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            sort: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        assert!(matches!(inner, PlanNode::Sort { .. }));
    }

    #[test]
    fn plan_sort_not_indexed_keeps_sort() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            sort: vec![Sort {
                field: "name".into(),
                direction: SortDirection::Desc,
            }],
            take: Some(5),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => {
                assert!(matches!(*input, PlanNode::Sort { .. }));
            }
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    #[test]
    fn plan_indexed_sort_with_indexed_filter_keeps_sort() {
        let indexed = vec!["status".to_string(), "score".to_string()];
        let q = Query {
            filter: Some(eq_raw("status", Bson::String("active".into()))),
            sort: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            take: Some(5),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::Sort { input, .. } => match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan { column, filter, .. } => {
                            assert_eq!(column, "status");
                            assert!(filter.is_some());
                        }
                        _ => panic!("expected IndexScan"),
                    },
                    _ => panic!("expected ReadRecord"),
                },
                _ => panic!("expected Sort"),
            },
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    // ── Multi-field indexed sort ────────────────────────────────

    #[test]
    fn plan_multi_sort_indexed_first_uses_indexed_sort() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            sort: vec![
                Sort {
                    field: "score".into(),
                    direction: SortDirection::Desc,
                },
                Sort {
                    field: "name".into(),
                    direction: SortDirection::Asc,
                },
            ],
            take: Some(200),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::Sort { sorts, input } => {
                    assert_eq!(sorts.len(), 2);
                    assert_eq!(sorts[0].field, "score");
                    assert_eq!(sorts[0].direction, SortDirection::Desc);
                    assert_eq!(sorts[1].field, "name");
                    match *input {
                        PlanNode::ReadRecord { input } => match *input {
                            PlanNode::IndexScan {
                                column,
                                filter,
                                direction,
                                limit,
                                complete_groups,
                                ..
                            } => {
                                assert_eq!(column, "score");
                                assert_eq!(filter, None);
                                assert_eq!(direction, SortDirection::Desc);
                                assert_eq!(limit, Some(200));
                                assert!(complete_groups);
                            }
                            _ => panic!("expected IndexScan"),
                        },
                        _ => panic!("expected ReadRecord"),
                    }
                }
                _ => panic!("expected Sort"),
            },
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    #[test]
    fn plan_multi_sort_indexed_first_with_skip() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            sort: vec![
                Sort {
                    field: "score".into(),
                    direction: SortDirection::Desc,
                },
                Sort {
                    field: "name".into(),
                    direction: SortDirection::Asc,
                },
            ],
            skip: Some(50),
            take: Some(200),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::Sort { input, .. } => match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan {
                            limit,
                            complete_groups,
                            ..
                        } => {
                            assert_eq!(limit, Some(250));
                            assert!(complete_groups);
                        }
                        _ => panic!("expected IndexScan"),
                    },
                    _ => panic!("expected ReadRecord"),
                },
                _ => panic!("expected Sort"),
            },
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    #[test]
    fn plan_multi_sort_first_not_indexed_keeps_sort() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            sort: vec![
                Sort {
                    field: "name".into(),
                    direction: SortDirection::Asc,
                },
                Sort {
                    field: "score".into(),
                    direction: SortDirection::Desc,
                },
            ],
            take: Some(200),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => {
                assert!(matches!(*input, PlanNode::Sort { .. }));
            }
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    #[test]
    fn plan_multi_sort_no_limit_keeps_sort() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            sort: vec![
                Sort {
                    field: "score".into(),
                    direction: SortDirection::Desc,
                },
                Sort {
                    field: "name".into(),
                    direction: SortDirection::Asc,
                },
            ],
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        assert!(matches!(inner, PlanNode::Sort { .. }));
    }

    #[test]
    fn plan_multi_sort_with_filter_uses_indexed_sort() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "age": { "$gt": 18_i64 } }),
            sort: vec![
                Sort {
                    field: "score".into(),
                    direction: SortDirection::Desc,
                },
                Sort {
                    field: "name".into(),
                    direction: SortDirection::Asc,
                },
            ],
            take: Some(200),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::Sort { input, .. } => match *input {
                    PlanNode::Filter { input, .. } => match *input {
                        PlanNode::ReadRecord { input } => match *input {
                            PlanNode::IndexScan {
                                column,
                                limit,
                                complete_groups,
                                ..
                            } => {
                                assert_eq!(column, "score");
                                assert_eq!(limit, None);
                                assert!(complete_groups);
                            }
                            _ => panic!("expected IndexScan"),
                        },
                        _ => panic!("expected ReadRecord"),
                    },
                    _ => panic!("expected Filter"),
                },
                _ => panic!("expected Sort"),
            },
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    #[test]
    fn plan_multi_sort_with_indexed_filter_keeps_sort() {
        let indexed = vec!["status".to_string(), "score".to_string()];
        let q = Query {
            filter: Some(eq_raw("status", Bson::String("active".into()))),
            sort: vec![
                Sort {
                    field: "score".into(),
                    direction: SortDirection::Desc,
                },
                Sort {
                    field: "name".into(),
                    direction: SortDirection::Asc,
                },
            ],
            take: Some(200),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::Sort { input, .. } => match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan { column, .. } => {
                            assert_eq!(column, "status");
                        }
                        _ => panic!("expected IndexScan"),
                    },
                    _ => panic!("expected ReadRecord"),
                },
                _ => panic!("expected Sort"),
            },
            _ => panic!("expected Limit, got {:?}", inner),
        }
    }

    // ── Distinct plan tests ─────────────────────────────────────

    #[test]
    fn plan_distinct_no_filter() {
        let q = slate_query::DistinctQuery {
            field: "status".into(),
            filter: None,
            sort: None,
            skip: None,
            take: None,
        };
        let stmt = Statement::Distinct(q);
        let prepared = prep(vec![], stmt);
        let p = plan(&prepared).unwrap();
        match p {
            PlanNode::Distinct { field, input } => {
                assert_eq!(field, "status");
                match *input {
                    PlanNode::Projection { columns, input } => {
                        assert_eq!(columns, Some(vec!["status".to_string()]));
                        assert!(matches!(*input, PlanNode::Scan));
                    }
                    _ => panic!("expected Projection"),
                }
            }
            _ => panic!("expected Distinct"),
        }
    }

    #[test]
    fn plan_distinct_with_filter_and_sort() {
        let q = slate_query::DistinctQuery {
            field: "status".into(),
            filter: Some(eq_raw("priority", Bson::String("high".into()))),
            sort: Some(SortDirection::Asc),
            skip: None,
            take: None,
        };
        let stmt = Statement::Distinct(q);
        let prepared = prep(vec![], stmt);
        let p = plan(&prepared).unwrap();
        match p {
            PlanNode::Sort { sorts, input } => {
                assert_eq!(sorts.len(), 1);
                assert_eq!(sorts[0].field, "status");
                assert_eq!(sorts[0].direction, SortDirection::Asc);
                match *input {
                    PlanNode::Distinct { field, input } => {
                        assert_eq!(field, "status");
                        match *input {
                            PlanNode::Projection { columns, input } => {
                                assert_eq!(columns, Some(vec!["status".to_string()]));
                                match *input {
                                    PlanNode::Filter { input, .. } => {
                                        assert!(matches!(*input, PlanNode::Scan));
                                    }
                                    _ => panic!("expected Filter"),
                                }
                            }
                            _ => panic!("expected Projection"),
                        }
                    }
                    _ => panic!("expected Distinct"),
                }
            }
            _ => panic!("expected Sort, got {:?}", p),
        }
    }

    #[test]
    fn plan_distinct_with_indexed_filter() {
        let q = slate_query::DistinctQuery {
            field: "status".into(),
            filter: Some(eq_raw("priority", Bson::String("high".into()))),
            sort: None,
            skip: None,
            take: None,
        };
        let indexed = vec!["priority".to_string()];
        let stmt = Statement::Distinct(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        match p {
            PlanNode::Distinct { input, .. } => match *input {
                PlanNode::Projection { input, .. } => match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan { column, filter, .. } => {
                            assert_eq!(column, "priority");
                            assert_eq!(filter, Some(IndexFilter::Eq(RawBsonRef::String("high"))));
                        }
                        _ => panic!("expected IndexScan"),
                    },
                    _ => panic!("expected ReadRecord"),
                },
                _ => panic!("expected Projection"),
            },
            _ => panic!("expected Distinct"),
        }
    }

    // ── Range scan planner tests ────────────────────────────────

    #[test]
    fn plan_single_gt_on_indexed_field() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "score": { "$gt": 50_i64 } }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::ReadRecord { input } => match *input {
                PlanNode::IndexScan { column, filter, .. } => {
                    assert_eq!(column, "score");
                    assert_eq!(filter, Some(IndexFilter::Gt(RawBsonRef::Int64(50))));
                }
                _ => panic!("expected IndexScan"),
            },
            _ => panic!("expected ReadRecord, got {:?}", inner),
        }
    }

    #[test]
    fn plan_single_lt_on_indexed_field() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "score": { "$lt": 90_i64 } }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::ReadRecord { input } => match *input {
                PlanNode::IndexScan { column, filter, .. } => {
                    assert_eq!(column, "score");
                    assert_eq!(filter, Some(IndexFilter::Lt(RawBsonRef::Int64(90))));
                }
                _ => panic!("expected IndexScan"),
            },
            _ => panic!("expected ReadRecord, got {:?}", inner),
        }
    }

    #[test]
    fn plan_dual_range_on_indexed_field() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "score": { "$gte": 50_i64, "$lte": 90_i64 } }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::ReadRecord { input } => match *input {
                PlanNode::IndexScan { column, filter, .. } => {
                    assert_eq!(column, "score");
                    assert_eq!(
                        filter,
                        Some(IndexFilter::Range {
                            lower: IndexBound {
                                value: RawBsonRef::Int64(50),
                                inclusive: true,
                            },
                            upper: IndexBound {
                                value: RawBsonRef::Int64(90),
                                inclusive: true,
                            },
                        })
                    );
                }
                _ => panic!("expected IndexScan"),
            },
            _ => panic!("expected ReadRecord, got {:?}", inner),
        }
    }

    #[test]
    fn plan_range_plus_residual() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "score": { "$gt": 50_i64 }, "name": "Alice" }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { predicate, input } => {
                assert!(residual_has_field(&predicate, "name"));
                match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan { column, filter, .. } => {
                            assert_eq!(column, "score");
                            assert_eq!(filter, Some(IndexFilter::Gt(RawBsonRef::Int64(50))));
                        }
                        _ => panic!("expected IndexScan"),
                    },
                    _ => panic!("expected ReadRecord"),
                }
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_eq_preferred_over_range() {
        let indexed = vec!["status".to_string(), "score".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "score": { "$gt": 50_i64 }, "status": "active" }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { predicate, input } => {
                assert!(residual_has_field(&predicate, "score"));
                match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan { column, filter, .. } => {
                            assert_eq!(column, "status");
                            assert_eq!(filter, Some(IndexFilter::Eq(RawBsonRef::String("active"))));
                        }
                        _ => panic!("expected IndexScan"),
                    },
                    _ => panic!("expected ReadRecord"),
                }
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_range_on_non_indexed_field_falls_back_to_scan() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(rawdoc! { "score": { "$gt": 50_i64 } }),
            ..empty_query()
        };
        let stmt = Statement::Find(q);
        let prepared = prep(indexed, stmt);
        let p = plan(&prepared).unwrap();
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(*input, PlanNode::Scan));
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }
}
