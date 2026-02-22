use slate_query::{
    DistinctQuery, FilterGroup, FilterNode, LogicalOp, Operator, Query, Sort, SortDirection,
};

/// Represents a database operation to be planned.
#[derive(Debug, Clone)]
pub enum Statement {
    /// Find documents matching a query (also used for count).
    Find(Query),
    /// Return distinct values for a field.
    Distinct(DistinctQuery),
    /// Update documents matching a filter (merge semantics).
    Update {
        filter: FilterGroup,
        update: bson::Document,
        limit: Option<usize>,
    },
    /// Replace the first document matching a filter entirely.
    Replace {
        filter: FilterGroup,
        replacement: bson::Document,
    },
    /// Delete documents matching a filter.
    Delete {
        filter: FilterGroup,
        limit: Option<usize>,
    },
    /// Insert documents from caller-supplied values.
    Insert { docs: Vec<bson::RawDocumentBuf> },
    /// Upsert (insert-or-replace) a batch of documents by `_id`.
    UpsertMany { docs: Vec<bson::RawDocumentBuf> },
    /// Merge (insert-or-patch) a batch of partial documents by `_id`.
    MergeMany { docs: Vec<bson::RawDocumentBuf> },
}

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
    Scan { collection: String },

    /// Index scan — yields record IDs from an index.
    /// `value: Some(v)` filters to entries matching `v` (Eq lookup).
    /// `value: None` scans the entire column index (ordered scan).
    /// `direction` controls iteration order (Asc = forward, Desc = reverse).
    /// `limit` caps the number of index entries to read (pushed down from Limit).
    /// `complete_groups: true` reads past the limit to finish the last value group,
    /// ensuring complete groups for downstream sub-sorting by secondary fields.
    IndexScan {
        collection: String,
        column: String,
        value: Option<bson::Bson>,
        direction: SortDirection,
        limit: Option<usize>,
        complete_groups: bool,
    },

    /// Combines ID sets from two child nodes using AND (intersect) or OR (union).
    IndexMerge {
        logical: LogicalOp,
        lhs: Box<PlanNode>,
        rhs: Box<PlanNode>,
    },

    /// Fetch raw records by ID. The boundary between the ID tier (below)
    /// and the document tier (above). Yields raw BSON bytes, not materialized documents.
    ReadRecord { input: Box<PlanNode> },

    /// Evaluate predicate against documents, skip non-matching.
    Filter {
        predicate: FilterGroup,
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

    /// Project fields and inject `_id`. Always emitted.
    /// `columns: None` copies all fields; `columns: Some(cols)` copies only named fields.
    Projection {
        columns: Option<Vec<String>>,
        input: Box<PlanNode>,
    },

    /// Extract unique values from a field across all matching records.
    /// Receives documents from Projection, extracts the field, deduplicates.
    /// Emits `(None, Some(scalar))`.
    Distinct { field: String, input: Box<PlanNode> },

    // ── Mutation nodes ──────────────────────────────────────────
    //
    // These form composable pipelines for streaming mutations.
    // Each node receives (id, doc) from below, performs a side effect,
    // and passes through (or transforms) the tuple.
    /// Delete index entries for each (id, doc) flowing through.
    /// Side effect: deletes index keys + TTL index for the record.
    /// Passes (id, doc) through unchanged.
    DeleteIndex {
        indexed_fields: Vec<String>,
        input: Box<PlanNode>,
    },

    /// Delete the record itself.
    /// Side effect: deletes the record key from the CF.
    /// Yields (id, None) — the doc is consumed.
    Delete { input: Box<PlanNode> },

    /// Merge update fields into the existing document.
    /// Side effect: writes the merged document to the CF.
    /// Yields (id, Some(new_doc)) if changed, (id, None) if unchanged.
    Update {
        update: bson::Document,
        input: Box<PlanNode>,
    },

    /// Replace the entire document with a new one.
    /// Side effect: writes the replacement document to the CF.
    /// Yields (id, Some(replacement)).
    Replace {
        replacement: bson::Document,
        input: Box<PlanNode>,
    },

    /// Insert index entries for each (id, doc) flowing through.
    /// Side effect: writes index keys + TTL index for the record.
    /// Passes (id, doc) through unchanged.
    InsertIndex {
        indexed_fields: Vec<String>,
        input: Box<PlanNode>,
    },

    /// Insert a record into the store.
    /// Side effect: checks for duplicate key, writes the record (without `_id` in value).
    /// Yields (id, Some(doc)) for each inserted record.
    InsertRecord { input: Box<PlanNode> },

    /// Upsert: for each doc from input, look up by `_id`, clean old indexes,
    /// write (replace or merge), and emit the written doc for InsertIndex.
    /// Handles DeleteIndex behavior internally to avoid pipeline ordering issues.
    Upsert {
        mode: UpsertMode,
        indexed_fields: Vec<String>,
        input: Box<PlanNode>,
    },

    /// Caller-provided documents. Each document must contain an `_id` field.
    /// Source node for insert/upsert pipelines and executor tests.
    Values { docs: Vec<bson::RawDocumentBuf> },
}

/// Build a plan from a statement, collection name, and indexed fields.
///
/// Single entry point for all plan generation. Dispatches to specialized
/// builders based on the statement variant.
pub fn plan(collection: &str, indexed_fields: Vec<String>, statement: Statement) -> PlanNode {
    match statement {
        Statement::Find(query) => plan_find(collection, &indexed_fields, &query),
        Statement::Distinct(query) => plan_distinct(collection, &indexed_fields, &query),
        Statement::Update {
            filter,
            update,
            limit,
        } => plan_update(collection, indexed_fields, &filter, update, limit),
        Statement::Replace {
            filter,
            replacement,
        } => plan_replace(collection, indexed_fields, &filter, replacement),
        Statement::Delete { filter, limit } => {
            plan_delete(collection, indexed_fields, &filter, limit)
        }
        Statement::Insert { docs } => plan_insert(indexed_fields, docs),
        Statement::UpsertMany { docs } => plan_upsert(indexed_fields, docs, UpsertMode::Replace),
        Statement::MergeMany { docs } => plan_upsert(indexed_fields, docs, UpsertMode::Merge),
    }
}

fn plan_find(collection: &str, indexed_fields: &[String], query: &Query) -> PlanNode {
    // Step 1: Plan the filter — split into ID-tier node + residual document-tier predicate
    let (id_node, residual_filter) = match &query.filter {
        Some(group) => {
            let (node, residual) = plan_filter(collection, indexed_fields, group);
            (node, residual)
        }
        None => (
            PlanNode::Scan {
                collection: collection.to_string(),
            },
            None,
        ),
    };

    // Capture before id_node is consumed — needed for indexed sort optimization in Step 4.
    let id_is_scan = matches!(id_node, PlanNode::Scan { .. });
    let has_residual_filter = residual_filter.is_some();

    // Step 2: ReadRecord — always fetch full documents.
    // TODO: Add CoverProject node to skip ReadRecord when the index covers the projection.
    let node = PlanNode::ReadRecord {
        input: Box::new(id_node),
    };

    // Step 3: Wrap with residual filter if any conditions remain
    let node = match residual_filter {
        Some(group) if !group.children.is_empty() => PlanNode::Filter {
            predicate: group,
            input: Box::new(node),
        },
        _ => node,
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
            collection,
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
            collection,
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

    // Step 6: Projection — always emitted, injects _id
    let node = PlanNode::Projection {
        columns: query.columns.clone(),
        input: Box::new(node),
    };

    node
}

/// Build a plan for a distinct query, reusing the filter planning logic.
///
/// Plan shape:
/// - No sort: `Distinct → Projection([field]) → Filter → ReadRecord → Scan`
/// - With sort: `Limit → Sort → Distinct → Projection([field]) → ...`
///
/// Projection extracts the distinct field; Distinct deduplicates;
/// Sort/Limit are standard pipeline nodes on top.
fn plan_distinct(collection: &str, indexed_fields: &[String], query: &DistinctQuery) -> PlanNode {
    // Step 1: Plan the filter — same as find
    let (id_node, residual_filter) = match &query.filter {
        Some(group) => plan_filter(collection, indexed_fields, group),
        None => (
            PlanNode::Scan {
                collection: collection.to_string(),
            },
            None,
        ),
    };

    // Step 2: ReadRecord
    let node = PlanNode::ReadRecord {
        input: Box::new(id_node),
    };

    // Step 3: Wrap with residual filter if any
    let node = match residual_filter {
        Some(group) if !group.children.is_empty() => PlanNode::Filter {
            predicate: group,
            input: Box::new(node),
        },
        _ => node,
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

    node
}

/// Replace the Scan node inside a plan subtree with an ordered IndexScan.
/// Handles both `ReadRecord(Scan)` and `Filter(ReadRecord(Scan))`.
fn replace_scan_with_index_order(
    node: PlanNode,
    collection: &str,
    sort_field: &str,
    direction: SortDirection,
    limit: Option<usize>,
    complete_groups: bool,
) -> PlanNode {
    match node {
        PlanNode::ReadRecord { input } if matches!(*input, PlanNode::Scan { .. }) => {
            PlanNode::ReadRecord {
                input: Box::new(PlanNode::IndexScan {
                    collection: collection.to_string(),
                    column: sort_field.to_string(),
                    value: None,
                    direction,
                    limit,
                    complete_groups,
                }),
            }
        }
        PlanNode::Filter { predicate, input } => PlanNode::Filter {
            predicate,
            input: Box::new(replace_scan_with_index_order(
                *input,
                collection,
                sort_field,
                direction,
                limit,
                complete_groups,
            )),
        },
        other => other,
    }
}

/// Plan a filter group, returning an ID-tier node and an optional residual predicate.
///
/// The residual predicate contains conditions that couldn't be pushed into the ID tier
/// and must be evaluated against materialized documents.
fn plan_filter(
    collection: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
) -> (PlanNode, Option<FilterGroup>) {
    match group.logical {
        LogicalOp::And => plan_and_group(collection, indexed_fields, group),
        LogicalOp::Or => plan_or_group(collection, indexed_fields, group),
    }
}

/// Plan an AND group.
///
/// Strategy: iterate indexed_fields in priority order, pick the first Eq condition
/// that matches. If an AND child is a fully-indexable OR group, it can also be
/// selected. All non-selected children become residual filter.
fn plan_and_group(
    collection: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
) -> (PlanNode, Option<FilterGroup>) {
    // Try to find the best ID-tier node from the AND children.
    // First, check for direct Eq conditions on indexed fields (in priority order).
    // Then, check for fully-indexable OR sub-groups.
    let best = find_best_and_child(collection, indexed_fields, group);

    match best {
        Some((id_node, consumed_index)) => {
            // Build residual: all children except the consumed one
            let remaining: Vec<FilterNode> = group
                .children
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != consumed_index)
                .map(|(_, c)| c.clone())
                .collect();

            let residual = if remaining.is_empty() {
                None
            } else {
                Some(FilterGroup {
                    logical: LogicalOp::And,
                    children: remaining,
                })
            };

            (id_node, residual)
        }
        None => {
            // No indexed condition found — full scan, entire group is residual
            (
                PlanNode::Scan {
                    collection: collection.to_string(),
                },
                Some(group.clone()),
            )
        }
    }
}

/// Find the best AND child to push into the ID tier.
///
/// Iterates indexed_fields in priority order. For each field, checks:
/// 1. Is there a direct Eq condition on this field? → IndexScan
/// 2. Is there a fully-indexable OR sub-group that uses this field? → IndexMerge(Or)
///
/// Returns the ID-tier node and the index of the consumed child.
fn find_best_and_child(
    collection: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
) -> Option<(PlanNode, usize)> {
    // Priority pass: iterate indexed fields in order, find first matching Eq condition
    for field in indexed_fields {
        for (i, child) in group.children.iter().enumerate() {
            if let FilterNode::Condition(filter) = child {
                if filter.operator == Operator::Eq && &filter.field == field {
                    let node = PlanNode::IndexScan {
                        collection: collection.to_string(),
                        column: filter.field.clone(),
                        value: Some(filter.value.clone()),
                        direction: SortDirection::Asc,
                        limit: None,
                        complete_groups: false,
                    };
                    return Some((node, i));
                }
            }
        }
    }

    // Second pass: check for fully-indexable OR sub-groups
    for (i, child) in group.children.iter().enumerate() {
        if let FilterNode::Group(sub_group) = child {
            if sub_group.logical == LogicalOp::Or {
                if let Some(id_node) = try_or_index_merge(collection, indexed_fields, sub_group) {
                    return Some((id_node, i));
                }
            }
        }
    }

    None
}

/// Plan an OR group.
///
/// Strategy: for each child, try to produce an IndexScan (or recurse for nested groups).
/// If every child produces an ID-tier node, combine with IndexMerge(Or).
/// If any child has zero indexed conditions, fall back to Scan with full predicate.
///
/// The full original OR group always becomes the residual filter (recheck),
/// because each IndexScan branch may over-fetch.
fn plan_or_group(
    collection: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
) -> (PlanNode, Option<FilterGroup>) {
    match try_or_index_merge(collection, indexed_fields, group) {
        Some(id_node) => {
            // All branches indexed — use IndexMerge(Or), full group is residual recheck
            (id_node, Some(group.clone()))
        }
        None => {
            // Can't fully index the OR — fall back to Scan
            (
                PlanNode::Scan {
                    collection: collection.to_string(),
                },
                Some(group.clone()),
            )
        }
    }
}

/// Try to build an IndexMerge(Or) from an OR group.
///
/// Returns Some(id_node) if every child can produce an ID-tier node.
/// Returns None if any child has zero indexed conditions.
fn try_or_index_merge(
    collection: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
) -> Option<PlanNode> {
    let mut id_nodes: Vec<PlanNode> = Vec::new();

    for child in &group.children {
        match child {
            FilterNode::Condition(filter) => {
                if filter.operator == Operator::Eq
                    && indexed_fields.iter().any(|f| f == &filter.field)
                {
                    id_nodes.push(PlanNode::IndexScan {
                        collection: collection.to_string(),
                        column: filter.field.clone(),
                        value: Some(filter.value.clone()),
                        direction: SortDirection::Asc,
                        limit: None,
                        complete_groups: false,
                    });
                } else {
                    // Non-indexed condition in OR — can't use indexes for this OR
                    return None;
                }
            }
            FilterNode::Group(sub_group) => {
                // Recurse: try to get an ID-tier node from the nested group
                let (id_node, _residual) = plan_filter(collection, indexed_fields, sub_group);
                match &id_node {
                    PlanNode::Scan { .. } => {
                        // Nested group fell back to scan — can't use indexes for this OR
                        return None;
                    }
                    _ => {
                        id_nodes.push(id_node);
                    }
                }
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
fn build_filtered_source(
    collection: &str,
    indexed_fields: &[String],
    filter: &FilterGroup,
    take: Option<usize>,
) -> PlanNode {
    let (id_node, residual_filter) = plan_filter(collection, indexed_fields, filter);

    let node = PlanNode::ReadRecord {
        input: Box::new(id_node),
    };

    let node = match residual_filter {
        Some(group) if !group.children.is_empty() => PlanNode::Filter {
            predicate: group,
            input: Box::new(node),
        },
        _ => node,
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
fn plan_delete(
    collection: &str,
    indexed_fields: Vec<String>,
    filter: &FilterGroup,
    take: Option<usize>,
) -> PlanNode {
    let source = build_filtered_source(collection, &indexed_fields, filter, take);

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
fn plan_update(
    collection: &str,
    indexed_fields: Vec<String>,
    filter: &FilterGroup,
    update: bson::Document,
    take: Option<usize>,
) -> PlanNode {
    let source = build_filtered_source(collection, &indexed_fields, filter, take);

    let node = PlanNode::DeleteIndex {
        indexed_fields: indexed_fields.clone(),
        input: Box::new(source),
    };

    let node = PlanNode::Update {
        update,
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
fn plan_replace(
    collection: &str,
    indexed_fields: Vec<String>,
    filter: &FilterGroup,
    replacement: bson::Document,
) -> PlanNode {
    let source = build_filtered_source(collection, &indexed_fields, filter, Some(1));

    let node = PlanNode::DeleteIndex {
        indexed_fields: indexed_fields.clone(),
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
fn plan_insert(indexed_fields: Vec<String>, docs: Vec<bson::RawDocumentBuf>) -> PlanNode {
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
fn plan_upsert(
    indexed_fields: Vec<String>,
    docs: Vec<bson::RawDocumentBuf>,
    mode: UpsertMode,
) -> PlanNode {
    let node = PlanNode::Values { docs };

    let node = PlanNode::Upsert {
        mode,
        indexed_fields: indexed_fields.clone(),
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
    use bson::Bson;
    use slate_query::{Filter, SortDirection};

    fn empty_query() -> Query {
        Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        }
    }

    fn eq_filter(field: &str, value: Bson) -> FilterGroup {
        FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: field.into(),
                operator: Operator::Eq,
                value,
            })],
        }
    }

    fn eq_condition(field: &str, value: Bson) -> FilterNode {
        FilterNode::Condition(Filter {
            field: field.into(),
            operator: Operator::Eq,
            value,
        })
    }

    fn gt_condition(field: &str, value: Bson) -> FilterNode {
        FilterNode::Condition(Filter {
            field: field.into(),
            operator: Operator::Gt,
            value,
        })
    }

    /// Unwrap the outermost Projection node (always present in plan output).
    /// Returns (columns, inner_node).
    fn unwrap_projection(node: PlanNode) -> (Option<Vec<String>>, PlanNode) {
        match node {
            PlanNode::Projection { columns, input } => (columns, *input),
            other => panic!("expected Projection at top, got {:?}", other),
        }
    }

    // ── Existing tests updated for always-present Projection ────

    #[test]
    fn plan_no_filter() {
        let p = plan("p1", vec![], Statement::Find(empty_query()));
        let (cols, inner) = unwrap_projection(p);
        assert_eq!(cols, None);
        assert!(matches!(
            inner,
            PlanNode::ReadRecord {
                input,
            } if matches!(*input, PlanNode::Scan { .. })
        ));
    }

    #[test]
    fn plan_with_filter_no_index() {
        let q = Query {
            filter: Some(eq_filter("status", Bson::String("active".into()))),
            ..empty_query()
        };
        let p = plan("p1", vec![], Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(*input, PlanNode::ReadRecord { .. }));
            }
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_with_indexed_eq_filter() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(eq_filter("status", Bson::String("active".into()))),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        // ReadRecord(IndexScan) — no residual filter since only condition is indexed
        match inner {
            PlanNode::ReadRecord { input, .. } => {
                assert_eq!(
                    *input,
                    PlanNode::IndexScan {
                        collection: "p1".into(),
                        column: "status".into(),
                        value: Some(Bson::String("active".into())),
                        direction: SortDirection::Asc,
                        limit: None,
                        complete_groups: false,
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
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    eq_condition("status", Bson::String("active".into())),
                    gt_condition("score", Bson::Int64(50)),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        // Filter(score > 50, ReadRecord(IndexScan(status)))
        match inner {
            PlanNode::Filter { predicate, input } => {
                assert_eq!(predicate.children.len(), 1);
                match &predicate.children[0] {
                    FilterNode::Condition(f) => assert_eq!(f.field, "score"),
                    _ => panic!("expected condition"),
                }
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
        let p = plan("p1", vec![], Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Sort { input, .. } => {
                assert!(matches!(*input, PlanNode::ReadRecord { .. }));
            }
            _ => panic!("expected Sort, got {:?}", inner),
        }
    }

    #[test]
    fn plan_with_filter_and_sort() {
        let q = Query {
            filter: Some(eq_filter("status", Bson::String("active".into()))),
            sort: vec![Sort {
                field: "name".into(),
                direction: SortDirection::Asc,
            }],
            ..empty_query()
        };
        let p = plan("p1", vec![], Statement::Find(q));
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
        let p = plan("p1", vec![], Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { skip, take, input } => {
                assert_eq!(skip, 10);
                assert_eq!(take, Some(5));
                assert!(matches!(*input, PlanNode::ReadRecord { .. }));
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
        let p = plan("p1", vec![], Statement::Find(q));
        let (cols, inner) = unwrap_projection(p);
        assert_eq!(cols, Some(vec!["name".to_string(), "status".to_string()]));
        assert!(matches!(inner, PlanNode::ReadRecord { .. }));
    }

    #[test]
    fn plan_full_query() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    eq_condition("status", Bson::String("active".into())),
                    gt_condition("score", Bson::Int64(50)),
                ],
            }),
            sort: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            skip: Some(10),
            take: Some(5),
            columns: Some(vec!["name".into(), "score".into()]),
        };
        let p = plan("p1", indexed, Statement::Find(q));
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
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    eq_condition("status", Bson::String("active".into())),
                    eq_condition("user_id", Bson::String("abc".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        // Filter(status = "active", ReadRecord(IndexScan(user_id = "abc")))
        match inner {
            PlanNode::Filter { predicate, input } => {
                match &predicate.children[0] {
                    FilterNode::Condition(f) => assert_eq!(f.field, "status"),
                    _ => panic!("expected status condition in residual"),
                }
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
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    eq_condition("user_id", Bson::String("abc".into())),
                    eq_condition("status", Bson::String("active".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
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
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    eq_condition("status", Bson::String("active".into())),
                    eq_condition("name", Bson::String("test".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => {
                    assert!(matches!(*input, PlanNode::Scan { .. }));
                }
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_or_same_field() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    eq_condition("status", Bson::String("active".into())),
                    eq_condition("status", Bson::String("archived".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
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
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    eq_condition("user_id", Bson::Int64(1)),
                    eq_condition("user_id", Bson::Int64(2)),
                    eq_condition("user_id", Bson::Int64(3)),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
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
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    eq_condition("user_id", Bson::String("abc".into())),
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::Or,
                        children: vec![
                            eq_condition("status", Bson::String("active".into())),
                            eq_condition("status", Bson::String("archived".into())),
                        ],
                    }),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { predicate, input } => {
                assert_eq!(predicate.children.len(), 1);
                match &predicate.children[0] {
                    FilterNode::Group(g) => assert_eq!(g.logical, LogicalOp::Or),
                    _ => panic!("expected OR group in residual"),
                }
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
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::And,
                        children: vec![
                            eq_condition("user_id", Bson::String("abc".into())),
                            eq_condition("status", Bson::String("active".into())),
                        ],
                    }),
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::And,
                        children: vec![
                            eq_condition("user_id", Bson::String("xyz".into())),
                            eq_condition("status", Bson::String("pending".into())),
                        ],
                    }),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
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
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::And,
                        children: vec![
                            eq_condition("user_id", Bson::String("abc".into())),
                            gt_condition("score", Bson::Int64(50)),
                        ],
                    }),
                    eq_condition("status", Bson::String("active".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
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
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::Or,
                        children: vec![
                            eq_condition("user_id", Bson::String("abc".into())),
                            eq_condition("status", Bson::String("active".into())),
                        ],
                    }),
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::Or,
                        children: vec![
                            gt_condition("count", Bson::Int64(5)),
                            eq_condition("name", Bson::String("foo".into())),
                        ],
                    }),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => {
                    assert!(matches!(*input, PlanNode::Scan { .. }));
                }
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_fully_unindexed() {
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![gt_condition("score", Bson::Int64(50))],
            }),
            ..empty_query()
        };
        let p = plan("p1", vec![], Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => {
                    assert!(matches!(*input, PlanNode::Scan { .. }));
                }
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", inner),
        }
    }

    #[test]
    fn plan_and_selects_or_subgroup_when_no_direct_eq() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::Or,
                        children: vec![
                            eq_condition("status", Bson::String("active".into())),
                            eq_condition("status", Bson::String("archived".into())),
                        ],
                    }),
                    gt_condition("score", Bson::Int64(50)),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Filter { predicate, input } => {
                assert_eq!(predicate.children.len(), 1);
                match &predicate.children[0] {
                    FilterNode::Condition(f) => assert_eq!(f.field, "score"),
                    _ => panic!("expected score condition"),
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
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::ReadRecord { input } => match *input {
                    PlanNode::IndexScan {
                        column,
                        value,
                        direction,
                        limit,
                        ..
                    } => {
                        assert_eq!(column, "score");
                        assert_eq!(value, None);
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
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::ReadRecord { input } => match *input {
                    PlanNode::IndexScan {
                        direction,
                        value,
                        limit,
                        ..
                    } => {
                        assert_eq!(direction, SortDirection::Asc);
                        assert_eq!(value, None);
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
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![gt_condition("name", Bson::String("a".into()))],
            }),
            sort: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            take: Some(5),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::Filter { input, .. } => match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan {
                            column,
                            value,
                            direction,
                            limit,
                            ..
                        } => {
                            assert_eq!(column, "score");
                            assert_eq!(value, None);
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
        let p = plan("p1", indexed, Statement::Find(q));
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
        let p = plan("p1", indexed, Statement::Find(q));
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
            filter: Some(eq_filter("status", Bson::String("active".into()))),
            sort: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            take: Some(5),
            ..empty_query()
        };
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        match inner {
            PlanNode::Limit { input, .. } => match *input {
                PlanNode::Sort { input, .. } => match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan { column, value, .. } => {
                            assert_eq!(column, "status");
                            assert!(value.is_some());
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
        let p = plan("p1", indexed, Statement::Find(q));
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
                                value,
                                direction,
                                limit,
                                complete_groups,
                                ..
                            } => {
                                assert_eq!(column, "score");
                                assert_eq!(value, None);
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
        let p = plan("p1", indexed, Statement::Find(q));
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
        let p = plan("p1", indexed, Statement::Find(q));
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
        let p = plan("p1", indexed, Statement::Find(q));
        let (_, inner) = unwrap_projection(p);
        assert!(matches!(inner, PlanNode::Sort { .. }));
    }

    #[test]
    fn plan_multi_sort_with_filter_uses_indexed_sort() {
        let indexed = vec!["score".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![gt_condition("age", Bson::Int64(18))],
            }),
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
        let p = plan("p1", indexed, Statement::Find(q));
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
            filter: Some(eq_filter("status", Bson::String("active".into()))),
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
        let p = plan("p1", indexed, Statement::Find(q));
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
    // New plan shape: Distinct → Projection([field]) → Filter → ReadRecord → Scan
    // With sort: Sort → Distinct → Projection([field]) → ...

    #[test]
    fn plan_distinct_no_filter() {
        let q = DistinctQuery {
            field: "status".into(),
            filter: None,
            sort: None,
            skip: None,
            take: None,
        };
        let p = plan("col", vec![], Statement::Distinct(q));
        // Distinct → Projection([status]) → ReadRecord → Scan
        match p {
            PlanNode::Distinct { field, input } => {
                assert_eq!(field, "status");
                match *input {
                    PlanNode::Projection { columns, input } => {
                        assert_eq!(columns, Some(vec!["status".to_string()]));
                        match *input {
                            PlanNode::ReadRecord { input } => match *input {
                                PlanNode::Scan { collection } => {
                                    assert_eq!(collection, "col");
                                }
                                _ => panic!("expected Scan"),
                            },
                            _ => panic!("expected ReadRecord"),
                        }
                    }
                    _ => panic!("expected Projection"),
                }
            }
            _ => panic!("expected Distinct"),
        }
    }

    #[test]
    fn plan_distinct_with_filter_and_sort() {
        let q = DistinctQuery {
            field: "status".into(),
            filter: Some(eq_filter("priority", Bson::String("high".into()))),
            sort: Some(SortDirection::Asc),
            skip: None,
            take: None,
        };
        let p = plan("col", vec![], Statement::Distinct(q));
        // Sort → Distinct → Projection([status]) → Filter → ReadRecord → Scan
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
                                    PlanNode::Filter { predicate, input } => {
                                        assert_eq!(predicate.children.len(), 1);
                                        assert!(matches!(*input, PlanNode::ReadRecord { .. }));
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
        let q = DistinctQuery {
            field: "status".into(),
            filter: Some(eq_filter("priority", Bson::String("high".into()))),
            sort: None,
            skip: None,
            take: None,
        };
        let indexed = vec!["priority".to_string()];
        let p = plan("col", indexed, Statement::Distinct(q));
        // Distinct → Projection([status]) → ReadRecord → IndexScan(priority)
        match p {
            PlanNode::Distinct { input, .. } => match *input {
                PlanNode::Projection { input, .. } => match *input {
                    PlanNode::ReadRecord { input } => match *input {
                        PlanNode::IndexScan { column, value, .. } => {
                            assert_eq!(column, "priority");
                            assert_eq!(value, Some(Bson::String("high".into())));
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
}
