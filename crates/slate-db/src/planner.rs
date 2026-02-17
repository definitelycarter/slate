use slate_query::{FilterGroup, FilterNode, LogicalOp, Operator, Query, Sort};

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    /// Full scan — yields record IDs in a partition.
    Scan { partition: String },

    /// Index lookup — yields record IDs matching a value from the index.
    IndexScan {
        partition: String,
        column: String,
        value: bson::Bson,
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

    /// Strip unneeded columns.
    Projection {
        columns: Vec<String>,
        input: Box<PlanNode>,
    },
}

/// Build a query plan from a collection partition and its indexed fields.
///
/// The planner splits the filter into two tiers:
/// - **ID tier** (below ReadRecord): IndexScan, IndexMerge, Scan — produce record IDs
/// - **Document tier** (above ReadRecord): Filter — evaluates predicates on materialized documents
///
/// For AND groups, the highest-priority indexed Eq condition (per `indexed_fields` order)
/// becomes an IndexScan. For OR groups, if every branch has an indexed Eq, they combine
/// into an IndexMerge(Or). Otherwise the OR falls back to Scan.
pub fn plan(partition: &str, indexed_fields: &[String], query: &Query) -> PlanNode {
    // Step 1: Plan the filter — split into ID-tier node + residual document-tier predicate
    let (id_node, residual_filter) = match &query.filter {
        Some(group) => {
            let (node, residual) = plan_filter(partition, indexed_fields, group);
            (node, residual)
        }
        None => (
            PlanNode::Scan {
                partition: partition.to_string(),
            },
            None,
        ),
    };

    // Step 2: ReadRecord — always present, fetches raw records from IDs
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
    let node = if !query.sort.is_empty() {
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

    // Step 6: Projection
    let node = match &query.columns {
        Some(cols) => PlanNode::Projection {
            columns: cols.clone(),
            input: Box::new(node),
        },
        None => node,
    };

    node
}

/// Plan a filter group, returning an ID-tier node and an optional residual predicate.
///
/// The residual predicate contains conditions that couldn't be pushed into the ID tier
/// and must be evaluated against materialized documents.
fn plan_filter(
    partition: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
) -> (PlanNode, Option<FilterGroup>) {
    match group.logical {
        LogicalOp::And => plan_and_group(partition, indexed_fields, group),
        LogicalOp::Or => plan_or_group(partition, indexed_fields, group),
    }
}

/// Plan an AND group.
///
/// Strategy: iterate indexed_fields in priority order, pick the first Eq condition
/// that matches. If an AND child is a fully-indexable OR group, it can also be
/// selected. All non-selected children become residual filter.
fn plan_and_group(
    partition: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
) -> (PlanNode, Option<FilterGroup>) {
    // Try to find the best ID-tier node from the AND children.
    // First, check for direct Eq conditions on indexed fields (in priority order).
    // Then, check for fully-indexable OR sub-groups.
    let best = find_best_and_child(partition, indexed_fields, group);

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
                    partition: partition.to_string(),
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
    partition: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
) -> Option<(PlanNode, usize)> {
    // Priority pass: iterate indexed fields in order, find first matching Eq condition
    for field in indexed_fields {
        for (i, child) in group.children.iter().enumerate() {
            if let FilterNode::Condition(filter) = child {
                if filter.operator == Operator::Eq && &filter.field == field {
                    let node = PlanNode::IndexScan {
                        partition: partition.to_string(),
                        column: filter.field.clone(),
                        value: query_value_to_bson(&filter.value),
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
                if let Some(id_node) = try_or_index_merge(partition, indexed_fields, sub_group) {
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
    partition: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
) -> (PlanNode, Option<FilterGroup>) {
    match try_or_index_merge(partition, indexed_fields, group) {
        Some(id_node) => {
            // All branches indexed — use IndexMerge(Or), full group is residual recheck
            (id_node, Some(group.clone()))
        }
        None => {
            // Can't fully index the OR — fall back to Scan
            (
                PlanNode::Scan {
                    partition: partition.to_string(),
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
    partition: &str,
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
                        partition: partition.to_string(),
                        column: filter.field.clone(),
                        value: query_value_to_bson(&filter.value),
                    });
                } else {
                    // Non-indexed condition in OR — can't use indexes for this OR
                    return None;
                }
            }
            FilterNode::Group(sub_group) => {
                // Recurse: try to get an ID-tier node from the nested group
                let (id_node, _residual) = plan_filter(partition, indexed_fields, sub_group);
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

fn query_value_to_bson(qv: &slate_query::QueryValue) -> bson::Bson {
    match qv {
        slate_query::QueryValue::String(s) => bson::Bson::String(s.clone()),
        slate_query::QueryValue::Int(i) => bson::Bson::Int64(*i),
        slate_query::QueryValue::Float(f) => bson::Bson::Double(*f),
        slate_query::QueryValue::Bool(b) => bson::Bson::Boolean(*b),
        slate_query::QueryValue::Date(d) => {
            bson::Bson::DateTime(bson::DateTime::from_millis(*d * 1000))
        }
        slate_query::QueryValue::Null => bson::Bson::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::Bson;
    use slate_query::{Filter, QueryValue, SortDirection};

    fn empty_query() -> Query {
        Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        }
    }

    fn eq_filter(field: &str, value: QueryValue) -> FilterGroup {
        FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: field.into(),
                operator: Operator::Eq,
                value,
            })],
        }
    }

    fn eq_condition(field: &str, value: QueryValue) -> FilterNode {
        FilterNode::Condition(Filter {
            field: field.into(),
            operator: Operator::Eq,
            value,
        })
    }

    fn gt_condition(field: &str, value: QueryValue) -> FilterNode {
        FilterNode::Condition(Filter {
            field: field.into(),
            operator: Operator::Gt,
            value,
        })
    }

    // ── Existing tests updated for ReadRecord ───────────────────

    #[test]
    fn plan_no_filter() {
        let p = plan("p1", &[], &empty_query());
        assert!(matches!(
            p,
            PlanNode::ReadRecord {
                input,
            } if matches!(*input, PlanNode::Scan { .. })
        ));
    }

    #[test]
    fn plan_with_filter_no_index() {
        let q = Query {
            filter: Some(eq_filter("status", QueryValue::String("active".into()))),
            ..empty_query()
        };
        let p = plan("p1", &[], &q);
        match p {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(*input, PlanNode::ReadRecord { .. }));
            }
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    #[test]
    fn plan_with_indexed_eq_filter() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(eq_filter("status", QueryValue::String("active".into()))),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // ReadRecord(IndexScan) — no residual filter since only condition is indexed
        match p {
            PlanNode::ReadRecord { input, .. } => {
                assert_eq!(
                    *input,
                    PlanNode::IndexScan {
                        partition: "p1".into(),
                        column: "status".into(),
                        value: Bson::String("active".into()),
                    }
                );
            }
            _ => panic!("expected ReadRecord, got {:?}", p),
        }
    }

    #[test]
    fn plan_indexed_eq_plus_residual() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    eq_condition("status", QueryValue::String("active".into())),
                    gt_condition("score", QueryValue::Int(50)),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Filter(score > 50, ReadRecord(IndexScan(status)))
        match p {
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
            _ => panic!("expected Filter, got {:?}", p),
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
        let p = plan("p1", &[], &q);
        match p {
            PlanNode::Sort { input, .. } => {
                assert!(matches!(*input, PlanNode::ReadRecord { .. }));
            }
            _ => panic!("expected Sort, got {:?}", p),
        }
    }

    #[test]
    fn plan_with_filter_and_sort() {
        let q = Query {
            filter: Some(eq_filter("status", QueryValue::String("active".into()))),
            sort: vec![Sort {
                field: "name".into(),
                direction: SortDirection::Asc,
            }],
            ..empty_query()
        };
        let p = plan("p1", &[], &q);
        match p {
            PlanNode::Sort { input, .. } => {
                assert!(matches!(*input, PlanNode::Filter { .. }));
            }
            _ => panic!("expected Sort, got {:?}", p),
        }
    }

    #[test]
    fn plan_with_skip_take() {
        let q = Query {
            skip: Some(10),
            take: Some(5),
            ..empty_query()
        };
        let p = plan("p1", &[], &q);
        match p {
            PlanNode::Limit { skip, take, input } => {
                assert_eq!(skip, 10);
                assert_eq!(take, Some(5));
                assert!(matches!(*input, PlanNode::ReadRecord { .. }));
            }
            _ => panic!("expected Limit, got {:?}", p),
        }
    }

    #[test]
    fn plan_with_projection() {
        let q = Query {
            columns: Some(vec!["name".into(), "status".into()]),
            ..empty_query()
        };
        let p = plan("p1", &[], &q);
        match p {
            PlanNode::Projection { columns, input } => {
                assert_eq!(columns, vec!["name".to_string(), "status".to_string()]);
                assert!(matches!(*input, PlanNode::ReadRecord { .. }));
            }
            _ => panic!("expected Projection, got {:?}", p),
        }
    }

    #[test]
    fn plan_full_query() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    eq_condition("status", QueryValue::String("active".into())),
                    gt_condition("score", QueryValue::Int(50)),
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
        let p = plan("p1", &indexed, &q);
        // Projection(Limit(Sort(Filter(ReadRecord(IndexScan)))))
        match p {
            PlanNode::Projection { input, .. } => match *input {
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
            },
            _ => panic!("expected Projection"),
        }
    }

    // ── AND priority selection ───────────────────────────────────

    #[test]
    fn plan_and_priority_selection() {
        // indexed_fields order: user_id first, status second
        // Filter has status first, user_id second — planner should pick user_id
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    eq_condition("status", QueryValue::String("active".into())),
                    eq_condition("user_id", QueryValue::String("abc".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Filter(status = "active", ReadRecord(IndexScan(user_id = "abc")))
        match p {
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
            _ => panic!("expected Filter, got {:?}", p),
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
                    eq_condition("user_id", QueryValue::String("abc".into())),
                    eq_condition("status", QueryValue::String("active".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Filter(recheck, ReadRecord(IndexMerge(Or, IndexScan, IndexScan)))
        match p {
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
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    #[test]
    fn plan_or_one_not_indexed() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    eq_condition("status", QueryValue::String("active".into())),
                    eq_condition("name", QueryValue::String("test".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // name is not indexed — OR falls back to Filter(ReadRecord(Scan))
        match p {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => {
                    assert!(matches!(*input, PlanNode::Scan { .. }));
                }
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    #[test]
    fn plan_or_same_field() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    eq_condition("status", QueryValue::String("active".into())),
                    eq_condition("status", QueryValue::String("archived".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Filter(recheck, ReadRecord(IndexMerge(Or, IndexScan, IndexScan)))
        match p {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => match *input {
                    PlanNode::IndexMerge { logical, .. } => {
                        assert_eq!(logical, LogicalOp::Or);
                    }
                    _ => panic!("expected IndexMerge"),
                },
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    #[test]
    fn plan_or_three_values() {
        let indexed = vec!["user_id".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    eq_condition("user_id", QueryValue::Int(1)),
                    eq_condition("user_id", QueryValue::Int(2)),
                    eq_condition("user_id", QueryValue::Int(3)),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Filter(recheck, ReadRecord(IndexMerge(Or, IndexMerge(Or, IndexScan, IndexScan), IndexScan)))
        // Left-associative fold: ((1 Or 2) Or 3)
        match p {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => match *input {
                    PlanNode::IndexMerge { logical, lhs, rhs } => {
                        assert_eq!(logical, LogicalOp::Or);
                        // lhs is a nested IndexMerge
                        assert!(matches!(*lhs, PlanNode::IndexMerge { .. }));
                        assert!(matches!(*rhs, PlanNode::IndexScan { .. }));
                    }
                    _ => panic!("expected IndexMerge"),
                },
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    // ── Nested AND/OR ───────────────────────────────────────────

    #[test]
    fn plan_and_with_nested_or_indexed() {
        // user_id = "abc" AND (status = "active" OR status = "archived")
        // Planner picks user_id (higher priority), OR becomes residual
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    eq_condition("user_id", QueryValue::String("abc".into())),
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::Or,
                        children: vec![
                            eq_condition("status", QueryValue::String("active".into())),
                            eq_condition("status", QueryValue::String("archived".into())),
                        ],
                    }),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Filter(status OR, ReadRecord(IndexScan(user_id)))
        match p {
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
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    #[test]
    fn plan_or_with_nested_ands() {
        // (user_id = "abc" AND status = "active") OR (user_id = "xyz" AND status = "pending")
        // Each AND branch picks one index, wrapped in IndexMerge(Or)
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::And,
                        children: vec![
                            eq_condition("user_id", QueryValue::String("abc".into())),
                            eq_condition("status", QueryValue::String("active".into())),
                        ],
                    }),
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::And,
                        children: vec![
                            eq_condition("user_id", QueryValue::String("xyz".into())),
                            eq_condition("status", QueryValue::String("pending".into())),
                        ],
                    }),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Filter(recheck, ReadRecord(IndexMerge(Or, IndexScan(user_id=abc), IndexScan(user_id=xyz))))
        match p {
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
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    #[test]
    fn plan_or_partial_index_per_branch() {
        // (user_id = "abc" AND score > 50) OR status = "active"
        // Each OR branch has one indexed Eq — IndexMerge(Or), full predicate as recheck
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::And,
                        children: vec![
                            eq_condition("user_id", QueryValue::String("abc".into())),
                            gt_condition("score", QueryValue::Int(50)),
                        ],
                    }),
                    eq_condition("status", QueryValue::String("active".into())),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Filter(recheck, ReadRecord(IndexMerge(Or, IndexScan(user_id), IndexScan(status))))
        match p {
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
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    #[test]
    fn plan_or_unindexed_branch_fallback() {
        // (user_id = "abc" OR status = "active") OR (count > 5 OR name = "foo")
        // Second OR branch has zero indexed conditions — entire OR falls back to Scan
        let indexed = vec!["user_id".to_string(), "status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::Or,
                        children: vec![
                            eq_condition("user_id", QueryValue::String("abc".into())),
                            eq_condition("status", QueryValue::String("active".into())),
                        ],
                    }),
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::Or,
                        children: vec![
                            gt_condition("count", QueryValue::Int(5)),
                            eq_condition("name", QueryValue::String("foo".into())),
                        ],
                    }),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Falls back to Filter(ReadRecord(Scan))
        match p {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => {
                    assert!(matches!(*input, PlanNode::Scan { .. }));
                }
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    #[test]
    fn plan_fully_unindexed() {
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![gt_condition("score", QueryValue::Int(50))],
            }),
            ..empty_query()
        };
        let p = plan("p1", &[], &q);
        // Filter(ReadRecord(Scan))
        match p {
            PlanNode::Filter { input, .. } => match *input {
                PlanNode::ReadRecord { input, .. } => {
                    assert!(matches!(*input, PlanNode::Scan { .. }));
                }
                _ => panic!("expected ReadRecord"),
            },
            _ => panic!("expected Filter, got {:?}", p),
        }
    }

    #[test]
    fn plan_and_selects_or_subgroup_when_no_direct_eq() {
        // No direct Eq on indexed fields, but an OR sub-group is fully indexable
        // (status = "active" OR status = "archived") AND score > 50
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    FilterNode::Group(FilterGroup {
                        logical: LogicalOp::Or,
                        children: vec![
                            eq_condition("status", QueryValue::String("active".into())),
                            eq_condition("status", QueryValue::String("archived".into())),
                        ],
                    }),
                    gt_condition("score", QueryValue::Int(50)),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // Filter(score > 50, ReadRecord(IndexMerge(Or, IndexScan, IndexScan)))
        match p {
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
            _ => panic!("expected Filter, got {:?}", p),
        }
    }
}
