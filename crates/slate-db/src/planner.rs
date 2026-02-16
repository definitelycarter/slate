use slate_query::{FilterGroup, FilterNode, LogicalOp, Operator, Query, Sort};

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    /// Full scan — yields all records in a partition.
    /// `columns` limits materialization to only these top-level keys (None = all).
    Scan {
        partition: String,
        columns: Option<Vec<String>>,
    },

    /// Index lookup — yields record IDs matching a value from the index.
    IndexScan {
        partition: String,
        column: String,
        value: bson::Bson,
        columns: Option<Vec<String>>,
    },

    /// Read filter columns per record, evaluate predicate, skip non-matching.
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

    /// Read remaining columns, strip unneeded.
    Projection {
        columns: Vec<String>,
        input: Box<PlanNode>,
    },
}

/// Build a query plan from a collection partition and its indexed fields.
///
/// The planner analyzes the filter to determine if an index can be used.
/// For top-level AND groups, it looks for `Eq` conditions on indexed columns
/// and rewrites them as `IndexScan`, leaving remaining conditions as a
/// residual `Filter` node.
pub fn plan(partition: &str, indexed_fields: &[String], query: &Query) -> PlanNode {
    // Compute the set of top-level keys the scan must materialize.
    let scan_columns = compute_scan_columns(query);

    // Step 1: Try to extract an indexed Eq condition from the filter
    let (input, residual_filter) = match &query.filter {
        Some(group) => {
            match try_index_scan(partition, indexed_fields, group, scan_columns.clone()) {
                Some((index_node, residual)) => (index_node, residual),
                None => (
                    PlanNode::Scan {
                        partition: partition.to_string(),
                        columns: scan_columns,
                    },
                    Some(group.clone()),
                ),
            }
        }
        None => (
            PlanNode::Scan {
                partition: partition.to_string(),
                columns: scan_columns,
            },
            None,
        ),
    };

    // Step 2: Wrap with residual filter if any conditions remain
    let node = match residual_filter {
        Some(group) if !group.children.is_empty() => PlanNode::Filter {
            predicate: group,
            input: Box::new(input),
        },
        _ => input,
    };

    // Step 3: Sort
    let node = if !query.sort.is_empty() {
        PlanNode::Sort {
            sorts: query.sort.clone(),
            input: Box::new(node),
        }
    } else {
        node
    };

    // Step 4: Limit
    let node = if query.skip.is_some() || query.take.is_some() {
        PlanNode::Limit {
            skip: query.skip.unwrap_or(0),
            take: query.take,
            input: Box::new(node),
        }
    } else {
        node
    };

    // Step 5: Projection
    let node = match &query.columns {
        Some(cols) => PlanNode::Projection {
            columns: cols.clone(),
            input: Box::new(node),
        },
        None => node,
    };

    node
}

/// Compute the top-level keys that scan nodes must materialize.
/// Returns None if no projection is specified (materialize everything).
fn compute_scan_columns(query: &Query) -> Option<Vec<String>> {
    let proj = query.columns.as_ref()?;

    let mut keys: Vec<String> = Vec::new();

    // Projection columns (top-level key for dot paths)
    for col in proj {
        let top = top_level_key(col);
        if !keys.contains(&top.to_string()) {
            keys.push(top.to_string());
        }
    }

    // Filter columns
    if let Some(group) = &query.filter {
        collect_filter_columns(group, &mut keys);
    }

    // Sort columns
    for sort in &query.sort {
        let top = top_level_key(&sort.field);
        if !keys.contains(&top.to_string()) {
            keys.push(top.to_string());
        }
    }

    Some(keys)
}

/// Extract top-level key from a potentially dotted path.
fn top_level_key(path: &str) -> &str {
    path.split('.').next().unwrap_or(path)
}

/// Collect all field names referenced in a filter group (as top-level keys).
fn collect_filter_columns(group: &FilterGroup, out: &mut Vec<String>) {
    for child in &group.children {
        match child {
            FilterNode::Condition(filter) => {
                let top = top_level_key(&filter.field).to_string();
                if !out.contains(&top) {
                    out.push(top);
                }
            }
            FilterNode::Group(sub) => collect_filter_columns(sub, out),
        }
    }
}

/// Try to extract an indexed Eq condition from a top-level AND group.
fn try_index_scan(
    partition: &str,
    indexed_fields: &[String],
    group: &FilterGroup,
    scan_columns: Option<Vec<String>>,
) -> Option<(PlanNode, Option<FilterGroup>)> {
    if group.logical != LogicalOp::And {
        return None;
    }

    // Find the first Eq condition on an indexed column
    let mut index_pos = None;
    for (i, child) in group.children.iter().enumerate() {
        if let FilterNode::Condition(filter) = child {
            if filter.operator == Operator::Eq && indexed_fields.iter().any(|f| f == &filter.field)
            {
                index_pos = Some(i);
                break;
            }
        }
    }

    let pos = index_pos?;
    let filter = match &group.children[pos] {
        FilterNode::Condition(f) => f,
        _ => unreachable!(),
    };

    let index_node = PlanNode::IndexScan {
        partition: partition.to_string(),
        column: filter.field.clone(),
        value: query_value_to_bson(&filter.value),
        columns: scan_columns,
    };

    // Build residual: all children except the one we consumed
    let remaining: Vec<FilterNode> = group
        .children
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != pos)
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

    Some((index_node, residual))
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

    #[test]
    fn plan_no_filter() {
        let p = plan("p1", &[], &empty_query());
        assert_eq!(
            p,
            PlanNode::Scan {
                partition: "p1".into(),
                columns: None,
            }
        );
    }

    #[test]
    fn plan_with_filter_no_index() {
        let q = Query {
            filter: Some(eq_filter("status", QueryValue::String("active".into()))),
            ..empty_query()
        };
        let p = plan("p1", &[], &q);
        assert!(
            matches!(p, PlanNode::Filter { input, .. } if matches!(*input, PlanNode::Scan { .. }))
        );
    }

    #[test]
    fn plan_with_indexed_eq_filter() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(eq_filter("status", QueryValue::String("active".into()))),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        assert_eq!(
            p,
            PlanNode::IndexScan {
                partition: "p1".into(),
                column: "status".into(),
                value: Bson::String("active".into()),
                columns: None,
            }
        );
    }

    #[test]
    fn plan_indexed_eq_plus_residual() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    FilterNode::Condition(Filter {
                        field: "status".into(),
                        operator: Operator::Eq,
                        value: QueryValue::String("active".into()),
                    }),
                    FilterNode::Condition(Filter {
                        field: "score".into(),
                        operator: Operator::Gt,
                        value: QueryValue::Int(50),
                    }),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        match p {
            PlanNode::Filter { predicate, input } => {
                assert!(matches!(*input, PlanNode::IndexScan { .. }));
                assert_eq!(predicate.children.len(), 1);
                match &predicate.children[0] {
                    FilterNode::Condition(f) => assert_eq!(f.field, "score"),
                    _ => panic!("expected condition"),
                }
            }
            _ => panic!("expected Filter node, got {:?}", p),
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
        assert!(
            matches!(p, PlanNode::Sort { input, .. } if matches!(*input, PlanNode::Scan { .. }))
        );
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
            _ => panic!("expected Sort node"),
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
            PlanNode::Limit {
                skip, take, input, ..
            } => {
                assert_eq!(skip, 10);
                assert_eq!(take, Some(5));
                assert!(matches!(*input, PlanNode::Scan { .. }));
            }
            _ => panic!("expected Limit node"),
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
                assert!(matches!(*input, PlanNode::Scan { .. }));
            }
            _ => panic!("expected Projection node"),
        }
    }

    #[test]
    fn plan_full_query() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![
                    FilterNode::Condition(Filter {
                        field: "status".into(),
                        operator: Operator::Eq,
                        value: QueryValue::String("active".into()),
                    }),
                    FilterNode::Condition(Filter {
                        field: "score".into(),
                        operator: Operator::Gt,
                        value: QueryValue::Int(50),
                    }),
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
        // Projection(Limit(Sort(Filter(IndexScan))))
        match p {
            PlanNode::Projection { input, .. } => match *input {
                PlanNode::Limit { input, .. } => match *input {
                    PlanNode::Sort { input, .. } => match *input {
                        PlanNode::Filter { input, .. } => {
                            assert!(matches!(*input, PlanNode::IndexScan { .. }));
                        }
                        _ => panic!("expected Filter"),
                    },
                    _ => panic!("expected Sort"),
                },
                _ => panic!("expected Limit"),
            },
            _ => panic!("expected Projection"),
        }
    }

    #[test]
    fn plan_or_filter_with_index() {
        let indexed = vec!["status".to_string()];
        let q = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::Or,
                children: vec![
                    FilterNode::Condition(Filter {
                        field: "status".into(),
                        operator: Operator::Eq,
                        value: QueryValue::String("active".into()),
                    }),
                    FilterNode::Condition(Filter {
                        field: "name".into(),
                        operator: Operator::Eq,
                        value: QueryValue::String("test".into()),
                    }),
                ],
            }),
            ..empty_query()
        };
        let p = plan("p1", &indexed, &q);
        // OR at top level can't use index — falls back to Filter(Scan)
        assert!(
            matches!(p, PlanNode::Filter { input, .. } if matches!(*input, PlanNode::Scan { .. }))
        );
    }
}
