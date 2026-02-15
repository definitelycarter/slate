use slate_query::{FilterGroup, FilterNode, LogicalOp, Operator, Query, Sort};

use crate::datasource::Datasource;
use crate::record::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    /// Full scan — yields all record IDs under a prefix.
    Scan { partition: String, prefix: String },

    /// Index lookup — yields record IDs matching a value from the index.
    IndexScan {
        partition: String,
        column: String,
        value: Value,
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

/// Build a query plan from a datasource and query.
///
/// The planner analyzes the filter to determine if an index can be used.
/// For top-level AND groups, it looks for `Eq` conditions on indexed columns
/// and rewrites them as `IndexScan`, leaving remaining conditions as a
/// residual `Filter` node.
pub fn plan(datasource: &Datasource, query: &Query) -> PlanNode {
    let partition = datasource.partition.clone();
    let prefix = format!("{}:", datasource.id);

    // Step 1: Try to extract an indexed Eq condition from the filter
    let (input, residual_filter) = match &query.filter {
        Some(group) => match try_index_scan(datasource, group) {
            Some((index_node, residual)) => (index_node, residual),
            None => (
                PlanNode::Scan {
                    partition: partition.clone(),
                    prefix,
                },
                Some(group.clone()),
            ),
        },
        None => (
            PlanNode::Scan {
                partition: partition.clone(),
                prefix,
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

/// Try to extract an indexed Eq condition from a top-level AND group.
///
/// Returns `Some((IndexScan node, residual filter))` if an indexed Eq
/// condition is found. The residual filter contains the remaining conditions.
///
/// Only works on top-level AND groups — OR groups can't use a single index
/// to narrow the scan.
fn try_index_scan(
    datasource: &Datasource,
    group: &FilterGroup,
) -> Option<(PlanNode, Option<FilterGroup>)> {
    if group.logical != LogicalOp::And {
        return None;
    }

    // Find the first Eq condition on an indexed column
    let mut index_pos = None;
    for (i, child) in group.children.iter().enumerate() {
        if let FilterNode::Condition(filter) = child {
            if filter.operator == Operator::Eq && is_indexed(datasource, &filter.field) {
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
        partition: datasource.partition.clone(),
        column: filter.field.clone(),
        value: query_value_to_value(&filter.value),
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

fn is_indexed(datasource: &Datasource, field: &str) -> bool {
    datasource
        .fields
        .iter()
        .any(|f| f.name == field && f.indexed)
}

fn query_value_to_value(qv: &slate_query::QueryValue) -> Value {
    match qv {
        slate_query::QueryValue::String(s) => Value::String(s.clone()),
        slate_query::QueryValue::Int(i) => Value::Int(*i),
        slate_query::QueryValue::Float(f) => Value::Float(*f),
        slate_query::QueryValue::Bool(b) => Value::Bool(*b),
        slate_query::QueryValue::Date(d) => Value::Date(*d),
        slate_query::QueryValue::Null => Value::Bool(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{FieldDef, FieldType};
    use slate_query::{Filter, QueryValue, SortDirection};

    fn test_datasource(indexed_fields: &[&str]) -> Datasource {
        Datasource {
            id: "ds1".into(),
            name: "Test".into(),
            fields: vec![
                FieldDef {
                    name: "name".into(),
                    field_type: FieldType::String,
                    ttl_seconds: None,
                    indexed: indexed_fields.contains(&"name"),
                },
                FieldDef {
                    name: "status".into(),
                    field_type: FieldType::String,
                    ttl_seconds: None,
                    indexed: indexed_fields.contains(&"status"),
                },
                FieldDef {
                    name: "score".into(),
                    field_type: FieldType::Int,
                    ttl_seconds: None,
                    indexed: indexed_fields.contains(&"score"),
                },
            ],
            partition: "p1".into(),
        }
    }

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
        let ds = test_datasource(&[]);
        let q = empty_query();
        let p = plan(&ds, &q);
        assert_eq!(
            p,
            PlanNode::Scan {
                partition: "p1".into(),
                prefix: "ds1:".into(),
            }
        );
    }

    #[test]
    fn plan_with_filter_no_index() {
        let ds = test_datasource(&[]);
        let q = Query {
            filter: Some(eq_filter("status", QueryValue::String("active".into()))),
            ..empty_query()
        };
        let p = plan(&ds, &q);
        assert!(
            matches!(p, PlanNode::Filter { input, .. } if matches!(*input, PlanNode::Scan { .. }))
        );
    }

    #[test]
    fn plan_with_indexed_eq_filter() {
        let ds = test_datasource(&["status"]);
        let q = Query {
            filter: Some(eq_filter("status", QueryValue::String("active".into()))),
            ..empty_query()
        };
        let p = plan(&ds, &q);
        // Should be IndexScan with no Filter wrapping
        assert_eq!(
            p,
            PlanNode::IndexScan {
                partition: "p1".into(),
                column: "status".into(),
                value: Value::String("active".into()),
            }
        );
    }

    #[test]
    fn plan_indexed_eq_plus_residual() {
        let ds = test_datasource(&["status"]);
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
        let p = plan(&ds, &q);
        // Should be Filter(IndexScan) — status consumed by index, score remains as filter
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
        let ds = test_datasource(&[]);
        let q = Query {
            sort: vec![Sort {
                field: "name".into(),
                direction: SortDirection::Asc,
            }],
            ..empty_query()
        };
        let p = plan(&ds, &q);
        assert!(
            matches!(p, PlanNode::Sort { input, .. } if matches!(*input, PlanNode::Scan { .. }))
        );
    }

    #[test]
    fn plan_with_filter_and_sort() {
        let ds = test_datasource(&[]);
        let q = Query {
            filter: Some(eq_filter("status", QueryValue::String("active".into()))),
            sort: vec![Sort {
                field: "name".into(),
                direction: SortDirection::Asc,
            }],
            ..empty_query()
        };
        let p = plan(&ds, &q);
        // Sort(Filter(Scan))
        match p {
            PlanNode::Sort { input, .. } => {
                assert!(matches!(*input, PlanNode::Filter { .. }));
            }
            _ => panic!("expected Sort node"),
        }
    }

    #[test]
    fn plan_with_skip_take() {
        let ds = test_datasource(&[]);
        let q = Query {
            skip: Some(10),
            take: Some(5),
            ..empty_query()
        };
        let p = plan(&ds, &q);
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
        let ds = test_datasource(&[]);
        let q = Query {
            columns: Some(vec!["name".into(), "status".into()]),
            ..empty_query()
        };
        let p = plan(&ds, &q);
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
        let ds = test_datasource(&["status"]);
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
        let p = plan(&ds, &q);
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
        let ds = test_datasource(&["status"]);
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
        let p = plan(&ds, &q);
        // OR at top level can't use index — falls back to Filter(Scan)
        assert!(
            matches!(p, PlanNode::Filter { input, .. } if matches!(*input, PlanNode::Scan { .. }))
        );
    }
}
