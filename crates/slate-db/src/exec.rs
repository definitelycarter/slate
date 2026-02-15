use slate_query::{Filter, FilterGroup, FilterNode, LogicalOp, Operator, QueryValue};

use crate::error::DbError;
use crate::record::{Record, Value};

// ── Filter matching ─────────────────────────────────────────────

pub(crate) fn matches_group(record: &Record, group: &FilterGroup) -> Result<bool, DbError> {
    match group.logical {
        LogicalOp::And => {
            for child in &group.children {
                if !matches_node(record, child)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        LogicalOp::Or => {
            for child in &group.children {
                if matches_node(record, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn matches_node(record: &Record, node: &FilterNode) -> Result<bool, DbError> {
    match node {
        FilterNode::Condition(filter) => matches_filter(record, filter),
        FilterNode::Group(group) => matches_group(record, group),
    }
}

fn matches_filter(record: &Record, filter: &Filter) -> Result<bool, DbError> {
    let field_value = record.cells.get(&filter.field).map(|cell| &cell.value);

    match filter.operator {
        Operator::IsNull => match &filter.value {
            QueryValue::Bool(true) => Ok(field_value.is_none()),
            QueryValue::Bool(false) => Ok(field_value.is_some()),
            _ => Ok(field_value.is_none()),
        },
        Operator::Eq => match field_value {
            Some(v) => Ok(values_eq(v, &filter.value)),
            None => Ok(false),
        },
        Operator::IContains => match (field_value, &filter.value) {
            (Some(Value::String(haystack)), QueryValue::String(needle)) => {
                Ok(haystack.to_lowercase().contains(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::IStartsWith => match (field_value, &filter.value) {
            (Some(Value::String(haystack)), QueryValue::String(needle)) => {
                Ok(haystack.to_lowercase().starts_with(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::IEndsWith => match (field_value, &filter.value) {
            (Some(Value::String(haystack)), QueryValue::String(needle)) => {
                Ok(haystack.to_lowercase().ends_with(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::Gt => compare_values(field_value, &filter.value, |ord| {
            ord == std::cmp::Ordering::Greater
        }),
        Operator::Gte => compare_values(field_value, &filter.value, |ord| {
            ord != std::cmp::Ordering::Less
        }),
        Operator::Lt => compare_values(field_value, &filter.value, |ord| {
            ord == std::cmp::Ordering::Less
        }),
        Operator::Lte => compare_values(field_value, &filter.value, |ord| {
            ord != std::cmp::Ordering::Greater
        }),
    }
}

fn values_eq(store_val: &Value, query_val: &QueryValue) -> bool {
    match (store_val, query_val) {
        (Value::String(a), QueryValue::String(b)) => a == b,
        (Value::Int(a), QueryValue::Int(b)) => a == b,
        (Value::Float(a), QueryValue::Float(b)) => a == b,
        (Value::Bool(a), QueryValue::Bool(b)) => a == b,
        (Value::Date(a), QueryValue::Date(b)) => a == b,
        _ => false,
    }
}

fn compare_values(
    field_value: Option<&Value>,
    query_val: &QueryValue,
    predicate: fn(std::cmp::Ordering) -> bool,
) -> Result<bool, DbError> {
    match field_value {
        Some(store_val) => match (store_val, query_val) {
            (Value::Int(a), QueryValue::Int(b)) => Ok(predicate(a.cmp(b))),
            (Value::Float(a), QueryValue::Float(b)) => Ok(predicate(
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            )),
            (Value::Date(a), QueryValue::Date(b)) => Ok(predicate(a.cmp(b))),
            (Value::String(a), QueryValue::String(b)) => Ok(predicate(a.cmp(b))),
            _ => Ok(false),
        },
        None => Ok(false),
    }
}

// ── Value comparison for sorting ─────────────────────────────────

pub(crate) fn compare_field_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => compare_two_values(a, b),
    }
}

fn compare_two_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}
