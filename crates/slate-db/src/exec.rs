use slate_query::{
    Filter, FilterGroup, FilterNode, LogicalOp, Operator, Query, QueryValue, Sort, SortDirection,
};
use slate_store::{Record, StoreError, Value};

use crate::error::DbError;

/// Executes a query against a record iterator using a pipeline approach.
///
/// Pipeline: scan → filter → sort → skip → take
///
/// Filtering is applied streaming (no full materialization).
/// Sorting requires collecting filtered results.
/// Skip/take are applied after sorting.
pub fn execute<I>(records: I, query: &Query) -> Result<Vec<Record>, DbError>
where
    I: Iterator<Item = Result<Record, StoreError>>,
{
    // Stage 1: Scan + Filter (streaming)
    let filtered = apply_filter(records, &query.filter);

    // Stage 2: Sort (requires collect if sorting, otherwise stays streaming)
    if !query.sort.is_empty() {
        let mut collected: Vec<Record> = collect_results(filtered)?;
        apply_sort(&mut collected, &query.sort);

        // Stage 3: Skip + Take on sorted results
        let skipped = collected.into_iter().skip(query.skip.unwrap_or(0));
        match query.take {
            Some(take) => Ok(skipped.take(take).collect()),
            None => Ok(skipped.collect()),
        }
    } else {
        // No sort: skip + take directly on the filtered stream
        let skipped = SkipIterator::new(collect_results_iter(filtered), query.skip.unwrap_or(0));
        match query.take {
            Some(take) => Ok(skipped.take(take).collect::<Result<Vec<_>, _>>()?),
            None => Ok(skipped.collect::<Result<Vec<_>, _>>()?),
        }
    }
}

// --- Filter pipeline ---

fn apply_filter<I>(records: I, filter: &Option<FilterGroup>) -> FilterIterator<I>
where
    I: Iterator<Item = Result<Record, StoreError>>,
{
    FilterIterator {
        source: records,
        filter: filter.clone(),
    }
}

struct FilterIterator<I> {
    source: I,
    filter: Option<FilterGroup>,
}

impl<I> Iterator for FilterIterator<I>
where
    I: Iterator<Item = Result<Record, StoreError>>,
{
    type Item = Result<Record, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.source.next()? {
                Err(e) => return Some(Err(e)),
                Ok(record) => match &self.filter {
                    None => return Some(Ok(record)),
                    Some(group) => match matches_group(&record, group) {
                        Ok(true) => return Some(Ok(record)),
                        Ok(false) => continue,
                        Err(e) => return Some(Err(StoreError::Storage(e.to_string()))),
                    },
                },
            }
        }
    }
}

// --- Skip iterator for streaming skip without collect ---

struct SkipIterator<I> {
    source: I,
    remaining: usize,
}

impl<I> SkipIterator<I> {
    fn new(source: I, skip: usize) -> Self {
        Self {
            source,
            remaining: skip,
        }
    }
}

impl<I> Iterator for SkipIterator<I>
where
    I: Iterator<Item = Result<Record, StoreError>>,
{
    type Item = Result<Record, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.remaining > 0 {
            match self.source.next()? {
                Err(e) => return Some(Err(e)),
                Ok(_) => {
                    self.remaining -= 1;
                }
            }
        }
        self.source.next()
    }
}

// --- Helper to collect results, propagating errors ---

fn collect_results<I>(iter: I) -> Result<Vec<Record>, DbError>
where
    I: Iterator<Item = Result<Record, StoreError>>,
{
    let mut results = Vec::new();
    for item in iter {
        results.push(item?);
    }
    Ok(results)
}

/// Wraps an iterator of Result<Record, StoreError> into Result<Record, DbError>
fn collect_results_iter<I>(iter: I) -> impl Iterator<Item = Result<Record, StoreError>>
where
    I: Iterator<Item = Result<Record, StoreError>>,
{
    iter
}

// --- Filter matching ---

fn matches_group(record: &Record, group: &FilterGroup) -> Result<bool, DbError> {
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
    let field_value = record.fields.get(&filter.field);

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

// --- Sorting ---

fn apply_sort(records: &mut [Record], sorts: &[Sort]) {
    records.sort_by(|a, b| {
        for sort in sorts {
            let a_val = a.fields.get(&sort.field);
            let b_val = b.fields.get(&sort.field);
            let ord = compare_field_values(a_val, b_val);
            let ord = match sort.direction {
                SortDirection::Asc => ord,
                SortDirection::Desc => ord.reverse(),
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn compare_field_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
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
