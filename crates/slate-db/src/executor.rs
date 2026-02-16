use std::collections::HashSet;

use bson::RawDocumentBuf;
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::exec;
use crate::planner::PlanNode;

type DocIter<'a> = Box<dyn Iterator<Item = Result<bson::Document, DbError>> + 'a>;

/// Executes a query plan tree, returning a lazy document iterator.
pub fn execute<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    node: &'a PlanNode,
) -> Result<DocIter<'a>, DbError> {
    execute_node(txn, node)
}

fn execute_node<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    node: &'a PlanNode,
) -> Result<DocIter<'a>, DbError> {
    match node {
        PlanNode::Scan { partition, columns } => execute_scan(txn, partition, columns.as_deref()),

        PlanNode::IndexScan {
            partition,
            column,
            value,
            columns,
        } => execute_index_scan(txn, partition, column, value, columns.as_deref()),

        PlanNode::Filter { predicate, input } => {
            let source = execute_node(txn, input)?;

            Ok(Box::new(source.filter_map(move |result| match result {
                Err(e) => Some(Err(e)),
                Ok(doc) => match exec::matches_group(&doc, predicate) {
                    Ok(true) => Some(Ok(doc)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                },
            })))
        }

        PlanNode::Sort { sorts, input } => {
            let source = execute_node(txn, input)?;

            let mut docs: Vec<bson::Document> = source.collect::<Result<Vec<_>, _>>()?;

            docs.sort_by(|a, b| {
                for sort in sorts {
                    let a_val = exec::get_path(a, &sort.field);
                    let b_val = exec::get_path(b, &sort.field);
                    let ord = exec::compare_field_values(a_val, b_val);
                    let ord = match sort.direction {
                        slate_query::SortDirection::Asc => ord,
                        slate_query::SortDirection::Desc => ord.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });

            Ok(Box::new(docs.into_iter().map(Ok)))
        }

        PlanNode::Limit { skip, take, input } => {
            let source = execute_node(txn, input)?;
            let iter = source.skip(*skip);
            match take {
                Some(take) => Ok(Box::new(iter.take(*take))),
                None => Ok(Box::new(iter)),
            }
        }

        PlanNode::Projection { columns, input } => {
            let source = execute_node(txn, input)?;
            let cols = columns.clone();

            Ok(Box::new(source.map(move |result| {
                let mut doc = result?;
                exec::apply_projection(&mut doc, &cols);
                Ok(doc)
            })))
        }
    }
}

/// Materialize a document from raw BSON bytes.
/// If `columns` is Some, only read the specified top-level keys (selective projection).
/// If None, materialize the entire document.
fn materialize_doc(
    raw: &RawDocumentBuf,
    columns: Option<&HashSet<&str>>,
) -> Result<bson::Document, DbError> {
    match columns {
        Some(cols) => {
            let mut doc = bson::Document::new();
            for result in raw.iter() {
                let (key, raw_val) = result?;
                if cols.contains(key) {
                    doc.insert(key, bson::Bson::try_from(raw_val)?);
                }
            }
            Ok(doc)
        }
        None => Ok(raw.to_document()?),
    }
}

/// Scan all records in a partition. Key format: `d:{_id}`.
fn execute_scan<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    partition: &'a str,
    columns: Option<&'a [String]>,
) -> Result<DocIter<'a>, DbError> {
    let scan_prefix = encoding::data_scan_prefix("");
    let iter = txn.scan_prefix(partition, &scan_prefix)?;

    let col_set: Option<HashSet<&str>> =
        columns.map(|cols| cols.iter().map(|s| s.as_str()).collect());

    Ok(Box::new(iter.filter_map(move |result| match result {
        Err(e) => Some(Err(DbError::from(e))),
        Ok((key, value)) => {
            let record_id = match encoding::parse_record_key(&key) {
                Some(id) => id,
                None => return None,
            };

            let raw = match RawDocumentBuf::from_bytes(value.into_vec()) {
                Ok(r) => r,
                Err(_) => return None,
            };

            let mut doc = match materialize_doc(&raw, col_set.as_ref()) {
                Ok(d) => d,
                Err(_) => return None,
            };

            doc.insert("_id", record_id);

            Some(Ok(doc))
        }
    })))
}

/// Index scan — look up record IDs from the index, multi_get full records.
fn execute_index_scan<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    partition: &'a str,
    column: &'a str,
    value: &'a bson::Bson,
    columns: Option<&'a [String]>,
) -> Result<DocIter<'a>, DbError> {
    let prefix = encoding::index_scan_prefix(column, value);
    let iter = txn.scan_prefix(partition, &prefix)?;

    let col_set: Option<HashSet<&str>> =
        columns.map(|cols| cols.iter().map(|s| s.as_str()).collect());

    // Collect record IDs from index
    let record_ids: Vec<String> = iter
        .filter_map(|result| match result {
            Ok((key, _)) => {
                let (_, record_id) = encoding::parse_index_key(&key)?;
                Some(record_id.to_string())
            }
            Err(_) => None,
        })
        .collect();

    // Batch read full records
    let keys: Vec<Vec<u8>> = record_ids
        .iter()
        .map(|id| encoding::record_key(id))
        .collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let values = txn.multi_get(partition, &key_refs)?;

    let mut docs = Vec::new();
    for (id, value_opt) in record_ids.iter().zip(values) {
        if let Some(bytes) = value_opt {
            let raw = match RawDocumentBuf::from_bytes(bytes.into_vec()) {
                Ok(r) => r,
                Err(_) => continue,
            };

            let mut doc = match materialize_doc(&raw, col_set.as_ref()) {
                Ok(d) => d,
                Err(_) => continue,
            };

            doc.insert("_id", id.as_str());

            docs.push(doc);
        }
    }

    Ok(Box::new(docs.into_iter().map(Ok)))
}
