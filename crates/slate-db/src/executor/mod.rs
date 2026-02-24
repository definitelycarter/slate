pub(crate) mod exec;
pub(crate) mod field_tree;
pub(crate) mod mutation_ops;
mod nodes;
pub(crate) mod raw_bson;
pub(crate) mod raw_mutation;
#[cfg(test)]
mod tests;

use std::cell::Cell;
use std::rc::Rc;

use bson::RawDocument;
use bson::raw::{RawBson, RawBsonRef};
use slate_store::Transaction;

use crate::error::DbError;
use crate::planner::PlanNode;

// ── RawValue ────────────────────────────────────────────────────

pub enum RawValue<'a> {
    Borrowed(RawBsonRef<'a>),
    Owned(RawBson),
}

impl<'a> RawValue<'a> {
    pub fn as_document(&self) -> Option<&RawDocument> {
        match self {
            Self::Borrowed(RawBsonRef::Document(d)) => Some(d),
            Self::Owned(RawBson::Document(d)) => Some(d),
            _ => None,
        }
    }

    pub fn into_raw_bson(self) -> Option<RawBson> {
        match self {
            Self::Owned(b) => Some(b),
            Self::Borrowed(r) => exec::to_raw_bson(r),
        }
    }

    pub fn into_document_buf(self) -> Option<bson::RawDocumentBuf> {
        match self {
            Self::Owned(RawBson::Document(buf)) => Some(buf),
            Self::Borrowed(RawBsonRef::Document(d)) => Some(d.to_raw_document_buf()),
            _ => None,
        }
    }
}

pub type RawIter<'a> = Box<dyn Iterator<Item = Result<Option<RawValue<'a>>, DbError>> + 'a>;

pub struct Executor<'c, T: Transaction> {
    txn: &'c T,
    cf: &'c T::Cf,
    now_millis: i64,
}

impl<'c, T: Transaction + 'c> Executor<'c, T> {
    pub fn new(txn: &'c T, cf: &'c T::Cf) -> Self {
        Self {
            txn,
            cf,
            now_millis: bson::DateTime::now().timestamp_millis(),
        }
    }

    /// Create an executor that skips TTL filtering.
    /// Used by `purge_expired` so the delete pipeline can read expired docs.
    /// Sets `now_millis` to `i64::MIN` so `is_ttl_expired` always returns `false`
    /// (no timestamp is less than `i64::MIN`).
    pub fn new_no_ttl_filter(txn: &'c T, cf: &'c T::Cf) -> Self {
        Self {
            txn,
            cf,
            now_millis: i64::MIN,
        }
    }

    /// Execute a plan node, returning a streaming iterator of rows.
    pub fn execute(&self, node: PlanNode) -> Result<RawIter<'c>, DbError> {
        self.execute_node(node)
    }

    /// Execute an upsert pipeline, returning the row iterator and shared counters
    /// that are populated as the iterator is drained.
    pub fn execute_upsert(
        &self,
        node: PlanNode,
    ) -> Result<(RawIter<'c>, Rc<Cell<u64>>, Rc<Cell<u64>>), DbError> {
        let inserted = Rc::new(Cell::new(0u64));
        let updated = Rc::new(Cell::new(0u64));
        let iter = self.execute_upsert_pipeline(node, &inserted, &updated)?;
        Ok((iter, inserted, updated))
    }

    /// Execute the full upsert pipeline, threading shared counters through the Upsert node.
    fn execute_upsert_pipeline(
        &self,
        node: PlanNode,
        inserted: &Rc<Cell<u64>>,
        updated: &Rc<Cell<u64>>,
    ) -> Result<RawIter<'c>, DbError> {
        match node {
            PlanNode::InsertIndex {
                indexed_fields,
                input,
            } => {
                let source = self.execute_upsert_pipeline(*input, inserted, updated)?;
                nodes::insert_index::execute(self.txn, self.cf, indexed_fields, source)
            }
            PlanNode::Upsert {
                mode,
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(*input)?;
                nodes::upsert::execute(
                    self.txn,
                    self.cf,
                    mode,
                    indexed_fields,
                    source,
                    Rc::clone(inserted),
                    Rc::clone(updated),
                    self.now_millis,
                )
            }
            _ => self.execute_node(node),
        }
    }

    fn execute_node(&self, node: PlanNode) -> Result<RawIter<'c>, DbError> {
        match node {
            PlanNode::Values { docs } => nodes::values::execute(docs),
            PlanNode::Projection { columns, input } => {
                let source = self.execute_node(*input)?;
                nodes::projection::execute(columns, source)
            }
            PlanNode::Limit { skip, take, input } => {
                let source = self.execute_node(*input)?;
                nodes::limit::execute(skip, take, source)
            }
            PlanNode::Sort { sorts, input } => {
                let source = self.execute_node(*input)?;
                nodes::sort::execute(sorts, source)
            }
            PlanNode::Distinct { field, input } => {
                let source = self.execute_node(*input)?;
                nodes::distinct::execute(field, source)
            }
            PlanNode::Filter { predicate, input } => {
                let source = self.execute_node(*input)?;
                nodes::filter::execute(predicate, source)
            }
            PlanNode::Scan => nodes::scan::execute(self.txn, self.cf, self.now_millis),
            PlanNode::IndexScan {
                column,
                filter,
                direction,
                limit,
                complete_groups,
                covered,
                ..
            } => nodes::index_scan::execute(
                self.txn,
                self.cf,
                column,
                filter.as_ref(),
                direction,
                limit,
                complete_groups,
                covered,
                self.now_millis,
            ),
            PlanNode::IndexMerge { logical, lhs, rhs } => {
                let left = self.execute_node(*lhs)?;
                let right = self.execute_node(*rhs)?;
                nodes::index_merge::execute(logical, left, right)
            }
            PlanNode::ReadRecord { input } => {
                let source = self.execute_node(*input)?;
                nodes::read_record::execute(self.txn, self.cf, source, self.now_millis)
            }
            PlanNode::Delete { input } => {
                let source = self.execute_node(*input)?;
                nodes::delete::execute(self.txn, self.cf, source)
            }
            PlanNode::Update { mutation, input } => {
                let source = self.execute_node(*input)?;
                nodes::mutate::execute(self.txn, self.cf, mutation, source)
            }
            PlanNode::Replace { replacement, input } => {
                let source = self.execute_node(*input)?;
                nodes::replace::execute(self.txn, self.cf, replacement, source)
            }
            PlanNode::InsertRecord { input } => {
                let source = self.execute_node(*input)?;
                nodes::insert_record::execute(self.txn, self.cf, source)
            }
            PlanNode::InsertIndex {
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(*input)?;
                nodes::insert_index::execute(self.txn, self.cf, indexed_fields, source)
            }
            PlanNode::DeleteIndex {
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(*input)?;
                nodes::delete_index::execute(self.txn, self.cf, indexed_fields, source)
            }
            PlanNode::Upsert {
                mode,
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(*input)?;
                // Standalone execute_node — counters are discarded.
                let inserted = Rc::new(Cell::new(0u64));
                let updated = Rc::new(Cell::new(0u64));
                nodes::upsert::execute(
                    self.txn,
                    self.cf,
                    mode,
                    indexed_fields,
                    source,
                    inserted,
                    updated,
                    self.now_millis,
                )
            }
        }
    }
}
