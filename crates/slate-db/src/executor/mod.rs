pub(crate) mod exec;
pub(crate) mod field_tree;
mod nodes;
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

pub enum ExecutionResult<'a> {
    Rows(RawIter<'a>),
    Delete { deleted: u64 },
    Update { matched: u64, modified: u64 },
    Insert { ids: Vec<String> },
    Upsert { inserted: u64, updated: u64 },
}

pub struct Executor<'c, T: Transaction> {
    txn: &'c T,
    cf: &'c T::Cf,
}

impl<'c, T: Transaction + 'c> Executor<'c, T> {
    pub fn new(txn: &'c T, cf: &'c T::Cf) -> Self {
        Self { txn, cf }
    }

    pub fn execute(&self, node: &'c PlanNode) -> Result<ExecutionResult<'c>, DbError> {
        match node {
            PlanNode::Delete { .. } => {
                let iter = self.execute_node(node)?;
                let mut deleted = 0u64;
                for result in iter {
                    result?;
                    deleted += 1;
                }
                Ok(ExecutionResult::Delete { deleted })
            }
            PlanNode::InsertIndex { input, .. }
                if matches!(**input, PlanNode::InsertRecord { .. }) =>
            {
                let iter = self.execute_node(node)?;
                let mut ids = Vec::new();
                for result in iter {
                    if let Some(val) = result? {
                        if let Some(raw) = val.as_document() {
                            if let Some(id_str) = exec::raw_extract_id(raw)? {
                                ids.push(id_str.to_string());
                            }
                        }
                    }
                }
                Ok(ExecutionResult::Insert { ids })
            }
            PlanNode::InsertIndex { input, .. } if matches!(**input, PlanNode::Upsert { .. }) => {
                let inserted = Rc::new(Cell::new(0u64));
                let updated = Rc::new(Cell::new(0u64));
                let iter = self.execute_upsert_pipeline(node, &inserted, &updated)?;
                for result in iter {
                    result?;
                }
                Ok(ExecutionResult::Upsert {
                    inserted: inserted.get(),
                    updated: updated.get(),
                })
            }
            PlanNode::InsertIndex { .. } => {
                let iter = self.execute_node(node)?;
                let mut matched = 0u64;
                let mut modified = 0u64;
                for result in iter {
                    let opt_val = result?;
                    matched += 1;
                    if opt_val.is_some() {
                        modified += 1;
                    }
                }
                Ok(ExecutionResult::Update { matched, modified })
            }
            _ => {
                let iter = self.execute_node(node)?;
                Ok(ExecutionResult::Rows(iter))
            }
        }
    }

    /// Execute the full upsert pipeline, threading shared counters through the Upsert node.
    fn execute_upsert_pipeline(
        &self,
        node: &'c PlanNode,
        inserted: &Rc<Cell<u64>>,
        updated: &Rc<Cell<u64>>,
    ) -> Result<RawIter<'c>, DbError> {
        match node {
            PlanNode::InsertIndex {
                indexed_fields,
                input,
            } => {
                let source = self.execute_upsert_pipeline(input, inserted, updated)?;
                nodes::insert_index::execute(self.txn, self.cf, indexed_fields, source)
            }
            PlanNode::Upsert {
                mode,
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(input)?;
                nodes::upsert::execute(
                    self.txn,
                    self.cf,
                    mode,
                    indexed_fields,
                    source,
                    Rc::clone(inserted),
                    Rc::clone(updated),
                )
            }
            _ => self.execute_node(node),
        }
    }

    fn execute_node(&self, node: &'c PlanNode) -> Result<RawIter<'c>, DbError> {
        match node {
            PlanNode::Values { docs } => nodes::values::execute(docs),
            PlanNode::Projection { columns, input } => {
                let source = self.execute_node(input)?;
                nodes::projection::execute(columns, source)
            }
            PlanNode::Limit { skip, take, input } => {
                let source = self.execute_node(input)?;
                nodes::limit::execute(*skip, *take, source)
            }
            PlanNode::Sort { sorts, input } => {
                let source = self.execute_node(input)?;
                nodes::sort::execute(sorts, source)
            }
            PlanNode::Distinct { field, input } => {
                let source = self.execute_node(input)?;
                nodes::distinct::execute(field, source)
            }
            PlanNode::Filter { predicate, input } => {
                let source = self.execute_node(input)?;
                nodes::filter::execute(predicate, source)
            }
            PlanNode::Scan { .. } => nodes::scan::execute(self.txn, self.cf),
            PlanNode::IndexScan {
                column,
                value,
                direction,
                limit,
                complete_groups,
                covered,
                ..
            } => nodes::index_scan::execute(
                self.txn,
                self.cf,
                column,
                value.as_ref(),
                *direction,
                *limit,
                *complete_groups,
                *covered,
            ),
            PlanNode::IndexMerge { logical, lhs, rhs } => {
                let left = self.execute_node(lhs)?;
                let right = self.execute_node(rhs)?;
                nodes::index_merge::execute(logical, left, right)
            }
            PlanNode::ReadRecord { input } => {
                let source = self.execute_node(input)?;
                nodes::read_record::execute(self.txn, self.cf, source)
            }
            PlanNode::Delete { input } => {
                let source = self.execute_node(input)?;
                nodes::delete::execute(self.txn, self.cf, source)
            }
            PlanNode::Update { update, input } => {
                let source = self.execute_node(input)?;
                nodes::update::execute(self.txn, self.cf, update, source)
            }
            PlanNode::Replace { replacement, input } => {
                let source = self.execute_node(input)?;
                nodes::replace::execute(self.txn, self.cf, replacement, source)
            }
            PlanNode::InsertRecord { input } => {
                let source = self.execute_node(input)?;
                nodes::insert_record::execute(self.txn, self.cf, source)
            }
            PlanNode::InsertIndex {
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(input)?;
                nodes::insert_index::execute(self.txn, self.cf, indexed_fields, source)
            }
            PlanNode::DeleteIndex {
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(input)?;
                nodes::delete_index::execute(self.txn, self.cf, indexed_fields, source)
            }
            PlanNode::Upsert {
                mode,
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(input)?;
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
                )
            }
        }
    }
}
