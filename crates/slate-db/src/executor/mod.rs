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

use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::planner::plan::{Node, Plan};

pub type RawIter<'a> = Box<dyn Iterator<Item = Result<Option<RawBson>, DbError>> + 'a>;

/// Executes query plans against an [`EngineTransaction`].
///
/// The returned `RawIter` borrows from the transaction, not from the executor.
/// Collection handles are passed by value into iterator closures, so the
/// executor can be used as a temporary:
///
/// ```ignore
/// let iter = Executor::new(&txn).execute(plan)?;
/// for row in iter { /* ... */ }
/// ```
pub struct Executor<'a, T: EngineTransaction> {
    txn: &'a T,
    now_millis: i64,
}

impl<'a, T: EngineTransaction> Executor<'a, T> {
    pub fn new(txn: &'a T) -> Self {
        Self {
            txn,
            now_millis: bson::DateTime::now().timestamp_millis(),
        }
    }

    /// Create an executor that skips TTL filtering.
    pub fn new_no_ttl_filter(txn: &'a T) -> Self {
        Self {
            txn,
            now_millis: i64::MIN,
        }
    }

    /// Execute a plan, returning a streaming iterator of rows.
    pub fn execute(&self, plan: Plan<T::Cf>) -> Result<RawIter<'a>, DbError> {
        match plan {
            Plan::Find(node) => self.execute_node(node),

            Plan::Insert { collection, source } => {
                let source = self.execute_node(source)?;
                nodes::insert_record::execute(self.txn, collection, source, self.now_millis)
            }

            Plan::Update {
                collection,
                mutation,
                source,
            } => {
                let source = self.execute_node(source)?;
                nodes::mutate::execute(self.txn, collection, mutation, source)
            }

            Plan::Replace {
                collection,
                replacement,
                source,
            } => {
                let source = self.execute_node(source)?;
                nodes::replace::execute(self.txn, collection, replacement, source)
            }

            Plan::Delete { collection, source } => {
                let source = self.execute_node(source)?;
                nodes::delete::execute(self.txn, collection, source)
            }

            Plan::Merge { collection, source } => {
                let (iter, _, _) =
                    self.execute_upsert(nodes::upsert::UpsertMode::Merge, collection, source)?;
                Ok(iter)
            }

            Plan::Upsert { collection, source } => {
                let (iter, _, _) =
                    self.execute_upsert(nodes::upsert::UpsertMode::Replace, collection, source)?;
                Ok(iter)
            }
        }
    }

    /// Execute an upsert pipeline, returning the row iterator and shared counters
    /// that are populated as the iterator is drained.
    pub fn execute_upsert_plan(
        &self,
        plan: Plan<T::Cf>,
    ) -> Result<(RawIter<'a>, Rc<Cell<u64>>, Rc<Cell<u64>>), DbError> {
        match plan {
            Plan::Merge { collection, source } => {
                self.execute_upsert(nodes::upsert::UpsertMode::Merge, collection, source)
            }
            Plan::Upsert { collection, source } => {
                self.execute_upsert(nodes::upsert::UpsertMode::Replace, collection, source)
            }
            _ => {
                let iter = self.execute(plan)?;
                Ok((iter, Rc::new(Cell::new(0)), Rc::new(Cell::new(0))))
            }
        }
    }

    fn execute_upsert(
        &self,
        mode: nodes::upsert::UpsertMode,
        collection: CollectionHandle<T::Cf>,
        source_node: Node<T::Cf>,
    ) -> Result<(RawIter<'a>, Rc<Cell<u64>>, Rc<Cell<u64>>), DbError> {
        let inserted = Rc::new(Cell::new(0u64));
        let updated = Rc::new(Cell::new(0u64));
        let source = self.execute_node(source_node)?;
        let iter = nodes::upsert::execute(
            self.txn,
            collection,
            mode,
            source,
            Rc::clone(&inserted),
            Rc::clone(&updated),
            self.now_millis,
        )?;
        Ok((iter, inserted, updated))
    }

    fn execute_node(&self, node: Node<T::Cf>) -> Result<RawIter<'a>, DbError> {
        match node {
            Node::Values(docs) => nodes::values::execute(docs),

            Node::Scan { collection } => {
                nodes::scan::execute(self.txn, collection, self.now_millis)
            }

            Node::IndexScan {
                collection,
                field,
                range,
                direction,
                limit,
                covered,
            } => nodes::index_scan::execute(
                self.txn,
                collection,
                field,
                &range,
                direction,
                limit,
                covered,
                self.now_millis,
            ),

            Node::IndexMerge { logical, lhs, rhs } => {
                let left = self.execute_node(*lhs)?;
                let right = self.execute_node(*rhs)?;
                nodes::index_merge::execute(logical, left, right)
            }

            Node::KeyLookup { collection, source } => {
                let source = self.execute_node(*source)?;
                nodes::read_record::execute(self.txn, collection, source, self.now_millis)
            }

            Node::Filter { predicate, source } => {
                let source = self.execute_node(*source)?;
                nodes::filter::execute(predicate, source)
            }

            Node::Sort { sorts, source } => {
                let source = self.execute_node(*source)?;
                nodes::sort::execute(sorts, source)
            }

            Node::Limit { skip, take, source } => {
                let source = self.execute_node(*source)?;
                nodes::limit::execute(skip, take, source)
            }

            Node::Projection { columns, source } => {
                let source = self.execute_node(*source)?;
                nodes::projection::execute(columns, source)
            }

            Node::Distinct { field, source } => {
                let source = self.execute_node(*source)?;
                nodes::distinct::execute(field, source)
            }
        }
    }
}
