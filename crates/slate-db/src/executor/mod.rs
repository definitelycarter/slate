pub(crate) mod exec;
pub(crate) mod field_tree;
mod nodes;
pub(crate) mod raw_bson;
#[cfg(test)]
mod tests;

use bson::RawBson;
use slate_engine::EngineTransaction;

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
                let source = self.execute_node(source)?;
                nodes::upsert::execute(
                    self.txn,
                    collection,
                    nodes::upsert::UpsertMode::Merge,
                    source,
                    self.now_millis,
                )
            }

            Plan::Upsert { collection, source } => {
                let source = self.execute_node(source)?;
                nodes::upsert::execute(
                    self.txn,
                    collection,
                    nodes::upsert::UpsertMode::Replace,
                    source,
                    self.now_millis,
                )
            }
        }
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
