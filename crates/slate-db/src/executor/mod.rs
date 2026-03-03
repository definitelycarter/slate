pub(crate) mod exec;
pub(crate) mod field_tree;
mod nodes;
pub(crate) mod raw_bson;
#[cfg(test)]
mod tests;

use bson::RawBson;
use slate_engine::{Catalog, EngineTransaction};

use slate_vm::pool::VmPool;

use crate::error::DbError;
use crate::planner::plan::{Node, Plan};

pub type RawIter<'a> = Box<dyn Iterator<Item = Result<Option<RawBson>, DbError>> + 'a>;

/// Executes query plans against an [`EngineTransaction`].
pub struct Executor<'a, T: EngineTransaction> {
    txn: &'a T,
    pool: Option<&'a VmPool>,
}

impl<'a, T: EngineTransaction> Executor<'a, T> {
    pub fn new(txn: &'a T, pool: Option<&'a VmPool>) -> Self {
        Self { txn, pool }
    }
}

impl<'a, T: EngineTransaction + Catalog> Executor<'a, T> {
    fn execute_node(&self, node: Node<T::Cf>) -> Result<RawIter<'a>, DbError> {
        match node {
            Node::Values(docs) => nodes::values::execute(docs),

            Node::Scan { collection } => nodes::scan::execute(self.txn, collection),

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
            ),

            Node::IndexMerge { collection, logical, lhs, rhs } => {
                let left = self.execute_node(*lhs)?;
                let right = self.execute_node(*rhs)?;
                nodes::index_merge::execute(collection.pk_path(), logical, left, right)
            }

            Node::KeyLookup { collection, source } => {
                let source = self.execute_node(*source)?;
                nodes::read_record::execute(self.txn, collection, source)
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

            Node::Projection { collection, columns, source } => {
                let source = self.execute_node(*source)?;
                nodes::projection::execute(collection.pk_path(), columns, source)
            }

            Node::Distinct { field, source } => {
                let source = self.execute_node(*source)?;
                nodes::distinct::execute(field, source)
            }

            Node::Validate { validators, source } => {
                let source = self.execute_node(*source)?;
                nodes::validate::execute(self.pool, validators, source)
            }

            Node::Trigger {
                cf,
                action,
                hooks,
                source,
            } => {
                let source = self.execute_node(*source)?;
                nodes::trigger::execute(self.txn, self.pool, cf, action, hooks, source)
            }
        }
    }

    /// Execute a plan, returning a streaming iterator of rows.
    pub fn execute(&self, plan: Plan<T::Cf>) -> Result<RawIter<'a>, DbError> {
        match plan {
            Plan::Find(node) => self.execute_node(node),

            Plan::Insert { collection, source } => {
                let source = self.execute_node(source)?;
                nodes::insert_record::execute(self.txn, collection, source)
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

            Plan::Merge {
                collection,
                hooks,
                source,
            } => {
                let source = self.execute_node(source)?;
                nodes::upsert::execute(
                    self.txn,
                    self.pool,
                    hooks,
                    collection,
                    nodes::upsert::UpsertMode::Merge,
                    source,
                )
            }

            Plan::Upsert {
                collection,
                hooks,
                source,
            } => {
                let source = self.execute_node(source)?;
                nodes::upsert::execute(
                    self.txn,
                    self.pool,
                    hooks,
                    collection,
                    nodes::upsert::UpsertMode::Replace,
                    source,
                )
            }

            Plan::Trigger {
                cf,
                action,
                hooks,
                plan,
            } => {
                let iter = self.execute(*plan)?;
                nodes::trigger::execute(self.txn, self.pool, cf, action, hooks, iter)
            }
        }
    }
}
