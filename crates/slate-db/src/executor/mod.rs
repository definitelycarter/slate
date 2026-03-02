mod context;
pub(crate) mod exec;
pub(crate) mod field_tree;
mod nodes;
pub(crate) mod raw_bson;
#[cfg(test)]
mod tests;

pub use context::Context;

use bson::RawBson;
use slate_engine::{Catalog, CollectionHandle, EngineTransaction};

use crate::database::VmFactory;
use crate::error::DbError;
use crate::planner::plan::{Node, Plan};

pub type RawIter<'a> = Box<dyn Iterator<Item = Result<Option<RawBson>, DbError>> + 'a>;

/// Executes query plans against an [`EngineTransaction`].
///
/// For mutation plans, the executor creates a [`Context`] with a pre-built VM
/// containing all functions for the target collection. Read-only plans use a
/// bare context with no VM.
pub struct Executor<'a, T: EngineTransaction> {
    txn: &'a T,
    vm_factory: Option<&'a VmFactory>,
}

impl<'a, T: EngineTransaction> Executor<'a, T> {
    pub fn new(txn: &'a T, vm_factory: Option<&'a VmFactory>) -> Self {
        Self { txn, vm_factory }
    }

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

            Node::IndexMerge { logical, lhs, rhs } => {
                let left = self.execute_node(*lhs)?;
                let right = self.execute_node(*rhs)?;
                nodes::index_merge::execute(logical, left, right)
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

impl<'a, T: EngineTransaction + Catalog> Executor<'a, T> {
    /// Create a context with a VM loaded for the given collection.
    fn create_context(
        &self,
        handle: &CollectionHandle<T::Cf>,
    ) -> Result<Context<'a, T>, DbError> {
        Context::with_vm(self.txn, self.vm_factory, handle.cf_name(), handle.name())
    }

    /// Execute a plan, returning a streaming iterator of rows.
    pub fn execute(&self, plan: Plan<T::Cf>) -> Result<RawIter<'a>, DbError> {
        match plan {
            Plan::Find(node) => self.execute_node(node),

            Plan::Insert { collection, source } => {
                let source = self.execute_node(source)?;
                let ctx = self.create_context(&collection)?;
                nodes::insert_record::execute(ctx, collection, source)
            }

            Plan::Update {
                collection,
                mutation,
                source,
            } => {
                let source = self.execute_node(source)?;
                let ctx = self.create_context(&collection)?;
                nodes::mutate::execute(ctx, collection, mutation, source)
            }

            Plan::Replace {
                collection,
                replacement,
                source,
            } => {
                let source = self.execute_node(source)?;
                let ctx = self.create_context(&collection)?;
                nodes::replace::execute(ctx, collection, replacement, source)
            }

            Plan::Delete { collection, source } => {
                let source = self.execute_node(source)?;
                let ctx = self.create_context(&collection)?;
                nodes::delete::execute(ctx, collection, source)
            }

            Plan::Merge { collection, source } => {
                let source = self.execute_node(source)?;
                let ctx = self.create_context(&collection)?;
                nodes::upsert::execute(
                    ctx,
                    collection,
                    nodes::upsert::UpsertMode::Merge,
                    source,
                )
            }

            Plan::Upsert { collection, source } => {
                let source = self.execute_node(source)?;
                let ctx = self.create_context(&collection)?;
                nodes::upsert::execute(
                    ctx,
                    collection,
                    nodes::upsert::UpsertMode::Replace,
                    source,
                )
            }
        }
    }
}
