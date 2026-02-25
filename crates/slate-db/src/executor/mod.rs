pub(crate) mod exec;
pub(crate) mod field_tree;
pub(crate) mod mutation_ops;
mod nodes;
pub(crate) mod raw_bson;
pub(crate) mod raw_mutation;
#[cfg(test)]
mod tests;

use std::cell::Cell;
use std::pin::Pin;
use std::rc::Rc;

use bson::RawDocument;
use bson::raw::{RawBson, RawBsonRef};
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::planner::plan::{Node, Plan};

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

/// Executes query plans against an [`EngineTransaction`].
///
/// # Safety contract
///
/// The executor stores [`CollectionHandle`]s in a heap-pinned arena.  The
/// returned `RawIter` borrows from those handles.  Callers **must** keep the
/// `Executor` alive at least as long as any iterator it produces:
///
/// ```ignore
/// let mut exec = Executor::new(&txn);   // bind to a local
/// let iter = exec.execute(plan)?;
/// for row in iter { /* ... */ }
/// // `exec` is dropped here — after `iter` is consumed
/// ```
///
/// Using the executor as a temporary (e.g. `Executor::new(&txn).execute(plan)`)
/// when the plan touches a collection is **unsound**.
pub struct Executor<'c, T: EngineTransaction> {
    txn: &'c T,
    now_millis: i64,
    handles: Vec<Pin<Box<CollectionHandle<T::Cf>>>>,
}

impl<'c, T: EngineTransaction + 'c> Executor<'c, T> {
    pub fn new(txn: &'c T) -> Self {
        Self {
            txn,
            now_millis: bson::DateTime::now().timestamp_millis(),
            handles: Vec::new(),
        }
    }

    /// Create an executor that skips TTL filtering.
    pub fn new_no_ttl_filter(txn: &'c T) -> Self {
        Self {
            txn,
            now_millis: i64::MIN,
            handles: Vec::new(),
        }
    }

    /// Store a handle on the heap and return a reference valid for `'c`.
    ///
    /// # Safety
    ///
    /// Sound only when the caller guarantees that `self` outlives the returned
    /// reference — i.e. the `Executor` must outlive any `RawIter` that captures
    /// the returned `&CollectionHandle`.
    fn store_handle(&mut self, handle: CollectionHandle<T::Cf>) -> &'c CollectionHandle<T::Cf> {
        self.handles.push(Box::pin(handle));
        let pinned: &CollectionHandle<T::Cf> = &self.handles.last().unwrap();
        unsafe { &*(pinned as *const CollectionHandle<T::Cf>) }
    }

    /// Execute a plan, returning a streaming iterator of rows.
    pub fn execute(&mut self, plan: Plan<T::Cf>) -> Result<RawIter<'c>, DbError> {
        match plan {
            Plan::Find(node) => self.execute_node(node),

            Plan::Insert { collection, source } => {
                let source = self.execute_node(source)?;
                let handle = self.store_handle(collection);
                nodes::insert_record::execute(self.txn, handle, source, self.now_millis)
            }

            Plan::Update {
                collection,
                mutation,
                source,
            } => {
                let source = self.execute_node(source)?;
                let handle = self.store_handle(collection);
                nodes::mutate::execute(self.txn, handle, mutation, source)
            }

            Plan::Replace {
                collection,
                replacement,
                source,
            } => {
                let source = self.execute_node(source)?;
                let handle = self.store_handle(collection);
                nodes::replace::execute(self.txn, handle, replacement, source)
            }

            Plan::Delete { collection, source } => {
                let source = self.execute_node(source)?;
                let handle = self.store_handle(collection);
                nodes::delete::execute(self.txn, handle, source)
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
        &mut self,
        plan: Plan<T::Cf>,
    ) -> Result<(RawIter<'c>, Rc<Cell<u64>>, Rc<Cell<u64>>), DbError> {
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
        &mut self,
        mode: nodes::upsert::UpsertMode,
        collection: CollectionHandle<T::Cf>,
        source_node: Node<T::Cf>,
    ) -> Result<(RawIter<'c>, Rc<Cell<u64>>, Rc<Cell<u64>>), DbError> {
        let inserted = Rc::new(Cell::new(0u64));
        let updated = Rc::new(Cell::new(0u64));
        let source = self.execute_node(source_node)?;
        let handle = self.store_handle(collection);
        let iter = nodes::upsert::execute(
            self.txn,
            handle,
            mode,
            source,
            Rc::clone(&inserted),
            Rc::clone(&updated),
            self.now_millis,
        )?;
        Ok((iter, inserted, updated))
    }

    fn execute_node(&mut self, node: Node<T::Cf>) -> Result<RawIter<'c>, DbError> {
        match node {
            Node::Values(docs) => nodes::values::execute(docs),

            Node::Scan { collection } => {
                let handle = self.store_handle(collection);
                nodes::scan::execute(self.txn, handle, self.now_millis)
            }

            Node::IndexScan {
                collection,
                field,
                range,
                direction,
                limit,
                covered,
            } => {
                let handle = self.store_handle(collection);
                nodes::index_scan::execute(
                    self.txn,
                    handle,
                    field,
                    &range,
                    direction,
                    limit,
                    covered,
                    self.now_millis,
                )
            }

            Node::IndexMerge { logical, lhs, rhs } => {
                let left = self.execute_node(*lhs)?;
                let right = self.execute_node(*rhs)?;
                nodes::index_merge::execute(logical, left, right)
            }

            Node::KeyLookup { collection, source } => {
                let source = self.execute_node(*source)?;
                let handle = self.store_handle(collection);
                nodes::read_record::execute(self.txn, handle, source, self.now_millis)
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
