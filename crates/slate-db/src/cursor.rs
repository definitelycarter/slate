use bson::RawDocumentBuf;
use slate_engine::{EngineTransaction, KvEngine};
use slate_store::Store;

use crate::error::DbError;
use crate::executor::Executor;
use crate::planner::plan::Plan;

type KvTxn<'a, S> = <KvEngine<S> as slate_engine::Engine>::Txn<'a>;

/// A prepared query that can be iterated or executed.
///
/// Owns a pre-built `Plan` and a reference to the transaction.
/// Call [`.iter()`](Cursor::iter) for streaming iteration, or
/// [`.execute()`](Cursor::execute) to drain and return a count.
pub struct Cursor<'db: 'txn, 'txn, S: Store + 'db> {
    txn: &'txn KvTxn<'db, S>,
    plan: Option<Plan<<KvTxn<'db, S> as EngineTransaction>::Cf>>,
}

impl<'db: 'txn, 'txn, S: Store + 'db> Cursor<'db, 'txn, S> {
    pub(crate) fn new(
        txn: &'txn KvTxn<'db, S>,
        plan: Plan<<KvTxn<'db, S> as EngineTransaction>::Cf>,
    ) -> Self {
        Self {
            txn,
            plan: Some(plan),
        }
    }

    /// Execute the plan and return a materialized iterator over documents.
    ///
    /// Results are collected eagerly so the executor (and its handle arena) can
    /// be dropped before the caller starts iterating.  For small-to-medium
    /// result sets this is fine; for very large ones, prefer `.execute()` which
    /// streams without materializing.
    pub fn iter(&self) -> Result<CursorIter, DbError> {
        let plan = self
            .plan
            .as_ref()
            .ok_or_else(|| DbError::InvalidQuery("cursor already consumed".into()))?;
        let mut exec = Executor::new(self.txn);
        let iter = exec.execute(plan.clone())?;

        // Materialize: the executor's handle arena must outlive the RawIter,
        // so we drain the iterator here while `exec` is still alive.
        let mut docs = Vec::new();
        for result in iter {
            match result? {
                None => continue,
                Some(val) => {
                    let doc = val
                        .into_document_buf()
                        .ok_or_else(|| DbError::InvalidQuery("expected document".into()))?;
                    docs.push(doc);
                }
            }
        }

        Ok(CursorIter {
            inner: docs.into_iter(),
        })
    }

    /// Consume the cursor, drain all rows, and return the count of affected rows.
    pub fn execute(mut self) -> Result<u64, DbError> {
        let plan = self
            .plan
            .take()
            .ok_or_else(|| DbError::InvalidQuery("cursor already consumed".into()))?;
        let mut exec = Executor::new(self.txn);
        let iter = exec.execute(plan)?;
        let mut count = 0u64;
        for result in iter {
            result?;
            count += 1;
        }
        Ok(count)
    }
}

/// A materialized iterator over query results.
///
/// Each call to [`next()`](Iterator::next) returns the next pre-fetched document.
pub struct CursorIter {
    inner: std::vec::IntoIter<RawDocumentBuf>,
}

impl Iterator for CursorIter {
    type Item = Result<RawDocumentBuf, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
