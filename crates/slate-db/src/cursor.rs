use bson::RawDocumentBuf;
use slate_store::{Store, Transaction};

use crate::error::DbError;
use crate::executor::{ExecutionResult, Executor, RawIter};
use crate::planner;

/// A prepared query that can be iterated over.
///
/// Borrows the transaction and owns a [`PreparedStatement`](planner::PreparedStatement)
/// that co-locates the `Statement` and `indexed_fields`.
/// Call [`.iter()`](Cursor::iter) to plan and produce a streaming
/// [`CursorIter`]. Planning is deferred so that `PlanNode<'_>` borrows
/// from the owned data via `&self` — no self-referential struct.
pub struct Cursor<'db, 'txn, S: Store + 'db> {
    txn: &'txn S::Txn<'db>,
    cf: <S::Txn<'db> as Transaction>::Cf,
    prepared: planner::PreparedStatement,
}

impl<'db, 'txn, S: Store + 'db> Cursor<'db, 'txn, S> {
    pub(crate) fn new(
        txn: &'txn S::Txn<'db>,
        cf: <S::Txn<'db> as Transaction>::Cf,
        prepared: planner::PreparedStatement,
    ) -> Self {
        Self { txn, cf, prepared }
    }

    /// Plan the query and return a streaming iterator.
    ///
    /// `PlanNode<'_>` borrows from `&self.prepared`; the executor iterator
    /// borrows from `self.txn` and `&self.cf`. All three are alive as long
    /// as `&self` — which the caller holds.
    pub fn iter(&self) -> Result<CursorIter<'_>, DbError> {
        let plan = planner::plan(&self.prepared)?;
        let exec = Executor::new(self.txn, &self.cf);
        match exec.execute(plan)? {
            ExecutionResult::Rows(inner) => Ok(CursorIter { inner }),
            _ => unreachable!(),
        }
    }
}

/// A streaming iterator over query results.
///
/// Yields one [`RawDocumentBuf`] at a time without materializing the full result set.
pub struct CursorIter<'a> {
    inner: RawIter<'a>,
}

impl Iterator for CursorIter<'_> {
    type Item = Result<RawDocumentBuf, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Err(e) => return Some(Err(e)),
                Ok(None) => continue,
                Ok(Some(val)) => {
                    return Some(
                        val.into_document_buf()
                            .ok_or_else(|| DbError::InvalidQuery("expected document".into())),
                    );
                }
            }
        }
    }
}
