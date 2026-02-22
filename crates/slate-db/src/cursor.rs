use bson::RawDocumentBuf;
use slate_store::{Store, Transaction};

use crate::error::DbError;
use crate::executor::{ExecutionResult, Executor, RawIter};
use crate::planner::PlanNode;

/// A prepared query that can be iterated over.
///
/// Owns the query plan and column family handle, borrows the transaction.
/// Call [`.iter()`](Cursor::iter) to produce a streaming [`CursorIter`].
pub struct Cursor<'db, 'txn, S: Store + 'db> {
    txn: &'txn S::Txn<'db>,
    cf: <S::Txn<'db> as Transaction>::Cf,
    plan: PlanNode,
}

impl<'db, 'txn, S: Store + 'db> Cursor<'db, 'txn, S> {
    pub(crate) fn new(
        txn: &'txn S::Txn<'db>,
        cf: <S::Txn<'db> as Transaction>::Cf,
        plan: PlanNode,
    ) -> Self {
        Self { txn, cf, plan }
    }

    /// Create a streaming iterator over the query results.
    pub fn iter(&self) -> Result<CursorIter<'_>, DbError> {
        let exec = Executor::new(self.txn, &self.cf);
        match exec.execute(&self.plan)? {
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
