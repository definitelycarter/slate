use bson::RawDocumentBuf;
use slate_store::{Store, Transaction};

use crate::error::DbError;
use crate::executor::{ExecutionResult, Executor, RawIter};
use crate::planner;

/// A prepared query that can be iterated over.
///
/// Borrows the transaction and owns the statement + planner inputs.
/// Call [`.iter()`](Cursor::iter) to plan and produce a streaming
/// [`CursorIter`]. Planning is deferred so that `PlanNode<'_>` borrows
/// from the owned `Statement` via `&self` — no self-referential struct.
pub struct Cursor<'db, 'txn, S: Store + 'db> {
    txn: &'txn S::Txn<'db>,
    cf: <S::Txn<'db> as Transaction>::Cf,
    collection: String,
    indexed_fields: Vec<String>,
    statement: planner::Statement,
}

impl<'db, 'txn, S: Store + 'db> Cursor<'db, 'txn, S> {
    pub(crate) fn new(
        txn: &'txn S::Txn<'db>,
        cf: <S::Txn<'db> as Transaction>::Cf,
        collection: String,
        indexed_fields: Vec<String>,
        statement: planner::Statement,
    ) -> Self {
        Self {
            txn,
            cf,
            collection,
            indexed_fields,
            statement,
        }
    }

    /// Plan the query and return a streaming iterator.
    ///
    /// `PlanNode<'_>` borrows from `&self.statement`; the executor iterator
    /// borrows from `self.txn` and `&self.cf`. All three are alive as long
    /// as `&self` — which the caller holds.
    pub fn iter(&self) -> Result<CursorIter<'_>, DbError> {
        let plan = planner::plan(
            &self.collection,
            self.indexed_fields.clone(),
            &self.statement,
        )?;
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
