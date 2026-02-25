use bson::RawDocumentBuf;
use slate_engine::{EngineTransaction, KvEngine};
use slate_store::Store;

use crate::error::DbError;
use crate::executor::{Executor, RawIter};
use crate::planner::plan::Plan;

type KvTxn<'a, S> = <KvEngine<S> as slate_engine::Engine>::Txn<'a>;

/// A prepared query that can be iterated or executed.
///
/// Owns a pre-built `Plan` and a reference to the transaction.
/// Call [`.iter()`](Cursor::iter) for streaming iteration, or
/// [`.execute()`](Cursor::execute) to drain and return a count.
pub struct Cursor<'db: 'txn, 'txn, S: Store + 'db> {
    txn: &'txn KvTxn<'db, S>,
    plan: Plan<<KvTxn<'db, S> as EngineTransaction>::Cf>,
}

impl<'db: 'txn, 'txn, S: Store + 'db> Cursor<'db, 'txn, S> {
    pub(crate) fn new(
        txn: &'txn KvTxn<'db, S>,
        plan: Plan<<KvTxn<'db, S> as EngineTransaction>::Cf>,
    ) -> Self {
        Self { txn, plan }
    }

    /// Consume the cursor and return a streaming iterator over documents.
    pub fn iter(self) -> Result<CursorIter<'txn>, DbError> {
        let iter = Executor::new(self.txn).execute(self.plan)?;
        Ok(CursorIter { inner: iter })
    }

    /// Consume the cursor, drain all rows, and return the count of affected rows.
    pub fn execute(self) -> Result<u64, DbError> {
        let iter = Executor::new(self.txn).execute(self.plan)?;
        let mut count = 0u64;
        for result in iter {
            result?;
            count += 1;
        }
        Ok(count)
    }
}

/// A streaming iterator over query results.
///
/// Each call to [`next()`](Iterator::next) returns the next document
/// lazily from the underlying query pipeline.
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
                Ok(Some(bson::RawBson::Document(buf))) => {
                    return Some(Ok(buf));
                }
                Ok(Some(_)) => {
                    return Some(Err(DbError::InvalidQuery("expected document".into())));
                }
            }
        }
    }
}
