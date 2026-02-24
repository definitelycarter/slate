use bson::RawDocumentBuf;
use slate_store::{Store, Transaction};

use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::{Executor, RawIter};
use crate::planner::{Planner, Statement};

/// A prepared query that can be iterated or executed.
///
/// Owns a catalog reference, collection name, and `Statement`.
/// Call [`.iter()`](Cursor::iter) for streaming iteration, or
/// [`.execute()`](Cursor::execute) to drain and return a count.
pub struct Cursor<'db, 'txn, S: Store + 'db> {
    txn: &'txn S::Txn<'db>,
    cf: <S::Txn<'db> as Transaction>::Cf,
    catalog: &'txn Catalog,
    collection: String,
    statement: Statement,
}

impl<'db, 'txn, S: Store + 'db> Cursor<'db, 'txn, S> {
    pub(crate) fn new(
        txn: &'txn S::Txn<'db>,
        cf: <S::Txn<'db> as Transaction>::Cf,
        catalog: &'txn Catalog,
        collection: String,
        statement: Statement,
    ) -> Self {
        Self {
            txn,
            cf,
            catalog,
            collection,
            statement,
        }
    }

    /// Plan the query and return a streaming iterator.
    pub fn iter(&self) -> Result<CursorIter<'_>, DbError> {
        let planner = Planner::new(self.txn, self.catalog);
        let plan = planner.plan(&self.collection, self.statement.clone())?;
        let exec = Executor::new(self.txn, &self.cf);
        let inner = exec.execute(plan)?;
        Ok(CursorIter { inner })
    }

    /// Consume the cursor, drain all rows, and return the count of affected rows.
    pub fn execute(self) -> Result<u64, DbError> {
        let planner = Planner::new(self.txn, self.catalog);
        let plan = planner.plan(&self.collection, self.statement)?;
        let exec = Executor::new(self.txn, &self.cf);
        let iter = exec.execute(plan)?;
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
