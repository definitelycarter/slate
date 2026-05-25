use std::marker::PhantomData;

use bson::RawDocumentBuf;
use serde::de::DeserializeOwned;
use slate_engine::{EngineTransaction, KvEngine};
use slate_store::Store;

use crate::error::DbError;
use crate::executor::{Executor, RawIter};
use crate::planner::plan::Plan;
use slate_vm::pool::VmPool;

type KvTxn<'a, S> = <KvEngine<S> as slate_engine::Engine>::Txn<'a>;

/// A prepared query that can be iterated or executed.
///
/// Owns a pre-built `Plan` and a reference to the transaction.
/// Call [`.iter()`](Cursor::iter) for deserialized iteration,
/// [`.iter_raw()`](Cursor::iter_raw) for raw BSON documents, or
/// [`.drain()`](Cursor::drain) to consume all rows and return a count.
pub struct Cursor<'db: 'txn, 'txn, S: Store + 'db> {
    txn: &'txn KvTxn<'db, S>,
    plan: Plan<<KvTxn<'db, S> as EngineTransaction>::Cf>,
    pool: Option<&'txn VmPool>,
}

impl<'db: 'txn, 'txn, S: Store + 'db> Cursor<'db, 'txn, S> {
    pub(crate) fn new(
        txn: &'txn KvTxn<'db, S>,
        plan: Plan<<KvTxn<'db, S> as EngineTransaction>::Cf>,
        pool: Option<&'txn VmPool>,
    ) -> Self {
        Self { txn, plan, pool }
    }

    /// Consume the cursor and return a streaming iterator that deserializes each document into `T`.
    pub fn iter<T: DeserializeOwned>(self) -> Result<CursorIter<'txn, T>, DbError> {
        let iter = Executor::new(self.txn, self.pool).execute(self.plan)?;
        Ok(CursorIter {
            inner: RawCursorIter { inner: iter },
            _marker: PhantomData,
        })
    }

    /// Consume the cursor and return a streaming iterator over raw BSON documents.
    pub fn iter_raw(self) -> Result<RawCursorIter<'txn>, DbError> {
        let iter = Executor::new(self.txn, self.pool).execute(self.plan)?;
        Ok(RawCursorIter { inner: iter })
    }

    /// Consume the cursor, drain all rows, and return the count of affected rows.
    pub fn drain(self) -> Result<u64, DbError> {
        let iter = Executor::new(self.txn, self.pool).execute(self.plan)?;
        let mut count = 0u64;
        for result in iter {
            result?;
            count += 1;
        }
        Ok(count)
    }
}

/// A streaming iterator that deserializes each document into `T`.
pub struct CursorIter<'a, T> {
    inner: RawCursorIter<'a>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned> Iterator for CursorIter<'_, T> {
    type Item = Result<T, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = self.inner.next()?;
        Some(
            raw.and_then(|buf| bson::deserialize_from_slice(buf.as_bytes()).map_err(DbError::from)),
        )
    }
}

/// A streaming iterator over raw BSON documents.
pub struct RawCursorIter<'a> {
    inner: RawIter<'a>,
}

impl Iterator for RawCursorIter<'_> {
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
