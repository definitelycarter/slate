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

/// A prepared plan from either query engine.
enum Prepared<Cf: Clone> {
    V1(Plan<Cf>),
    V2(slate_planner::Plan),
}

/// A prepared query that can be iterated or executed.
///
/// Owns a pre-built plan (from either engine) and a reference to the
/// transaction. Call [`.iter()`](Cursor::iter) for deserialized iteration,
/// [`.iter_raw()`](Cursor::iter_raw) for raw BSON documents, or
/// [`.drain()`](Cursor::drain) to consume all rows and return a count.
pub struct Cursor<'db: 'txn, 'txn, S: Store + 'db> {
    txn: &'txn KvTxn<'db, S>,
    plan: Prepared<<KvTxn<'db, S> as EngineTransaction>::Cf>,
    pool: Option<&'txn VmPool>,
}

impl<'db: 'txn, 'txn, S: Store + 'db> Cursor<'db, 'txn, S> {
    pub(crate) fn new(
        txn: &'txn KvTxn<'db, S>,
        plan: Plan<<KvTxn<'db, S> as EngineTransaction>::Cf>,
        pool: Option<&'txn VmPool>,
    ) -> Self {
        Self {
            txn,
            plan: Prepared::V1(plan),
            pool,
        }
    }

    pub(crate) fn new_v2(
        txn: &'txn KvTxn<'db, S>,
        plan: slate_planner::Plan,
        pool: Option<&'txn VmPool>,
    ) -> Self {
        Self {
            txn,
            plan: Prepared::V2(plan),
            pool,
        }
    }

    /// Execute the plan on the appropriate engine, normalizing both to a
    /// `RawIter` of `Result<Option<RawBson>, DbError>`.
    fn execute(self) -> Result<RawIter<'txn>, DbError> {
        match self.plan {
            Prepared::V1(plan) => Executor::new(self.txn, self.pool).execute(plan),
            Prepared::V2(plan) => {
                let iter =
                    slate_executor::Executor::with_pool(self.txn, self.pool).execute(plan)?;
                Ok(Box::new(iter.map(|r| r.map_err(DbError::from))))
            }
        }
    }

    /// Consume the cursor and return a streaming iterator that deserializes each document into `T`.
    pub fn iter<T: DeserializeOwned>(self) -> Result<CursorIter<'txn, T>, DbError> {
        Ok(CursorIter {
            inner: RawCursorIter {
                inner: self.execute()?,
            },
            _marker: PhantomData,
        })
    }

    /// Consume the cursor and return a streaming iterator over raw BSON documents.
    pub fn iter_raw(self) -> Result<RawCursorIter<'txn>, DbError> {
        Ok(RawCursorIter {
            inner: self.execute()?,
        })
    }

    /// Consume the cursor, drain all rows, and return the count of affected rows.
    pub fn drain(self) -> Result<u64, DbError> {
        let iter = self.execute()?;
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
