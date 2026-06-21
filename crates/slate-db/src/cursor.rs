use std::marker::PhantomData;

use bson::RawDocumentBuf;
use serde::de::DeserializeOwned;
use slate_engine::{EngineTransaction, KvEngine};
use slate_store::Store;

use crate::error::DbError;
use slate_vm::pool::VmPool;

type KvTxn<'a, S> = <KvEngine<S> as slate_engine::Engine>::Txn<'a>;

/// The streamed unit out of execution: a value (`Some`), an undefined row
/// (`None`, dropped at the output boundary), or an error.
type RawIter<'a> = Box<dyn Iterator<Item = Result<Option<bson::RawBson>, DbError>> + 'a>;

/// A prepared query that can be iterated or executed.
///
/// Owns a pre-built plan and a reference to the transaction. Call
/// [`.iter()`](Cursor::iter) for deserialized iteration,
/// [`.iter_raw()`](Cursor::iter_raw) for raw BSON documents, or
/// [`.drain()`](Cursor::drain) to consume all rows and return a count.
pub struct Cursor<'db: 'txn, 'txn, S: Store + 'db> {
    txn: &'txn KvTxn<'db, S>,
    plan: slate_planner::Plan,
    pool: Option<&'txn VmPool>,
    /// SQL `@`-parameter values, supplied by `query_with_params`. Owned here and
    /// shared into the executor (by `Rc`) at execution time.
    params: Option<RawDocumentBuf>,
}

impl<'db: 'txn, 'txn, S: Store + 'db> Cursor<'db, 'txn, S> {
    pub(crate) fn new(
        txn: &'txn KvTxn<'db, S>,
        plan: slate_planner::Plan,
        pool: Option<&'txn VmPool>,
    ) -> Self {
        Self {
            txn,
            plan,
            pool,
            params: None,
        }
    }

    pub(crate) fn new_with_params(
        txn: &'txn KvTxn<'db, S>,
        plan: slate_planner::Plan,
        pool: Option<&'txn VmPool>,
        params: RawDocumentBuf,
    ) -> Self {
        Self {
            txn,
            plan,
            pool,
            params: Some(params),
        }
    }

    /// Execute the plan, streaming `Result<Option<RawBson>, DbError>`.
    fn execute(self) -> Result<RawIter<'txn>, DbError> {
        // Inject `$now` (epoch ms, captured when the txn began from the engine's
        // injectable clock) so the SQL `GETCURRENT*` functions resolve against
        // it — consistent across the txn and wasm-clean (no syscall in the
        // evaluator). Threaded via the existing params channel, so it reaches
        // every evaluating node.
        let mut doc: bson::Document = match &self.params {
            Some(p) => bson::deserialize_from_slice(p.as_bytes())?,
            None => bson::Document::new(),
        };
        doc.insert("$now", self.txn.now_millis());
        let params = Some(std::rc::Rc::new(bson::serialize_to_raw_document_buf(&doc)?));
        let iter = slate_executor::Executor::with_pool_and_params(self.txn, self.pool, params)
            .execute(self.plan)?;
        Ok(Box::new(iter.map(|r| r.map_err(DbError::from))))
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

    /// Iterate the result *values*, deserializing each into `T`.
    ///
    /// Unlike [`iter`](Self::iter), this accepts non-document values — e.g. a
    /// SQL `SELECT VALUE c.name` yields strings — so it is the accessor for SQL
    /// scalar projections. For `find` (always documents) prefer [`iter`](Self::iter).
    pub fn iter_values<T: DeserializeOwned>(self) -> Result<ValuesIter<'txn, T>, DbError> {
        Ok(ValuesIter {
            inner: self.iter_raw_values()?,
            _marker: PhantomData,
        })
    }

    /// Iterate the raw result values (scalars, documents, or arrays) with no
    /// deserialization. The value-level counterpart of [`iter_raw`](Self::iter_raw).
    pub fn iter_raw_values(self) -> Result<RawValuesIter<'txn>, DbError> {
        Ok(RawValuesIter {
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

/// A streaming iterator over raw result *values* — any `RawBson` (scalar,
/// document, or array), not just documents. Undefined rows are skipped.
pub struct RawValuesIter<'a> {
    inner: RawIter<'a>,
}

impl Iterator for RawValuesIter<'_> {
    type Item = Result<bson::RawBson, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Err(e) => return Some(Err(e)),
                Ok(None) => continue, // undefined — dropped at the output boundary
                Ok(Some(value)) => return Some(Ok(value)),
            }
        }
    }
}

/// A streaming iterator that deserializes each result *value* into `T`.
pub struct ValuesIter<'a, T> {
    inner: RawValuesIter<'a>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned> Iterator for ValuesIter<'_, T> {
    type Item = Result<T, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = self.inner.next()?;
        Some(raw.and_then(|value| {
            let bson = bson::Bson::try_from(value.as_raw_bson_ref()).map_err(DbError::from)?;
            bson::deserialize_from_bson(bson).map_err(DbError::from)
        }))
    }
}
