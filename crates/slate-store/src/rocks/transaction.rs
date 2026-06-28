use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, Direction, ErrorKind, IteratorMode, MultiThreaded, OptimisticTransactionDB,
    OptimisticTransactionOptions, Options, ReadOptions, WriteOptions,
};

use crate::error::StoreError;
use crate::store::{Durability, Transaction, increment_prefix};

type DB = OptimisticTransactionDB<MultiThreaded>;

/// Build the `WriteOptions` that realize a [`Durability`] level on RocksDB.
///
/// The transaction's `commit()` honors the `WriteOptions` it was created with:
/// - `Strict`   → `set_sync(true)`: fsync the WAL before commit returns.
/// - `Buffered` → defaults: WAL on, no fsync (survives a crash, not power loss).
/// - `Relaxed`  → `disable_wal(true)`: skip the WAL entirely (fastest, least safe).
fn write_options_for(durability: Durability) -> WriteOptions {
    let mut opts = WriteOptions::new();
    match durability {
        Durability::Strict => opts.set_sync(true),
        Durability::Buffered => {}
        Durability::Relaxed => opts.disable_wal(true),
    }
    opts
}

/// Begin a fresh inner rocksdb transaction with the given durability's
/// `WriteOptions`. Used at construction and to re-create the (empty) inner txn
/// when `set_durability` changes the level before any writes.
fn begin_inner(db: &DB, durability: Durability) -> rocksdb::Transaction<'_, DB> {
    // `set_snapshot(true)` pins a snapshot at `begin`. Reads threaded through it
    // (see `RocksTransaction::read_options`) observe that one consistent view —
    // snapshot isolation — and commit-time conflict detection validates the
    // write set against the begin sequence. Without it, optimistic transactions
    // read latest-committed (read-committed) and validate per-key-first-access,
    // which would not match the snapshot-isolation contract memory/redb provide.
    let mut txn_opts = OptimisticTransactionOptions::default();
    txn_opts.set_snapshot(true);
    db.transaction_opt(&write_options_for(durability), &txn_opts)
}

/// Translate a RocksDB commit error into a [`StoreError`].
///
/// `OptimisticTransactionDB` validates a transaction's write set at commit and
/// reports a write-write conflict as [`ErrorKind::Busy`]; [`ErrorKind::TryAgain`]
/// means the memtable history was too short to *prove* there was no conflict.
/// Both are resolved by retrying the whole transaction, so both surface as
/// [`StoreError::Conflict`] — the distinct, retryable shape callers need.
/// Anything else is a genuine storage failure and keeps its message.
fn map_commit_error(e: rocksdb::Error) -> StoreError {
    match e.kind() {
        ErrorKind::Busy | ErrorKind::TryAgain => StoreError::Conflict,
        _ => StoreError::Storage(e.to_string()),
    }
}

/// Build a `ReadOptions` pinned to `txn`'s begin snapshot — the single place the
/// snapshot is threaded into reads, so every read observes one consistent view
/// (snapshot isolation) layered with the transaction's own staged writes
/// (read-your-writes). A read that used latest-committed instead would silently
/// downgrade the transaction to read-committed.
///
/// `snapshot()` only borrows `txn` for this call; `set_snapshot` copies the
/// underlying begin-snapshot pointer into the options, and that pointer stays
/// valid for `txn`'s whole life (the transaction owns the snapshot until
/// commit/rollback). So the options can be cached alongside `txn` and reused for
/// every read — they outlive the transient `snapshot()` wrapper, and dropping a
/// `ReadOptions` frees only its own handle, never the snapshot.
fn pinned_read_options(txn: &rocksdb::Transaction<'_, DB>) -> ReadOptions {
    let mut opts = ReadOptions::default();
    opts.set_snapshot(&txn.snapshot());
    opts
}

/// Pre-resolved column family handle for reads.
#[derive(Clone)]
pub struct RocksCf<'db> {
    handle: Arc<BoundColumnFamily<'db>>,
}

pub struct RocksTransaction<'db> {
    txn: Option<rocksdb::Transaction<'db, DB>>,
    db: &'db DB,
    read_only: bool,
    durability: Durability,
    /// `ReadOptions` pinned to the begin snapshot, built once and reused by every
    /// point read (`get` / `multi_get`) so they pay no per-read allocation.
    /// Rebuilt whenever the inner txn is re-created (see `set_durability`). The
    /// scan paths can't share it (their `iterator_cf_opt` takes `ReadOptions` by
    /// value), so they mint a fresh one via [`pinned_read_options`] — amortized
    /// over the whole scan, it's free.
    read_opts: ReadOptions,
    cf_cache: RefCell<HashMap<String, Arc<BoundColumnFamily<'db>>>>,
}

impl<'db> RocksTransaction<'db> {
    pub fn new(db: &'db DB, read_only: bool, durability: Durability) -> Result<Self, StoreError> {
        let txn = begin_inner(db, durability);
        let read_opts = pinned_read_options(&txn);
        Ok(Self {
            txn: Some(txn),
            db,
            read_only,
            durability,
            read_opts,
            cf_cache: RefCell::new(HashMap::new()),
        })
    }

    fn txn(&self) -> Result<&rocksdb::Transaction<'db, DB>, StoreError> {
        self.txn.as_ref().ok_or(StoreError::TransactionConsumed)
    }

    /// A freshly-allocated `ReadOptions` pinned to the begin snapshot, for the
    /// scan paths whose `iterator_cf_opt` consumes the options by value. Point
    /// reads reuse the cached `read_opts` field instead.
    fn read_options(&self) -> Result<ReadOptions, StoreError> {
        Ok(pinned_read_options(self.txn()?))
    }

    fn check_writable(&self) -> Result<(), StoreError> {
        if self.read_only {
            return Err(StoreError::ReadOnly);
        }
        Ok(())
    }

    /// Resolve a CF handle, caching it for reuse.
    fn cf_handle(&self, cf: &str) -> Result<Arc<BoundColumnFamily<'db>>, StoreError> {
        if let Some(handle) = self.cf_cache.borrow().get(cf) {
            return Ok(Arc::clone(handle));
        }
        let handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        self.cf_cache
            .borrow_mut()
            .insert(cf.to_string(), Arc::clone(&handle));
        Ok(handle)
    }
}

impl<'db> Transaction for RocksTransaction<'db> {
    type Cf = RocksCf<'db>;

    fn cf(&self, name: &str) -> Result<Self::Cf, StoreError> {
        let handle = self.cf_handle(name)?;
        Ok(RocksCf { handle })
    }

    fn get(&self, cf: &Self::Cf, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let data = self
            .txn()?
            .get_cf_opt(&cf.handle, key, &self.read_opts)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(data)
    }

    fn multi_get(&self, cf: &Self::Cf, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, StoreError> {
        let txn = self.txn()?;
        let cf_keys: Vec<_> = keys.iter().map(|k| (&cf.handle, *k)).collect();
        let results = txn.multi_get_cf_opt(cf_keys, &self.read_opts);
        results
            .into_iter()
            .map(|r| r.map_err(|e| StoreError::Storage(e.to_string())))
            .collect()
    }

    fn scan_prefix<'a>(
        &'a self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>
    {
        let prefix_owned = prefix.to_vec();
        let readopts = self.read_options()?;
        let iter = self.txn()?.iterator_cf_opt(
            &cf.handle,
            readopts,
            IteratorMode::From(prefix, Direction::Forward),
        );
        Ok(Box::new(
            iter.take_while(move |item| match item {
                Ok((key, _)) => key.starts_with(&prefix_owned),
                Err(_) => true,
            })
            .map(|item| {
                item.map(|(k, v)| (k.into_vec(), v.into_vec()))
                    .map_err(|e| StoreError::Storage(e.to_string()))
            }),
        ))
    }

    fn scan_prefix_rev<'a>(
        &'a self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>
    {
        let prefix_owned = prefix.to_vec();
        let upper = increment_prefix(prefix);
        let mode = match upper.as_deref() {
            Some(u) => IteratorMode::From(u, Direction::Reverse),
            None => IteratorMode::End,
        };
        let readopts = self.read_options()?;
        let iter = self.txn()?.iterator_cf_opt(&cf.handle, readopts, mode);
        Ok(Box::new(
            iter.take_while(move |item| match item {
                Ok((key, _)) => key.starts_with(&prefix_owned),
                Err(_) => true,
            })
            .map(|item| {
                item.map(|(k, v)| (k.into_vec(), v.into_vec()))
                    .map_err(|e| StoreError::Storage(e.to_string()))
            }),
        ))
    }

    fn scan_range<'a, R: RangeBounds<Vec<u8>>>(
        &'a self,
        cf: &Self::Cf,
        range: R,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>
    {
        // RocksDB iterators take a seek point + direction, not a RangeBounds, so
        // resolve the bounds to owned `Vec<u8>`s captured into the closures.
        let lo = range.start_bound().cloned();
        let hi = range.end_bound().cloned();
        // One snapshot-pinned options for whichever direction runs; moved into
        // the single `iterator_cf_opt` call the chosen branch makes.
        let readopts = self.read_options()?;
        let txn = self.txn()?;

        if reverse {
            // Seek descending from the upper bound; the start bound stops us.
            let mode = match &hi {
                Bound::Included(e) | Bound::Excluded(e) => {
                    IteratorMode::From(e.as_slice(), Direction::Reverse)
                }
                Bound::Unbounded => IteratorMode::End,
            };
            let iter = txn.iterator_cf_opt(&cf.handle, readopts, mode);
            // Drop an excluded upper-bound key (the seek lands exactly on it).
            let hi_excluded = match hi {
                Bound::Excluded(e) => Some(e),
                _ => None,
            };
            let mapped = iter
                .skip_while(move |item| match item {
                    Ok((key, _)) => hi_excluded
                        .as_ref()
                        .is_some_and(|e| key.as_ref() == e.as_slice()),
                    Err(_) => false,
                })
                .take_while(move |item| match item {
                    Ok((key, _)) => match &lo {
                        Bound::Included(s) => key.as_ref() >= s.as_slice(),
                        Bound::Excluded(s) => key.as_ref() > s.as_slice(),
                        Bound::Unbounded => true,
                    },
                    Err(_) => true,
                })
                .map(|item| {
                    item.map(|(k, v)| (k.into_vec(), v.into_vec()))
                        .map_err(|e| StoreError::Storage(e.to_string()))
                });
            Ok(Box::new(mapped))
        } else {
            // Seek ascending from the lower bound; the end bound stops us.
            let mode = match &lo {
                Bound::Included(s) | Bound::Excluded(s) => {
                    IteratorMode::From(s.as_slice(), Direction::Forward)
                }
                Bound::Unbounded => IteratorMode::Start,
            };
            let iter = txn.iterator_cf_opt(&cf.handle, readopts, mode);
            // Drop an excluded lower-bound key (the seek lands exactly on it).
            let lo_excluded = match lo {
                Bound::Excluded(s) => Some(s),
                _ => None,
            };
            let mapped = iter
                .skip_while(move |item| match item {
                    Ok((key, _)) => lo_excluded
                        .as_ref()
                        .is_some_and(|s| key.as_ref() == s.as_slice()),
                    Err(_) => false,
                })
                .take_while(move |item| match item {
                    Ok((key, _)) => match &hi {
                        Bound::Included(e) => key.as_ref() <= e.as_slice(),
                        Bound::Excluded(e) => key.as_ref() < e.as_slice(),
                        Bound::Unbounded => true,
                    },
                    Err(_) => true,
                })
                .map(|item| {
                    item.map(|(k, v)| (k.into_vec(), v.into_vec()))
                        .map_err(|e| StoreError::Storage(e.to_string()))
                });
            Ok(Box::new(mapped))
        }
    }

    fn put(&self, cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.txn()?
            .put_cf(&cf.handle, key, value)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn put_batch(&self, cf: &Self::Cf, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        let txn = self.txn()?;
        for (key, value) in entries {
            txn.put_cf(&cf.handle, key, value)
                .map_err(|e| StoreError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn delete(&self, cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.txn()?
            .delete_cf(&cf.handle, key)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn delete_batch(&self, cf: &Self::Cf, keys: &[&[u8]]) -> Result<(), StoreError> {
        self.check_writable()?;
        let txn = self.txn()?;
        for key in keys {
            txn.delete_cf(&cf.handle, key)
                .map_err(|e| StoreError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn create_cf(&self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        if self.db.cf_handle(name).is_none() {
            let opts = Options::default();
            self.db
                .create_cf(name, &opts)
                .map_err(|e| StoreError::Storage(e.to_string()))?;
        }
        // Pre-warm cache for the newly created CF
        if let Some(handle) = self.db.cf_handle(name) {
            self.cf_cache.borrow_mut().insert(name.to_string(), handle);
        }
        Ok(())
    }

    fn drop_cf(&self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        self.cf_cache.borrow_mut().remove(name);
        self.db
            .drop_cf(name)
            .map_err(|e| StoreError::Storage(e.to_string()))
    }

    fn set_durability(&mut self, durability: Durability) {
        // RocksDB fixes the flush policy in the transaction's WriteOptions at
        // creation, so changing the level means re-creating the inner txn. This
        // is intended to be called immediately after `begin` (before any
        // writes), where the txn is empty and re-creating it is free of any
        // staged changes; the level is otherwise inherited from the store.
        if self.durability == durability {
            return;
        }
        self.durability = durability;
        // A read-only txn makes no durability promise — keep its (empty) inner
        // txn untouched. Otherwise swap in a fresh inner txn with the new level,
        // and re-pin `read_opts` to the *new* txn's begin snapshot before the old
        // txn drops (releasing the old snapshot the stale options point at).
        if !self.read_only && self.txn.is_some() {
            let txn = begin_inner(self.db, durability);
            self.read_opts = pinned_read_options(&txn);
            self.txn = Some(txn);
        }
    }

    fn commit(mut self) -> Result<(), StoreError> {
        let txn = self.txn.take().ok_or(StoreError::TransactionConsumed)?;
        txn.commit().map_err(map_commit_error)?;
        Ok(())
    }

    fn rollback(mut self) -> Result<(), StoreError> {
        let txn = self.txn.take().ok_or(StoreError::TransactionConsumed)?;
        txn.rollback()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }
}
