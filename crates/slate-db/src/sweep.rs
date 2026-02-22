use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Duration;

use slate_store::{Store, Transaction};

use crate::catalog::Catalog;
use crate::encoding;
use crate::error::DbError;
use crate::executor::exec;

const SYS_CF: &str = "_sys";

pub(crate) struct StoreInner<S: Store> {
    pub store: S,
    pub catalog: Catalog,
}

pub(crate) struct TtlHandle {
    shutdown: Arc<AtomicBool>,
    notify: Arc<(Mutex<()>, Condvar)>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TtlHandle {
    pub(crate) fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.notify.1.notify_one();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for TtlHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Spawn the background TTL sweep thread if an interval is configured.
/// Returns `None` when `interval_secs == u64::MAX` (no sweep).
pub(crate) fn spawn<S: Store + Send + Sync + 'static>(
    inner: Arc<StoreInner<S>>,
    interval_secs: u64,
) -> Option<TtlHandle> {
    if interval_secs == u64::MAX {
        return None;
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let notify = Arc::new((Mutex::new(()), Condvar::new()));
    let sweep_inner = inner;
    let sweep_flag = Arc::clone(&shutdown);
    let sweep_notify = Arc::clone(&notify);
    let interval = Duration::from_secs(interval_secs);
    let handle = thread::spawn(move || {
        loop {
            let (lock, cvar) = &*sweep_notify;
            let guard = lock.lock().unwrap();
            let _ = cvar.wait_timeout(guard, interval).unwrap();
            if sweep_flag.load(Ordering::Relaxed) {
                break;
            }
            let collections = match sweep_inner.store.begin(true) {
                Ok(mut txn) => match txn.cf(SYS_CF) {
                    Ok(sys) => match Catalog.list_collections(&txn, &sys) {
                        Ok(c) => {
                            let _ = txn.rollback();
                            c
                        }
                        Err(_) => continue,
                    },
                    Err(_) => continue,
                },
                Err(_) => continue,
            };
            for col in &collections {
                let _ = purge_expired(&sweep_inner, col);
            }
        }
    });

    Some(TtlHandle {
        shutdown,
        notify,
        handle: Some(handle),
    })
}

/// Purge expired documents from a collection.
pub(crate) fn purge_expired<S: Store>(
    inner: &StoreInner<S>,
    collection: &str,
) -> Result<u64, DbError> {
    let mut txn = inner.store.begin(false).map_err(DbError::Store)?;
    let now_bytes = encoding::encode_datetime_millis(bson::DateTime::now().timestamp_millis());

    let cf = txn.cf(collection).map_err(DbError::Store)?;
    let prefix = encoding::index_scan_field_prefix("ttl");

    let val_start = prefix.len();
    let val_end = val_start + 8;
    let id_start = val_end + 1;

    let mut entries: Vec<(Vec<u8>, String)> = Vec::new();
    for result in txn.scan_prefix(&cf, &prefix).map_err(DbError::Store)? {
        let (key, _) = result.map_err(DbError::Store)?;
        if key.len() < id_start {
            continue;
        }
        let value_bytes = &key[val_start..val_end];
        if value_bytes >= now_bytes.as_slice() {
            break;
        }
        let record_id = match std::str::from_utf8(&key[id_start..]) {
            Ok(s) => s,
            Err(_) => continue,
        };
        entries.push((key.to_vec(), record_id.to_string()));
    }

    let mut deleted = 0u64;
    let sys = txn.cf(SYS_CF).map_err(DbError::Store)?;
    let indexed_fields = inner.catalog.list_indexes(&txn, &sys, collection)?;

    for (ttl_key, record_id) in &entries {
        let data_key = encoding::record_key(record_id);
        if let Some(bytes) = txn.get(&cf, &data_key).map_err(DbError::Store)? {
            let raw = bson::RawDocument::from_bytes(&bytes)?;
            for field in &indexed_fields {
                for value in exec::raw_get_path_values(raw, field)? {
                    let idx_key = encoding::raw_index_key(field, value, record_id);
                    txn.delete(&cf, &idx_key).map_err(DbError::Store)?;
                }
            }
            txn.delete(&cf, &data_key).map_err(DbError::Store)?;
        }
        txn.delete(&cf, ttl_key).map_err(DbError::Store)?;
        deleted += 1;
    }

    txn.commit().map_err(DbError::Store)?;
    Ok(deleted)
}
