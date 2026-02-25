use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Duration;

use slate_engine::{Engine, KvEngine};
use slate_store::Store;

use crate::database::Transaction;

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
    engine: Arc<KvEngine<S>>,
    interval_secs: u64,
) -> Option<TtlHandle> {
    if interval_secs == u64::MAX {
        return None;
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let notify = Arc::new((Mutex::new(()), Condvar::new()));
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
            let collections = {
                let txn = match engine.begin(true) {
                    Ok(t) => Transaction::from_engine_txn(t),
                    Err(_) => continue,
                };
                let names = match txn.list_collections() {
                    Ok(n) => n,
                    Err(_) => {
                        let _ = txn.rollback();
                        continue;
                    }
                };
                let _ = txn.rollback();
                names
            };
            for col in &collections {
                let txn = match engine.begin(false) {
                    Ok(t) => Transaction::from_engine_txn(t),
                    Err(_) => continue,
                };
                let mut txn = txn;
                match txn.purge_expired(col) {
                    Ok(_) => {
                        let _ = txn.commit();
                    }
                    Err(_) => {
                        let _ = txn.rollback();
                    }
                }
            }
        }
    });

    Some(TtlHandle {
        shutdown,
        notify,
        handle: Some(handle),
    })
}
