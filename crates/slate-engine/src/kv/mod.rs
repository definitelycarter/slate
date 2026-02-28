mod catalog;
mod transaction;

pub use transaction::KvTransaction;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use slate_store::Store;

use crate::error::EngineError;

pub const SYS_CF: &str = "_sys_";
pub(crate) const DEFAULT_CF: &str = "default_cf";

/// Serializable collection metadata stored in the `_sys_` CF.
#[derive(Serialize, Deserialize)]
pub(crate) struct CollectionMeta {
    pub cf: String,
    pub pk: String,
    pub ttl: String,
}

fn default_clock() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ── KvEngine ───────────────────────────────────────────────────

pub struct KvEngine<S> {
    store: S,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl<S: Store> KvEngine<S> {
    pub fn new(store: S) -> Self {
        let _ = store.create_cf(SYS_CF);
        Self {
            store,
            clock: Arc::new(default_clock),
        }
    }

    pub fn with_clock(store: S, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        let _ = store.create_cf(SYS_CF);
        Self {
            store,
            clock: Arc::new(clock),
        }
    }
}

impl<S: Store> crate::traits::Engine for KvEngine<S> {
    type Txn<'a>
        = KvTransaction<'a, S>
    where
        S: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, EngineError> {
        let now_millis = (self.clock)();
        let txn = self.store.begin(read_only)?;
        Ok(KvTransaction { txn, now_millis })
    }
}
