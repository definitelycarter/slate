use std::sync::Arc;

use slate_store::Store;

use crate::engine::{SlateEngine, Transaction};
use crate::error::DbError;
use crate::sweep::{self, TtlHandle};

pub struct DatabaseConfig {
    /// Interval in seconds between TTL sweep runs.
    pub ttl_sweep_interval_secs: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            ttl_sweep_interval_secs: u64::MAX,
        }
    }
}

pub struct Database<S: Store> {
    engine: Arc<SlateEngine<S>>,
    ttl_handle: Option<TtlHandle>,
}

impl<S: Store> Database<S> {
    pub fn begin(&self, read_only: bool) -> Result<Transaction<'_, S>, DbError> {
        self.engine.begin(read_only)
    }

    /// Purge expired documents from a collection.
    pub fn purge_expired(&self, collection: &str) -> Result<u64, DbError> {
        self.engine.purge_expired(collection)
    }

    /// Gracefully stop background tasks.
    pub fn shutdown(&mut self) {
        if let Some(mut handle) = self.ttl_handle.take() {
            handle.stop();
        }
    }
}

impl<S: Store + Send + Sync + 'static> Database<S> {
    pub fn open(store: S, config: DatabaseConfig) -> Self {
        let engine = Arc::new(SlateEngine::new(store));
        let ttl_handle = sweep::spawn(Arc::clone(&engine), config.ttl_sweep_interval_secs);
        Self { engine, ttl_handle }
    }
}
