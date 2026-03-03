use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arc_swap::ArcSwap;
use slate_engine::{Catalog, EngineError, FunctionKind};
use slate_vm::RuntimeKind;

/// Map a stored runtime tag byte to a [`RuntimeKind`].
pub fn runtime_kind(tag: u8) -> RuntimeKind {
    #[cfg(feature = "lua")]
    if tag == 0x01 {
        return RuntimeKind::Lua;
    }

    #[cfg(feature = "js")]
    if tag == 0x03 {
        return RuntimeKind::Js;
    }

    panic!("unsupported runtime tag: {tag:#x}")
}

// ── ResolvedHook ────────────────────────────────────────────

/// A pre-resolved hook definition with everything needed to fire it.
#[derive(Debug, Clone)]
pub struct ResolvedHook {
    pub name: String,
    pub runtime: u8,
    pub source: Vec<u8>,
    pub source_hash: u64,
}

fn hash_source(source: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    source.hash(&mut hasher);
    hasher.finish()
}

// ── HookSnapshot ────────────────────────────────────────────

/// A frozen view of all hook definitions at a point in time.
///
/// Captured at `begin()` time — transactions see a consistent snapshot
/// regardless of concurrent modifications.
pub struct HookSnapshot {
    triggers: HashMap<(String, String), Vec<ResolvedHook>>,
    validators: HashMap<(String, String), Vec<ResolvedHook>>,
}

impl HookSnapshot {
    /// Build a snapshot by loading all functions from the catalog.
    pub fn load_all<T: Catalog>(txn: &T) -> Result<Self, EngineError> {
        let collections = txn.list_collections(None)?;
        let mut triggers: HashMap<(String, String), Vec<ResolvedHook>> = HashMap::new();
        let mut validators: HashMap<(String, String), Vec<ResolvedHook>> = HashMap::new();

        for handle in &collections {
            let cf = handle.cf_name().to_string();
            let name = handle.name().to_string();

            let trigger_entries = txn.load_functions(&cf, &name, FunctionKind::Trigger)?;
            if !trigger_entries.is_empty() {
                let hooks: Vec<ResolvedHook> = trigger_entries
                    .into_iter()
                    .map(|e| ResolvedHook {
                        source_hash: hash_source(&e.source),
                        name: e.name,
                        runtime: e.runtime,
                        source: e.source,
                    })
                    .collect();
                triggers.insert((cf.clone(), name.clone()), hooks);
            }

            let validator_entries = txn.load_functions(&cf, &name, FunctionKind::Validator)?;
            if !validator_entries.is_empty() {
                let hooks: Vec<ResolvedHook> = validator_entries
                    .into_iter()
                    .map(|e| ResolvedHook {
                        source_hash: hash_source(&e.source),
                        name: e.name,
                        runtime: e.runtime,
                        source: e.source,
                    })
                    .collect();
                validators.insert((cf, name), hooks);
            }
        }

        Ok(Self {
            triggers,
            validators,
        })
    }

    /// An empty snapshot with no hooks.
    pub fn empty() -> Self {
        Self {
            triggers: HashMap::new(),
            validators: HashMap::new(),
        }
    }

    /// Get triggers for a (cf, collection) pair.
    pub fn triggers_for(&self, cf: &str, collection: &str) -> &[ResolvedHook] {
        self.triggers
            .get(&(cf.to_string(), collection.to_string()))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get validators for a (cf, collection) pair.
    pub fn validators_for(&self, cf: &str, collection: &str) -> &[ResolvedHook] {
        self.validators
            .get(&(cf.to_string(), collection.to_string()))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

// ── HookRegistry ────────────────────────────────────────────

/// Lock-free, swappable registry of hook definitions.
///
/// Lives on `Database`. Readers call `snapshot()` to get an `Arc<HookSnapshot>`
/// that won't change under their feet. Writers call `swap()` after committing
/// function changes.
pub struct HookRegistry {
    inner: ArcSwap<HookSnapshot>,
}

impl HookRegistry {
    pub fn new(snapshot: HookSnapshot) -> Self {
        Self {
            inner: ArcSwap::from_pointee(snapshot),
        }
    }

    /// Get a snapshot of the current hook state.
    pub fn snapshot(&self) -> Arc<HookSnapshot> {
        self.inner.load_full()
    }

    /// Replace the current snapshot with a new one.
    pub fn swap(&self, snapshot: HookSnapshot) {
        self.inner.store(Arc::new(snapshot));
    }
}
