use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arc_swap::ArcSwap;
use slate_engine::{Catalog, EngineError, FunctionKind, runtime_tag};

// `ResolvedHook` now lives in `slate-vm` (shared with the executor).
pub use slate_vm::ResolvedHook;

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
    /// Per-collection validator *bindings*: an ordered list of
    /// `(validator_name, native_function_name)`. Like the UDF bindings below — and
    /// unlike triggers, still carried as `ResolvedHook` source — a native
    /// validator is resolved from the live bag at exec time, so the snapshot holds
    /// only the name mapping, no source.
    validators: HashMap<(String, String), Vec<(String, String)>>,
    /// Per-collection UDF *bindings*: `query_name -> native_function_name`. This
    /// is the resolved identity the planner bakes into a plan — the UDF analog
    /// of the trigger `ResolvedHook` lists above — while the live bag supplies the
    /// code at exec time. Deliberately *not* a `ResolvedHook`: a binding is just a
    /// name mapping, with no source.
    udf_bindings: HashMap<(String, String), HashMap<String, String>>,
}

impl HookSnapshot {
    /// Build a snapshot by loading all functions from the catalog.
    pub fn load_all<T: Catalog>(txn: &T) -> Result<Self, EngineError> {
        let collections = txn.list_collections(None)?;
        let mut triggers: HashMap<(String, String), Vec<ResolvedHook>> = HashMap::new();
        let mut validators: HashMap<(String, String), Vec<(String, String)>> = HashMap::new();
        let mut udf_bindings: HashMap<(String, String), HashMap<String, String>> = HashMap::new();

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

            // Validator *bindings*: native (`runtime_tag::NATIVE`) entries whose
            // bytes are the target function name — the same shape as UDF bindings,
            // loaded as an ordered `(validator_name, native_name)` list (sorted for
            // a deterministic firing order). The live bag supplies the code.
            let validator_entries = txn.load_functions(&cf, &name, FunctionKind::Validator)?;
            let mut validator_bindings: Vec<(String, String)> = Vec::new();
            for entry in validator_entries {
                if entry.runtime != runtime_tag::NATIVE {
                    continue;
                }
                let func = String::from_utf8(entry.source).map_err(|_| {
                    EngineError::InvalidDocument(format!(
                        "a validator binding on {cf}.{name} has a non-UTF-8 target name"
                    ))
                })?;
                validator_bindings.push((entry.name, func));
            }
            if !validator_bindings.is_empty() {
                validator_bindings.sort();
                validators.insert((cf.clone(), name.clone()), validator_bindings);
            }

            // UDF *bindings*: native (`runtime_tag::NATIVE`) entries whose bytes
            // are the target function name. Loaded as a plain `query_name ->
            // native_name` map — no `ResolvedHook`, since a binding has no source.
            let udf_entries = txn.load_functions(&cf, &name, FunctionKind::Udf)?;
            let mut bindings: HashMap<String, String> = HashMap::new();
            for entry in udf_entries {
                if entry.runtime != runtime_tag::NATIVE {
                    continue;
                }
                let func = String::from_utf8(entry.source).map_err(|_| {
                    EngineError::InvalidDocument(format!(
                        "a UDF binding on {cf}.{name} has a non-UTF-8 target name"
                    ))
                })?;
                bindings.insert(entry.name, func);
            }
            if !bindings.is_empty() {
                udf_bindings.insert((cf, name), bindings);
            }
        }

        Ok(Self {
            triggers,
            validators,
            udf_bindings,
        })
    }

    /// An empty snapshot with no hooks.
    pub fn empty() -> Self {
        Self {
            triggers: HashMap::new(),
            validators: HashMap::new(),
            udf_bindings: HashMap::new(),
        }
    }

    /// Get triggers for a (cf, collection) pair.
    pub fn triggers_for(&self, cf: &str, collection: &str) -> &[ResolvedHook] {
        self.triggers
            .get(&(cf.to_string(), collection.to_string()))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// The validator bindings for a (cf, collection): an ordered list of
    /// `(validator_name, native_name)`. Empty when the collection has none. The
    /// executor resolves each native name against the live bag at fire time.
    pub fn validators_for(&self, cf: &str, collection: &str) -> &[(String, String)] {
        self.validators
            .get(&(cf.to_string(), collection.to_string()))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// All validator bindings across every collection:
    /// `(cf, collection) -> [(validator_name, native_name)]`. Used to find
    /// bindings whose target function is unregistered (a dangling validator
    /// blocks all writes to its collection).
    pub fn all_validator_bindings(&self) -> &HashMap<(String, String), Vec<(String, String)>> {
        &self.validators
    }

    /// The UDF bindings for a (cf, collection): `query_name -> native_name`.
    /// `None` when the collection has no bindings (so any `udf.*` reference there
    /// is unbound).
    pub fn udf_bindings_for(&self, cf: &str, collection: &str) -> Option<&HashMap<String, String>> {
        self.udf_bindings
            .get(&(cf.to_string(), collection.to_string()))
    }

    /// All UDF bindings across every collection: `(cf, collection) -> {query_name
    /// -> native_name}`. Used to find bindings whose target is unregistered.
    pub fn all_udf_bindings(&self) -> &HashMap<(String, String), HashMap<String, String>> {
        &self.udf_bindings
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
