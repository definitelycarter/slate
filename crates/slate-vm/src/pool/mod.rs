use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::error::VmError;
use crate::{RuntimeKind, ScriptHandle, ScriptRuntime};

// ---------------------------------------------------------------------------
// RuntimeRegistry
// ---------------------------------------------------------------------------

/// Maps runtime kinds to their implementations.
pub struct RuntimeRegistry {
    runtimes: HashMap<RuntimeKind, Arc<dyn ScriptRuntime>>,
}

impl RuntimeRegistry {
    pub fn new() -> Self {
        Self {
            runtimes: HashMap::new(),
        }
    }

    pub fn register(&mut self, kind: RuntimeKind, runtime: Arc<dyn ScriptRuntime>) {
        self.runtimes.insert(kind, runtime);
    }

    pub fn get(&self, kind: &RuntimeKind) -> Option<&Arc<dyn ScriptRuntime>> {
        self.runtimes.get(kind)
    }
}

// ---------------------------------------------------------------------------
// ScriptKey
// ---------------------------------------------------------------------------

/// Cache key for a compiled script: name + version + runtime.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ScriptKey {
    pub name: String,
    pub version: u64,
    pub runtime: RuntimeKind,
}

// ---------------------------------------------------------------------------
// VmPool
// ---------------------------------------------------------------------------

const DEFAULT_MAX_ENTRIES: usize = 256;

struct CachedHandle {
    handle: Arc<dyn ScriptHandle>,
    last_used: Instant,
}

/// Compile-caching pool for script handles.
///
/// The pool caches compiled artifacts (bytecode / modules) keyed by
/// `ScriptKey { name, version, runtime }`. Capabilities are not part of
/// the cache — they are injected at call time.
pub struct VmPool {
    runtimes: Arc<HashMap<RuntimeKind, Arc<dyn ScriptRuntime>>>,
    handles: RwLock<HashMap<ScriptKey, CachedHandle>>,
    max_entries: usize,
}

#[allow(unreachable_code, unused_variables)]
impl VmPool {
    pub fn new(registry: RuntimeRegistry) -> Self {
        Self {
            runtimes: Arc::new(registry.runtimes),
            handles: RwLock::new(HashMap::new()),
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }

    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    /// Get a cached handle or compile the source and cache it.
    pub fn get_or_load(
        &self,
        runtime: RuntimeKind,
        name: &str,
        version: u64,
        source: &[u8],
    ) -> Result<Arc<dyn ScriptHandle>, VmError> {
        let key = ScriptKey {
            name: name.to_string(),
            version,
            runtime: runtime.clone(),
        };

        // Fast path: read lock
        {
            let handles = self.handles.read().unwrap();
            if let Some(cached) = handles.get(&key) {
                return Ok(Arc::clone(&cached.handle));
            }
        }

        // Slow path: write lock, double-check, compile, cache
        let mut handles = self.handles.write().unwrap();

        // Double-check after acquiring write lock
        if let Some(cached) = handles.get_mut(&key) {
            cached.last_used = Instant::now();
            return Ok(Arc::clone(&cached.handle));
        }

        let rt = self
            .runtimes
            .get(&key.runtime)
            .ok_or(VmError::UnsupportedRuntime(key.runtime.clone()))?;

        let handle: Arc<dyn ScriptHandle> = Arc::from(rt.load(name, source)?);

        // Evict LRU if at capacity
        if handles.len() >= self.max_entries {
            Self::evict_lru(&mut handles);
        }

        handles.insert(
            key,
            CachedHandle {
                handle: Arc::clone(&handle),
                last_used: Instant::now(),
            },
        );

        Ok(handle)
    }

    /// Remove all cached handles for a given script name (any version/runtime).
    pub fn invalidate(&self, script_name: &str) {
        let mut handles = self.handles.write().unwrap();
        handles.retain(|k, _| k.name != script_name);
    }

    /// Remove all cached handles.
    pub fn invalidate_all(&self) {
        self.handles.write().unwrap().clear();
    }

    /// Remove entries older than `max_age`.
    pub fn sweep_stale(&self, max_age: Duration) {
        let cutoff = Instant::now() - max_age;
        let mut handles = self.handles.write().unwrap();
        handles.retain(|_, cached| cached.last_used > cutoff);
    }

    fn evict_lru(handles: &mut HashMap<ScriptKey, CachedHandle>) {
        if let Some(oldest) = handles
            .iter()
            .min_by_key(|(_, v)| v.last_used)
            .map(|(k, _)| k.clone())
        {
            handles.remove(&oldest);
        }
    }
}

#[cfg(all(test, feature = "lua"))]
mod tests {
    use super::*;
    use crate::ScriptCapabilities;
    use bson::raw::RawDocumentBuf;

    /// Dummy runtime that records load calls.
    struct MockRuntime {
        load_count: std::sync::atomic::AtomicUsize,
    }

    impl MockRuntime {
        fn new() -> Self {
            Self {
                load_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn loads(&self) -> usize {
            self.load_count
                .load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    struct MockHandle;

    impl ScriptHandle for MockHandle {
        fn call(
            &self,
            _input: &RawDocumentBuf,
            _capabilities: &ScriptCapabilities<'_>,
        ) -> Result<RawDocumentBuf, VmError> {
            Ok(bson::rawdoc! {})
        }
    }

    impl ScriptRuntime for MockRuntime {
        fn load(&self, _name: &str, _source: &[u8]) -> Result<Box<dyn ScriptHandle>, VmError> {
            self.load_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(Box::new(MockHandle))
        }

        fn runtime_kind(&self) -> RuntimeKind {
            RuntimeKind::Lua
        }
    }

    fn pool_with_mock() -> (VmPool, Arc<MockRuntime>) {
        let rt = Arc::new(MockRuntime::new());
        let mut registry = RuntimeRegistry::new();
        registry.register(RuntimeKind::Lua, rt.clone());
        (VmPool::new(registry), rt)
    }

    #[test]
    fn get_or_load_compiles_once() {
        let (pool, rt) = pool_with_mock();

        let _h1 = pool
            .get_or_load(RuntimeKind::Lua, "test", 1, b"src")
            .unwrap();
        let _h2 = pool
            .get_or_load(RuntimeKind::Lua, "test", 1, b"src")
            .unwrap();

        assert_eq!(rt.loads(), 1);
    }

    #[test]
    fn different_version_recompiles() {
        let (pool, rt) = pool_with_mock();

        pool.get_or_load(RuntimeKind::Lua, "test", 1, b"v1")
            .unwrap();
        pool.get_or_load(RuntimeKind::Lua, "test", 2, b"v2")
            .unwrap();

        assert_eq!(rt.loads(), 2);
    }

    #[test]
    fn invalidate_removes_all_versions() {
        let (pool, rt) = pool_with_mock();

        pool.get_or_load(RuntimeKind::Lua, "test", 1, b"v1")
            .unwrap();
        pool.get_or_load(RuntimeKind::Lua, "test", 2, b"v2")
            .unwrap();
        assert_eq!(rt.loads(), 2);

        pool.invalidate("test");

        pool.get_or_load(RuntimeKind::Lua, "test", 1, b"v1")
            .unwrap();
        assert_eq!(rt.loads(), 3);
    }

    #[test]
    fn invalidate_all_clears_cache() {
        let (pool, rt) = pool_with_mock();

        pool.get_or_load(RuntimeKind::Lua, "a", 1, b"src")
            .unwrap();
        pool.get_or_load(RuntimeKind::Lua, "b", 1, b"src")
            .unwrap();

        pool.invalidate_all();

        pool.get_or_load(RuntimeKind::Lua, "a", 1, b"src")
            .unwrap();
        assert_eq!(rt.loads(), 3);
    }

    #[test]
    #[cfg(feature = "wasm")]
    fn unsupported_runtime_errors() {
        let (pool, _) = pool_with_mock();

        let result = pool.get_or_load(RuntimeKind::Wasm, "test", 1, b"src");
        assert!(matches!(result, Err(VmError::UnsupportedRuntime(_))));
    }

    #[test]
    fn evicts_lru_at_capacity() {
        let rt = Arc::new(MockRuntime::new());
        let mut registry = RuntimeRegistry::new();
        registry.register(RuntimeKind::Lua, rt.clone());
        let pool = VmPool::new(registry).with_max_entries(2);

        pool.get_or_load(RuntimeKind::Lua, "a", 1, b"src")
            .unwrap();
        pool.get_or_load(RuntimeKind::Lua, "b", 1, b"src")
            .unwrap();
        // Cache full (2/2). Next load should evict "a" (oldest).
        pool.get_or_load(RuntimeKind::Lua, "c", 1, b"src")
            .unwrap();

        assert_eq!(rt.loads(), 3);

        // "a" was evicted, loading it again should recompile
        pool.get_or_load(RuntimeKind::Lua, "a", 1, b"src")
            .unwrap();
        assert_eq!(rt.loads(), 4);
    }
}
