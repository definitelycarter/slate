//! Native triggers.
//!
//! A trigger is a write-path side effect: when a document is inserted, updated,
//! or deleted, each trigger bound to the collection fires, receiving the action
//! and the candidate document and a transaction view it may read from and write
//! to. It is fundamentally a `Fn(&TriggerCtx) -> Result<(), TriggerError>` — the
//! native counterpart of the Lua triggers it replaces, minus the VM. A Lua (or
//! other runtime) trigger arrives later as just another [`Trigger`] implementor
//! whose body drives the VM; it does not change anything here.
//!
//! ## `ReadWrite` — a column-family-confined transaction view
//!
//! Unlike a validator (which is `Pure` and sees only the candidate document), a
//! trigger keeps full read-write power over *other* documents — the audit-log
//! pattern, cascading writes, derived collections. That power is handed to it as
//! a [`TriggerTxn`]: `get`/`put`/`delete` keyed by `(collection, id)`.
//!
//! Crucially, [`TriggerTxn`] has **no column-family parameter**. The column
//! family is baked into the implementor at construction (by `slate-executor`,
//! from the firing context) and is *not reachable* through this surface, so a
//! trigger structurally cannot read or write across column families — only other
//! collections within its own. Confinement falls out of the type, not a runtime
//! check. The [`TriggerCtx`] is a newtype so the surface can grow (a narrower
//! capability tier, an allowlist of reachable collections) without changing the
//! [`Trigger`] signature.
//!
//! ## The bag
//!
//! Native triggers are registered into a flat, **database-scoped** bag
//! ([`TriggerBag`]): a `name -> Arc<dyn Trigger>` map of *code*, mutated with no
//! transaction. Think of it as the shared library in a dynamic-linking model —
//! one `audit`, reusable everywhere. Which collection a trigger actually fires on
//! is the separate, durable **binding** concern handled by `slate-db` at the
//! catalog level; this crate owns only the trait and the code bag.
//!
//! ## Panic boundary
//!
//! A native trigger is user code in-process. The write-path seam in
//! `slate-executor` wraps [`Trigger::fire`] in `catch_unwind`; a panic **aborts
//! the transaction** (fail-safe), as does a returned [`TriggerError`] or a
//! dangling binding (bound but never registered). The boundary lives at the
//! firing site, not in this crate, so the trait stays a plain function.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bson::RawDocument;
use bson::raw::{RawBsonRef, RawDocumentBuf};

/// A native trigger: a write-path side effect over a candidate document.
///
/// Fires `inserting`/`inserted`/`updating`/`updated`/`deleting`/`deleted` (the
/// trigger inspects [`TriggerCtx::action`] to branch). The blanket impl below
/// means any matching closure *is* a `Trigger`, so callers register
/// `|ctx| { ... }` directly.
pub trait Trigger: Send + Sync {
    fn fire(&self, ctx: &TriggerCtx<'_>) -> Result<(), TriggerError>;
}

impl<F> Trigger for F
where
    F: Fn(&TriggerCtx<'_>) -> Result<(), TriggerError> + Send + Sync,
{
    fn fire(&self, ctx: &TriggerCtx<'_>) -> Result<(), TriggerError> {
        self(ctx)
    }
}

/// What a trigger sees: the firing action, the candidate document (read-only),
/// and a column-family-confined transaction view ([`TriggerTxn`]) for reading
/// and writing *other* documents.
///
/// A newtype rather than a bare tuple so the surface can grow without changing
/// the [`Trigger`] signature or breaking construction sites. The fields are
/// private; construct with [`new`](TriggerCtx::new) and use the accessors and
/// the delegating [`get`](TriggerCtx::get)/[`put`](TriggerCtx::put)/
/// [`delete`](TriggerCtx::delete) methods.
pub struct TriggerCtx<'a> {
    action: &'a str,
    doc: &'a RawDocument,
    txn: &'a dyn TriggerTxn,
}

impl<'a> TriggerCtx<'a> {
    /// Wrap a firing `action`, the candidate `doc`, and the confined `txn` view.
    pub fn new(action: &'a str, doc: &'a RawDocument, txn: &'a dyn TriggerTxn) -> Self {
        Self { action, doc, txn }
    }

    /// The mutation that fired this trigger — one of `inserting`, `inserted`,
    /// `updating`, `updated`, `deleting`, `deleted`.
    pub fn action(&self) -> &str {
        self.action
    }

    /// The candidate document, read-only. For a `*ing` action this is the
    /// pre-image (or the document about to be written); for a `*ed` action it is
    /// the written document.
    pub fn doc(&self) -> &RawDocument {
        self.doc
    }

    /// Read a document by id from `collection` (within this trigger's column
    /// family). Returns `None` if absent.
    pub fn get(
        &self,
        collection: &str,
        id: RawBsonRef<'_>,
    ) -> Result<Option<RawDocumentBuf>, TriggerError> {
        self.txn.get(collection, id)
    }

    /// Write `doc` into `collection` (within this trigger's column family).
    pub fn put(&self, collection: &str, doc: &RawDocument) -> Result<(), TriggerError> {
        self.txn.put(collection, doc)
    }

    /// Delete the document with `id` from `collection` (within this trigger's
    /// column family).
    pub fn delete(&self, collection: &str, id: RawBsonRef<'_>) -> Result<(), TriggerError> {
        self.txn.delete(collection, id)
    }
}

/// The transaction surface a trigger acts through — the `ReadWrite` capability,
/// confined to a single column family.
///
/// Implemented by `slate-executor` over the live engine transaction. There is
/// deliberately **no column-family parameter**: the implementor binds one column
/// family at construction, so every `get`/`put`/`delete` is confined to it and a
/// trigger cannot reach across column families. A failing capability operation
/// surfaces as [`TriggerError::Txn`], which (like any trigger error) aborts the
/// write.
pub trait TriggerTxn {
    /// Read a document by `id` from `collection`. `None` if absent.
    fn get(
        &self,
        collection: &str,
        id: RawBsonRef<'_>,
    ) -> Result<Option<RawDocumentBuf>, TriggerError>;

    /// Write `doc` into `collection`.
    fn put(&self, collection: &str, doc: &RawDocument) -> Result<(), TriggerError>;

    /// Delete the document with `id` from `collection`.
    fn delete(&self, collection: &str, id: RawBsonRef<'_>) -> Result<(), TriggerError>;
}

/// Why a trigger *failed* — either the trigger body raised, or a transaction
/// capability operation it invoked failed. Both abort the write (fail-safe);
/// the split exists only to make diagnostics legible.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TriggerError {
    /// The trigger body raised an error.
    #[error("{0}")]
    Body(String),
    /// A `get`/`put`/`delete` through the confined transaction failed.
    #[error("trigger transaction error: {0}")]
    Txn(String),
}

/// The live, in-process **bag** of native triggers — a flat, database-scoped map
/// from trigger name to implementation.
///
/// This is the *code* half of the dynamic-linking model: one `audit`, registered
/// once, reusable from any collection that *binds* to it (bindings are the
/// per-collection, durable half, held in the catalog by `slate-db`). It is
/// runtime state, not durable: native closures can't be serialized, so they live
/// only here and are (re-)registered by application code each process start.
/// Registration takes no transaction — it is a DB-lifetime operation, like the
/// watch registry.
#[derive(Default)]
pub struct TriggerBag {
    triggers: RwLock<HashMap<String, Arc<dyn Trigger>>>,
}

impl TriggerBag {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `trigger` under `name`, replacing any existing trigger of that
    /// name (rebinding the code is allowed and idempotent). A bare closure is
    /// accepted via the blanket [`Trigger`] impl.
    pub fn register(&self, name: &str, trigger: impl Trigger + 'static) {
        let mut triggers = self
            .triggers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        triggers.insert(name.to_string(), Arc::new(trigger));
    }

    /// Remove the trigger named `name`. Returns whether one was present.
    pub fn unregister(&self, name: &str) -> bool {
        let mut triggers = self
            .triggers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        triggers.remove(name).is_some()
    }

    /// Look up the trigger named `name`, cloning the `Arc` so the caller holds it
    /// independent of later bag mutations.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Trigger>> {
        let triggers = self
            .triggers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        triggers.get(name).map(Arc::clone)
    }

    /// Whether the bag holds no triggers. The write path uses this as a cheap
    /// gate: a database that never registers a trigger resolves nothing.
    pub fn is_empty(&self) -> bool {
        self.triggers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;
    use std::cell::RefCell;

    /// An in-memory [`TriggerTxn`] for exercising triggers without an engine.
    /// Keys by `(collection, id)` where the id is a string (the tests use string
    /// ids). Single-threaded test use, so `RefCell` interior mutability suffices.
    #[derive(Default)]
    struct MockTxn {
        store: RefCell<HashMap<(String, String), RawDocumentBuf>>,
        deletes: RefCell<Vec<(String, String)>>,
    }

    fn id_key(id: RawBsonRef<'_>) -> String {
        id.as_str().expect("test ids are strings").to_string()
    }

    impl TriggerTxn for MockTxn {
        fn get(
            &self,
            collection: &str,
            id: RawBsonRef<'_>,
        ) -> Result<Option<RawDocumentBuf>, TriggerError> {
            Ok(self
                .store
                .borrow()
                .get(&(collection.to_string(), id_key(id)))
                .cloned())
        }

        fn put(&self, collection: &str, doc: &RawDocument) -> Result<(), TriggerError> {
            let id = doc
                .get("_id")
                .map_err(|e| TriggerError::Txn(e.to_string()))?
                .ok_or_else(|| TriggerError::Txn("missing _id".into()))?;
            self.store
                .borrow_mut()
                .insert((collection.to_string(), id_key(id)), doc.to_owned());
            Ok(())
        }

        fn delete(&self, collection: &str, id: RawBsonRef<'_>) -> Result<(), TriggerError> {
            let key = (collection.to_string(), id_key(id));
            self.store.borrow_mut().remove(&key);
            self.deletes.borrow_mut().push(key);
            Ok(())
        }
    }

    // A representative trigger: on `inserted`, mirror the candidate into an
    // `audit` collection in the same column family.
    fn audit(ctx: &TriggerCtx<'_>) -> Result<(), TriggerError> {
        if ctx.action() == "inserted" {
            ctx.put("audit", ctx.doc())?;
        }
        Ok(())
    }

    #[test]
    fn closure_registers_and_fires_a_write() {
        let bag = TriggerBag::new();
        bag.register("audit", audit);

        let txn = MockTxn::default();
        let doc = rawdoc! { "_id": "u1", "name": "ada" };
        let ctx = TriggerCtx::new("inserted", &doc, &txn);

        bag.get("audit").expect("registered").fire(&ctx).unwrap();

        let mirrored = txn
            .get("audit", RawBsonRef::String("u1"))
            .unwrap()
            .expect("audit row written");
        assert_eq!(mirrored.get_str("name").unwrap(), "ada");
    }

    #[test]
    fn non_matching_action_is_a_noop() {
        let bag = TriggerBag::new();
        bag.register("audit", audit);

        let txn = MockTxn::default();
        let doc = rawdoc! { "_id": "u1", "name": "ada" };
        // `deleting` is not `inserted`, so the trigger writes nothing.
        let ctx = TriggerCtx::new("deleting", &doc, &txn);
        bag.get("audit").unwrap().fire(&ctx).unwrap();

        assert!(
            txn.get("audit", RawBsonRef::String("u1"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn trigger_reads_then_deletes_via_ctx() {
        // A trigger that, on `deleted`, removes a matching row from `mirror`.
        fn cascade(ctx: &TriggerCtx<'_>) -> Result<(), TriggerError> {
            if ctx.action() == "deleted" {
                let id = ctx
                    .doc()
                    .get("_id")
                    .map_err(|e| TriggerError::Body(e.to_string()))?;
                if let Some(id) = id
                    && ctx.get("mirror", id)?.is_some()
                {
                    ctx.delete("mirror", id)?;
                }
            }
            Ok(())
        }

        let txn = MockTxn::default();
        let seed = rawdoc! { "_id": "u1", "name": "ada" };
        txn.put("mirror", &seed).unwrap();

        let bag = TriggerBag::new();
        bag.register("cascade", cascade);

        let doc = rawdoc! { "_id": "u1" };
        let ctx = TriggerCtx::new("deleted", &doc, &txn);
        bag.get("cascade").unwrap().fire(&ctx).unwrap();

        assert!(
            txn.get("mirror", RawBsonRef::String("u1"))
                .unwrap()
                .is_none()
        );
        assert_eq!(txn.deletes.borrow().len(), 1);
    }

    #[test]
    fn action_is_visible() {
        let txn = MockTxn::default();
        let doc = rawdoc! { "_id": "u1" };
        let ctx = TriggerCtx::new("updating", &doc, &txn);
        assert_eq!(ctx.action(), "updating");
        assert_eq!(ctx.doc().get_str("_id").unwrap(), "u1");
    }

    #[test]
    fn body_error_propagates() {
        let bag = TriggerBag::new();
        bag.register("boom", |_: &TriggerCtx<'_>| {
            Err(TriggerError::Body("kaboom".to_string()))
        });

        let txn = MockTxn::default();
        let doc = rawdoc! { "_id": "u1" };
        let ctx = TriggerCtx::new("inserted", &doc, &txn);
        assert!(matches!(
            bag.get("boom").unwrap().fire(&ctx),
            Err(TriggerError::Body(_))
        ));
    }

    #[test]
    fn register_replaces_same_name() {
        let bag = TriggerBag::new();
        bag.register("t", audit);
        // Replace with a no-op; the audit write should no longer happen.
        bag.register("t", |_: &TriggerCtx<'_>| Ok(()));

        let txn = MockTxn::default();
        let doc = rawdoc! { "_id": "u1", "name": "ada" };
        let ctx = TriggerCtx::new("inserted", &doc, &txn);
        bag.get("t").unwrap().fire(&ctx).unwrap();

        assert!(
            txn.get("audit", RawBsonRef::String("u1"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unregister_removes() {
        let bag = TriggerBag::new();
        bag.register("audit", audit);
        assert!(bag.unregister("audit"));
        assert!(!bag.unregister("audit"));
        assert!(bag.get("audit").is_none());
    }

    #[test]
    fn is_empty_reflects_contents() {
        let bag = TriggerBag::new();
        assert!(bag.is_empty());
        bag.register("audit", audit);
        assert!(!bag.is_empty());
        bag.unregister("audit");
        assert!(bag.is_empty());
    }
}
