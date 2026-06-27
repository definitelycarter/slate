//! Native validators.
//!
//! A validator is a write-path gate: before a document is written it inspects
//! the candidate and either accepts it or rejects it with a reason. It is
//! fundamentally a `Fn(&ValidatorCtx) -> Result<Verdict, ValidatorError>` — the
//! native counterpart of the Lua validators it replaces, minus the VM. A Lua
//! (or other runtime) validator arrives later as just another [`Validator`]
//! implementor whose body drives the VM; it does not change anything here.
//!
//! ## `Pure` — sees only the candidate
//!
//! A validator receives the candidate document and nothing else: no
//! transaction, no other documents. This keeps it free of phantom-read and
//! consistency questions and makes "a validator cannot write" a *compile-time*
//! guarantee — the [`ValidatorCtx`] exposes only [`doc()`](ValidatorCtx::doc),
//! a read-only `&RawDocument`. (Promoting validators to read other documents is
//! a future capability, deliberately out of scope.) The context is a newtype so
//! that future capability can be added without changing the [`Validator`]
//! signature or breaking construction sites.
//!
//! ## The bag
//!
//! Native validators are registered into a flat, **database-scoped** bag
//! ([`ValidatorBag`]): a `name -> Arc<dyn Validator>` map of *code*, mutated
//! with no transaction. Think of it as the shared library in a dynamic-linking
//! model — one `adults_only`, reusable everywhere. Which collection a validator
//! actually guards is the separate, durable **binding** concern handled by
//! `slate-db` at the catalog level; this crate owns only the trait and the code
//! bag.
//!
//! ## Panic boundary
//!
//! A native validator is user code in-process. The write-path seam in
//! `slate-executor` wraps [`Validator::check`] in `catch_unwind`; a panic
//! **aborts the transaction** (fail-safe), as does a [`Verdict::Reject`] or a
//! returned [`ValidatorError`]. The boundary lives at the firing site, not in
//! this crate, so the trait stays a plain function.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bson::RawDocument;

/// A native validator: a write-path gate over a candidate document.
///
/// `Pure` over the candidate — it receives only [`ValidatorCtx`] (a read-only
/// document view) and no transaction. The blanket impl below means any matching
/// closure *is* a `Validator`, so callers register `|ctx| { ... }` directly.
pub trait Validator: Send + Sync {
    fn check(&self, ctx: &ValidatorCtx<'_>) -> Result<Verdict, ValidatorError>;
}

impl<F> Validator for F
where
    F: Fn(&ValidatorCtx<'_>) -> Result<Verdict, ValidatorError> + Send + Sync,
{
    fn check(&self, ctx: &ValidatorCtx<'_>) -> Result<Verdict, ValidatorError> {
        self(ctx)
    }
}

/// What a validator sees: the candidate document, read-only.
///
/// A newtype rather than a bare `&RawDocument` so the surface can grow (e.g. a
/// future `ReadOnly` capability to look up other documents) without changing the
/// [`Validator`] signature. The single field is private; construct with
/// [`new`](ValidatorCtx::new) and read with [`doc`](ValidatorCtx::doc).
pub struct ValidatorCtx<'a> {
    doc: &'a RawDocument,
}

impl<'a> ValidatorCtx<'a> {
    /// Wrap the candidate document.
    pub fn new(doc: &'a RawDocument) -> Self {
        Self { doc }
    }

    /// The candidate document under validation, read-only.
    pub fn doc(&self) -> &RawDocument {
        self.doc
    }
}

/// A validator's decision about a candidate document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The document passes; let the write proceed.
    Accept,
    /// The document is rejected; abort the write. The string is the reason
    /// surfaced to the caller.
    Reject(String),
}

impl Verdict {
    /// Reject with a reason. Convenience over `Verdict::Reject(reason.into())`.
    pub fn reject(reason: impl Into<String>) -> Self {
        Verdict::Reject(reason.into())
    }

    /// Whether this verdict accepts the document.
    pub fn is_accept(&self) -> bool {
        matches!(self, Verdict::Accept)
    }
}

/// Why a validator *failed* — distinct from a [`Verdict::Reject`], which is a
/// normal "document is invalid" outcome. This is the validator itself
/// malfunctioning; like a reject, it aborts the write (fail-safe).
#[derive(Debug, Clone, thiserror::Error)]
pub enum ValidatorError {
    /// The validator body raised an error.
    #[error("{0}")]
    Body(String),
}

/// The live, in-process **bag** of native validators — a flat, database-scoped
/// map from validator name to implementation.
///
/// This is the *code* half of the dynamic-linking model: one `adults_only`,
/// registered once, reusable from any collection that *binds* to it (bindings
/// are the per-collection, durable half, held in the catalog by `slate-db`). It
/// is runtime state, not durable: native closures can't be serialized, so they
/// live only here and are (re-)registered by application code each process
/// start. Registration takes no transaction — it is a DB-lifetime operation,
/// like the watch registry.
#[derive(Default)]
pub struct ValidatorBag {
    validators: RwLock<HashMap<String, Arc<dyn Validator>>>,
}

impl ValidatorBag {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `validator` under `name`, replacing any existing validator of
    /// that name (rebinding the code is allowed and idempotent). A bare closure
    /// is accepted via the blanket [`Validator`] impl.
    pub fn register(&self, name: &str, validator: impl Validator + 'static) {
        let mut validators = self
            .validators
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        validators.insert(name.to_string(), Arc::new(validator));
    }

    /// Remove the validator named `name`. Returns whether one was present.
    pub fn unregister(&self, name: &str) -> bool {
        let mut validators = self
            .validators
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        validators.remove(name).is_some()
    }

    /// Look up the validator named `name`, cloning the `Arc` so the caller holds
    /// it independent of later bag mutations.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Validator>> {
        let validators = self
            .validators
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        validators.get(name).map(Arc::clone)
    }

    /// Whether the bag holds no validators. The write path uses this as a cheap
    /// gate: a database that never registers a validator resolves nothing.
    pub fn is_empty(&self) -> bool {
        self.validators
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;

    // A representative validator: reject documents whose `name` is missing or
    // not a non-empty string.
    fn require_name(ctx: &ValidatorCtx<'_>) -> Result<Verdict, ValidatorError> {
        match ctx.doc().get_str("name") {
            Ok(s) if !s.is_empty() => Ok(Verdict::Accept),
            _ => Ok(Verdict::reject("name is required")),
        }
    }

    #[test]
    fn closure_registers_and_accepts() {
        let bag = ValidatorBag::new();
        bag.register("require_name", require_name);

        let validator = bag.get("require_name").expect("registered");
        let doc = rawdoc! { "name": "ada" };
        assert_eq!(
            validator.check(&ValidatorCtx::new(&doc)).unwrap(),
            Verdict::Accept
        );
    }

    #[test]
    fn closure_rejects_with_reason() {
        let bag = ValidatorBag::new();
        bag.register("require_name", require_name);

        let validator = bag.get("require_name").unwrap();
        let doc = rawdoc! { "age": 30 };
        assert_eq!(
            validator.check(&ValidatorCtx::new(&doc)).unwrap(),
            Verdict::reject("name is required")
        );
    }

    #[test]
    fn register_replaces_same_name() {
        let bag = ValidatorBag::new();
        bag.register("v", require_name);
        bag.register("v", |_: &ValidatorCtx<'_>| Ok(Verdict::Accept));
        let validator = bag.get("v").unwrap();
        let doc = rawdoc! { "age": 30 };
        assert_eq!(
            validator.check(&ValidatorCtx::new(&doc)).unwrap(),
            Verdict::Accept
        );
    }

    #[test]
    fn unregister_removes() {
        let bag = ValidatorBag::new();
        bag.register("require_name", require_name);
        assert!(bag.unregister("require_name"));
        assert!(!bag.unregister("require_name"));
        assert!(bag.get("require_name").is_none());
    }

    #[test]
    fn is_empty_reflects_contents() {
        let bag = ValidatorBag::new();
        assert!(bag.is_empty());
        bag.register("require_name", require_name);
        assert!(!bag.is_empty());
        bag.unregister("require_name");
        assert!(bag.is_empty());
    }

    #[test]
    fn body_error_propagates() {
        let bag = ValidatorBag::new();
        bag.register("boom", |_: &ValidatorCtx<'_>| {
            Err(ValidatorError::Body("kaboom".to_string()))
        });
        let validator = bag.get("boom").unwrap();
        let doc = rawdoc! { "name": "ada" };
        assert!(matches!(
            validator.check(&ValidatorCtx::new(&doc)),
            Err(ValidatorError::Body(_))
        ));
    }
}
