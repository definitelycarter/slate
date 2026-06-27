//! Native user-defined functions (UDFs).
//!
//! A UDF is a scalar function callable from SQL as `udf.name(args...)`. It is
//! fundamentally a `Fn(&[Value]) -> Result<Value, UdfError>` — the *same shape*
//! as the engine's built-in scalar functions (`slate_eval::functions::call`),
//! minus the name. A Lua (or other VM) UDF arrives later as just another [`Udf`]
//! implementor whose body drives the VM; it does not change anything here.
//!
//! Arguments arrive as owned [`Value`]s, exactly like the built-ins: the raw
//! evaluator already materializes every function argument to an owned `Value`
//! before dispatch, so a UDF taking `&[Value]` adds no cost over a built-in
//! call. The crate depends only on [`slate_value`], not the evaluator, so it
//! stays light.
//!
//! ## The bag
//!
//! Native functions are registered into a flat, **database-scoped** bag
//! ([`UdfBag`]): a `name -> Arc<dyn Udf>` map of *code*, mutated with no
//! transaction. Think of it as the shared library in a dynamic-linking model —
//! one `compute_tax`, reusable everywhere. Which query name resolves to which
//! bag function, *per collection*, is the separate, durable **binding** concern
//! handled by `slate-db` at the catalog level; this crate owns only the trait
//! and the code bag.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use slate_value::Value;

/// A native scalar user-defined function.
///
/// Pure over its already-evaluated arguments — it receives no document and no
/// transaction (that is the validator/trigger story, not the UDF one). The
/// blanket impl below means any matching closure *is* a `Udf`, so callers
/// register `|args| { ... }` directly.
pub trait Udf: Send + Sync {
    fn call(&self, args: &[Value]) -> Result<Value, UdfError>;
}

impl<F> Udf for F
where
    F: Fn(&[Value]) -> Result<Value, UdfError> + Send + Sync,
{
    fn call(&self, args: &[Value]) -> Result<Value, UdfError> {
        self(args)
    }
}

/// Why a UDF call failed. Kept deliberately small; the evaluator maps this onto
/// its own error type at the call boundary.
#[derive(Debug, Clone, thiserror::Error)]
pub enum UdfError {
    /// The function was called with the wrong number of arguments.
    #[error("{name}: expected {expected} argument(s), got {got}")]
    Arity {
        name: String,
        expected: usize,
        got: usize,
    },
    /// An argument was not of a type the function accepts.
    #[error("{name}: {message}")]
    InvalidArgument { name: String, message: String },
    /// The function body raised an error.
    #[error("{0}")]
    Body(String),
}

/// The live, in-process **bag** of native UDFs — a flat, database-scoped map
/// from function name to implementation.
///
/// This is the *code* half of the dynamic-linking model: one `compute_tax`,
/// registered once, reusable from any collection that *binds* to it (bindings
/// are the per-collection, durable half, held in the catalog by `slate-db`).
/// It is runtime state, not durable: native closures can't be serialized, so
/// they live only here and are (re-)registered by application code each process
/// start. Registration takes no transaction — it is a DB-lifetime operation,
/// like the watch registry.
#[derive(Default)]
pub struct UdfBag {
    udfs: RwLock<HashMap<String, Arc<dyn Udf>>>,
}

impl UdfBag {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `udf` under `name`, replacing any existing function of that name
    /// (rebinding the code is allowed and idempotent). A bare closure is
    /// accepted via the blanket [`Udf`] impl.
    pub fn register(&self, name: &str, udf: impl Udf + 'static) {
        let mut udfs = self
            .udfs
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        udfs.insert(name.to_string(), Arc::new(udf));
    }

    /// Remove the function named `name`. Returns whether one was present.
    pub fn unregister(&self, name: &str) -> bool {
        let mut udfs = self
            .udfs
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        udfs.remove(name).is_some()
    }

    /// Look up the function named `name`, cloning the `Arc` so the caller holds
    /// it independent of later bag mutations.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Udf>> {
        let udfs = self
            .udfs
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        udfs.get(name).map(Arc::clone)
    }

    /// Whether the bag holds no functions. The query path uses this as a cheap
    /// gate: a database that never registers a UDF resolves nothing, so it can
    /// skip building any per-query resolution state.
    pub fn is_empty(&self) -> bool {
        self.udfs
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::Bson;

    // A representative UDF: read arg 0 as a number, double it. Same argument
    // shape as a built-in scalar function.
    fn double(args: &[Value]) -> Result<Value, UdfError> {
        let n = match args.first().and_then(Value::as_bson) {
            Some(Bson::Int32(i)) => f64::from(*i),
            Some(Bson::Int64(i)) => *i as f64,
            Some(Bson::Double(d)) => *d,
            _ => {
                return Err(UdfError::InvalidArgument {
                    name: "double".to_string(),
                    message: "expected a number".to_string(),
                });
            }
        };
        Ok(Value::defined(n * 2.0))
    }

    #[test]
    fn closure_registers_and_resolves() {
        let bag = UdfBag::new();
        bag.register("double", double);

        let udf = bag.get("double").expect("registered");
        let out = udf.call(&[Value::defined(21_i32)]).unwrap();
        assert_eq!(out, Value::defined(42.0));
    }

    #[test]
    fn register_replaces_same_name() {
        let bag = UdfBag::new();
        bag.register("f", double);
        bag.register("f", |_: &[Value]| Ok(Value::defined(7_i32)));
        let udf = bag.get("f").unwrap();
        assert_eq!(udf.call(&[]).unwrap(), Value::defined(7_i32));
    }

    #[test]
    fn unregister_removes() {
        let bag = UdfBag::new();
        bag.register("double", double);
        assert!(bag.unregister("double"));
        assert!(!bag.unregister("double"));
        assert!(bag.get("double").is_none());
    }

    #[test]
    fn is_empty_reflects_contents() {
        let bag = UdfBag::new();
        assert!(bag.is_empty());
        bag.register("double", double);
        assert!(!bag.is_empty());
        bag.unregister("double");
        assert!(bag.is_empty());
    }

    #[test]
    fn arg_type_error_propagates() {
        let bag = UdfBag::new();
        bag.register("double", double);
        let udf = bag.get("double").unwrap();
        assert!(matches!(
            udf.call(&[Value::defined("nope")]),
            Err(UdfError::InvalidArgument { .. })
        ));
    }

    #[test]
    fn body_error_propagates() {
        let bag = UdfBag::new();
        bag.register("boom", |_: &[Value]| {
            Err(UdfError::Body("kaboom".to_string()))
        });
        let udf = bag.get("boom").unwrap();
        assert!(matches!(udf.call(&[]), Err(UdfError::Body(_))));
    }
}
