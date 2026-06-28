//! v2 schema surface: the per-kind hook sub-handles.
//!
//! Phase 0, slice D. A collection carries three kinds of hook, and they are *not*
//! the same shape — triggers and validators feed the write-time hook snapshot
//! (registering one marks it stale), while UDFs are called from SQL at query time
//! and touch no hooks. All three are now native **bindings** — a `name -> native
//! function` mapping resolved against a live, database-scoped bag at run time,
//! never code. So each kind gets its **own** sub-handle rather than one
//! `scripts()` grouping:
//!
//! ```ignore
//! collection.triggers().create(name, TriggerFunction::from_name(f)).execute(&txn)?;
//! collection.validators().create(name, ValidatorFunction::from_name(f)).execute(&txn)?;
//! collection.functions().create(name, UdfFunction::from_name(f)).execute(&txn)?;
//! collection.triggers().register(name, |ctx| …);  // no-txn: put native code in the bag
//! collection.triggers().remove(name).execute(&txn)?;
//! collection.triggers().list(&txn)?;
//! ```
//!
//! Each kind returns its **own** `create` builder type ([`CreateTrigger`] /
//! [`CreateValidator`] / [`CreateFunction`]), so when a kind grows options they
//! land as builder stages on that type alone — the others are untouched. `remove`
//! is uniform across kinds, so the three share one [`RemoveScript`] builder.
//! `remove`/`create` per *handle* are unambiguous about the kind, so there is no
//! `remove_trigger`/`remove_validator` split. Bodies are self-contained — they
//! call the engine transaction's catalog `create_function`/`drop_function`/
//! `load_functions` directly, not a v1 verb.

use std::sync::Arc;

use slate_engine::{Catalog, EngineError, FunctionKind, runtime_tag};
use slate_store::Store;
use slate_trigger::{Trigger, TriggerBag};
use slate_udf::{Udf, UdfBag};
use slate_validator::{Validator, ValidatorBag};

use crate::database::Transaction;
use crate::error::DbError;

/// Whether a function kind feeds the cached catalog snapshot, so
/// registering/removing one must mark it stale. All three kinds do now: triggers
/// and validators feed the write-time hooks, and UDF *bindings* feed the
/// query-time resolution map — both live in `HookSnapshot`.
fn affects_hooks(kind: FunctionKind) -> bool {
    matches!(
        kind,
        FunctionKind::Trigger | FunctionKind::Validator | FunctionKind::Udf
    )
}

/// Remove a function of `kind` by name, marking the hook snapshot stale when the
/// kind feeds it.
fn remove_script<S: Store>(
    txn: &Transaction<'_, S>,
    cf: &str,
    collection: &str,
    kind: FunctionKind,
    name: &str,
) -> Result<(), DbError> {
    txn.engine_txn().drop_function(cf, collection, kind, name)?;
    if affects_hooks(kind) {
        txn.mark_hooks_dirty();
    }
    Ok(())
}

/// List a kind's native bindings as `(name, func)` pairs, sorted by name — the
/// durable symbol table. Shared by `functions().list()` and `validators().list()`:
/// both store a `runtime_tag::NATIVE` entry whose bytes are the target function
/// name (non-native rows, if any, are skipped).
fn list_bindings<S: Store>(
    txn: &Transaction<'_, S>,
    cf: &str,
    collection: &str,
    kind: FunctionKind,
) -> Result<Vec<(String, String)>, DbError> {
    let mut bindings = txn
        .engine_txn()
        .load_functions(cf, collection, kind)?
        .into_iter()
        .filter(|e| e.runtime == runtime_tag::NATIVE)
        .map(|e| {
            let func = String::from_utf8(e.source).map_err(|_| {
                DbError::from(EngineError::InvalidDocument(format!(
                    "a native binding on {cf}.{collection} has a non-UTF-8 target name"
                )))
            })?;
            Ok((e.name, func))
        })
        .collect::<Result<Vec<(String, String)>, DbError>>()?;
    bindings.sort();
    Ok(bindings)
}

// ── Triggers ─────────────────────────────────────────────────────────

/// The `triggers()` sub-handle, from [`Collection::triggers`](super::Collection::triggers).
pub struct Triggers<'a> {
    cf: &'a str,
    collection: &'a str,
    /// The database-scoped trigger bag, for the no-txn native
    /// `register`/`unregister` verbs (the `create`/`remove`/`list` binding verbs
    /// go through the catalog).
    trigger_bag: &'a Arc<TriggerBag>,
}

impl<'a> Triggers<'a> {
    pub(crate) fn new(cf: &'a str, collection: &'a str, trigger_bag: &'a Arc<TriggerBag>) -> Self {
        Self {
            cf,
            collection,
            trigger_bag,
        }
    }

    /// Register a native trigger named `name` in the database-scoped bag. No
    /// transaction — this mutates live runtime state immediately, like
    /// `watch`/`stream`. A bare closure is accepted via the blanket [`Trigger`]
    /// impl. (Database-scoped even though it hangs off a collection handle: the
    /// *binding* that fires it on one collection is a later, durable concern.)
    pub fn register<T: Trigger + 'static>(&self, name: &str, trigger: T) {
        self.trigger_bag.register(name, trigger);
    }

    /// Unregister the native trigger named `name` from the bag. Returns whether
    /// one was present. No transaction.
    pub fn unregister(&self, name: &str) -> bool {
        self.trigger_bag.unregister(name)
    }

    /// Bind the trigger `name` to a native function, durably and per-collection —
    /// so writes to this collection fire it. The target is a [`TriggerFunction`];
    /// `TriggerFunction::from_name(func)` references a trigger registered in the
    /// bag (via `register` or `with_trigger`). Returns a builder; nothing runs
    /// until `.execute(&txn)`. Lazy: the target need not be registered yet — a
    /// dangling binding is caught when a write exercises it (fail-safe), not here.
    pub fn create(&self, name: &str, func: TriggerFunction) -> CreateTrigger<'a> {
        CreateTrigger {
            cf: self.cf,
            collection: self.collection,
            name: name.to_string(),
            func,
        }
    }

    /// Remove the trigger binding named `name`. Returns a builder; nothing runs
    /// until `.execute(&txn)`.
    pub fn remove(&self, name: &str) -> RemoveScript<'a> {
        RemoveScript::new(self.cf, self.collection, FunctionKind::Trigger, name)
    }

    /// List the collection's trigger bindings as `(trigger_name,
    /// native_function_name)` pairs, sorted by name — the durable symbol table
    /// from the catalog.
    pub fn list<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<Vec<(String, String)>, DbError> {
        list_bindings(txn, self.cf, self.collection, FunctionKind::Trigger)
    }
}

/// A pending trigger binding, from [`Triggers::create`]. Its own type so future
/// trigger-specific options (timing, operations) land here without touching
/// validators or functions.
#[must_use = "a create-trigger builder does nothing until .execute(&txn) runs it"]
pub struct CreateTrigger<'a> {
    cf: &'a str,
    collection: &'a str,
    name: String,
    func: TriggerFunction,
}

impl CreateTrigger<'_> {
    /// Write the binding — a `runtime_tag::NATIVE` catalog entry whose bytes are
    /// the target function name — and mark the catalog snapshot stale so the next
    /// transaction's writes resolve it.
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        txn.engine_txn().create_function(
            self.cf,
            self.collection,
            FunctionKind::Trigger,
            &self.name,
            runtime_tag::NATIVE,
            self.func.encode(),
        )?;
        txn.mark_hooks_dirty();
        Ok(())
    }
}

/// The target of a trigger binding — the native trigger a write fires. A struct
/// (not a bare string) so future options (timing, the operations it fires on)
/// can land as constructors or builder stages without changing
/// [`Triggers::create`]'s signature. Today it carries one thing: the name of a
/// native trigger registered in the bag.
pub struct TriggerFunction {
    func: String,
}

impl TriggerFunction {
    /// Bind to the native trigger named `func` (registered via
    /// `triggers().register` or `DatabaseBuilder::with_trigger`).
    pub fn from_name(func: &str) -> Self {
        Self {
            func: func.to_string(),
        }
    }

    /// The bytes stored in the catalog for this binding — the target name.
    fn encode(&self) -> &[u8] {
        self.func.as_bytes()
    }
}

// ── Validators ───────────────────────────────────────────────────────

/// The `validators()` sub-handle, from [`Collection::validators`](super::Collection::validators).
pub struct Validators<'a> {
    cf: &'a str,
    collection: &'a str,
    /// The database-scoped validator bag, for the no-txn native
    /// `register`/`unregister` verbs (the `create`/`remove`/`list` binding verbs
    /// go through the catalog).
    validator_bag: &'a Arc<ValidatorBag>,
}

impl<'a> Validators<'a> {
    pub(crate) fn new(
        cf: &'a str,
        collection: &'a str,
        validator_bag: &'a Arc<ValidatorBag>,
    ) -> Self {
        Self {
            cf,
            collection,
            validator_bag,
        }
    }

    /// Register a native validator named `name` in the database-scoped bag. No
    /// transaction — this mutates live runtime state immediately, like
    /// `watch`/`stream`. A bare closure is accepted via the blanket
    /// [`Validator`] impl. (Database-scoped even though it hangs off a collection
    /// handle: the *binding* that guards one collection is a later, durable
    /// concern.)
    pub fn register<V: Validator + 'static>(&self, name: &str, validator: V) {
        self.validator_bag.register(name, validator);
    }

    /// Unregister the native validator named `name` from the bag. Returns whether
    /// one was present. No transaction.
    pub fn unregister(&self, name: &str) -> bool {
        self.validator_bag.unregister(name)
    }

    /// Bind the validator `name` to a native function, durably and
    /// per-collection — so writes to this collection run it as a gate. The target
    /// is a [`ValidatorFunction`]; `ValidatorFunction::from_name(func)` references
    /// a validator registered in the bag (via `register` or `with_validator`).
    /// Returns a builder; nothing runs until `.execute(&txn)`. Lazy: the target
    /// need not be registered yet — a dangling binding is caught when a write
    /// exercises it (fail-safe), not here.
    pub fn create(&self, name: &str, func: ValidatorFunction) -> CreateValidator<'a> {
        CreateValidator {
            cf: self.cf,
            collection: self.collection,
            name: name.to_string(),
            func,
        }
    }

    /// Remove the validator binding named `name`. Returns a builder; nothing runs
    /// until `.execute(&txn)`.
    pub fn remove(&self, name: &str) -> RemoveScript<'a> {
        RemoveScript::new(self.cf, self.collection, FunctionKind::Validator, name)
    }

    /// List the collection's validator bindings as `(validator_name,
    /// native_function_name)` pairs, sorted by name — the durable symbol table
    /// from the catalog.
    pub fn list<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<Vec<(String, String)>, DbError> {
        list_bindings(txn, self.cf, self.collection, FunctionKind::Validator)
    }
}

/// A pending validator binding, from [`Validators::create`].
#[must_use = "a create-validator builder does nothing until .execute(&txn) runs it"]
pub struct CreateValidator<'a> {
    cf: &'a str,
    collection: &'a str,
    name: String,
    func: ValidatorFunction,
}

impl CreateValidator<'_> {
    /// Write the binding — a `runtime_tag::NATIVE` catalog entry whose bytes are
    /// the target function name — and mark the catalog snapshot stale so the next
    /// transaction's writes resolve it.
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        txn.engine_txn().create_function(
            self.cf,
            self.collection,
            FunctionKind::Validator,
            &self.name,
            runtime_tag::NATIVE,
            self.func.encode(),
        )?;
        txn.mark_hooks_dirty();
        Ok(())
    }
}

/// The target of a validator binding — the native validator a write runs as a
/// gate. A struct (not a bare string) so future options (timing, an inline
/// closure variant, …) can land as constructors or builder stages without
/// changing [`Validators::create`]'s signature. Today it carries one thing: the
/// name of a native validator registered in the bag.
pub struct ValidatorFunction {
    func: String,
}

impl ValidatorFunction {
    /// Bind to the native validator named `func` (registered via
    /// `validators().register` or `DatabaseBuilder::with_validator`).
    pub fn from_name(func: &str) -> Self {
        Self {
            func: func.to_string(),
        }
    }

    /// The bytes stored in the catalog for this binding — the target name.
    fn encode(&self) -> &[u8] {
        self.func.as_bytes()
    }
}

// ── Functions (UDFs) ─────────────────────────────────────────────────

/// The `functions()` sub-handle (user-defined functions), from
/// [`Collection::functions`](super::Collection::functions).
pub struct Functions<'a> {
    cf: &'a str,
    collection: &'a str,
    /// The database-scoped UDF bag, for the no-txn native `register`/`unregister`
    /// verbs (the scripted `create`/`remove`/`list` go through the catalog).
    udf_bag: &'a Arc<UdfBag>,
}

impl<'a> Functions<'a> {
    pub(crate) fn new(cf: &'a str, collection: &'a str, udf_bag: &'a Arc<UdfBag>) -> Self {
        Self {
            cf,
            collection,
            udf_bag,
        }
    }

    /// Register a native UDF named `name` in the database-scoped bag. No
    /// transaction — this mutates live runtime state immediately, like
    /// `watch`/`stream`. A bare closure is accepted via the blanket [`Udf`] impl.
    /// (Database-scoped even though it hangs off a collection handle: the
    /// *binding* that scopes a name to one collection is a later, durable
    /// concern.)
    pub fn register<U: Udf + 'static>(&self, name: &str, udf: U) {
        self.udf_bag.register(name, udf);
    }

    /// Unregister the native UDF named `name` from the bag. Returns whether one
    /// was present. No transaction.
    pub fn unregister(&self, name: &str) -> bool {
        self.udf_bag.unregister(name)
    }

    /// Bind the query-facing name `name` to a native function, durably and
    /// per-collection — so `udf.name` resolves to it only on this collection. The
    /// target is a [`UdfFunction`]; `UdfFunction::from_name(func)` references a
    /// function registered in the bag (via `register` or `with_udf`). Returns a
    /// builder; nothing runs until `.execute(&txn)`. Lazy: the target need not be
    /// registered yet — a dangling binding is caught at resolution, not here.
    pub fn create(&self, name: &str, func: UdfFunction) -> CreateFunction<'a> {
        CreateFunction {
            cf: self.cf,
            collection: self.collection,
            name: name.to_string(),
            func,
        }
    }

    /// Remove the UDF named `name`. Returns a builder; nothing runs until
    /// `.execute(&txn)`.
    pub fn remove(&self, name: &str) -> RemoveScript<'a> {
        RemoveScript::new(self.cf, self.collection, FunctionKind::Udf, name)
    }

    /// List the collection's UDF bindings as `(query_name, native_function_name)`
    /// pairs, sorted by query name — the durable symbol table from the catalog.
    pub fn list<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<Vec<(String, String)>, DbError> {
        list_bindings(txn, self.cf, self.collection, FunctionKind::Udf)
    }
}

/// The target of a UDF binding — what `udf.name` resolves to.
///
/// A struct rather than a bare string so future options (timing, an inline
/// closure variant, …) can land as constructors or builder stages without
/// changing [`Functions::create`]'s signature. Today it carries one thing: the
/// name of a native function registered in the bag.
pub struct UdfFunction {
    func: String,
}

impl UdfFunction {
    /// Bind to the native function named `func` (registered in the bag via
    /// `functions().register` or `DatabaseBuilder::with_udf`).
    pub fn from_name(func: &str) -> Self {
        Self {
            func: func.to_string(),
        }
    }

    /// The bytes stored in the catalog for this binding — currently just the
    /// target function's name.
    fn encode(&self) -> &[u8] {
        self.func.as_bytes()
    }
}

/// A pending UDF binding, from [`Functions::create`].
#[must_use = "a create-function builder does nothing until .execute(&txn) runs it"]
pub struct CreateFunction<'a> {
    cf: &'a str,
    collection: &'a str,
    name: String,
    func: UdfFunction,
}

impl CreateFunction<'_> {
    /// Write the binding — a `runtime_tag::NATIVE` catalog entry whose bytes are
    /// the target function name — and mark the catalog snapshot stale so the next
    /// transaction resolves it.
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        txn.engine_txn().create_function(
            self.cf,
            self.collection,
            FunctionKind::Udf,
            &self.name,
            runtime_tag::NATIVE,
            self.func.encode(),
        )?;
        txn.mark_hooks_dirty();
        Ok(())
    }
}

// ── Shared remove builder ────────────────────────────────────────────

/// A pending script removal, from any kind's `remove(name)`. Removal is uniform
/// across kinds, so the three handles share this builder (it carries the kind).
#[must_use = "a remove-script builder does nothing until .execute(&txn) runs it"]
pub struct RemoveScript<'a> {
    cf: &'a str,
    collection: &'a str,
    kind: FunctionKind,
    name: String,
}

impl<'a> RemoveScript<'a> {
    fn new(cf: &'a str, collection: &'a str, kind: FunctionKind, name: &str) -> Self {
        Self {
            cf,
            collection,
            kind,
            name: name.to_string(),
        }
    }

    /// Remove the function.
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        remove_script(txn, self.cf, self.collection, self.kind, &self.name)
    }
}

#[cfg(test)]
mod tests {
    use bson::{Bson, doc};
    use slate_store::MemoryStore;

    use crate::{
        BindingKind, Database, DatabaseBuilder, TriggerCtx, UdfError, ValidatorCtx, Value, Verdict,
    };

    /// A native UDF: read arg 0 as a number and double it.
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

    /// Run `sql` over `collection` and collect the produced scalars as `f64`s.
    fn query_f64s(
        db: &Database<MemoryStore>,
        collection: &str,
        sql: &str,
    ) -> Result<Vec<f64>, crate::DbError> {
        let txn = db.begin(true).unwrap();
        db.collection(collection)
            .query(sql)
            .iter::<f64>(&txn)?
            .collect::<Result<Vec<f64>, _>>()
    }

    fn db_with_users() -> Database<MemoryStore> {
        with_users(DatabaseBuilder::new())
    }

    fn with_users(builder: DatabaseBuilder) -> Database<MemoryStore> {
        let db = builder.open(MemoryStore::new()).unwrap();
        let txn = db.begin(false).unwrap();
        db.collections().create("users").execute(&txn).unwrap();
        txn.commit().unwrap();
        db
    }

    #[test]
    fn create_list_remove_per_kind() {
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        users
            .triggers()
            .create(
                "on_write",
                super::TriggerFunction::from_name("on_write_impl"),
            )
            .execute(&txn)
            .unwrap();
        users
            .validators()
            .create(
                "must_pass",
                super::ValidatorFunction::from_name("pass_impl"),
            )
            .execute(&txn)
            .unwrap();
        users
            .functions()
            .create("add", super::UdfFunction::from_name("add_impl"))
            .execute(&txn)
            .unwrap();

        // each kind lists only its own
        assert_eq!(
            users.triggers().list(&txn).unwrap(),
            vec![("on_write".to_string(), "on_write_impl".to_string())]
        );
        assert_eq!(
            users.validators().list(&txn).unwrap(),
            vec![("must_pass".to_string(), "pass_impl".to_string())]
        );
        assert_eq!(
            users.functions().list(&txn).unwrap(),
            vec![("add".to_string(), "add_impl".to_string())]
        );

        // remove is per-handle, unambiguous about the kind
        users.triggers().remove("on_write").execute(&txn).unwrap();
        assert!(users.triggers().list(&txn).unwrap().is_empty());
        // removing the trigger left the validator and udf alone
        assert_eq!(
            users.validators().list(&txn).unwrap(),
            vec![("must_pass".to_string(), "pass_impl".to_string())]
        );
        assert_eq!(
            users.functions().list(&txn).unwrap(),
            vec![("add".to_string(), "add_impl".to_string())]
        );

        txn.commit().unwrap();
    }

    #[test]
    fn validator_runs_on_writes() {
        // A native validator, registered in the bag and bound through v2, is
        // enforced on a later transaction's insert — proving the catalog snapshot
        // picked up the binding (the `hooks_dirty` flip `create()` makes) and the
        // write path resolves it from the bag.
        let db = db_with_users();
        db.collection("users")
            .validators()
            .register("adults_only", |ctx: &ValidatorCtx<'_>| {
                match ctx.doc().get_i32("age") {
                    Ok(age) if age >= 18 => Ok(Verdict::Accept),
                    _ => Ok(Verdict::reject("must be 18+")),
                }
            });

        let txn = db.begin(false).unwrap();
        db.collection("users")
            .validators()
            .create(
                "adults_only",
                super::ValidatorFunction::from_name("adults_only"),
            )
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let txn = db.begin(false).unwrap();
        // an adult passes
        db.collection("users")
            .insert_one(doc! { "_id": 1, "age": 30 })
            .execute(&txn)
            .unwrap();
        // a minor is rejected by the validator
        let rejected = db
            .collection("users")
            .insert_one(doc! { "_id": 2, "age": 10 })
            .execute(&txn);
        assert!(rejected.is_err(), "validator should reject the minor");
        txn.commit().unwrap();
    }

    #[test]
    fn native_udf_registered_at_build_runs_in_a_query() {
        // A native UDF registered before open resolves and runs end-to-end:
        // `SELECT VALUE udf.double(c.x)` over two rows.
        let db = with_users(DatabaseBuilder::new().with_udf("double", double));

        let txn = db.begin(false).unwrap();
        // Bind `udf.double` on this collection to the registered `double` fn.
        db.collection("users")
            .functions()
            .create("double", super::UdfFunction::from_name("double"))
            .execute(&txn)
            .unwrap();
        db.collection("users")
            .insert_many(vec![doc! { "_id": 1, "x": 21 }, doc! { "_id": 2, "x": 50 }])
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let mut out = query_f64s(&db, "users", "SELECT VALUE udf.double(c.x) FROM c").unwrap();
        out.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        assert_eq!(out, vec![42.0, 100.0]);
    }

    #[test]
    fn registered_but_unbound_udf_does_not_resolve() {
        // The bag has `double`, but it is not *bound* on the collection — so the
        // isolation model leaves `udf.double` unbound. Registration alone is not
        // enough; a binding is required.
        let db = with_users(DatabaseBuilder::new().with_udf("double", double));

        let txn = db.begin(false).unwrap();
        db.collection("users")
            .insert_one(doc! { "_id": 1, "x": 21 })
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let err = query_f64s(&db, "users", "SELECT VALUE udf.double(c.x) FROM c").unwrap_err();
        assert!(
            err.to_string().contains("not bound"),
            "expected an unbound-udf error, got: {err}"
        );
    }

    #[test]
    fn native_udf_registered_at_runtime_runs_then_unregisters() {
        let db = db_with_users();

        // Register at runtime (no transaction), bind it on the collection, then
        // query through it.
        db.collection("users")
            .functions()
            .register("double", double);

        let txn = db.begin(false).unwrap();
        db.collection("users")
            .functions()
            .create("double", super::UdfFunction::from_name("double"))
            .execute(&txn)
            .unwrap();
        db.collection("users")
            .insert_many(vec![doc! { "_id": 1, "x": 21 }, doc! { "_id": 2, "x": 50 }])
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let mut out = query_f64s(&db, "users", "SELECT VALUE udf.double(c.x) FROM c").unwrap();
        out.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        assert_eq!(out, vec![42.0, 100.0]);

        // Unregister, and the same query no longer resolves.
        assert!(db.collection("users").functions().unregister("double"));
        let err = query_f64s(&db, "users", "SELECT VALUE udf.double(c.x) FROM c").unwrap_err();
        assert!(
            err.to_string().contains("udf.double"),
            "expected an unregistered-udf error after unregister, got: {err}"
        );
    }

    #[test]
    fn udf_binding_persists_and_lists_across_transactions() {
        let db = db_with_users();

        let txn = db.begin(false).unwrap();
        db.collection("users")
            .functions()
            .create("tax", super::UdfFunction::from_name("compute_tax"))
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        // A fresh transaction still lists the binding — it persisted in the
        // catalog through the commit (and the snapshot reload).
        let txn = db.begin(true).unwrap();
        assert_eq!(
            db.collection("users").functions().list(&txn).unwrap(),
            vec![("tax".to_string(), "compute_tax".to_string())]
        );
    }

    #[test]
    fn dangling_bindings_reports_unregistered_targets() {
        let db = db_with_users();

        // Bind `udf.tax -> compute_tax`, but never register `compute_tax`.
        let txn = db.begin(false).unwrap();
        db.collection("users")
            .functions()
            .create("tax", super::UdfFunction::from_name("compute_tax"))
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let dangling = db.dangling_bindings();
        assert_eq!(dangling.len(), 1);
        assert_eq!(dangling[0].collection, "users");
        assert_eq!(dangling[0].name, "tax");
        assert_eq!(dangling[0].func, "compute_tax");

        // Register the target → no longer dangling.
        db.collection("users")
            .functions()
            .register("compute_tax", |_: &[Value]| Ok(Value::defined(0_i32)));
        assert!(db.dangling_bindings().is_empty());
    }

    #[test]
    fn native_validator_registers_and_unregisters_at_runtime() {
        // The no-txn `register`/`unregister` verbs reach the database-scoped bag
        // off a collection handle (like `functions().register`). The bag is not
        // yet consulted on writes — that is the next slice — so this proves only
        // the wiring: a registered validator is present, then absent.
        let db = db_with_users();
        let users = db.collection("users");
        users
            .validators()
            .register("require_name", |ctx: &ValidatorCtx<'_>| {
                match ctx.doc().get_str("name") {
                    Ok(s) if !s.is_empty() => Ok(Verdict::Accept),
                    _ => Ok(Verdict::reject("name is required")),
                }
            });

        assert!(users.validators().unregister("require_name"));
        assert!(!users.validators().unregister("require_name"));
    }

    #[test]
    fn dangling_validator_binding_is_reported() {
        // A validator bound but whose impl is never registered is an unresolved
        // symbol — surfaced by `dangling_bindings()` with the `Validator` kind.
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        db.collection("users")
            .validators()
            .create(
                "adults_only",
                super::ValidatorFunction::from_name("adults_impl"),
            )
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let dangling = db.dangling_bindings();
        assert_eq!(dangling.len(), 1);
        assert_eq!(dangling[0].kind, BindingKind::Validator);
        assert_eq!(dangling[0].collection, "users");
        assert_eq!(dangling[0].name, "adults_only");
        assert_eq!(dangling[0].func, "adults_impl");

        // Register the impl → no longer dangling.
        db.collection("users")
            .validators()
            .register("adults_impl", |_: &ValidatorCtx<'_>| Ok(Verdict::Accept));
        assert!(db.dangling_bindings().is_empty());
    }

    #[test]
    fn dangling_validator_aborts_writes() {
        // The write-path counterpart to the read-path unbound-UDF error: a
        // validator bound to an unregistered function blocks *all* writes to the
        // collection (fail-safe), rather than silently allowing them.
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        db.collection("users")
            .validators()
            .create(
                "adults_only",
                super::ValidatorFunction::from_name("adults_impl"),
            )
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let txn = db.begin(false).unwrap();
        let result = db
            .collection("users")
            .insert_one(doc! { "_id": 1, "age": 30 })
            .execute(&txn);
        assert!(result.is_err(), "a dangling validator must abort the write");
    }

    #[test]
    fn native_validator_registered_at_build_is_in_the_bag() {
        // `DatabaseBuilder::with_validator` populates the same database-scoped bag
        // before open; the runtime handle sees it (unregister finds it).
        let db = with_users(
            DatabaseBuilder::new().with_validator("v", |_: &ValidatorCtx<'_>| Ok(Verdict::Accept)),
        );
        assert!(db.collection("users").validators().unregister("v"));
    }

    #[test]
    fn native_trigger_register_round_trips_through_the_handle() {
        // The collection handle's no-txn `register`/`unregister` reach the same
        // database-scoped bag (slice 2 wiring); no durable binding is involved.
        let db = db_with_users();
        db.collection("users")
            .triggers()
            .register("audit", |_: &TriggerCtx<'_>| Ok(()));
        assert!(db.collection("users").triggers().unregister("audit"));
        assert!(!db.collection("users").triggers().unregister("audit"));
    }

    #[test]
    fn native_trigger_registered_at_build_is_in_the_bag() {
        // `DatabaseBuilder::with_trigger` populates the same database-scoped bag
        // before open; the runtime handle sees it (unregister finds it).
        let db = with_users(DatabaseBuilder::new().with_trigger("t", |_: &TriggerCtx<'_>| Ok(())));
        assert!(db.collection("users").triggers().unregister("t"));
    }
}
