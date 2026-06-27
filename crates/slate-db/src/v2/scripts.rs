//! v2 schema surface: the per-kind script sub-handles.
//!
//! Phase 0, slice D. A collection carries three kinds of stored Lua function,
//! and they are *not* the same shape — triggers and validators feed the
//! write-time hook snapshot (registering one marks it stale), while UDFs are
//! called from SQL at query time and touch no hooks. They will diverge further:
//! triggers in particular will grow timing/operation options. So each kind gets
//! its **own** sub-handle rather than one `scripts()` grouping:
//!
//! ```ignore
//! collection.triggers().create(name, source).execute(&txn)?;
//! collection.validators().create(name, source).execute(&txn)?;
//! collection.functions().create(name, source).execute(&txn)?;   // UDFs
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

use slate_engine::{Catalog, FunctionKind, runtime_tag};
use slate_store::Store;
use slate_udf::{Udf, UdfBag};

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

/// Store a Lua function of `kind`, marking the hook snapshot stale when the kind
/// feeds it. The engine call is identical across kinds today; the divergence is
/// in the public `create` builders, not this lowering.
fn create_script<S: Store>(
    txn: &Transaction<'_, S>,
    cf: &str,
    collection: &str,
    kind: FunctionKind,
    name: &str,
    source: &str,
) -> Result<(), DbError> {
    txn.engine_txn().create_function(
        cf,
        collection,
        kind,
        name,
        runtime_tag::LUA,
        source.as_bytes(),
    )?;
    if affects_hooks(kind) {
        txn.mark_hooks_dirty();
    }
    Ok(())
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

/// The names of every function of `kind` registered on the collection.
fn list_scripts<S: Store>(
    txn: &Transaction<'_, S>,
    cf: &str,
    collection: &str,
    kind: FunctionKind,
) -> Result<Vec<String>, DbError> {
    Ok(txn
        .engine_txn()
        .load_functions(cf, collection, kind)?
        .into_iter()
        .map(|entry| entry.name)
        .collect())
}

// ── Triggers ─────────────────────────────────────────────────────────

/// The `triggers()` sub-handle, from [`Collection::triggers`](super::Collection::triggers).
pub struct Triggers<'a> {
    cf: &'a str,
    collection: &'a str,
}

impl<'a> Triggers<'a> {
    pub(crate) fn new(cf: &'a str, collection: &'a str) -> Self {
        Self { cf, collection }
    }

    /// Register a trigger named `name` with Lua `source`. Returns a builder;
    /// nothing runs until `.execute(&txn)`.
    pub fn create(&self, name: &str, source: &str) -> CreateTrigger<'a> {
        CreateTrigger {
            cf: self.cf,
            collection: self.collection,
            name: name.to_string(),
            source: source.to_string(),
        }
    }

    /// Remove the trigger named `name`. Returns a builder; nothing runs until
    /// `.execute(&txn)`.
    pub fn remove(&self, name: &str) -> RemoveScript<'a> {
        RemoveScript::new(self.cf, self.collection, FunctionKind::Trigger, name)
    }

    /// List the registered trigger names.
    pub fn list<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<Vec<String>, DbError> {
        list_scripts(txn, self.cf, self.collection, FunctionKind::Trigger)
    }
}

/// A pending trigger registration, from [`Triggers::create`]. Its own type so
/// future trigger-specific options (timing, operations) land here without
/// touching validators or functions.
#[must_use = "a create-trigger builder does nothing until .execute(&txn) runs it"]
pub struct CreateTrigger<'a> {
    cf: &'a str,
    collection: &'a str,
    name: String,
    source: String,
}

impl CreateTrigger<'_> {
    /// Register the trigger.
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        create_script(
            txn,
            self.cf,
            self.collection,
            FunctionKind::Trigger,
            &self.name,
            &self.source,
        )
    }
}

// ── Validators ───────────────────────────────────────────────────────

/// The `validators()` sub-handle, from [`Collection::validators`](super::Collection::validators).
pub struct Validators<'a> {
    cf: &'a str,
    collection: &'a str,
}

impl<'a> Validators<'a> {
    pub(crate) fn new(cf: &'a str, collection: &'a str) -> Self {
        Self { cf, collection }
    }

    /// Register a validator named `name` with Lua `source`. Returns a builder;
    /// nothing runs until `.execute(&txn)`.
    pub fn create(&self, name: &str, source: &str) -> CreateValidator<'a> {
        CreateValidator {
            cf: self.cf,
            collection: self.collection,
            name: name.to_string(),
            source: source.to_string(),
        }
    }

    /// Remove the validator named `name`. Returns a builder; nothing runs until
    /// `.execute(&txn)`.
    pub fn remove(&self, name: &str) -> RemoveScript<'a> {
        RemoveScript::new(self.cf, self.collection, FunctionKind::Validator, name)
    }

    /// List the registered validator names.
    pub fn list<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<Vec<String>, DbError> {
        list_scripts(txn, self.cf, self.collection, FunctionKind::Validator)
    }
}

/// A pending validator registration, from [`Validators::create`].
#[must_use = "a create-validator builder does nothing until .execute(&txn) runs it"]
pub struct CreateValidator<'a> {
    cf: &'a str,
    collection: &'a str,
    name: String,
    source: String,
}

impl CreateValidator<'_> {
    /// Register the validator.
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        create_script(
            txn,
            self.cf,
            self.collection,
            FunctionKind::Validator,
            &self.name,
            &self.source,
        )
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

    /// List the registered UDF names.
    pub fn list<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<Vec<String>, DbError> {
        list_scripts(txn, self.cf, self.collection, FunctionKind::Udf)
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
    use std::sync::Arc;

    use bson::{Bson, doc};
    use slate_store::MemoryStore;
    use slate_vm::{LuaScriptRuntime, RuntimeKind};

    use crate::{Database, DatabaseBuilder, RuntimeRegistry, UdfError, Value, VmPool};

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

    /// A database with the Lua runtime wired in, so registered validators and
    /// triggers actually *execute* on writes.
    fn scripting_db_with_users() -> Database<MemoryStore> {
        let mut reg = RuntimeRegistry::new();
        reg.register(RuntimeKind::Lua, Arc::new(LuaScriptRuntime::new()));
        with_users(DatabaseBuilder::new().with_scripting(VmPool::new(reg)))
    }

    fn with_users(builder: DatabaseBuilder) -> Database<MemoryStore> {
        let db = builder.open(MemoryStore::new()).unwrap();
        let txn = db.begin(false).unwrap();
        db.collections().create("users").execute(&txn).unwrap();
        txn.commit().unwrap();
        db
    }

    // Trigger/validator scripts are bare Lua bodies; a UDF is now a native
    // binding (`UdfFunction::from_name`), not source.
    const PASS_VALIDATOR: &str = "assert(true)";
    const NOOP_TRIGGER: &str = "print('write')";

    #[test]
    fn create_list_remove_per_kind() {
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        users
            .triggers()
            .create("on_write", NOOP_TRIGGER)
            .execute(&txn)
            .unwrap();
        users
            .validators()
            .create("must_pass", PASS_VALIDATOR)
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
            vec!["on_write".to_string()]
        );
        assert_eq!(
            users.validators().list(&txn).unwrap(),
            vec!["must_pass".to_string()]
        );
        assert_eq!(
            users.functions().list(&txn).unwrap(),
            vec!["add".to_string()]
        );

        // remove is per-handle, unambiguous about the kind
        users.triggers().remove("on_write").execute(&txn).unwrap();
        assert!(users.triggers().list(&txn).unwrap().is_empty());
        // removing the trigger left the validator and udf alone
        assert_eq!(
            users.validators().list(&txn).unwrap(),
            vec!["must_pass".to_string()]
        );
        assert_eq!(
            users.functions().list(&txn).unwrap(),
            vec!["add".to_string()]
        );

        txn.commit().unwrap();
    }

    #[test]
    fn validator_runs_on_writes() {
        // A validator registered through v2 is enforced on a later transaction's
        // insert — proving the hook snapshot picked up the registration (the
        // `hooks_dirty` flip `create()` makes for a validator).
        let db = scripting_db_with_users();
        let txn = db.begin(false).unwrap();
        db.collection("users")
            .validators()
            .create(
                "adults_only",
                r#"
                return function(event)
                  local raw = event.doc.age
                  local age = (type(raw) == "userdata") and raw:value() or raw
                  if type(age) ~= "number" or age < 18 then
                    return { ok = false, reason = "must be 18+" }
                  end
                  return { ok = true }
                end
                "#,
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
            vec!["tax".to_string()]
        );
    }
}
