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

use slate_engine::{Catalog, FunctionKind, runtime_tag};
use slate_store::Store;

use crate::database::Transaction;
use crate::error::DbError;

/// Whether a function kind participates in the write-time hook snapshot. Triggers
/// and validators do (so registering/removing one marks it stale); UDFs don't.
fn affects_hooks(kind: FunctionKind) -> bool {
    matches!(kind, FunctionKind::Trigger | FunctionKind::Validator)
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
}

impl<'a> Functions<'a> {
    pub(crate) fn new(cf: &'a str, collection: &'a str) -> Self {
        Self { cf, collection }
    }

    /// Register a UDF named `name` with Lua `source`. Returns a builder; nothing
    /// runs until `.execute(&txn)`.
    pub fn create(&self, name: &str, source: &str) -> CreateFunction<'a> {
        CreateFunction {
            cf: self.cf,
            collection: self.collection,
            name: name.to_string(),
            source: source.to_string(),
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

/// A pending UDF registration, from [`Functions::create`].
#[must_use = "a create-function builder does nothing until .execute(&txn) runs it"]
pub struct CreateFunction<'a> {
    cf: &'a str,
    collection: &'a str,
    name: String,
    source: String,
}

impl CreateFunction<'_> {
    /// Register the UDF.
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        create_script(
            txn,
            self.cf,
            self.collection,
            FunctionKind::Udf,
            &self.name,
            &self.source,
        )
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

    use bson::doc;
    use slate_store::MemoryStore;
    use slate_vm::{LuaScriptRuntime, RuntimeKind};

    use crate::{CollectionConfig, Database, DatabaseBuilder, RuntimeRegistry, VmPool};

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
        txn.create_collection(&CollectionConfig {
            name: "users".to_string(),
            ..Default::default()
        })
        .unwrap();
        txn.commit().unwrap();
        db
    }

    // Scripts are bare Lua bodies: a validator `assert`s with `doc` in scope, a
    // trigger runs for side effects, a UDF `return`s from its named-global args.
    const PASS_VALIDATOR: &str = "assert(true)";
    const NOOP_TRIGGER: &str = "print('write')";
    const UDF_ADD: &str = "return a + b";

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
            .create("add", UDF_ADD)
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
}
