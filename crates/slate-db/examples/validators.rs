use std::sync::Arc;

use bson::doc;
use slate_db::{CollectionConfig, DatabaseBuilder, DbError, RuntimeRegistry, VmPool};
use slate_store::MemoryStore;
use slate_vm::{LuaScriptRuntime, RuntimeKind};

fn main() -> Result<(), DbError> {
    // ── Open an in-memory database with Lua scripting ──────────
    let mut reg = RuntimeRegistry::new();
    reg.register(RuntimeKind::Lua, Arc::new(LuaScriptRuntime::new()));
    let db = DatabaseBuilder::new()
        .with_scripting(VmPool::new(reg))
        .open(MemoryStore::new())?;

    // ── Create a "users" collection ─────────────────────────────
    let mut txn = db.begin(false)?;
    txn.create_collection(&CollectionConfig {
        cf: "app".into(),
        name: "users".into(),
        ..Default::default()
    })?;
    txn.commit()?;

    // ── Register validators ─────────────────────────────────────
    // Validators are pure functions: they receive { doc = <the document> }
    // and must return { ok = true } to pass, or { ok = false, reason = "..." }
    // to reject the document.
    let mut txn = db.begin(false)?;

    // 1) "name" must be a non-empty string
    txn.register_validator(
        "app",
        "users",
        "require_name",
        r#"
        return function(event)
          local doc = event.doc
          if type(doc.name) ~= "string" or doc.name == "" then
            return { ok = false, reason = "name is required and must be a non-empty string" }
          end
          return { ok = true }
        end
        "#,
    )?;

    // 2) "age", if present, must be a non-negative number.
    //    BSON i32 values arrive in Lua as userdata with a :value() method,
    //    so we normalize before comparing.
    txn.register_validator(
        "app",
        "users",
        "valid_age",
        r#"
        return function(event)
          local raw = event.doc.age
          if raw == nil then return { ok = true } end

          -- Normalize BSON integers (userdata) to plain Lua numbers.
          local age = (type(raw) == "userdata") and raw:value() or raw
          if type(age) ~= "number" or age < 0 then
            return { ok = false, reason = "age must be a non-negative number" }
          end
          return { ok = true }
        end
        "#,
    )?;

    txn.commit()?;

    // ── Successful insert ───────────────────────────────────────
    println!("--- insert valid document ---");
    let mut txn = db.begin(false)?;
    txn.insert_one(
        "app",
        "users",
        doc! { "_id": "u1", "name": "Alice", "age": 30 },
    )?
    .drain()?;
    txn.commit()?;
    println!("  OK: inserted Alice\n");

    // ── Another successful insert (age is optional) ─────────────
    println!("--- insert valid document (no age) ---");
    let mut txn = db.begin(false)?;
    txn.insert_one(
        "app",
        "users",
        doc! { "_id": "u2", "name": "Bob" },
    )?
    .drain()?;
    txn.commit()?;
    println!("  OK: inserted Bob\n");

    // ── Failed insert: missing name ─────────────────────────────
    // Each failure case uses a block so the transaction is dropped
    // (releasing the write lock) before we begin the next one.
    println!("--- insert document with missing name ---");
    {
        let mut txn = db.begin(false)?;
        let result = txn
            .insert_one(
                "app",
                "users",
                doc! { "_id": "u3", "age": 25 },
            )?
            .drain();
        match result {
            Ok(_) => println!("  BUG: should have been rejected"),
            Err(e) => println!("  REJECTED: {e}\n"),
        }
    }

    // ── Failed insert: negative age ─────────────────────────────
    println!("--- insert document with negative age ---");
    {
        let mut txn = db.begin(false)?;
        let result = txn
            .insert_one(
                "app",
                "users",
                doc! { "_id": "u4", "name": "Charlie", "age": -5 },
            )?
            .drain();
        match result {
            Ok(_) => println!("  BUG: should have been rejected"),
            Err(e) => println!("  REJECTED: {e}\n"),
        }
    }

    // ── Failed insert: empty name ───────────────────────────────
    println!("--- insert document with empty name ---");
    {
        let mut txn = db.begin(false)?;
        let result = txn
            .insert_one(
                "app",
                "users",
                doc! { "_id": "u5", "name": "", "age": 20 },
            )?
            .drain();
        match result {
            Ok(_) => println!("  BUG: should have been rejected"),
            Err(e) => println!("  REJECTED: {e}\n"),
        }
    }

    // ── Verify only valid documents were persisted ──────────────
    println!("--- final users ---");
    let txn = db.begin(true)?;
    let users: Vec<_> = txn
        .find("app", "users", bson::rawdoc! {}, Default::default())?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;

    for u in &users {
        print!("  {} — {}", u.get_str("_id")?, u.get_str("name")?);
        if let Ok(age) = u.get_i32("age") {
            print!(", age {age}");
        }
        println!();
    }
    println!("Total users: {} (expected 2)\n", users.len());
    drop(txn);

    println!("Done!");
    Ok(())
}
