use std::sync::Arc;

use bson::{doc, rawdoc};
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

    // ── Set up collections ────────────────────────────────────
    let mut txn = db.begin(false)?;
    txn.create_collection(&CollectionConfig {
        cf: "app".into(),
        name: "users".into(),
        ..Default::default()
    })?;
    txn.create_collection(&CollectionConfig {
        cf: "app".into(),
        name: "audit".into(),
        ..Default::default()
    })?;
    txn.commit()?;

    // ── Register a trigger on "users" ─────────────────────────
    // The trigger fires on every mutation (insert, update, delete).
    // It logs each action to the "audit" collection and prints
    // the lifecycle event so you can see the before/after pairs.
    let mut txn = db.begin(false)?;
    txn.register_trigger(
        "app",
        "users",
        "audit_trigger",
        r#"
        return function(ctx, event)
          local action = event.action
          local id     = event.doc._id

          print("[trigger] " .. action .. " → _id=" .. tostring(id))

          -- Write an audit record for every lifecycle event.
          -- Build a unique _id from the doc id + action so that
          -- each event is stored separately.
          ctx.put("audit", {
            _id       = tostring(id) .. ":" .. action,
            action    = action,
            doc_id    = id,
            timestamp = bson.now(),
          })

          return event
        end
        "#,
    )?;
    txn.commit()?;

    // ── INSERT ────────────────────────────────────────────────
    // Fires: inserting → inserted
    println!("--- insert_one ---");
    let mut txn = db.begin(false)?;
    txn.insert_one(
        "app",
        "users",
        doc! { "_id": "u1", "name": "Alice", "role": "engineer" },
    )?
    .drain()?;

    println!("\n--- insert_many ---");
    txn.insert_many(
        "app",
        "users",
        vec![
            doc! { "_id": "u2", "name": "Bob",     "role": "designer" },
            doc! { "_id": "u3", "name": "Charlie", "role": "manager" },
        ],
    )?
    .drain()?;
    txn.commit()?;

    // ── UPDATE ────────────────────────────────────────────────
    // Fires: updating → updated
    println!("\n--- update_one ($set) ---");
    let txn = db.begin(false)?;
    txn.update_one(
        "app",
        "users",
        rawdoc! { "_id": "u1" },
        rawdoc! { "$set": { "role": "senior engineer" } },
    )?
    .drain()?;
    txn.commit()?;

    println!("\n--- update_many ($set) ---");
    let txn = db.begin(false)?;
    txn.update_many(
        "app",
        "users",
        rawdoc! { "role": "designer" },
        rawdoc! { "$set": { "active": true } },
    )?
    .drain()?;
    txn.commit()?;

    // ── REPLACE ───────────────────────────────────────────────
    // Fires: updating → updated (same lifecycle as update)
    println!("\n--- replace_one ---");
    let txn = db.begin(false)?;
    txn.replace_one(
        "app",
        "users",
        rawdoc! { "_id": "u2" },
        doc! { "_id": "u2", "name": "Bob", "role": "lead designer", "active": true },
    )?
    .drain()?;
    txn.commit()?;

    // ── DELETE ─────────────────────────────────────────────────
    // Fires: deleting → deleted
    println!("\n--- delete_one ---");
    let txn = db.begin(false)?;
    txn.delete_one("app", "users", rawdoc! { "_id": "u3" })?
        .drain()?;
    txn.commit()?;

    println!("\n--- delete_many ---");
    let mut txn = db.begin(false)?;
    txn.insert_one(
        "app",
        "users",
        doc! { "_id": "u4", "name": "Diana", "role": "intern" },
    )?
    .drain()?;
    txn.delete_many("app", "users", rawdoc! { "role": "intern" })?
        .drain()?;
    txn.commit()?;

    // ── UPSERT ────────────────────────────────────────────────
    // Fires inserting/inserted for new docs, updating/updated for existing ones.
    println!("\n--- upsert_many (insert new + update existing) ---");
    let txn = db.begin(false)?;
    txn.upsert_many(
        "app",
        "users",
        vec![
            doc! { "_id": "u1", "name": "Alice", "role": "staff engineer", "active": true }, // exists → update
            doc! { "_id": "u5", "name": "Eve",   "role": "engineer",       "active": true }, // new → insert
        ],
    )?
    .drain()?;
    txn.commit()?;

    // ── Verify the audit trail ────────────────────────────────
    println!("\n--- audit log ---");
    let txn = db.begin(true)?;
    let audit: Vec<_> = txn
        .find("app", "audit", rawdoc! {}, Default::default())?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;

    for entry in &audit {
        let ts = entry.get_datetime("timestamp")?;
        println!(
            "  {}: {} on {} at {}",
            entry.get_str("_id")?,
            entry.get_str("action")?,
            entry.get_str("doc_id")?,
            ts.try_to_rfc3339_string().unwrap_or_default(),
        );
    }
    println!("Total audit entries: {}", audit.len());
    drop(txn);

    // ── Final state of users ──────────────────────────────────
    println!("\n--- final users ---");
    let txn = db.begin(true)?;
    let users: Vec<_> = txn
        .find("app", "users", rawdoc! {}, Default::default())?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;

    for u in &users {
        println!("  {} — {}", u.get_str("name")?, u.get_str("role")?);
    }
    println!("Total users: {}", users.len());
    drop(txn);

    println!("\nDone!");
    Ok(())
}
