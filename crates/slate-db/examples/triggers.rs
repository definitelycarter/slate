use std::sync::Arc;

use bson::{doc, rawdoc};
use slate_db::{DatabaseBuilder, DbError, RuntimeRegistry, VmPool};
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
    let txn = db.begin(false)?;
    db.cf("app").collections().create("users").execute(&txn)?;
    db.cf("app").collections().create("audit").execute(&txn)?;
    txn.commit()?;

    // ── Register a trigger on "users" ─────────────────────────
    // The trigger fires on every mutation (insert, update, delete).
    // It logs each action to the "audit" collection and prints
    // the lifecycle event so you can see the before/after pairs.
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .triggers()
        .create(
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
        )
        .execute(&txn)?;
    txn.commit()?;

    // ── INSERT ────────────────────────────────────────────────
    // Fires: inserting → inserted
    println!("--- insert_one ---");
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .insert_one(doc! { "_id": "u1", "name": "Alice", "role": "engineer" })
        .execute(&txn)?;

    println!("\n--- insert_many ---");
    db.cf("app")
        .collection("users")
        .insert_many(vec![
            doc! { "_id": "u2", "name": "Bob",     "role": "designer" },
            doc! { "_id": "u3", "name": "Charlie", "role": "manager" },
        ])
        .execute(&txn)?;
    txn.commit()?;

    // ── UPDATE ────────────────────────────────────────────────
    // Fires: updating → updated
    println!("\n--- update_one ($set) ---");
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .find(rawdoc! { "_id": "u1" })
        .update(rawdoc! { "$set": { "role": "senior engineer" } })
        .one()
        .execute(&txn)?;
    txn.commit()?;

    println!("\n--- update_many ($set) ---");
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .find(rawdoc! { "role": "designer" })
        .update(rawdoc! { "$set": { "active": true } })
        .execute(&txn)?;
    txn.commit()?;

    // ── REPLACE ───────────────────────────────────────────────
    // Fires: updating → updated (same lifecycle as update)
    println!("\n--- replace_one ---");
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .find(rawdoc! { "_id": "u2" })
        .replace(doc! { "_id": "u2", "name": "Bob", "role": "lead designer", "active": true })
        .execute(&txn)?;
    txn.commit()?;

    // ── DELETE ─────────────────────────────────────────────────
    // Fires: deleting → deleted
    println!("\n--- delete_one ---");
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .find(rawdoc! { "_id": "u3" })
        .delete()
        .one()
        .execute(&txn)?;
    txn.commit()?;

    println!("\n--- delete_many ---");
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .insert_one(doc! { "_id": "u4", "name": "Diana", "role": "intern" })
        .execute(&txn)?;
    db.cf("app")
        .collection("users")
        .find(rawdoc! { "role": "intern" })
        .delete()
        .execute(&txn)?;
    txn.commit()?;

    // ── UPSERT ────────────────────────────────────────────────
    // Fires inserting/inserted for new docs, updating/updated for existing ones.
    println!("\n--- upsert_many (insert new + update existing) ---");
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .upsert_many(vec![
            doc! { "_id": "u1", "name": "Alice", "role": "staff engineer", "active": true }, // exists → update
            doc! { "_id": "u5", "name": "Eve",   "role": "engineer",       "active": true }, // new → insert
        ])
        .execute(&txn)?;
    txn.commit()?;

    // ── Verify the audit trail ────────────────────────────────
    println!("\n--- audit log ---");
    let txn = db.begin(true)?;
    let audit: Vec<_> = db
        .cf("app")
        .collection("audit")
        .find(rawdoc! {})
        .iter_raw(&txn)?
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
    let users: Vec<_> = db
        .cf("app")
        .collection("users")
        .find(rawdoc! {})
        .iter_raw(&txn)?
        .collect::<Result<Vec<_>, _>>()?;

    for u in &users {
        println!("  {} — {}", u.get_str("name")?, u.get_str("role")?);
    }
    println!("Total users: {}", users.len());
    drop(txn);

    println!("\nDone!");
    Ok(())
}
