use bson::{doc, rawdoc};
use slate_db::v2::TriggerFunction;
use slate_db::{DatabaseBuilder, DbError, TriggerCtx, TriggerError};
use slate_store::MemoryStore;

/// A native trigger that mirrors every lifecycle event into an `audit`
/// collection. It fires on each mutation (`inserting`/`inserted`,
/// `updating`/`updated`, `deleting`/`deleted`); the audit record's `_id` is built
/// from the document id + action so each event is stored separately. Writing to a
/// *sibling* collection in the same column family is exactly what the trigger's
/// `ctx` permits — it cannot reach across column families.
fn audit(ctx: &TriggerCtx<'_>) -> Result<(), TriggerError> {
    let action = ctx.action();
    let id = ctx
        .doc()
        .get_str("_id")
        .map_err(|e| TriggerError::Body(e.to_string()))?;

    println!("[trigger] {action} → _id={id}");

    ctx.put(
        "audit",
        &rawdoc! {
            "_id": format!("{id}:{action}"),
            "action": action,
            "doc_id": id,
        },
    )?;
    Ok(())
}

fn main() -> Result<(), DbError> {
    // ── Open an in-memory database with the native trigger registered ──
    // `with_trigger` puts the closure in the database-scoped bag at open; the
    // per-collection binding below points at it by name.
    let db = DatabaseBuilder::new()
        .with_trigger("audit", audit)
        .open(MemoryStore::new())?;

    // ── Set up collections ────────────────────────────────────
    let txn = db.begin(false)?;
    db.cf("app").collections().create("users").execute(&txn)?;
    db.cf("app").collections().create("audit").execute(&txn)?;
    txn.commit()?;

    // ── Bind the trigger on "users" ───────────────────────────
    // The binding is durable (catalog) and names the native function `audit`
    // registered in the bag above; it fires on every mutation to "users".
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .triggers()
        .create("audit_trigger", TriggerFunction::from_name("audit"))
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
        println!(
            "  {}: {} on {}",
            entry.get_str("_id")?,
            entry.get_str("action")?,
            entry.get_str("doc_id")?,
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
