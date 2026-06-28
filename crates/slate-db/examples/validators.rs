use bson::doc;
use slate_db::v2::ValidatorFunction;
use slate_db::{DatabaseBuilder, DbError, ValidatorCtx, Verdict};
use slate_store::MemoryStore;

fn main() -> Result<(), DbError> {
    // ── Open an in-memory database with native validators ──────
    // A validator is a native Rust function that inspects the candidate
    // document and returns `Verdict::Accept` to pass, or
    // `Verdict::reject(reason)` to reject the write. They are registered in a
    // database-scoped bag (here, before open via `with_validator`) and bound
    // per-collection below.
    let db = DatabaseBuilder::new()
        // 1) "name" must be a non-empty string.
        .with_validator("require_name", |ctx: &ValidatorCtx<'_>| {
            match ctx.doc().get_str("name") {
                Ok(name) if !name.is_empty() => Ok(Verdict::Accept),
                _ => Ok(Verdict::reject(
                    "name is required and must be a non-empty string",
                )),
            }
        })
        // 2) "age", if present, must be a non-negative number.
        .with_validator("valid_age", |ctx: &ValidatorCtx<'_>| {
            match ctx.doc().get_i32("age") {
                // Absent (or not an i32) → no constraint to enforce; age is optional.
                Err(_) => Ok(Verdict::Accept),
                Ok(age) if age >= 0 => Ok(Verdict::Accept),
                Ok(_) => Ok(Verdict::reject("age must be a non-negative number")),
            }
        })
        .open(MemoryStore::new())?;

    // ── Create a "users" collection ─────────────────────────────
    let txn = db.begin(false)?;
    db.cf("app").collections().create("users").execute(&txn)?;
    txn.commit()?;

    // ── Bind the validators to the collection ───────────────────
    // The binding is durable and per-collection: it maps the validator name to
    // a native function in the bag. (The name and the bag function happen to
    // match here, but they need not.)
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .validators()
        .create("require_name", ValidatorFunction::from_name("require_name"))
        .execute(&txn)?;
    db.cf("app")
        .collection("users")
        .validators()
        .create("valid_age", ValidatorFunction::from_name("valid_age"))
        .execute(&txn)?;
    txn.commit()?;

    // ── Successful insert ───────────────────────────────────────
    println!("--- insert valid document ---");
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .insert_one(doc! { "_id": "u1", "name": "Alice", "age": 30 })
        .execute(&txn)?;
    txn.commit()?;
    println!("  OK: inserted Alice\n");

    // ── Another successful insert (age is optional) ─────────────
    println!("--- insert valid document (no age) ---");
    let txn = db.begin(false)?;
    db.cf("app")
        .collection("users")
        .insert_one(doc! { "_id": "u2", "name": "Bob" })
        .execute(&txn)?;
    txn.commit()?;
    println!("  OK: inserted Bob\n");

    // ── Failed insert: missing name ─────────────────────────────
    // Each failure case uses a block so the transaction is dropped
    // (releasing the write lock) before we begin the next one.
    println!("--- insert document with missing name ---");
    {
        let txn = db.begin(false)?;
        let result = db
            .cf("app")
            .collection("users")
            .insert_one(doc! { "_id": "u3", "age": 25 })
            .execute(&txn);
        match result {
            Ok(_) => println!("  BUG: should have been rejected"),
            Err(e) => println!("  REJECTED: {e}\n"),
        }
    }

    // ── Failed insert: negative age ─────────────────────────────
    println!("--- insert document with negative age ---");
    {
        let txn = db.begin(false)?;
        let result = db
            .cf("app")
            .collection("users")
            .insert_one(doc! { "_id": "u4", "name": "Charlie", "age": -5 })
            .execute(&txn);
        match result {
            Ok(_) => println!("  BUG: should have been rejected"),
            Err(e) => println!("  REJECTED: {e}\n"),
        }
    }

    // ── Failed insert: empty name ───────────────────────────────
    println!("--- insert document with empty name ---");
    {
        let txn = db.begin(false)?;
        let result = db
            .cf("app")
            .collection("users")
            .insert_one(doc! { "_id": "u5", "name": "", "age": 20 })
            .execute(&txn);
        match result {
            Ok(_) => println!("  BUG: should have been rejected"),
            Err(e) => println!("  REJECTED: {e}\n"),
        }
    }

    // ── Verify only valid documents were persisted ──────────────
    println!("--- final users ---");
    let txn = db.begin(true)?;
    let users: Vec<_> = db
        .cf("app")
        .collection("users")
        .find(bson::rawdoc! {})
        .iter_raw(&txn)?
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
