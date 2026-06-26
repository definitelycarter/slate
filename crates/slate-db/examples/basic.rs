use bson::{Bson, doc, rawdoc};
use slate_db::v2::IndexOptions;
use slate_db::{DatabaseBuilder, DbError};
use slate_query::SortDirection;
use slate_store::MemoryStore;

fn main() -> Result<(), DbError> {
    // ── Open an in-memory database ──────────────────────────────
    let db = DatabaseBuilder::new().open(MemoryStore::new())?;

    // ── Create a collection ─────────────────────────────────────
    let txn = db.begin(false)?;
    db.collections().create("users").execute(&txn)?;
    txn.commit()?;

    // ── Insert documents ────────────────────────────────────────
    let txn = db.begin(false)?;

    db.collection("users")
        .insert_one(doc! {
            "_id": "user-1",
            "name": "Alice",
            "age": 32,
            "role": "engineer",
            "address": { "city": "Austin", "state": "TX" }
        })
        .execute(&txn)?;

    db.collection("users")
        .insert_many(vec![
            doc! { "_id": "user-2", "name": "Bob",     "age": 28, "role": "designer",  "address": { "city": "Denver",  "state": "CO" } },
            doc! { "_id": "user-3", "name": "Charlie", "age": 45, "role": "engineer",  "address": { "city": "Austin",  "state": "TX" } },
            doc! { "_id": "user-4", "name": "Diana",   "age": 38, "role": "manager",   "address": { "city": "Seattle", "state": "WA" } },
            doc! { "_id": "user-5", "name": "Eve",     "age": 25, "role": "engineer",  "address": { "city": "Denver",  "state": "CO" } },
        ])
        .execute(&txn)?;

    txn.commit()?;
    println!("Inserted 5 users.");

    // ── Find all documents ──────────────────────────────────────
    let txn = db.begin(true)?;
    let all: Vec<_> = db
        .collection("users")
        .find(rawdoc! {})
        .iter_raw(&txn)?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Total users: {}", all.len());

    // ── Find one by _id ─────────────────────────────────────────
    let alice = db
        .collection("users")
        .find(rawdoc! { "_id": "user-1" })
        .iter_raw(&txn)?
        .next()
        .transpose()?
        .expect("alice should exist");
    println!(
        "Found: {} (age {})",
        alice.get_str("name")?,
        alice.get_i32("age")?
    );

    // ── Filter: equality ────────────────────────────────────────
    let engineers: Vec<_> = db
        .collection("users")
        .find(rawdoc! { "role": "engineer" })
        .iter_raw(&txn)?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Engineers: {}", engineers.len());

    // ── Filter: comparison ($gt, $lte) ──────────────────────────
    let over_30: Vec<_> = db
        .collection("users")
        .find(rawdoc! { "age": { "$gt": 30 } })
        .iter_raw(&txn)?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Users over 30: {}", over_30.len());

    // ── Filter: dot-notation on nested fields ───────────────────
    let in_austin: Vec<_> = db
        .collection("users")
        .find(rawdoc! { "address.city": "Austin" })
        .iter_raw(&txn)?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Users in Austin: {}", in_austin.len());

    // ── Sort + pagination ───────────────────────────────────────
    let page: Vec<_> = db
        .collection("users")
        .find(rawdoc! {})
        .sort("age", SortDirection::Desc)
        .offset(1)
        .limit(2)
        .iter_raw(&txn)?
        .collect::<Result<Vec<_>, _>>()?;
    println!(
        "Page (sorted by age desc, skip 1, take 2): {}",
        page.iter()
            .map(|d| d.get_str("name").unwrap())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // ── Projection: only return specific fields ─────────────────
    let names_only: Vec<_> = db
        .collection("users")
        .find(rawdoc! {})
        .project(vec!["name".into(), "role".into()])
        .iter_raw(&txn)?
        .collect::<Result<Vec<_>, _>>()?;
    for d in &names_only {
        // _id is always included; only projected columns are returned
        assert!(d.get("age").unwrap().is_none());
    }
    println!(
        "Projection returned {} docs (age field excluded)",
        names_only.len()
    );

    // ── Count ───────────────────────────────────────────────────
    let count = db
        .collection("users")
        .find(rawdoc! { "role": "engineer" })
        .iter_raw(&txn)?
        .count();
    println!("Engineer count: {}", count);
    drop(txn);

    // ── Update documents ────────────────────────────────────────
    let txn = db.begin(false)?;

    // $set a field
    db.collection("users")
        .find(rawdoc! { "_id": "user-1" })
        .update(rawdoc! { "$set": { "age": 33 } })
        .one()
        .execute(&txn)?;

    // $inc a numeric field
    db.collection("users")
        .find(rawdoc! { "role": "engineer" })
        .update(rawdoc! { "$inc": { "age": 1 } })
        .execute(&txn)?;

    txn.commit()?;
    println!("Updated ages.");

    // ── Create an index ─────────────────────────────────────────
    let txn = db.begin(false)?;
    db.collection("users")
        .indexes()
        .create("role", IndexOptions::default())
        .execute(&txn)?;
    txn.commit()?;
    println!("Created index on 'role'.");

    // Queries on indexed fields are automatically accelerated
    let txn = db.begin(true)?;
    let designers: Vec<_> = db
        .collection("users")
        .find(rawdoc! { "role": "designer" })
        .iter_raw(&txn)?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Designers (via index scan): {}", designers.len());
    drop(txn);

    // ── Delete documents ────────────────────────────────────────
    let txn = db.begin(false)?;
    db.collection("users")
        .find(rawdoc! { "_id": "user-5" })
        .delete()
        .one()
        .execute(&txn)?;
    txn.commit()?;

    let txn = db.begin(true)?;
    let remaining = db
        .collection("users")
        .find(rawdoc! {})
        .iter_raw(&txn)?
        .count();
    println!("Users after delete: {}", remaining);
    drop(txn);

    // ── Distinct values ─────────────────────────────────────────
    let txn = db.begin(true)?;
    let roles: Vec<Bson> = db
        .collection("users")
        .find(rawdoc! {})
        .distinct("role")
        .iter_raw(&txn)?
        .map(|r| Bson::try_from(r.unwrap()).unwrap())
        .collect();
    println!("Distinct roles: {:?}", roles);
    drop(txn);

    // ── Custom column family ─────────────────────────────────────
    // Collections can be scoped to a column family instead of DEFAULT_CF.
    // The same collection name in different CFs are fully isolated.
    let txn = db.begin(false)?;
    db.cf("analytics")
        .collections()
        .create("events")
        .execute(&txn)?;
    db.cf("analytics")
        .collection("events")
        .insert_many(vec![
            doc! { "_id": "e1", "type": "page_view", "url": "/home" },
            doc! { "_id": "e2", "type": "click",     "url": "/signup" },
            doc! { "_id": "e3", "type": "page_view", "url": "/docs" },
        ])
        .execute(&txn)?;
    txn.commit()?;

    let txn = db.begin(true)?;
    let views: Vec<_> = db
        .cf("analytics")
        .collection("events")
        .find(rawdoc! { "type": "page_view" })
        .iter_raw(&txn)?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Events in 'analytics' CF: {} page views", views.len());
    drop(txn);

    println!("\nDone!");
    Ok(())
}
