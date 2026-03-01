use bson::{Bson, RawBson, doc, rawdoc};
use slate_db::{CollectionConfig, Database, DatabaseConfig, DbError, DEFAULT_CF};
use slate_query::{FindOptions, Sort, SortDirection};
use slate_store::MemoryStore;

fn main() -> Result<(), DbError> {
    // ── Open an in-memory database ──────────────────────────────
    let store = MemoryStore::new();
    let db = Database::open(store, DatabaseConfig::default());

    // ── Create a collection ─────────────────────────────────────
    let mut txn = db.begin(false)?;
    txn.create_collection(&CollectionConfig {
        name: "users".into(),
        ..Default::default()
    })?;
    txn.commit()?;

    // ── Insert documents ────────────────────────────────────────
    let mut txn = db.begin(false)?;

    txn.insert_one(
        DEFAULT_CF,
        "users",
        doc! {
            "_id": "user-1",
            "name": "Alice",
            "age": 32,
            "role": "engineer",
            "address": { "city": "Austin", "state": "TX" }
        },
    )?
    .drain()?;

    txn.insert_many(
        DEFAULT_CF,
        "users",
        vec![
            doc! { "_id": "user-2", "name": "Bob",     "age": 28, "role": "designer",  "address": { "city": "Denver",  "state": "CO" } },
            doc! { "_id": "user-3", "name": "Charlie", "age": 45, "role": "engineer",  "address": { "city": "Austin",  "state": "TX" } },
            doc! { "_id": "user-4", "name": "Diana",   "age": 38, "role": "manager",   "address": { "city": "Seattle", "state": "WA" } },
            doc! { "_id": "user-5", "name": "Eve",     "age": 25, "role": "engineer",  "address": { "city": "Denver",  "state": "CO" } },
        ],
    )?
    .drain()?;

    txn.commit()?;
    println!("Inserted 5 users.");

    // ── Find all documents ──────────────────────────────────────
    let txn = db.begin(true)?;
    let all: Vec<_> = txn
        .find(DEFAULT_CF, "users", rawdoc! {}, FindOptions::default())?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Total users: {}", all.len());

    // ── Find one by _id ─────────────────────────────────────────
    let alice = txn
        .find_one(DEFAULT_CF, "users", rawdoc! { "_id": "user-1" })?
        .expect("alice should exist");
    println!("Found: {} (age {})", alice.get_str("name")?, alice.get_i32("age")?);

    // ── Filter: equality ────────────────────────────────────────
    let engineers: Vec<_> = txn
        .find(
            DEFAULT_CF,
            "users",
            rawdoc! { "role": "engineer" },
            FindOptions::default(),
        )?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Engineers: {}", engineers.len());

    // ── Filter: comparison ($gt, $lte) ──────────────────────────
    let over_30: Vec<_> = txn
        .find(
            DEFAULT_CF,
            "users",
            rawdoc! { "age": { "$gt": 30 } },
            FindOptions::default(),
        )?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Users over 30: {}", over_30.len());

    // ── Filter: dot-notation on nested fields ───────────────────
    let in_austin: Vec<_> = txn
        .find(
            DEFAULT_CF,
            "users",
            rawdoc! { "address.city": "Austin" },
            FindOptions::default(),
        )?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Users in Austin: {}", in_austin.len());

    // ── Sort + pagination ───────────────────────────────────────
    let page: Vec<_> = txn
        .find(
            DEFAULT_CF,
            "users",
            rawdoc! {},
            FindOptions {
                sort: vec![Sort {
                    field: "age".into(),
                    direction: SortDirection::Desc,
                }],
                skip: Some(1),
                take: Some(2),
                ..Default::default()
            },
        )?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;
    println!(
        "Page (sorted by age desc, skip 1, take 2): {}",
        page.iter()
            .map(|d| d.get_str("name").unwrap())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // ── Projection: only return specific fields ─────────────────
    let names_only: Vec<_> = txn
        .find(
            DEFAULT_CF,
            "users",
            rawdoc! {},
            FindOptions {
                columns: Some(vec!["name".into(), "role".into()]),
                ..Default::default()
            },
        )?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;
    for d in &names_only {
        // _id is always included; only projected columns are returned
        assert!(d.get("age").unwrap().is_none());
    }
    println!("Projection returned {} docs (age field excluded)", names_only.len());

    // ── Count ───────────────────────────────────────────────────
    let count = txn.count(DEFAULT_CF, "users", rawdoc! { "role": "engineer" })?;
    println!("Engineer count: {}", count);
    drop(txn);

    // ── Update documents ────────────────────────────────────────
    let txn = db.begin(false)?;

    // $set a field
    txn.update_one(
        DEFAULT_CF,
        "users",
        rawdoc! { "_id": "user-1" },
        rawdoc! { "$set": { "age": 33 } },
    )?
    .drain()?;

    // $inc a numeric field
    txn.update_many(
        DEFAULT_CF,
        "users",
        rawdoc! { "role": "engineer" },
        rawdoc! { "$inc": { "age": 1 } },
    )?
    .drain()?;

    txn.commit()?;
    println!("Updated ages.");

    // ── Create an index ─────────────────────────────────────────
    let mut txn = db.begin(false)?;
    txn.create_index(DEFAULT_CF, "users", "role")?;
    txn.commit()?;
    println!("Created index on 'role'.");

    // Queries on indexed fields are automatically accelerated
    let txn = db.begin(true)?;
    let designers: Vec<_> = txn
        .find(
            DEFAULT_CF,
            "users",
            rawdoc! { "role": "designer" },
            FindOptions::default(),
        )?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;
    println!("Designers (via index scan): {}", designers.len());
    drop(txn);

    // ── Delete documents ────────────────────────────────────────
    let txn = db.begin(false)?;
    txn.delete_one(DEFAULT_CF, "users", rawdoc! { "_id": "user-5" })?
        .drain()?;
    txn.commit()?;

    let txn = db.begin(true)?;
    let remaining = txn.count(DEFAULT_CF, "users", rawdoc! {})?;
    println!("Users after delete: {}", remaining);
    drop(txn);

    // ── Distinct values ─────────────────────────────────────────
    let txn = db.begin(true)?;
    let raw = txn.distinct(DEFAULT_CF, "users", "role", rawdoc! {}, Default::default())?;
    if let RawBson::Array(arr) = raw {
        let roles: Vec<Bson> = arr
            .into_iter()
            .map(|r| Bson::try_from(r.unwrap()).unwrap())
            .collect();
        println!("Distinct roles: {:?}", roles);
    }
    drop(txn);

    // ── Custom column family ─────────────────────────────────────
    // Collections can be scoped to a column family instead of DEFAULT_CF.
    // The same collection name in different CFs are fully isolated.
    let mut txn = db.begin(false)?;
    txn.create_collection(&CollectionConfig {
        name: "events".into(),
        cf: "analytics".into(),
        ..Default::default()
    })?;
    txn.insert_many(
        "analytics",
        "events",
        vec![
            doc! { "_id": "e1", "type": "page_view", "url": "/home" },
            doc! { "_id": "e2", "type": "click",     "url": "/signup" },
            doc! { "_id": "e3", "type": "page_view", "url": "/docs" },
        ],
    )?
    .drain()?;
    txn.commit()?;

    let txn = db.begin(true)?;
    let views: Vec<_> = txn
        .find(
            "analytics",
            "events",
            rawdoc! { "type": "page_view" },
            FindOptions::default(),
        )?
        .iter()?
        .collect::<Result<Vec<_>, _>>()?;
    println!(
        "Events in 'analytics' CF: {} page views",
        views.len()
    );
    drop(txn);

    println!("\nDone!");
    Ok(())
}
