//! `Database::stats` / `Transaction::collection_stats` — the size/cardinality
//! introspection surface. These pin the exact document and index-entry counts
//! and the distinct-value cardinality, and the database-wide roll-up.

use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_store::MemoryStore;

fn seeded() -> Database<MemoryStore> {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "people".into(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "people", "city").unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "people",
        vec![
            bson::doc! { "_id": "1", "name": "ada", "city": "london" },
            bson::doc! { "_id": "2", "name": "alan", "city": "london" },
            bson::doc! { "_id": "3", "name": "grace", "city": "baltimore" },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

/// Find an index's stats by field, asserting it exists.
fn index_by_field<'a>(
    stats: &'a slate_db::CollectionStats,
    field: &str,
) -> &'a slate_db::IndexStats {
    stats
        .indexes
        .iter()
        .find(|i| i.field == field)
        .unwrap_or_else(|| panic!("no index on `{field}`: {:?}", stats.indexes))
}

#[test]
fn collection_stats_counts_documents_and_index_entries() {
    let db = seeded();
    let stats = db.collection_stats(DEFAULT_CF, "people").unwrap();

    assert_eq!(stats.cf, DEFAULT_CF);
    assert_eq!(stats.name, "people");
    assert_eq!(stats.document_count, 3);
    assert!(!stats.approximate, "scan-based counts are exact");

    // The `city` index: 3 entries (one per doc), 2 distinct values (london,
    // baltimore). (A collection also carries an auto-created TTL index, which is
    // reported too — find by field rather than position.)
    let city = index_by_field(&stats, "city");
    assert_eq!(city.entry_count, 3);
    assert_eq!(city.cardinality, 2);
}

#[test]
fn database_stats_rolls_up_documents() {
    let db = seeded();
    let stats = db.stats().unwrap();
    assert_eq!(stats.total_documents, 3);
    assert!(
        stats.collections.iter().any(|c| c.name == "people"),
        "people should appear in the roll-up: {stats:?}"
    );
    // MemoryStore reports no on-disk size.
    assert_eq!(stats.disk_size_bytes, None);
}

#[test]
fn stats_track_writes() {
    let db = seeded();
    // Add a fourth person in a new city.
    let txn = db.begin(false).unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "people",
        vec![bson::doc! { "_id": "4", "name": "kay", "city": "paris" }],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let stats = db.collection_stats(DEFAULT_CF, "people").unwrap();
    assert_eq!(stats.document_count, 4);
    let city = index_by_field(&stats, "city");
    assert_eq!(city.entry_count, 4);
    assert_eq!(city.cardinality, 3); // london, baltimore, paris
}

#[test]
fn empty_collection_reports_zero() {
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "empty".into(),
        ..Default::default()
    })
    .unwrap();
    txn.commit().unwrap();

    let stats = db.collection_stats(DEFAULT_CF, "empty").unwrap();
    assert_eq!(stats.document_count, 0);
    // The auto-created TTL index exists but has no live entries.
    for idx in &stats.indexes {
        assert_eq!(idx.entry_count, 0, "{} should have no entries", idx.field);
        assert_eq!(idx.cardinality, 0);
    }
}
