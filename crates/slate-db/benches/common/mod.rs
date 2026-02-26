#![allow(dead_code)]

use bson::raw::RawDocumentBuf;
use bson::rawdoc;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use slate_db::bench::{Database, RawIter};
use slate_db::{CollectionConfig, DatabaseConfig};
use slate_store::MemoryStore;

// ── Constants ───────────────────────────────────────────────

pub const STATUSES: &[&str] = &["active", "rejected"];
pub const REC1: &[&str] = &["ProductA", "ProductB", "ProductC"];
pub const REC2: &[&str] = &["ProductX", "ProductY", "ProductZ"];
pub const REC3: &[&str] = &["Widget1", "Widget2", "Widget3"];
pub const TAGS: &[&str] = &[
    "renewal_due",
    "high_value",
    "churning",
    "new_customer",
    "enterprise",
];

// ── Helpers ─────────────────────────────────────────────────

pub fn generate_docs(n: usize) -> Vec<RawDocumentBuf> {
    (0..n)
        .map(|i| {
            rawdoc! {
                "_id": format!("rec-{i}"),
                "name": format!("User {i}"),
                "status": if i % 2 == 0 { "active" } else { "rejected" },
                "contacts_count": (i % 100) as i32,
                "product_recommendation1": "ProductA",
            }
        })
        .collect()
}

pub fn consume_rows(iter: RawIter) -> usize {
    iter.count()
}

/// Create a seeded MemoryStore-backed Engine with `n` documents and indexes
/// on `status` and `contacts_count`.
pub fn seeded_engine(n: usize) -> Database<MemoryStore> {
    let engine = Database::open(MemoryStore::new(), DatabaseConfig::default());
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "test".into(),
        indexes: vec!["status".into(), "contacts_count".into()],
    })
    .unwrap();
    let docs: Vec<bson::Document> = (0..n)
        .map(|i| {
            bson::doc! {
                "_id": format!("rec-{i}"),
                "name": format!("User {i}"),
                "status": if i % 2 == 0 { "active" } else { "rejected" },
                "contacts_count": (i % 100) as i32,
                "product_recommendation1": "ProductA",
            }
        })
        .collect();
    txn.insert_many("test", docs).unwrap().drain().unwrap();
    txn.commit().unwrap();
    engine
}

pub fn generate_realistic_doc(rng: &mut StdRng, seq: usize) -> bson::Document {
    let mut doc = bson::doc! {
        "_id": format!("rec-{seq}"),
        "name": format!("Company-{seq}"),
        "status": STATUSES[rng.gen_range(0..STATUSES.len())],
        "contacts_count": rng.gen_range(0_i32..100),
        "product_recommendation1": REC1[rng.gen_range(0..REC1.len())],
        "product_recommendation2": REC2[rng.gen_range(0..REC2.len())],
        "product_recommendation3": REC3[rng.gen_range(0..REC3.len())],
    };

    let tag_count = rng.gen_range(2..=4);
    let tags: Vec<&str> = (0..tag_count)
        .map(|_| TAGS[rng.gen_range(0..TAGS.len())])
        .collect();
    doc.insert("tags", tags);

    if rng.gen_ratio(7, 10) {
        let epoch_secs = rng.gen_range(1_700_000_000_i64..1_740_000_000);
        doc.insert(
            "last_contacted_at",
            bson::Bson::DateTime(bson::DateTime::from_millis(epoch_secs * 1000)),
        );
    }

    if rng.gen_bool(0.5) {
        doc.insert("notes", format!("Note for {seq}"));
    }

    doc
}

pub fn generate_realistic_batch(count: usize) -> Vec<bson::Document> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count)
        .map(|i| generate_realistic_doc(&mut rng, i))
        .collect()
}

pub fn realistic_seeded_engine(n: usize) -> Database<MemoryStore> {
    let engine = Database::open(MemoryStore::new(), DatabaseConfig::default());
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "bench".into(),
        indexes: vec!["status".into(), "contacts_count".into()],
    })
    .unwrap();
    let docs = generate_realistic_batch(n);
    for chunk in docs.chunks(1000) {
        txn.insert_many("bench", chunk.to_vec())
            .unwrap()
            .drain()
            .unwrap();
    }
    txn.commit().unwrap();
    engine
}
