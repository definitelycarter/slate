use bson::raw::RawDocument;
use bson::{Bson, doc};
use slate_db::{CollectionConfig, Database, DatabaseConfig};
use slate_store::MemoryStore;

pub trait HasKey {
    fn get_check(&self, key: &str) -> bool;
}

impl HasKey for RawDocument {
    fn get_check(&self, key: &str) -> bool {
        self.get(key).ok().flatten().is_some()
    }
}

impl HasKey for bson::RawDocumentBuf {
    fn get_check(&self, key: &str) -> bool {
        self.get(key).ok().flatten().is_some()
    }
}

pub const COLLECTION: &str = "accounts";

pub fn temp_db() -> (Database<MemoryStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryStore::new();
    let db = Database::open(store, DatabaseConfig::default());
    (db, dir)
}

pub fn eq_filter(field: &str, value: Bson) -> bson::RawDocumentBuf {
    let mut doc = bson::Document::new();
    doc.insert(field.to_string(), value);
    bson::RawDocumentBuf::try_from(&doc).unwrap()
}

pub fn create_collection(db: &Database<MemoryStore>, name: &str) {
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: name.to_string(),
        indexes: vec![],
        ..Default::default()
    })
    .unwrap();
    txn.commit().unwrap();
}

/// Insert 5 seed records.
pub fn seed_records(db: &Database<MemoryStore>) {
    create_collection(db, COLLECTION);
    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "acct-1", "name": "Acme Corp", "revenue": 50000.0, "status": "active", "active": true },
            doc! { "_id": "acct-2", "name": "Globex", "revenue": 80000.0, "status": "snoozed", "active": true },
            doc! { "_id": "acct-3", "name": "Initech", "revenue": 12000.0, "status": "rejected", "active": false },
            doc! { "_id": "acct-4", "name": "Umbrella", "revenue": 95000.0, "status": "active", "active": true },
            doc! { "_id": "acct-5", "name": "Stark Industries", "revenue": 200000.0, "status": "active", "active": false },
        ],
    )
    .unwrap().drain().unwrap();
    txn.commit().unwrap();
}
