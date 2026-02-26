use std::i64;

use bson::raw::RawBsonRef;
use slate_engine::{BsonValue, Catalog, Engine, EngineTransaction, IndexRange, KvEngine};
use slate_store::MemoryStore;

fn engine() -> KvEngine<MemoryStore> {
    KvEngine::new(MemoryStore::new())
}

fn str_id(s: &str) -> BsonValue<'static> {
    BsonValue::from_raw_bson_ref(RawBsonRef::String(s))
        .unwrap()
        .into_owned()
}

fn oid_id(oid: bson::oid::ObjectId) -> BsonValue<'static> {
    BsonValue::from_raw_bson_ref(RawBsonRef::ObjectId(oid))
        .unwrap()
        .into_owned()
}

fn i32_id(n: i32) -> BsonValue<'static> {
    BsonValue::from_raw_bson_ref(RawBsonRef::Int32(n))
        .unwrap()
        .into_owned()
}

fn i64_id(n: i64) -> BsonValue<'static> {
    BsonValue::from_raw_bson_ref(RawBsonRef::Int64(n))
        .unwrap()
        .into_owned()
}

// ── Catalog ──────────────────────────────────────────────────

#[test]
fn create_and_list_collection() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let configs = txn.list_collections().unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].name, "users");
    txn.commit().unwrap();
}

#[test]
fn collection_not_found() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    let err = txn.collection("nope");
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn create_collection_idempotent() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.create_collection(None, "users").unwrap();
    let configs = txn.list_collections().unwrap();
    assert_eq!(configs.len(), 1);
    txn.commit().unwrap();
}

#[test]
fn drop_collection_removes_metadata() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.drop_collection("users").unwrap();
    let configs = txn.list_collections().unwrap();
    assert_eq!(configs.len(), 0);
    txn.commit().unwrap();
}

#[test]
fn drop_nonexistent_collection_is_noop() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.drop_collection("nope").unwrap();
    txn.commit().unwrap();
}

// ── Document CRUD ────────────────────────────────────────────

#[test]
fn put_get_roundtrip() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();

    let doc = bson::rawdoc! { "_id": "alice", "name": "Alice" };
    let id = str_id("alice");
    txn.put(&handle, &doc, &id).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    let fetched = txn.get(&handle, &id, i64::MIN).unwrap().unwrap();
    assert_eq!(fetched, doc);
    txn.rollback().unwrap();
}

#[test]
fn get_missing_returns_none() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();

    let id = str_id("missing");
    assert!(txn.get(&handle, &id, i64::MIN).unwrap().is_none());
    txn.rollback().unwrap();
}

#[test]
fn put_overwrite() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();

    let id = str_id("alice");
    let doc1 = bson::rawdoc! { "_id": "alice", "v": 1 };
    let doc2 = bson::rawdoc! { "_id": "alice", "v": 2 };
    txn.put(&handle, &doc1, &id).unwrap();
    txn.put(&handle, &doc2, &id).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    let fetched = txn.get(&handle, &id, i64::MIN).unwrap().unwrap();
    assert_eq!(fetched, doc2);
    txn.rollback().unwrap();
}

#[test]
fn delete_removes_document() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();

    let id = str_id("alice");
    let doc = bson::rawdoc! { "_id": "alice" };
    txn.put(&handle, &doc, &id).unwrap();
    txn.delete(&handle, &id).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    assert!(txn.get(&handle, &id, i64::MIN).unwrap().is_none());
    txn.rollback().unwrap();
}

#[test]
fn scan_returns_all_documents() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();

    for i in 0..3 {
        let name = format!("user-{i}");
        let doc = bson::rawdoc! { "_id": name.as_str() };
        let id = str_id(&name);
        txn.put(&handle, &doc, &id).unwrap();
    }
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    let results: Vec<_> = txn
        .scan(&handle, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);
    txn.rollback().unwrap();
}

// ── Drop collection cleans up data ──────────────────────────

#[test]
fn drop_collection_removes_records() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();

    let doc = bson::rawdoc! { "_id": "alice" };
    let id = str_id("alice");
    txn.put(&handle, &doc, &id).unwrap();
    txn.commit().unwrap();

    let mut txn = engine.begin(false).unwrap();
    txn.drop_collection("users").unwrap();
    txn.commit().unwrap();

    // Recreate and verify empty.
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();
    let results: Vec<_> = txn
        .scan(&handle, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);
    txn.rollback().unwrap();
}

// ── Index operations ─────────────────────────────────────────

#[test]
fn create_index_backfills_existing_records() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();

    // Insert docs before index exists.
    let doc1 = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    let doc2 = bson::rawdoc! { "_id": "b", "email": "b@test.com" };
    let doc3 = bson::rawdoc! { "_id": "c" }; // no email field
    txn.put(&handle, &doc1, &str_id("a")).unwrap();
    txn.put(&handle, &doc2, &str_id("b")).unwrap();
    txn.put(&handle, &doc3, &str_id("c")).unwrap();

    // Create index — should backfill a and b but not c.
    txn.create_index("users", "email").unwrap();

    let handle = txn.collection("users").unwrap();
    assert!(handle.indexes.contains(&"email".to_string()));

    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2);
    txn.commit().unwrap();
}

#[test]
fn drop_index_removes_entries() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    txn.put(&handle, &doc, &str_id("a")).unwrap();
    txn.create_index("users", "email").unwrap();
    txn.commit().unwrap();

    // Drop the index.
    let mut txn = engine.begin(false).unwrap();
    txn.drop_index("users", "email").unwrap();
    txn.commit().unwrap();

    // Verify index is gone from config.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    assert!(!handle.indexes.contains(&"email".to_string()));

    // Verify no index entries remain.
    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 0);
    txn.rollback().unwrap();
}

#[test]
fn put_maintains_index() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.create_index("users", "age").unwrap();
    let handle = txn.collection("users").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "age": 25 };
    let doc2 = bson::rawdoc! { "_id": "b", "age": 30 };
    let doc3 = bson::rawdoc! { "_id": "c", "age": 25 };
    txn.put(&handle, &doc1, &str_id("a")).unwrap();
    txn.put(&handle, &doc2, &str_id("b")).unwrap();
    txn.put(&handle, &doc3, &str_id("c")).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();

    // Full scan should have 3 entries.
    let all: Vec<_> = txn
        .scan_index(&handle, "age", IndexRange::Full, false, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(all.len(), 3);

    // Eq scan for age=25 should match 2.
    let age_25 = BsonValue::from_raw_bson_ref(RawBsonRef::Int32(25)).unwrap();
    let entries: Vec<_> = txn
        .scan_index(
            &handle,
            "age",
            IndexRange::Eq(&age_25.to_vec()),
            false,
            i64::MIN,
        )
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2);
    txn.rollback().unwrap();
}

#[test]
fn put_overwrite_updates_index() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.create_index("users", "email").unwrap();
    let handle = txn.collection("users").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "email": "old@test.com" };
    txn.put(&handle, &doc1, &str_id("a")).unwrap();
    txn.commit().unwrap();

    // Overwrite with a new email.
    let txn = engine.begin(false).unwrap();
    let handle = txn.collection("users").unwrap();
    let doc2 = bson::rawdoc! { "_id": "a", "email": "new@test.com" };
    txn.put(&handle, &doc2, &str_id("a")).unwrap();
    txn.commit().unwrap();

    // Should have exactly 1 index entry (the new one), not 2.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 1);
    txn.rollback().unwrap();
}

#[test]
fn delete_removes_index_entries() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.create_index("users", "email").unwrap();
    let handle = txn.collection("users").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    txn.put(&handle, &doc, &str_id("a")).unwrap();
    txn.commit().unwrap();

    // Delete the document.
    let txn = engine.begin(false).unwrap();
    let handle = txn.collection("users").unwrap();
    txn.delete(&handle, &str_id("a")).unwrap();
    txn.commit().unwrap();

    // Index should be empty.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 0);
    txn.rollback().unwrap();
}

#[test]
fn drop_collection_removes_index_entries() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    txn.put(&handle, &doc, &str_id("a")).unwrap();
    txn.create_index("users", "email").unwrap();
    txn.commit().unwrap();

    // Drop the entire collection.
    let mut txn = engine.begin(false).unwrap();
    txn.drop_collection("users").unwrap();
    txn.commit().unwrap();

    // Recreate and verify no index entries leak.
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.create_index("users", "email").unwrap();
    let handle = txn.collection("users").unwrap();
    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 0);
    txn.rollback().unwrap();
}

// ── Stale handle ─────────────────────────────────────────────

#[test]
fn stale_handle_misses_index_on_put() {
    let engine = engine();

    // Setup: create collection, no indexes.
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.commit().unwrap();

    // Another transaction creates an index and commits.
    let mut txn2 = engine.begin(false).unwrap();
    txn2.create_index("users", "email").unwrap();
    txn2.commit().unwrap();

    // Simulate a stale handle: resolve the collection fresh (loads CF into
    // the snapshot) but replace indexes with an empty vec — as if this handle
    // had been resolved before the index existed.
    let txn3 = engine.begin(false).unwrap();
    let mut stale_handle = txn3.collection("users").unwrap();
    assert!(stale_handle.indexes.contains(&"email".to_string()));
    stale_handle.indexes.clear(); // simulate stale snapshot

    let doc = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    txn3.put(&stale_handle, &doc, &str_id("a")).unwrap();
    txn3.commit().unwrap();

    // Verify: the record exists but the index entry is missing.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    assert!(handle.indexes.contains(&"email".to_string()));
    assert!(txn.get(&handle, &str_id("a"), i64::MIN).unwrap().is_some());

    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    // Index entry is missing because the stale handle didn't know about the index.
    assert_eq!(entries.len(), 0);
    txn.rollback().unwrap();
}

// ── Transaction isolation ────────────────────────────────────

#[test]
fn commit_persists_across_transactions() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    let handle = txn.collection("users").unwrap();
    let doc = bson::rawdoc! { "_id": "alice" };
    txn.put(&handle, &doc, &str_id("alice")).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    assert!(
        txn.get(&handle, &str_id("alice"), i64::MIN)
            .unwrap()
            .is_some()
    );
    txn.rollback().unwrap();
}

#[test]
fn rollback_discards_changes() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(false).unwrap();
    let handle = txn.collection("users").unwrap();
    let doc = bson::rawdoc! { "_id": "alice" };
    txn.put(&handle, &doc, &str_id("alice")).unwrap();
    txn.rollback().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("users").unwrap();
    assert!(
        txn.get(&handle, &str_id("alice"), i64::MIN)
            .unwrap()
            .is_none()
    );
    txn.rollback().unwrap();
}

// ── _id type roundtrips ─────────────────────────────────────

#[test]
fn put_get_string_id() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    let handle = txn.collection("c").unwrap();

    let id = str_id("hello");
    let doc = bson::rawdoc! { "_id": "hello", "v": 1 };
    txn.put(&handle, &doc, &id).unwrap();

    let fetched = txn.get(&handle, &id, i64::MIN).unwrap().unwrap();
    assert_eq!(fetched.get_str("_id").unwrap(), "hello");
    txn.commit().unwrap();
}

#[test]
fn put_get_objectid_id() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    let handle = txn.collection("c").unwrap();

    let oid = bson::oid::ObjectId::new();
    let id = oid_id(oid);
    let doc = bson::rawdoc! { "_id": oid, "v": 1 };
    txn.put(&handle, &doc, &id).unwrap();

    let fetched = txn.get(&handle, &id, i64::MIN).unwrap().unwrap();
    assert_eq!(fetched.get_object_id("_id").unwrap(), oid);
    txn.commit().unwrap();
}

#[test]
fn put_get_i32_id() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    let handle = txn.collection("c").unwrap();

    let id = i32_id(42);
    let doc = bson::rawdoc! { "_id": 42_i32, "v": 1 };
    txn.put(&handle, &doc, &id).unwrap();

    let fetched = txn.get(&handle, &id, i64::MIN).unwrap().unwrap();
    assert_eq!(fetched.get_i32("_id").unwrap(), 42);
    txn.commit().unwrap();
}

#[test]
fn put_get_i64_id() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    let handle = txn.collection("c").unwrap();

    let id = i64_id(999_999_999_999);
    let doc = bson::rawdoc! { "_id": 999_999_999_999_i64, "v": 1 };
    txn.put(&handle, &doc, &id).unwrap();

    let fetched = txn.get(&handle, &id, i64::MIN).unwrap().unwrap();
    assert_eq!(fetched.get_i64("_id").unwrap(), 999_999_999_999);
    txn.commit().unwrap();
}

#[test]
fn put_nx_objectid_id() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    let handle = txn.collection("c").unwrap();

    let oid = bson::oid::ObjectId::new();
    let id = oid_id(oid);
    let doc = bson::rawdoc! { "_id": oid, "v": 1 };
    txn.put_nx(&handle, &doc, &id, i64::MIN).unwrap();

    let fetched = txn.get(&handle, &id, i64::MIN).unwrap().unwrap();
    assert_eq!(fetched.get_object_id("_id").unwrap(), oid);
    txn.commit().unwrap();
}

#[test]
fn put_nx_i32_id_duplicate_errors() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    let handle = txn.collection("c").unwrap();

    let id = i32_id(7);
    let doc = bson::rawdoc! { "_id": 7_i32, "v": 1 };
    txn.put_nx(&handle, &doc, &id, i64::MIN).unwrap();

    let err = txn.put_nx(&handle, &doc, &id, i64::MIN);
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn delete_objectid_id() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    let handle = txn.collection("c").unwrap();

    let oid = bson::oid::ObjectId::new();
    let id = oid_id(oid);
    let doc = bson::rawdoc! { "_id": oid, "v": 1 };
    txn.put(&handle, &doc, &id).unwrap();
    txn.delete(&handle, &id).unwrap();

    assert!(txn.get(&handle, &id, i64::MIN).unwrap().is_none());
    txn.commit().unwrap();
}

#[test]
fn scan_mixed_id_types() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    let handle = txn.collection("c").unwrap();

    let oid = bson::oid::ObjectId::new();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "str", "v": 1 },
        &str_id("str"),
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": oid, "v": 2 },
        &oid_id(oid),
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": 42_i32, "v": 3 },
        &i32_id(42),
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    let results: Vec<_> = txn
        .scan(&handle, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);

    // Verify each can be fetched individually.
    assert!(txn.get(&handle, &str_id("str"), i64::MIN).unwrap().is_some());
    assert!(txn.get(&handle, &oid_id(oid), i64::MIN).unwrap().is_some());
    assert!(txn.get(&handle, &i32_id(42), i64::MIN).unwrap().is_some());
    txn.rollback().unwrap();
}
