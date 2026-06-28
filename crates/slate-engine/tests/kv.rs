use bson::raw::RawBsonRef;
use slate_engine::{
    Catalog, CollectionHandle, CompoundRange, CompoundTail, DEFAULT_CF, Engine, EngineError,
    EngineTransaction, FunctionKind, IndexOptions, IndexRange, KvEngine, VectorDataType,
    VectorIndexSpec, VectorMetric, join_index_fields, runtime_tag,
};
use slate_store::MemoryStore;

fn engine() -> KvEngine<MemoryStore> {
    KvEngine::new(MemoryStore::new())
}

/// Shorthand for the unique index options used throughout the unique tests.
fn unique() -> IndexOptions {
    IndexOptions { unique: true }
}

// ── Catalog ──────────────────────────────────────────────────

#[test]
fn create_and_list_collection() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let configs = txn.list_collections(None).unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].name(), "users");
    txn.commit().unwrap();
}

#[test]
fn collection_not_found() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    let err = txn.collection(DEFAULT_CF, "nope");
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn create_collection_idempotent() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let configs = txn.list_collections(None).unwrap();
    assert_eq!(configs.len(), 1);
    txn.commit().unwrap();
}

#[test]
fn drop_collection_removes_metadata() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.drop_collection(DEFAULT_CF, "users").unwrap();
    let configs = txn.list_collections(None).unwrap();
    assert_eq!(configs.len(), 0);
    txn.commit().unwrap();
}

#[test]
fn drop_nonexistent_collection_is_noop() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.drop_collection(DEFAULT_CF, "nope").unwrap();
    txn.commit().unwrap();
}

// ── Document CRUD ────────────────────────────────────────────

#[test]
fn put_get_roundtrip() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let doc = bson::rawdoc! { "_id": "alice", "name": "Alice" };
    let id = RawBsonRef::String("alice");
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let fetched = txn.get(&handle, &id).unwrap().unwrap();
    assert_eq!(fetched, doc);
    txn.rollback().unwrap();
}

#[test]
fn get_missing_returns_none() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let id = RawBsonRef::String("missing");
    assert!(txn.get(&handle, &id).unwrap().is_none());
    txn.rollback().unwrap();
}

#[test]
fn put_overwrite() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let id = RawBsonRef::String("alice");
    let doc1 = bson::rawdoc! { "_id": "alice", "v": 1 };
    let doc2 = bson::rawdoc! { "_id": "alice", "v": 2 };
    txn.put(&handle, &doc1).unwrap();
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let fetched = txn.get(&handle, &id).unwrap().unwrap();
    assert_eq!(fetched, doc2);
    txn.rollback().unwrap();
}

#[test]
fn delete_removes_document() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let id = RawBsonRef::String("alice");
    let doc = bson::rawdoc! { "_id": "alice" };
    txn.put(&handle, &doc).unwrap();
    txn.delete(&handle, &id).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    assert!(txn.get(&handle, &id).unwrap().is_none());
    txn.rollback().unwrap();
}

#[test]
fn scan_returns_all_documents() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    for i in 0..3 {
        let name = format!("user-{i}");
        let doc = bson::rawdoc! { "_id": name.as_str() };
        txn.put(&handle, &doc).unwrap();
    }
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let results: Vec<_> = txn
        .scan(&handle)
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
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let doc = bson::rawdoc! { "_id": "alice" };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(false).unwrap();
    txn.drop_collection(DEFAULT_CF, "users").unwrap();
    txn.commit().unwrap();

    // Recreate and verify empty.
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let results: Vec<_> = txn
        .scan(&handle)
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
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    // Insert docs before index exists.
    let doc1 = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    let doc2 = bson::rawdoc! { "_id": "b", "email": "b@test.com" };
    let doc3 = bson::rawdoc! { "_id": "c" }; // no email field
    txn.put(&handle, &doc1).unwrap();
    txn.put(&handle, &doc2).unwrap();
    txn.put(&handle, &doc3).unwrap();

    // Create index — should backfill a and b but not c.
    txn.create_index(DEFAULT_CF, "users", "email").unwrap();

    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    assert!(handle.indexes().contains(&"email".to_string()));

    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2);
    txn.commit().unwrap();
}

#[test]
fn drop_index_removes_entries() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    txn.put(&handle, &doc).unwrap();
    txn.create_index(DEFAULT_CF, "users", "email").unwrap();
    txn.commit().unwrap();

    // Drop the index.
    let txn = engine.begin(false).unwrap();
    txn.drop_index(DEFAULT_CF, "users", "email").unwrap();
    txn.commit().unwrap();

    // Verify index is gone from config.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    assert!(!handle.indexes().contains(&"email".to_string()));

    // Verify no index entries remain.
    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 0);
    txn.rollback().unwrap();
}

#[test]
fn put_maintains_index() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_index(DEFAULT_CF, "users", "age").unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "age": 25 };
    let doc2 = bson::rawdoc! { "_id": "b", "age": 30 };
    let doc3 = bson::rawdoc! { "_id": "c", "age": 25 };
    txn.put(&handle, &doc1).unwrap();
    txn.put(&handle, &doc2).unwrap();
    txn.put(&handle, &doc3).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    // Full scan should have 3 entries.
    let all: Vec<_> = txn
        .scan_index(&handle, "age", IndexRange::Full, false)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(all.len(), 3);

    // Eq scan for age=25 should match 2.
    let age_25 = bson::Bson::Int32(25);
    let entries: Vec<_> = txn
        .scan_index(&handle, "age", IndexRange::Eq(&age_25), false)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2);
    txn.rollback().unwrap();
}

#[test]
fn put_overwrite_updates_index() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_index(DEFAULT_CF, "users", "email").unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "email": "old@test.com" };
    txn.put(&handle, &doc1).unwrap();
    txn.commit().unwrap();

    // Overwrite with a new email.
    let txn = engine.begin(false).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let doc2 = bson::rawdoc! { "_id": "a", "email": "new@test.com" };
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    // Should have exactly 1 index entry (the new one), not 2.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 1);
    txn.rollback().unwrap();
}

#[test]
fn delete_removes_index_entries() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_index(DEFAULT_CF, "users", "email").unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    // Delete the document.
    let txn = engine.begin(false).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    txn.delete(&handle, &RawBsonRef::String("a")).unwrap();
    txn.commit().unwrap();

    // Index should be empty.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 0);
    txn.rollback().unwrap();
}

#[test]
fn drop_collection_removes_index_entries() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    txn.put(&handle, &doc).unwrap();
    txn.create_index(DEFAULT_CF, "users", "email").unwrap();
    txn.commit().unwrap();

    // Drop the entire collection.
    let txn = engine.begin(false).unwrap();
    txn.drop_collection(DEFAULT_CF, "users").unwrap();
    txn.commit().unwrap();

    // Recreate and verify no index entries leak.
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_index(DEFAULT_CF, "users", "email").unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false)
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
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.commit().unwrap();

    // Another transaction creates an index and commits.
    let txn2 = engine.begin(false).unwrap();
    txn2.create_index(DEFAULT_CF, "users", "email").unwrap();
    txn2.commit().unwrap();

    // Simulate a stale handle: resolve the collection to get the CF, then
    // construct a new handle with empty indexes — as if resolved before the
    // index existed.
    let txn3 = engine.begin(false).unwrap();
    let fresh_handle = txn3.collection(DEFAULT_CF, "users").unwrap();
    assert!(fresh_handle.indexes().contains(&"email".to_string()));
    let stale_handle = CollectionHandle::new(
        "users".to_string(),
        DEFAULT_CF.to_string(),
        fresh_handle.cf().clone(),
        vec![],
        vec![],
        vec![],
        "_id".to_string(),
        "ttl".to_string(),
    );

    let doc = bson::rawdoc! { "_id": "a", "email": "a@test.com" };
    txn3.put(&stale_handle, &doc).unwrap();
    txn3.commit().unwrap();

    // Verify: the record exists but the index entry is missing.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    assert!(handle.indexes().contains(&"email".to_string()));
    assert!(
        txn.get(&handle, &RawBsonRef::String("a"))
            .unwrap()
            .is_some()
    );

    let entries: Vec<_> = txn
        .scan_index(&handle, "email", IndexRange::Full, false)
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
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let doc = bson::rawdoc! { "_id": "alice" };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    assert!(
        txn.get(&handle, &RawBsonRef::String("alice"))
            .unwrap()
            .is_some()
    );
    txn.rollback().unwrap();
}

#[test]
fn rollback_discards_changes() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(false).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let doc = bson::rawdoc! { "_id": "alice" };
    txn.put(&handle, &doc).unwrap();
    txn.rollback().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    assert!(
        txn.get(&handle, &RawBsonRef::String("alice"))
            .unwrap()
            .is_none()
    );
    txn.rollback().unwrap();
}

// ── _id type roundtrips ─────────────────────────────────────

#[test]
fn put_get_string_id() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();

    let id = RawBsonRef::String("hello");
    let doc = bson::rawdoc! { "_id": "hello", "v": 1 };
    txn.put(&handle, &doc).unwrap();

    let fetched = txn.get(&handle, &id).unwrap().unwrap();
    assert_eq!(fetched.get_str("_id").unwrap(), "hello");
    txn.commit().unwrap();
}

#[test]
fn put_get_objectid_id() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();

    let oid = bson::oid::ObjectId::new();
    let id = RawBsonRef::ObjectId(oid);
    let doc = bson::rawdoc! { "_id": oid, "v": 1 };
    txn.put(&handle, &doc).unwrap();

    let fetched = txn.get(&handle, &id).unwrap().unwrap();
    assert_eq!(fetched.get_object_id("_id").unwrap(), oid);
    txn.commit().unwrap();
}

#[test]
fn put_get_i32_id() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();

    let id = RawBsonRef::Int32(42);
    let doc = bson::rawdoc! { "_id": 42_i32, "v": 1 };
    txn.put(&handle, &doc).unwrap();

    let fetched = txn.get(&handle, &id).unwrap().unwrap();
    assert_eq!(fetched.get_i32("_id").unwrap(), 42);
    txn.commit().unwrap();
}

#[test]
fn put_get_i64_id() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();

    let id = RawBsonRef::Int64(999_999_999_999);
    let doc = bson::rawdoc! { "_id": 999_999_999_999_i64, "v": 1 };
    txn.put(&handle, &doc).unwrap();

    let fetched = txn.get(&handle, &id).unwrap().unwrap();
    assert_eq!(fetched.get_i64("_id").unwrap(), 999_999_999_999);
    txn.commit().unwrap();
}

#[test]
fn put_nx_objectid_id() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();

    let oid = bson::oid::ObjectId::new();
    let id = RawBsonRef::ObjectId(oid);
    let doc = bson::rawdoc! { "_id": oid, "v": 1 };
    txn.put_nx(&handle, &doc).unwrap();

    let fetched = txn.get(&handle, &id).unwrap().unwrap();
    assert_eq!(fetched.get_object_id("_id").unwrap(), oid);
    txn.commit().unwrap();
}

#[test]
fn put_nx_i32_id_duplicate_errors() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();

    let doc = bson::rawdoc! { "_id": 7_i32, "v": 1 };
    txn.put_nx(&handle, &doc).unwrap();

    let err = txn.put_nx(&handle, &doc);
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn delete_objectid_id() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();

    let oid = bson::oid::ObjectId::new();
    let id = RawBsonRef::ObjectId(oid);
    let doc = bson::rawdoc! { "_id": oid, "v": 1 };
    txn.put(&handle, &doc).unwrap();
    txn.delete(&handle, &id).unwrap();

    assert!(txn.get(&handle, &id).unwrap().is_none());
    txn.commit().unwrap();
}

#[test]
fn scan_mixed_id_types() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();

    let oid = bson::oid::ObjectId::new();
    txn.put(&handle, &bson::rawdoc! { "_id": "str", "v": 1 })
        .unwrap();
    txn.put(&handle, &bson::rawdoc! { "_id": oid, "v": 2 })
        .unwrap();
    txn.put(&handle, &bson::rawdoc! { "_id": 42_i32, "v": 3 })
        .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let results: Vec<_> = txn
        .scan(&handle)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);

    // Verify each can be fetched individually.
    assert!(
        txn.get(&handle, &RawBsonRef::String("str"))
            .unwrap()
            .is_some()
    );
    assert!(
        txn.get(&handle, &RawBsonRef::ObjectId(oid))
            .unwrap()
            .is_some()
    );
    assert!(txn.get(&handle, &RawBsonRef::Int32(42)).unwrap().is_some());
    txn.rollback().unwrap();
}

// ── Function catalog ────────────────────────────────────────

#[test]
fn create_and_load_trigger() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Trigger,
        "audit",
        runtime_tag::LUA,
        b"print('audit')",
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let entries = txn
        .load_functions(DEFAULT_CF, "users", FunctionKind::Trigger)
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "audit");
    assert_eq!(entries[0].source, b"print('audit')");
    txn.rollback().unwrap();
}

#[test]
fn create_and_load_validator() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Validator,
        "require_name",
        runtime_tag::LUA,
        b"assert(doc.name)",
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let entries = txn
        .load_functions(DEFAULT_CF, "users", FunctionKind::Validator)
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "require_name");
    assert_eq!(entries[0].source, b"assert(doc.name)");
    txn.rollback().unwrap();
}

#[test]
fn create_and_load_udf() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Udf,
        "full_name",
        runtime_tag::LUA,
        b"return first .. last",
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let entries = txn
        .load_functions(DEFAULT_CF, "users", FunctionKind::Udf)
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "full_name");
    assert_eq!(entries[0].source, b"return first .. last");
    txn.rollback().unwrap();
}

#[test]
fn multiple_functions_per_collection() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Trigger,
        "audit",
        runtime_tag::LUA,
        b"src1",
    )
    .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Trigger,
        "notify",
        runtime_tag::LUA,
        b"src2",
    )
    .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Validator,
        "check",
        runtime_tag::LUA,
        b"src3",
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let triggers = txn
        .load_functions(DEFAULT_CF, "users", FunctionKind::Trigger)
        .unwrap();
    assert_eq!(triggers.len(), 2);
    let names: Vec<&str> = triggers.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"audit"));
    assert!(names.contains(&"notify"));

    // Validators are separate from triggers.
    let validators = txn
        .load_functions(DEFAULT_CF, "users", FunctionKind::Validator)
        .unwrap();
    assert_eq!(validators.len(), 1);
    assert_eq!(validators[0].name, "check");
    txn.rollback().unwrap();
}

#[test]
fn drop_function() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Trigger,
        "audit",
        runtime_tag::LUA,
        b"src",
    )
    .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Trigger,
        "notify",
        runtime_tag::LUA,
        b"src2",
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(false).unwrap();
    txn.drop_function(DEFAULT_CF, "users", FunctionKind::Trigger, "audit")
        .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let entries = txn
        .load_functions(DEFAULT_CF, "users", FunctionKind::Trigger)
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "notify");
    txn.rollback().unwrap();
}

#[test]
fn drop_collection_cleans_functions() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Trigger,
        "audit",
        runtime_tag::LUA,
        b"t",
    )
    .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Validator,
        "check",
        runtime_tag::LUA,
        b"v",
    )
    .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Udf,
        "full_name",
        runtime_tag::LUA,
        b"d",
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(false).unwrap();
    txn.drop_collection(DEFAULT_CF, "users").unwrap();
    txn.commit().unwrap();

    // Recreate and verify no functions leak.
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    for kind in [
        FunctionKind::Trigger,
        FunctionKind::Validator,
        FunctionKind::Udf,
    ] {
        let entries = txn.load_functions(DEFAULT_CF, "users", kind).unwrap();
        assert_eq!(
            entries.len(),
            0,
            "expected no {:?} entries after drop",
            kind
        );
    }
    txn.rollback().unwrap();
}

#[test]
fn function_requires_existing_collection() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    let err = txn.create_function(
        DEFAULT_CF,
        "nope",
        FunctionKind::Trigger,
        "audit",
        runtime_tag::LUA,
        b"src",
    );
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn functions_isolated_across_collections() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_collection(DEFAULT_CF, "posts", &Default::default())
        .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Trigger,
        "audit",
        runtime_tag::LUA,
        b"users_src",
    )
    .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "posts",
        FunctionKind::Trigger,
        "audit",
        runtime_tag::LUA,
        b"posts_src",
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let user_triggers = txn
        .load_functions(DEFAULT_CF, "users", FunctionKind::Trigger)
        .unwrap();
    assert_eq!(user_triggers.len(), 1);
    assert_eq!(user_triggers[0].source, b"users_src");

    let post_triggers = txn
        .load_functions(DEFAULT_CF, "posts", FunctionKind::Trigger)
        .unwrap();
    assert_eq!(post_triggers.len(), 1);
    assert_eq!(post_triggers[0].source, b"posts_src");
    txn.rollback().unwrap();
}

#[test]
fn create_index_duplicate_errors() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_index(DEFAULT_CF, "users", "email").unwrap();

    let err = txn.create_index(DEFAULT_CF, "users", "email");
    assert!(
        err.is_err(),
        "expected IndexExists error on duplicate create_index"
    );
    txn.rollback().unwrap();
}

#[test]
fn create_function_duplicate_errors() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Trigger,
        "audit",
        runtime_tag::LUA,
        b"src1",
    )
    .unwrap();

    let err = txn.create_function(
        DEFAULT_CF,
        "users",
        FunctionKind::Trigger,
        "audit",
        runtime_tag::LUA,
        b"src2",
    );
    assert!(
        err.is_err(),
        "expected FunctionExists error on duplicate create_function"
    );
    txn.rollback().unwrap();
}

// ── Unique indexes ───────────────────────────────────────────
//
// Enforcement is via an in-snapshot existence check on the `u` key plus the
// store's write-write conflict detection at commit (for concurrent writers).
// These tests cover the deterministic, single-writer behaviour on MemoryStore;
// MemoryStore is last-write-wins and does not detect concurrent conflicts, so
// the concurrent-race guarantee is exercised by the rocks/redb backends, not
// here.

fn users_with_unique_email(txn: &impl Catalog) {
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    txn.create_index_with_options(DEFAULT_CF, "users", "email", &unique())
        .unwrap();
}

#[test]
fn unique_index_rejects_duplicate_value() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    users_with_unique_email(&txn);
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "x@test.com" },
    )
    .unwrap();

    let err = txn
        .put_nx(
            &handle,
            &bson::rawdoc! { "_id": "b", "email": "x@test.com" },
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::UniqueViolation { .. }),
        "expected UniqueViolation, got {err:?}"
    );
    txn.rollback().unwrap();
}

#[test]
fn unique_index_allows_distinct_values() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    users_with_unique_email(&txn);
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "a@test.com" },
    )
    .unwrap();
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "b", "email": "b@test.com" },
    )
    .unwrap();
    txn.commit().unwrap();
}

#[test]
fn unique_index_keeps_numeric_types_distinct() {
    // Current (transitional) behavior: uniqueness is per-(type, value). The `u` key
    // folds the BSON type byte in, so Int32(5), Int64(5), and Double(5.0) occupy
    // three distinct slots and may coexist — even though `compare_bson` treats them
    // as equal. The decision (book/src/rfcs/unique-indexes.md, "Numeric uniqueness
    // across types") is to collapse these onto one f64 slot, matching the query layer
    // and Cosmos; this test asserts today's per-type behavior and flips to assert
    // collapse when that migration lands.
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "scores", &Default::default())
        .unwrap();
    txn.create_index_with_options(DEFAULT_CF, "scores", "score", &unique())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "scores").unwrap();

    txn.put_nx(&handle, &bson::rawdoc! { "_id": "i32", "score": 5_i32 })
        .unwrap();
    txn.put_nx(&handle, &bson::rawdoc! { "_id": "i64", "score": 5_i64 })
        .unwrap();
    txn.put_nx(&handle, &bson::rawdoc! { "_id": "f64", "score": 5.0_f64 })
        .unwrap();

    // Within a single type, uniqueness still holds: a second Int32(5) collides.
    let err = txn
        .put_nx(&handle, &bson::rawdoc! { "_id": "dup", "score": 5_i32 })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::UniqueViolation { .. }),
        "expected UniqueViolation on same-type duplicate, got {err:?}"
    );
    txn.rollback().unwrap();
}

#[test]
fn unique_index_is_sparse() {
    // Documents missing the unique field are not constrained — any number may
    // omit it.
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    users_with_unique_email(&txn);
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    txn.put_nx(&handle, &bson::rawdoc! { "_id": "a" }).unwrap();
    txn.put_nx(&handle, &bson::rawdoc! { "_id": "b" }).unwrap();
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "c", "email": "c@test.com" },
    )
    .unwrap();
    txn.commit().unwrap();
}

#[test]
fn unique_index_same_document_update_is_idempotent() {
    // Rewriting a document while keeping its unique value must not collide with
    // itself. Change a non-indexed field so the identical-bytes fast path does
    // not short-circuit.
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    users_with_unique_email(&txn);
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "x@test.com", "n": 1 },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "x@test.com", "n": 2 },
    )
    .unwrap();
    txn.commit().unwrap();
}

#[test]
fn unique_index_value_change_frees_slot() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    users_with_unique_email(&txn);
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "x@test.com" },
    )
    .unwrap();
    // Move 'a' off of x@test.com.
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "y@test.com" },
    )
    .unwrap();
    // The freed value can now be claimed by another document.
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "b", "email": "x@test.com" },
    )
    .unwrap();
    txn.commit().unwrap();
}

#[test]
fn unique_index_delete_frees_slot() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    users_with_unique_email(&txn);
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "x@test.com" },
    )
    .unwrap();
    txn.delete(&handle, &RawBsonRef::String("a")).unwrap();
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "b", "email": "x@test.com" },
    )
    .unwrap();
    txn.commit().unwrap();
}

#[test]
fn unique_index_backfill_detects_existing_duplicates() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    // Two documents share a value before the index exists.
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "x@test.com" },
    )
    .unwrap();
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "b", "email": "x@test.com" },
    )
    .unwrap();

    let err = txn
        .create_index_with_options(DEFAULT_CF, "users", "email", &unique())
        .unwrap_err();
    assert!(
        matches!(err, EngineError::UniqueViolation { .. }),
        "expected UniqueViolation on dirty backfill, got {err:?}"
    );
    txn.rollback().unwrap();
}

#[test]
fn unique_index_backfill_succeeds_when_distinct() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "a@test.com" },
    )
    .unwrap();
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "b", "email": "b@test.com" },
    )
    .unwrap();
    txn.create_index_with_options(DEFAULT_CF, "users", "email", &unique())
        .unwrap();

    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    assert!(handle.unique_indexes().contains(&"email".to_string()));
    // Enforcement is live after backfill.
    let err = txn
        .put_nx(
            &handle,
            &bson::rawdoc! { "_id": "c", "email": "a@test.com" },
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::UniqueViolation { .. }));
    txn.rollback().unwrap();
}

#[test]
fn unique_index_rejects_multikey_path() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "users", &Default::default())
        .unwrap();
    let err = txn
        .create_index_with_options(DEFAULT_CF, "users", "tags.[]", &unique())
        .unwrap_err();
    assert!(
        matches!(err, EngineError::InvalidDocument(_)),
        "expected InvalidDocument for multikey unique path, got {err:?}"
    );
    txn.rollback().unwrap();
}

#[test]
fn dropping_unique_index_lifts_enforcement() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    users_with_unique_email(&txn);
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "a", "email": "x@test.com" },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(false).unwrap();
    txn.drop_index(DEFAULT_CF, "users", "email").unwrap();
    txn.commit().unwrap();

    // With the index gone, the previously-constrained value is free again.
    let txn = engine.begin(false).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    assert!(!handle.unique_indexes().contains(&"email".to_string()));
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "b", "email": "x@test.com" },
    )
    .unwrap();
    txn.commit().unwrap();
}

#[test]
fn unique_index_blocks_value_held_by_expired_document() {
    // Per design: a unique value held by an expired-but-unpurged document still
    // blocks new inserts (conservative — we never silently accept a duplicate).
    // The slot is reclaimed only by purging the dead document.
    let engine = KvEngine::with_clock(MemoryStore::new(), || 10_000);
    let txn = engine.begin(false).unwrap();
    users_with_unique_email(&txn);
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();

    // 'a' carries a TTL in the past (1_000 < clock 10_000) → expired.
    let expired = bson::rawdoc! {
        "_id": "a", "email": "x@test.com", "ttl": bson::DateTime::from_millis(1_000),
    };
    txn.put_nx(&handle, &expired).unwrap();
    // It is invisible to reads.
    assert!(
        txn.get(&handle, &RawBsonRef::String("a"))
            .unwrap()
            .is_none()
    );
    txn.commit().unwrap();

    // A new doc cannot steal the expired slot.
    let txn = engine.begin(false).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    let err = txn
        .put_nx(
            &handle,
            &bson::rawdoc! { "_id": "b", "email": "x@test.com" },
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::UniqueViolation { .. }),
        "expected expired slot to still block, got {err:?}"
    );
    txn.rollback().unwrap();

    // Purging the dead document frees the slot.
    let txn = engine.begin(false).unwrap();
    let handle = txn.collection(DEFAULT_CF, "users").unwrap();
    txn.purge(&handle).unwrap();
    txn.put_nx(
        &handle,
        &bson::rawdoc! { "_id": "b", "email": "x@test.com" },
    )
    .unwrap();
    txn.commit().unwrap();
}

// ── Compound (multi-field) indexes ──────────────────────────────

fn compound_orders() -> KvEngine<MemoryStore> {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "orders", &Default::default())
        .unwrap();
    txn.create_compound_index(
        DEFAULT_CF,
        "orders",
        &["status".to_string(), "created_at".to_string()],
    )
    .unwrap();
    let handle = txn.collection(DEFAULT_CF, "orders").unwrap();
    for doc in [
        bson::rawdoc! { "_id": "a", "status": "active", "created_at": 10i64 },
        bson::rawdoc! { "_id": "b", "status": "active", "created_at": 20i64 },
        bson::rawdoc! { "_id": "c", "status": "active2", "created_at": 15i64 },
        bson::rawdoc! { "_id": "d", "status": "archived", "created_at": 5i64 },
    ] {
        txn.put(&handle, &doc).unwrap();
    }
    txn.commit().unwrap();
    engine
}

fn compound_field() -> String {
    join_index_fields(&["status".to_string(), "created_at".to_string()])
}

fn doc_ids(entries: Vec<slate_engine::IndexEntry>) -> Vec<String> {
    let mut ids: Vec<String> = entries
        .iter()
        .map(|e| match e.doc_id().unwrap() {
            bson::RawBson::String(s) => s,
            other => panic!("unexpected doc_id {other:?}"),
        })
        .collect();
    ids.sort();
    ids
}

#[test]
fn scan_compound_eq_prefix_overreads_byte_prefix() {
    // The engine seek over `status = "active"` byte-prefix sweeps in the
    // "active2" entry (id "c"); the engine does NOT recheck (the executor does),
    // so this raw scan returns a, b, AND c. The executor-level test pins the
    // exact exclusion.
    let engine = compound_orders();
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "orders").unwrap();
    let field = compound_field();
    let active = bson::Bson::String("active".into());
    let entries: Vec<_> = txn
        .scan_compound_index(
            &handle,
            &field,
            CompoundRange {
                eq_prefix: std::slice::from_ref(&active),
                tail: CompoundTail::Unbounded,
            },
            false,
        )
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    // Conservative superset: includes the "active2" spillover.
    let ids = doc_ids(entries);
    assert!(ids.contains(&"a".to_string()));
    assert!(ids.contains(&"b".to_string()));
    assert!(ids.contains(&"c".to_string()));
    assert!(!ids.contains(&"d".to_string()));
    txn.rollback().unwrap();
}

#[test]
fn scan_compound_full_equality_is_exact() {
    // A full equality on both components seeks a tight prefix — exactly one row.
    let engine = compound_orders();
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "orders").unwrap();
    let field = compound_field();
    let active = bson::Bson::String("active".into());
    let twenty = bson::Bson::Int64(20);
    let entries: Vec<_> = txn
        .scan_compound_index(
            &handle,
            &field,
            CompoundRange {
                eq_prefix: std::slice::from_ref(&active),
                tail: CompoundTail::Eq(&twenty),
            },
            false,
        )
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(doc_ids(entries), vec!["b".to_string()]);
    txn.rollback().unwrap();
}

#[test]
fn compound_index_lists_joined_identity() {
    let engine = compound_orders();
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "orders").unwrap();
    assert!(handle.indexes().iter().any(|i| i == &compound_field()));
    txn.rollback().unwrap();
}

#[test]
fn drop_compound_index_removes_entries() {
    let engine = compound_orders();
    let txn = engine.begin(false).unwrap();
    txn.drop_index(DEFAULT_CF, "orders", &compound_field())
        .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "orders").unwrap();
    assert!(handle.indexes().is_empty());
    txn.rollback().unwrap();
}

// ── Value-side-offsets format guardrails ─────────────────────

#[test]
fn single_and_compound_index_prefixes_do_not_leak() {
    // Prefix isolation: a collection carries BOTH a single-field index on
    // `status` and a compound index on `[status, created_at]`. Their value
    // prefixes share `i\0orders\0status`, but the key byte right after diverges —
    // `\0` (SEP) for the single index, `\x01` (FIELD_SEP) for the compound — and
    // `FIELD_SEP > SEP`, so neither scan can sweep in the other's entries.
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "orders", &Default::default())
        .unwrap();
    txn.create_index(DEFAULT_CF, "orders", "status").unwrap();
    txn.create_compound_index(
        DEFAULT_CF,
        "orders",
        &["status".to_string(), "created_at".to_string()],
    )
    .unwrap();
    let handle = txn.collection(DEFAULT_CF, "orders").unwrap();
    for doc in [
        bson::rawdoc! { "_id": "a", "status": "active", "created_at": 10i64 },
        bson::rawdoc! { "_id": "b", "status": "active", "created_at": 20i64 },
        bson::rawdoc! { "_id": "c", "status": "archived", "created_at": 5i64 },
    ] {
        txn.put(&handle, &doc).unwrap();
    }
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "orders").unwrap();

    // The single-field `status` scan sees every row exactly once — it must NOT
    // also pick up the compound `status\x01created_at` entries (which would
    // double-count a/b/c).
    let single: Vec<_> = txn
        .scan_index(&handle, "status", IndexRange::Full, false)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        doc_ids(single),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    // Each single-field entry is one component: `value()` is the status string.
    let single_again: Vec<_> = txn
        .scan_index(&handle, "status", IndexRange::Full, false)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    for entry in &single_again {
        assert!(matches!(entry.value().unwrap(), bson::RawBson::String(_)));
    }

    // The compound scan over the `active` prefix sees only its own entries; no
    // single-index entry leaks in (which would fail per-component decode).
    let active = bson::Bson::String("active".into());
    let compound: Vec<_> = txn
        .scan_compound_index(
            &handle,
            &compound_field(),
            CompoundRange {
                eq_prefix: std::slice::from_ref(&active),
                tail: CompoundTail::Unbounded,
            },
            false,
        )
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    // Only the two `active` rows — `archived` (c) is outside the eq-prefix.
    assert_eq!(doc_ids(compound), vec!["a".to_string(), "b".to_string()]);
    txn.rollback().unwrap();
}

#[test]
fn string_index_full_scan_is_byte_sorted() {
    // String ordering guardrail: with the key-side length suffixes gone, the
    // sort-relevant region is exactly the raw value bytes followed by the doc_id.
    // Index values where one is a byte-prefix of another (`"x"` vs `"xy"`) and one
    // embeds a low byte (`"x\u{1}y"`) must come back in correct byte-sorted order
    // from a full ascending scan.
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "t", &Default::default())
        .unwrap();
    txn.create_index(DEFAULT_CF, "t", "name").unwrap();
    let handle = txn.collection(DEFAULT_CF, "t").unwrap();
    // Insert in deliberately scrambled order.
    for (id, name) in [
        ("3", "xy"),
        ("1", "x"),
        ("4", "xyz"),
        ("2", "x\u{1}y"),
        ("0", "abc"),
    ] {
        let doc = bson::rawdoc! { "_id": id, "name": name };
        txn.put(&handle, &doc).unwrap();
    }
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "t").unwrap();
    let entries: Vec<_> = txn
        .scan_index(&handle, "name", IndexRange::Full, false)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let names: Vec<String> = entries
        .iter()
        .map(|e| match e.value().unwrap() {
            bson::RawBson::String(s) => s,
            other => panic!("unexpected value {other:?}"),
        })
        .collect();
    // The key is `{value_bytes}{doc_id_lp}` (no suffix), so ordering compares the
    // raw value bytes first, then the doc_id bytes that immediately follow. After
    // the shared "x":
    //   - "x"      → next byte is the doc_id type tag 0x02,
    //   - "x\u{1}y" → next byte is the value's own 0x01,
    // and 0x01 < 0x02, so "x\u{1}y" sorts BEFORE "x". "xy" ('y' = 0x79) and "xyz"
    // follow. A scan that mis-resolved the value boundary would scramble this.
    assert_eq!(
        names,
        vec![
            "abc".to_string(),
            "x\u{1}y".to_string(),
            "x".to_string(),
            "xy".to_string(),
            "xyz".to_string(),
        ]
    );
    txn.rollback().unwrap();
}

// ── Vector index (flat) ──────────────────────────────────────

/// Collect a vector scan into a deterministic `(string_id, vector)` list, so
/// assertions don't depend on the store's key-iteration order.
fn sorted_vectors<T: EngineTransaction>(
    txn: &T,
    handle: &CollectionHandle<T::Cf>,
    field: &str,
) -> Vec<(String, Vec<f32>)> {
    let mut out: Vec<(String, Vec<f32>)> = txn
        .scan_vectors(handle, field)
        .unwrap()
        .map(|r| {
            let (id, v) = r.unwrap();
            let id = match id {
                bson::RawBson::String(s) => s,
                other => panic!("expected string doc_id, got {other:?}"),
            };
            (id, v)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// A 3-d `float32` cosine spec on `embedding`.
fn embedding_spec() -> VectorIndexSpec {
    VectorIndexSpec::float32("embedding", 3, VectorMetric::Cosine)
}

#[test]
fn create_vector_index_then_scan_yields_packed_vectors() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 0.0, 0.0] },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "b", "embedding": [0.0, 2.0, 0.5] },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    assert_eq!(
        sorted_vectors(&txn, &handle, "embedding"),
        vec![
            ("a".to_string(), vec![1.0, 0.0, 0.0]),
            ("b".to_string(), vec![0.0, 2.0, 0.5]),
        ]
    );
    txn.rollback().unwrap();
}

#[test]
fn float16_index_scan_yields_dequantized_vectors() {
    // A `float16` index stores a half-width approximate copy; `scan_vectors`
    // resolves the dtype from the field's spec and dequantizes it back to `f32`.
    // The values here are exactly representable in f16, so the scan returns them
    // unchanged (the dtype wiring, not the approximation, is under test).
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(
        DEFAULT_CF,
        "photos",
        &VectorIndexSpec::float16("embedding", 3, VectorMetric::Cosine),
    )
    .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 0.0, 0.5] },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "b", "embedding": [0.5, 0.25, 0.0] },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    assert_eq!(
        sorted_vectors(&txn, &handle, "embedding"),
        vec![
            ("a".to_string(), vec![1.0, 0.0, 0.5]),
            ("b".to_string(), vec![0.5, 0.25, 0.0]),
        ]
    );
    txn.rollback().unwrap();
}

#[test]
fn int8_index_scan_yields_dequantized_vectors() {
    // An `int8` index stores a per-vector-scaled approximate copy; `scan_vectors`
    // resolves the dtype and dequantizes back to `f32` within int8's precision
    // (~max|x| / 254). The scan is the approximate side — exactness is restored by
    // the executor's rescore, so here we only assert the dequantization tolerance.
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(
        DEFAULT_CF,
        "photos",
        &VectorIndexSpec::int8("embedding", 3, VectorMetric::Cosine),
    )
    .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, -0.5, 0.25] },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    let scanned = sorted_vectors(&txn, &handle, "embedding");
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].0, "a");
    for (got, want) in scanned[0].1.iter().zip([1.0_f32, -0.5, 0.25]) {
        assert!((got - want).abs() < 0.01, "got {got}, want {want}");
    }
    txn.rollback().unwrap();
}

#[test]
fn vector_index_backfills_existing_records() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    // Insert before the index exists; one doc has no embedding (stays sparse).
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0, 3.0] },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "b", "embedding": [4.0, 5.0, 6.0] },
    )
    .unwrap();
    txn.put(&handle, &bson::rawdoc! { "_id": "c", "name": "no vec" })
        .unwrap();

    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();

    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    assert_eq!(
        sorted_vectors(&txn, &handle, "embedding"),
        vec![
            ("a".to_string(), vec![1.0, 2.0, 3.0]),
            ("b".to_string(), vec![4.0, 5.0, 6.0]),
        ]
    );
    txn.commit().unwrap();
}

#[test]
fn vector_index_is_sparse_for_missing_field() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    txn.put(&handle, &bson::rawdoc! { "_id": "a", "name": "no vec" })
        .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    assert!(sorted_vectors(&txn, &handle, "embedding").is_empty());
    txn.rollback().unwrap();
}

#[test]
fn put_overwrite_updates_vector_in_place() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 1.0, 1.0] },
    )
    .unwrap();
    // Overwrite the same doc_id with a new embedding.
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [9.0, 8.0, 7.0] },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    assert_eq!(
        sorted_vectors(&txn, &handle, "embedding"),
        vec![("a".to_string(), vec![9.0, 8.0, 7.0])]
    );
    txn.rollback().unwrap();
}

#[test]
fn put_removing_vector_field_drops_entry() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0, 3.0] },
    )
    .unwrap();
    // Overwrite without the embedding — the stale entry must be removed.
    txn.put(&handle, &bson::rawdoc! { "_id": "a", "name": "dropped" })
        .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    assert!(sorted_vectors(&txn, &handle, "embedding").is_empty());
    txn.rollback().unwrap();
}

#[test]
fn delete_removes_vector_entry() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0, 3.0] },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "b", "embedding": [4.0, 5.0, 6.0] },
    )
    .unwrap();
    txn.delete(&handle, &RawBsonRef::String("a")).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    assert_eq!(
        sorted_vectors(&txn, &handle, "embedding"),
        vec![("b".to_string(), vec![4.0, 5.0, 6.0])]
    );
    txn.rollback().unwrap();
}

#[test]
fn vector_dims_mismatch_on_insert_is_an_error() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    // 2 dims where the index declares 3.
    let err = txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0] },
    );
    match err {
        Err(EngineError::VectorDimsMismatch {
            field,
            expected,
            found,
        }) => {
            assert_eq!(field, "embedding");
            assert_eq!(expected, 3);
            assert_eq!(found, 2);
        }
        other => panic!("expected VectorDimsMismatch, got {other:?}"),
    }
    txn.rollback().unwrap();
}

#[test]
fn vector_dims_mismatch_on_backfill_fails_create() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    // A pre-existing doc with the wrong dimensionality.
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0, 3.0, 4.0] },
    )
    .unwrap();

    assert!(matches!(
        txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec()),
        Err(EngineError::VectorDimsMismatch { found: 4, .. })
    ));
    txn.rollback().unwrap();
}

#[test]
fn non_array_vector_field_is_an_error() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    assert!(matches!(
        txn.put(
            &handle,
            &bson::rawdoc! { "_id": "a", "embedding": "not a vector" }
        ),
        Err(EngineError::InvalidDocument(_))
    ));
    txn.rollback().unwrap();
}

#[test]
fn duplicate_vector_index_errors() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    assert!(matches!(
        txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec()),
        Err(EngineError::IndexExists(_))
    ));
    txn.rollback().unwrap();
}

#[test]
fn drop_vector_index_removes_entries_and_config() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0, 3.0] },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(false).unwrap();
    txn.drop_vector_index(DEFAULT_CF, "photos", "embedding")
        .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    // Config gone from the handle, and no data entries remain.
    assert!(handle.vector_indexes().is_empty());
    assert!(sorted_vectors(&txn, &handle, "embedding").is_empty());
    txn.rollback().unwrap();
}

#[test]
fn drop_collection_removes_vector_entries() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0, 3.0] },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(false).unwrap();
    txn.drop_collection(DEFAULT_CF, "photos").unwrap();
    // Recreate the collection + index: a stale vector entry would resurface here.
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    assert!(sorted_vectors(&txn, &handle, "embedding").is_empty());
    txn.commit().unwrap();
}

#[test]
fn multiple_vector_indexes_per_collection_route_by_field() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "items", &Default::default())
        .unwrap();
    // Two embeddings of different dimensionality and metric on one collection.
    txn.create_vector_index(
        DEFAULT_CF,
        "items",
        &VectorIndexSpec::float32("image_embedding", 2, VectorMetric::Cosine),
    )
    .unwrap();
    txn.create_vector_index(
        DEFAULT_CF,
        "items",
        &VectorIndexSpec::float32("text_embedding", 3, VectorMetric::Euclidean),
    )
    .unwrap();
    let handle = txn.collection(DEFAULT_CF, "items").unwrap();

    txn.put(
        &handle,
        &bson::rawdoc! {
            "_id": "a",
            "image_embedding": [1.0, 2.0],
            "text_embedding": [3.0, 4.0, 5.0],
        },
    )
    .unwrap();
    // A doc carrying only one of the two embeddings.
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "b", "image_embedding": [6.0, 7.0] },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "items").unwrap();
    assert_eq!(handle.vector_indexes().len(), 2);
    assert_eq!(
        sorted_vectors(&txn, &handle, "image_embedding"),
        vec![
            ("a".to_string(), vec![1.0, 2.0]),
            ("b".to_string(), vec![6.0, 7.0]),
        ]
    );
    assert_eq!(
        sorted_vectors(&txn, &handle, "text_embedding"),
        vec![("a".to_string(), vec![3.0, 4.0, 5.0])]
    );
    txn.rollback().unwrap();
}

#[test]
fn vector_index_spec_persists_across_reload() {
    let engine = engine();
    let spec = VectorIndexSpec {
        path: "meta.vec".to_string(),
        dims: 4,
        metric: VectorMetric::Euclidean,
        dtype: VectorDataType::Float32,
    };
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "docs", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "docs", &spec).unwrap();
    txn.commit().unwrap();

    // A fresh transaction reloads the handle from the catalog.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "docs").unwrap();
    assert_eq!(handle.vector_indexes(), &[spec]);
    txn.rollback().unwrap();
}

#[test]
fn purge_removes_vector_entries() {
    let engine = engine();
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    // One doc already expired (ttl in the distant past), one with no ttl.
    let past = bson::DateTime::from_millis(1_000);
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0, 3.0], "ttl": past },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "b", "embedding": [4.0, 5.0, 6.0] },
    )
    .unwrap();
    let purged = txn.purge(&handle).unwrap();
    assert_eq!(purged, 1);
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    // The expired doc's vector entry is gone; the live one remains.
    assert_eq!(
        sorted_vectors(&txn, &handle, "embedding"),
        vec![("b".to_string(), vec![4.0, 5.0, 6.0])]
    );
    txn.rollback().unwrap();
}

#[test]
fn scan_vectors_skips_expired_but_unpurged_documents() {
    // A document expired per `now_millis` but not yet purged must NOT be yielded
    // by `scan_vectors` — otherwise a top-k would return fewer than k once the
    // downstream key-lookup drops it. The clock is 10_000; 'a' has a past TTL.
    let engine = KvEngine::with_clock(MemoryStore::new(), || 10_000);
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    // 'a' expired (ttl 1_000 < clock 10_000); 'b' lives in the future; 'c' has no
    // TTL at all. None are purged.
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0, 3.0], "ttl": bson::DateTime::from_millis(1_000) },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "b", "embedding": [4.0, 5.0, 6.0], "ttl": bson::DateTime::from_millis(900_000) },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "c", "embedding": [7.0, 8.0, 9.0] },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    // Only the live doc 'b' and the TTL-free doc 'c' are yielded — 'a' is skipped
    // even though it was never purged.
    assert_eq!(
        sorted_vectors(&txn, &handle, "embedding"),
        vec![
            ("b".to_string(), vec![4.0, 5.0, 6.0]),
            ("c".to_string(), vec![7.0, 8.0, 9.0]),
        ]
    );
    txn.rollback().unwrap();
}

#[test]
fn scan_vectors_unaffected_when_no_ttl() {
    // Documents with no TTL field are never expired, so a vector scan yields all
    // of them regardless of the clock — the fast path stays correct (and pays only
    // a single tag-byte check per entry).
    let engine = KvEngine::with_clock(MemoryStore::new(), || i64::MAX);
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 0.0, 0.0] },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "b", "embedding": [0.0, 1.0, 0.0] },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
    assert_eq!(
        sorted_vectors(&txn, &handle, "embedding"),
        vec![
            ("a".to_string(), vec![1.0, 0.0, 0.0]),
            ("b".to_string(), vec![0.0, 1.0, 0.0]),
        ]
    );
    txn.rollback().unwrap();
}

#[test]
fn scan_vectors_skips_expired_after_backfill() {
    // Backfill must frame the TTL into the vector entry too: a doc inserted (with
    // a past TTL) *before* the index exists is backfilled, then skipped by a scan.
    let engine = KvEngine::with_clock(MemoryStore::new(), || 10_000);
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "photos", &Default::default())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "a", "embedding": [1.0, 2.0, 3.0], "ttl": bson::DateTime::from_millis(1_000) },
    )
    .unwrap();
    txn.put(
        &handle,
        &bson::rawdoc! { "_id": "b", "embedding": [4.0, 5.0, 6.0] },
    )
    .unwrap();

    // Build the index after the rows exist — the backfill path frames the TTL.
    txn.create_vector_index(DEFAULT_CF, "photos", &embedding_spec())
        .unwrap();
    let handle = txn.collection(DEFAULT_CF, "photos").unwrap();

    // The expired 'a' is skipped by the scan even straight after backfill.
    assert_eq!(
        sorted_vectors(&txn, &handle, "embedding"),
        vec![("b".to_string(), vec![4.0, 5.0, 6.0])]
    );
    txn.rollback().unwrap();
}
