use bson::raw::RawBsonRef;
use slate_engine::{
    Catalog, CollectionHandle, DEFAULT_CF, Engine, EngineError, EngineTransaction, FunctionKind,
    IndexOptions, IndexRange, KvEngine, runtime_tag,
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
    // Contract: uniqueness is per-(type, value). The `u` key folds the BSON type
    // byte in, so Int32(5), Int64(5), and Double(5.0) occupy three distinct slots
    // and may coexist — even though `compare_bson` treats them as equal. This pins
    // the per-type contract in book/src/rfcs/unique-indexes.md so the unique index
    // cannot silently drift into the regular index's f64 collapse.
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
