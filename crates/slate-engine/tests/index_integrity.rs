use std::time::{SystemTime, UNIX_EPOCH};

use bson::raw::RawBsonRef;
use slate_engine::{Catalog, Engine, EngineTransaction, IndexRange, KvEngine};
use slate_store::MemoryStore;

fn engine() -> KvEngine<MemoryStore> {
    KvEngine::new(MemoryStore::new())
}

/// Count all index entries for a field across all values.
fn count_index<Txn: EngineTransaction>(
    txn: &Txn,
    handle: &slate_engine::CollectionHandle<Txn::Cf>,
    field: &str,
) -> usize {
    txn.scan_index(handle, field, IndexRange::Full, false, i64::MIN)
        .unwrap()
        .count()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ── put_nx creates index entries ────────────────────────────

#[test]
fn put_nx_creates_index_entries() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "name": "Alice" };
    txn.put_nx(&handle, &doc, i64::MIN).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 1);
    txn.rollback().unwrap();
}

#[test]
fn put_nx_no_indexed_field_creates_no_entries() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "age": 30 };
    txn.put_nx(&handle, &doc, i64::MIN).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 0);
    txn.rollback().unwrap();
}

// ── Overwrite with same value leaves exactly 1 entry ────────

#[test]
fn put_overwrite_same_value_exactly_one_entry() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "name": "Alice" };
    txn.put(&handle, &doc).unwrap();
    txn.put(&handle, &doc).unwrap();
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 1);
    txn.rollback().unwrap();
}

// ── Overwrite changing value: old gone, new present ─────────

#[test]
fn put_overwrite_changed_value_replaces_entry() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "name": "Alice" };
    let doc2 = bson::rawdoc! { "_id": "a", "name": "Bob" };
    txn.put(&handle, &doc1).unwrap();
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 1);

    // Verify it's "Bob", not "Alice".
    let bob = bson::Bson::String("Bob".to_string());
    let entries: Vec<_> = txn
        .scan_index(&handle, "name", IndexRange::Eq(&bob), false, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 1);
    txn.rollback().unwrap();
}

// ── Overwrite removing indexed field cleans up ──────────────

#[test]
fn put_overwrite_removing_field_deletes_entry() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "name": "Alice" };
    txn.put(&handle, &doc1).unwrap();

    let doc2 = bson::rawdoc! { "_id": "a", "age": 30 };
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 0);
    txn.rollback().unwrap();
}

// ── Overwrite adding indexed field creates entry ────────────

#[test]
fn put_overwrite_adding_field_creates_entry() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "age": 30 };
    txn.put(&handle, &doc1).unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 0);

    let doc2 = bson::rawdoc! { "_id": "a", "name": "Alice" };
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 1);
    txn.rollback().unwrap();
}

// ── Multiple indexes: each maintained independently ─────────

#[test]
fn multiple_indexes_maintained() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    txn.create_index("c", "age").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "name": "Alice", "age": 30 };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 1);
    assert_eq!(count_index(&txn, &handle, "age"), 1);
    txn.rollback().unwrap();
}

#[test]
fn multiple_indexes_overwrite_partial_change() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    txn.create_index("c", "age").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "name": "Alice", "age": 30 };
    txn.put(&handle, &doc1).unwrap();

    // Change name but keep age.
    let doc2 = bson::rawdoc! { "_id": "a", "name": "Bob", "age": 30 };
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 1);
    assert_eq!(count_index(&txn, &handle, "age"), 1);
    txn.rollback().unwrap();
}

#[test]
fn delete_cleans_all_indexes() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    txn.create_index("c", "age").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "name": "Alice", "age": 30 };
    txn.put(&handle, &doc).unwrap();
    txn.delete(&handle, &RawBsonRef::String("a")).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "name"), 0);
    assert_eq!(count_index(&txn, &handle, "age"), 0);
    txn.rollback().unwrap();
}

// ── Dot notation (nested paths) ─────────────────────────────

#[test]
fn index_on_nested_path() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "address.city").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "address": { "city": "Austin", "state": "TX" } };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "address.city"), 1);
    txn.rollback().unwrap();
}

#[test]
fn index_on_nested_path_missing_parent() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "address.city").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "name": "Alice" };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "address.city"), 0);
    txn.rollback().unwrap();
}

#[test]
fn index_on_nested_path_overwrite() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "address.city").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "address": { "city": "Austin" } };
    txn.put(&handle, &doc1).unwrap();

    let doc2 = bson::rawdoc! { "_id": "a", "address": { "city": "Denver" } };
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "address.city"), 1);
    txn.rollback().unwrap();
}

// ── Array multi-key indexing ────────────────────────────────

#[test]
fn array_multikey_creates_entry_per_element() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "tags.[]").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "tags": ["rust", "db", "engine"] };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "tags.[]"), 3);
    txn.rollback().unwrap();
}

#[test]
fn array_multikey_overwrite_partial_change() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "tags.[]").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "tags": ["rust", "db"] };
    txn.put(&handle, &doc1).unwrap();

    let doc2 = bson::rawdoc! { "_id": "a", "tags": ["rust", "engine"] };
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    // "rust" kept, "db" removed, "engine" added → 2 total
    assert_eq!(count_index(&txn, &handle, "tags.[]"), 2);
    txn.rollback().unwrap();
}

#[test]
fn array_multikey_overwrite_to_empty_array() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "tags.[]").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "tags": ["rust", "db"] };
    txn.put(&handle, &doc1).unwrap();

    let doc2 = bson::rawdoc! { "_id": "a", "tags": [] };
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "tags.[]"), 0);
    txn.rollback().unwrap();
}

#[test]
fn array_multikey_delete_cleans_all() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "tags.[]").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "tags": ["rust", "db", "engine"] };
    txn.put(&handle, &doc).unwrap();
    txn.delete(&handle, &RawBsonRef::String("a")).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "tags.[]"), 0);
    txn.rollback().unwrap();
}

#[test]
fn array_multikey_multiple_docs() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "tags.[]").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc1 = bson::rawdoc! { "_id": "a", "tags": ["rust", "db"] };
    let doc2 = bson::rawdoc! { "_id": "b", "tags": ["rust", "engine"] };
    txn.put(&handle, &doc1).unwrap();
    txn.put(&handle, &doc2).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    // doc a: 2, doc b: 2 → 4 total
    assert_eq!(count_index(&txn, &handle, "tags.[]"), 4);

    // EQ scan for "rust" should match both docs.
    let rust = bson::Bson::String("rust".to_string());
    let entries: Vec<_> = txn
        .scan_index(&handle, "tags.[]", IndexRange::Eq(&rust), false, i64::MIN)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2);
    txn.rollback().unwrap();
}

// ── Nested array of objects ─────────────────────────────────

#[test]
fn nested_array_objects_multikey() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "items.[].sku").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! {
        "_id": "order1",
        "items": [
            { "sku": "A1", "qty": 2 },
            { "sku": "B2", "qty": 1 },
            { "sku": "C3", "qty": 5 }
        ]
    };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "items.[].sku"), 3);
    txn.rollback().unwrap();
}

// ── TTL: expired docs still have index entries ──────────────
// (Index entries have their own TTL metadata and are filtered
// during scan_index, but they exist in storage.)

#[test]
fn ttl_expired_doc_hidden_from_index_scan() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    let handle = txn.collection("c").unwrap();

    let expired_dt = bson::DateTime::from_millis(1_000_000); // long past
    let doc = bson::rawdoc! { "_id": "a", "name": "Alice", "ttl": expired_dt };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();

    // With current TTL, index entry should be filtered out.
    let now = now_millis();
    let entries: Vec<_> = txn
        .scan_index(&handle, "name", IndexRange::Full, false, now)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 0);
    txn.rollback().unwrap();
}

#[test]
fn ttl_unexpired_doc_visible_in_index_scan() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    let handle = txn.collection("c").unwrap();

    let future_dt = bson::DateTime::from_millis(i64::MAX / 2); // far future
    let doc = bson::rawdoc! { "_id": "a", "name": "Alice", "ttl": future_dt };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();

    let now = now_millis();
    let entries: Vec<_> = txn
        .scan_index(&handle, "name", IndexRange::Full, false, now)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 1);
    txn.rollback().unwrap();
}

#[test]
fn ttl_no_ttl_field_always_visible() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "name").unwrap();
    let handle = txn.collection("c").unwrap();

    let doc = bson::rawdoc! { "_id": "a", "name": "Alice" };
    txn.put(&handle, &doc).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();

    let now = now_millis();
    let entries: Vec<_> = txn
        .scan_index(&handle, "name", IndexRange::Full, false, now)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 1);
    txn.rollback().unwrap();
}

// ── Many docs: verify exact counts ──────────────────────────

#[test]
fn many_docs_exact_index_count() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "v").unwrap();
    let handle = txn.collection("c").unwrap();

    for i in 0..100 {
        let name = format!("doc-{i}");
        let doc = bson::rawdoc! { "_id": name.as_str(), "v": i };
        txn.put(&handle, &doc).unwrap();
    }
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "v"), 100);
    txn.rollback().unwrap();
}

#[test]
fn many_docs_delete_half_exact_count() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "v").unwrap();
    let handle = txn.collection("c").unwrap();

    for i in 0..100 {
        let name = format!("doc-{i}");
        let doc = bson::rawdoc! { "_id": name.as_str(), "v": i };
        txn.put(&handle, &doc).unwrap();
    }

    // Delete even-numbered docs.
    for i in (0..100).step_by(2) {
        let name = format!("doc-{i}");
        txn.delete(&handle, &RawBsonRef::String(&name)).unwrap();
    }
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    assert_eq!(count_index(&txn, &handle, "v"), 50);
    txn.rollback().unwrap();
}

#[test]
fn many_docs_overwrite_all_exact_count() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "c").unwrap();
    txn.create_index("c", "v").unwrap();
    let handle = txn.collection("c").unwrap();

    for i in 0..50 {
        let name = format!("doc-{i}");
        let doc = bson::rawdoc! { "_id": name.as_str(), "v": i };
        txn.put(&handle, &doc).unwrap();
    }

    // Overwrite all docs with new value.
    for i in 0..50 {
        let name = format!("doc-{i}");
        let doc = bson::rawdoc! { "_id": name.as_str(), "v": i + 1000 };
        txn.put(&handle, &doc).unwrap();
    }
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection("c").unwrap();
    // Still exactly 50 — no duplicates from overwrites.
    assert_eq!(count_index(&txn, &handle, "v"), 50);
    txn.rollback().unwrap();
}
