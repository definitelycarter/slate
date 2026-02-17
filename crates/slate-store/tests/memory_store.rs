#![cfg(feature = "memory")]

use slate_store::{MemoryStore, Store, Transaction};

fn mem_store() -> MemoryStore {
    let store = MemoryStore::new();
    store.create_cf("test").unwrap();
    store
}

const CF: &str = "test";

#[test]
fn put_and_get() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let result = txn.get(CF, b"key1").unwrap().unwrap();
    assert_eq!(&*result, b"value1");
}

#[test]
fn get_missing_key_returns_none() {
    let store = mem_store();
    let mut txn = store.begin(true).unwrap();
    let result = txn.get(CF, b"nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn put_and_delete() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.delete(CF, b"key1").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let result = txn.get(CF, b"key1").unwrap();
    assert!(result.is_none());
}

#[test]
fn put_batch() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put_batch(
        CF,
        &[
            (b"accounts:1:email" as &[u8], b"a@test.com" as &[u8]),
            (b"accounts:1:name", b"Alice"),
            (b"accounts:1:status", b"active"),
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    assert_eq!(
        &*txn.get(CF, b"accounts:1:email").unwrap().unwrap(),
        b"a@test.com"
    );
    assert_eq!(
        &*txn.get(CF, b"accounts:1:name").unwrap().unwrap(),
        b"Alice"
    );
    assert_eq!(
        &*txn.get(CF, b"accounts:1:status").unwrap().unwrap(),
        b"active"
    );
}

#[test]
fn scan_prefix_returns_matching_pairs() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"accounts:1:email", b"a@test.com").unwrap();
    txn.put(CF, b"accounts:1:name", b"Alice").unwrap();
    txn.put(CF, b"accounts:2:email", b"b@test.com").unwrap();
    txn.put(CF, b"other:1:foo", b"bar").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix(CF, b"accounts:1:")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(&*entries[0].0, b"accounts:1:email");
    assert_eq!(&*entries[0].1, b"a@test.com");
    assert_eq!(&*entries[1].0, b"accounts:1:name");
    assert_eq!(&*entries[1].1, b"Alice");
}

#[test]
fn scan_prefix_no_matches() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"accounts:1:email", b"a@test.com").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix(CF, b"contacts:")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(entries.is_empty());
}

#[test]
fn scan_prefix_broader() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"accounts:1:email", b"a@test.com").unwrap();
    txn.put(CF, b"accounts:1:name", b"Alice").unwrap();
    txn.put(CF, b"accounts:2:email", b"b@test.com").unwrap();
    txn.put(CF, b"accounts:2:name", b"Bob").unwrap();
    txn.put(CF, b"other:1:foo", b"bar").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix(CF, b"accounts:")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(entries.len(), 4);
}

#[test]
fn scan_prefix_rev_returns_reverse_order() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"accounts:1:email", b"a@test.com").unwrap();
    txn.put(CF, b"accounts:1:name", b"Alice").unwrap();
    txn.put(CF, b"accounts:2:email", b"b@test.com").unwrap();
    txn.put(CF, b"other:1:foo", b"bar").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix_rev(CF, b"accounts:1:")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(entries.len(), 2);
    // Reverse order: name before email
    assert_eq!(&*entries[0].0, b"accounts:1:name");
    assert_eq!(&*entries[0].1, b"Alice");
    assert_eq!(&*entries[1].0, b"accounts:1:email");
    assert_eq!(&*entries[1].1, b"a@test.com");
}

#[test]
fn scan_prefix_rev_no_matches() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"accounts:1:email", b"a@test.com").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix_rev(CF, b"contacts:")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(entries.is_empty());
}

#[test]
fn read_only_rejects_put() {
    let store = mem_store();
    let mut txn = store.begin(true).unwrap();
    let result = txn.put(CF, b"key1", b"value1");
    assert!(result.is_err());
}

#[test]
fn read_only_rejects_put_batch() {
    let store = mem_store();
    let mut txn = store.begin(true).unwrap();
    let result = txn.put_batch(CF, &[(b"key1" as &[u8], b"value1" as &[u8])]);
    assert!(result.is_err());
}

#[test]
fn read_only_rejects_delete() {
    let store = mem_store();
    let mut txn = store.begin(true).unwrap();
    let result = txn.delete(CF, b"key1");
    assert!(result.is_err());
}

#[test]
fn overwrite_key() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"key1", b"old").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"key1", b"new").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let result = txn.get(CF, b"key1").unwrap().unwrap();
    assert_eq!(&*result, b"new");
}

#[test]
fn rollback_discards_writes() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"key1", b"value1").unwrap();
    txn.rollback().unwrap();

    let mut txn = store.begin(true).unwrap();
    let result = txn.get(CF, b"key1").unwrap();
    assert!(result.is_none());
}

#[test]
fn rollback_does_not_affect_committed_data() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"key2", b"value2").unwrap();
    txn.delete(CF, b"key1").unwrap();
    txn.rollback().unwrap();

    let mut txn = store.begin(true).unwrap();
    assert!(txn.get(CF, b"key1").unwrap().is_some());
    assert!(txn.get(CF, b"key2").unwrap().is_none());
}

#[test]
fn empty_value() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"index:key", b"").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let result = txn.get(CF, b"index:key").unwrap().unwrap();
    assert_eq!(&*result, b"");
}

// --- Column family tests ---

#[test]
fn create_and_use_cf() {
    let store = MemoryStore::new();
    store.create_cf("accounts").unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.put("accounts", b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let result = txn.get("accounts", b"key1").unwrap().unwrap();
    assert_eq!(&*result, b"value1");
}

#[test]
fn cf_isolation() {
    let store = MemoryStore::new();
    store.create_cf("cf_a").unwrap();
    store.create_cf("cf_b").unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.put("cf_a", b"key1", b"value_a").unwrap();
    txn.put("cf_b", b"key1", b"value_b").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    assert_eq!(&*txn.get("cf_a", b"key1").unwrap().unwrap(), b"value_a");
    assert_eq!(&*txn.get("cf_b", b"key1").unwrap().unwrap(), b"value_b");
}

#[test]
fn get_on_missing_cf_returns_error() {
    let store = MemoryStore::new();

    let mut txn = store.begin(true).unwrap();
    let result = txn.get("nonexistent", b"key1");
    assert!(result.is_err());
}

#[test]
fn drop_cf_removes_data() {
    let store = MemoryStore::new();
    store.create_cf("temp").unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.put("temp", b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    store.drop_cf("temp").unwrap();

    let mut txn = store.begin(true).unwrap();
    let result = txn.get("temp", b"key1");
    assert!(result.is_err()); // CF no longer exists
}

#[test]
fn delete_range_clears_matching_keys() {
    let store = MemoryStore::new();
    store.create_cf("data").unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.put("data", b"a", b"1").unwrap();
    txn.put("data", b"b", b"2").unwrap();
    txn.put("data", b"c", b"3").unwrap();
    txn.put("data", b"d", b"4").unwrap();
    txn.put("data", b"e", b"5").unwrap();
    txn.commit().unwrap();

    // Delete range [b, d) — should remove b and c
    store
        .delete_range("data", b"b".to_vec()..b"d".to_vec())
        .unwrap();

    let mut txn = store.begin(true).unwrap();
    assert!(txn.get("data", b"a").unwrap().is_some());
    assert!(txn.get("data", b"b").unwrap().is_none());
    assert!(txn.get("data", b"c").unwrap().is_none());
    assert!(txn.get("data", b"d").unwrap().is_some());
    assert!(txn.get("data", b"e").unwrap().is_some());
}

#[test]
fn delete_range_unbounded_clears_all() {
    let store = MemoryStore::new();
    store.create_cf("cache").unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.put("cache", b"x", b"1").unwrap();
    txn.put("cache", b"y", b"2").unwrap();
    txn.put("cache", b"z", b"3").unwrap();
    txn.commit().unwrap();

    store.delete_range("cache", ..).unwrap();

    let mut txn = store.begin(true).unwrap();
    assert!(txn.get("cache", b"x").unwrap().is_none());
    assert!(txn.get("cache", b"y").unwrap().is_none());
    assert!(txn.get("cache", b"z").unwrap().is_none());
}

#[test]
fn delete_range_inclusive_end() {
    let store = MemoryStore::new();
    store.create_cf("data").unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.put("data", b"a", b"1").unwrap();
    txn.put("data", b"b", b"2").unwrap();
    txn.put("data", b"c", b"3").unwrap();
    txn.commit().unwrap();

    // Delete range [a, b] inclusive — should remove a and b
    store
        .delete_range("data", b"a".to_vec()..=b"b".to_vec())
        .unwrap();

    let mut txn = store.begin(true).unwrap();
    assert!(txn.get("data", b"a").unwrap().is_none());
    assert!(txn.get("data", b"b").unwrap().is_none());
    assert!(txn.get("data", b"c").unwrap().is_some());
}

#[test]
fn multi_get_returns_matching_values() {
    let store = mem_store();
    let mut txn = store.begin(false).unwrap();
    txn.put(CF, b"k1", b"v1").unwrap();
    txn.put(CF, b"k2", b"v2").unwrap();
    txn.put(CF, b"k3", b"v3").unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(true).unwrap();
    let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"missing", b"k3"];
    let results = txn.multi_get(CF, &keys).unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(&**results[0].as_ref().unwrap(), b"v1");
    assert_eq!(&**results[1].as_ref().unwrap(), b"v2");
    assert!(results[2].is_none());
    assert_eq!(&**results[3].as_ref().unwrap(), b"v3");
}
