use slate_store::{RocksStore, Store, Transaction};

fn temp_store() -> (RocksStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    store.create_cf("test").unwrap();
    (store, dir)
}

const CF: &str = "test";

#[test]
fn put_and_get() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let result = txn.get(&cf, b"key1").unwrap().unwrap();
    assert_eq!(&*result, b"value1");
}

#[test]
fn get_missing_key_returns_none() {
    let (store, _dir) = temp_store();
    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let result = txn.get(&cf, b"nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn put_and_delete() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.delete(&cf, b"key1").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let result = txn.get(&cf, b"key1").unwrap();
    assert!(result.is_none());
}

#[test]
fn put_batch() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put_batch(
        &cf,
        &[
            (b"accounts:1:email" as &[u8], b"a@test.com" as &[u8]),
            (b"accounts:1:name", b"Alice"),
            (b"accounts:1:status", b"active"),
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    assert_eq!(
        &*txn.get(&cf, b"accounts:1:email").unwrap().unwrap(),
        b"a@test.com"
    );
    assert_eq!(
        &*txn.get(&cf, b"accounts:1:name").unwrap().unwrap(),
        b"Alice"
    );
    assert_eq!(
        &*txn.get(&cf, b"accounts:1:status").unwrap().unwrap(),
        b"active"
    );
}

#[test]
fn scan_prefix_returns_matching_pairs() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"accounts:1:email", b"a@test.com").unwrap();
    txn.put(&cf, b"accounts:1:name", b"Alice").unwrap();
    txn.put(&cf, b"accounts:2:email", b"b@test.com").unwrap();
    txn.put(&cf, b"other:1:foo", b"bar").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix(&cf, b"accounts:1:")
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
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"accounts:1:email", b"a@test.com").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix(&cf, b"contacts:")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(entries.is_empty());
}

#[test]
fn scan_prefix_broader() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"accounts:1:email", b"a@test.com").unwrap();
    txn.put(&cf, b"accounts:1:name", b"Alice").unwrap();
    txn.put(&cf, b"accounts:2:email", b"b@test.com").unwrap();
    txn.put(&cf, b"accounts:2:name", b"Bob").unwrap();
    txn.put(&cf, b"other:1:foo", b"bar").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix(&cf, b"accounts:")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(entries.len(), 4);
}

#[test]
fn scan_prefix_rev_returns_reverse_order() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"accounts:1:email", b"a@test.com").unwrap();
    txn.put(&cf, b"accounts:1:name", b"Alice").unwrap();
    txn.put(&cf, b"accounts:2:email", b"b@test.com").unwrap();
    txn.put(&cf, b"other:1:foo", b"bar").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix_rev(&cf, b"accounts:1:")
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
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"accounts:1:email", b"a@test.com").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let entries: Vec<_> = txn
        .scan_prefix_rev(&cf, b"contacts:")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(entries.is_empty());
}

#[test]
fn read_only_rejects_put() {
    let (store, _dir) = temp_store();
    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let result = txn.put(&cf, b"key1", b"value1");
    assert!(result.is_err());
}

#[test]
fn read_only_rejects_put_batch() {
    let (store, _dir) = temp_store();
    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let result = txn.put_batch(&cf, &[(b"key1" as &[u8], b"value1" as &[u8])]);
    assert!(result.is_err());
}

#[test]
fn read_only_rejects_delete() {
    let (store, _dir) = temp_store();
    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let result = txn.delete(&cf, b"key1");
    assert!(result.is_err());
}

#[test]
fn overwrite_key() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"key1", b"old").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"key1", b"new").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let result = txn.get(&cf, b"key1").unwrap().unwrap();
    assert_eq!(&*result, b"new");
}

#[test]
fn rollback_discards_writes() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"key1", b"value1").unwrap();
    txn.rollback().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let result = txn.get(&cf, b"key1").unwrap();
    assert!(result.is_none());
}

#[test]
fn rollback_does_not_affect_committed_data() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"key2", b"value2").unwrap();
    txn.delete(&cf, b"key1").unwrap();
    txn.rollback().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    assert!(txn.get(&cf, b"key1").unwrap().is_some());
    assert!(txn.get(&cf, b"key2").unwrap().is_none());
}

#[test]
fn empty_value() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"index:key", b"").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let result = txn.get(&cf, b"index:key").unwrap().unwrap();
    assert_eq!(&*result, b"");
}

// --- Column family tests ---

#[test]
fn create_and_use_cf() {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    store.create_cf("accounts").unwrap();

    let txn = store.begin(false).unwrap();
    let cf = txn.cf("accounts").unwrap();
    txn.put(&cf, b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf("accounts").unwrap();
    let result = txn.get(&cf, b"key1").unwrap().unwrap();
    assert_eq!(&*result, b"value1");
}

#[test]
fn cf_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    store.create_cf("cf_a").unwrap();
    store.create_cf("cf_b").unwrap();

    let txn = store.begin(false).unwrap();
    let cf_a = txn.cf("cf_a").unwrap();
    let cf_b = txn.cf("cf_b").unwrap();
    txn.put(&cf_a, b"key1", b"value_a").unwrap();
    txn.put(&cf_b, b"key1", b"value_b").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf_a = txn.cf("cf_a").unwrap();
    let cf_b = txn.cf("cf_b").unwrap();
    assert_eq!(&*txn.get(&cf_a, b"key1").unwrap().unwrap(), b"value_a");
    assert_eq!(&*txn.get(&cf_b, b"key1").unwrap().unwrap(), b"value_b");
}

#[test]
fn get_on_missing_cf_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();

    let txn = store.begin(true).unwrap();
    let result = txn.cf("nonexistent");
    assert!(result.is_err());
}

#[test]
fn drop_cf_removes_data() {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    store.create_cf("temp").unwrap();

    let txn = store.begin(false).unwrap();
    let cf = txn.cf("temp").unwrap();
    txn.put(&cf, b"key1", b"value1").unwrap();
    txn.commit().unwrap();

    store.drop_cf("temp").unwrap();

    let txn = store.begin(true).unwrap();
    let result = txn.cf("temp");
    assert!(result.is_err()); // CF no longer exists
}

#[test]
fn delete_range_clears_matching_keys() {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    store.create_cf("data").unwrap();

    let txn = store.begin(false).unwrap();
    let cf = txn.cf("data").unwrap();
    txn.put(&cf, b"a", b"1").unwrap();
    txn.put(&cf, b"b", b"2").unwrap();
    txn.put(&cf, b"c", b"3").unwrap();
    txn.put(&cf, b"d", b"4").unwrap();
    txn.put(&cf, b"e", b"5").unwrap();
    txn.commit().unwrap();

    // Delete range [b, d) — should remove b and c
    store
        .delete_range("data", b"b".to_vec()..b"d".to_vec())
        .unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf("data").unwrap();
    assert!(txn.get(&cf, b"a").unwrap().is_some());
    assert!(txn.get(&cf, b"b").unwrap().is_none());
    assert!(txn.get(&cf, b"c").unwrap().is_none());
    assert!(txn.get(&cf, b"d").unwrap().is_some());
    assert!(txn.get(&cf, b"e").unwrap().is_some());
}

#[test]
fn delete_range_unbounded_clears_all() {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    store.create_cf("cache").unwrap();

    let txn = store.begin(false).unwrap();
    let cf = txn.cf("cache").unwrap();
    txn.put(&cf, b"x", b"1").unwrap();
    txn.put(&cf, b"y", b"2").unwrap();
    txn.put(&cf, b"z", b"3").unwrap();
    txn.commit().unwrap();

    store.delete_range("cache", ..).unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf("cache").unwrap();
    assert!(txn.get(&cf, b"x").unwrap().is_none());
    assert!(txn.get(&cf, b"y").unwrap().is_none());
    assert!(txn.get(&cf, b"z").unwrap().is_none());
}

#[test]
fn delete_range_inclusive_end() {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    store.create_cf("data").unwrap();

    let txn = store.begin(false).unwrap();
    let cf = txn.cf("data").unwrap();
    txn.put(&cf, b"a", b"1").unwrap();
    txn.put(&cf, b"b", b"2").unwrap();
    txn.put(&cf, b"c", b"3").unwrap();
    txn.commit().unwrap();

    // Delete range [a, b] inclusive — should remove a and b
    store
        .delete_range("data", b"a".to_vec()..=b"b".to_vec())
        .unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf("data").unwrap();
    assert!(txn.get(&cf, b"a").unwrap().is_none());
    assert!(txn.get(&cf, b"b").unwrap().is_none());
    assert!(txn.get(&cf, b"c").unwrap().is_some());
}

#[test]
fn cfs_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let store = RocksStore::open(dir.path()).unwrap();
        store.create_cf("persistent").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("persistent").unwrap();
        txn.put(&cf, b"key", b"value").unwrap();
        txn.commit().unwrap();
    }

    // Reopen — CF and data should still be there
    let store = RocksStore::open(dir.path()).unwrap();
    let txn = store.begin(true).unwrap();
    let cf = txn.cf("persistent").unwrap();
    let result = txn.get(&cf, b"key").unwrap().unwrap();
    assert_eq!(&*result, b"value");
}

#[test]
fn multi_get_returns_matching_values() {
    let (store, _dir) = temp_store();
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(CF).unwrap();
    txn.put(&cf, b"k1", b"v1").unwrap();
    txn.put(&cf, b"k2", b"v2").unwrap();
    txn.put(&cf, b"k3", b"v3").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"missing", b"k3"];
    let results = txn.multi_get(&cf, &keys).unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(&**results[0].as_ref().unwrap(), b"v1");
    assert_eq!(&**results[1].as_ref().unwrap(), b"v2");
    assert!(results[2].is_none());
    assert_eq!(&**results[3].as_ref().unwrap(), b"v3");
}
