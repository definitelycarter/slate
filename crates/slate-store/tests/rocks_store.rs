use std::collections::HashMap;

use slate_store::{Record, RocksStore, Store, Transaction, Value};

fn temp_store() -> (RocksStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    (store, dir)
}

fn make_record(id: &str, name: &str, revenue: f64) -> Record {
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), Value::String(name.to_string()));
    fields.insert("revenue".to_string(), Value::Float(revenue));
    Record {
        id: id.to_string(),
        fields,
    }
}

#[test]
fn insert_and_get_by_id() {
    let (store, _dir) = temp_store();
    let mut txn = store.begin(false).unwrap();

    let record = make_record("acct-1", "Acme Corp", 50000.0);
    txn.insert(record).unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let result = txn.get_by_id("acct-1").unwrap().unwrap();
    assert_eq!(result.id, "acct-1");
    assert_eq!(
        result.fields.get("name"),
        Some(&Value::String("Acme Corp".to_string()))
    );
    assert_eq!(result.fields.get("revenue"), Some(&Value::Float(50000.0)));
}

#[test]
fn get_by_id_not_found() {
    let (store, _dir) = temp_store();
    let txn = store.begin(true).unwrap();
    let result = txn.get_by_id("nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn insert_and_delete() {
    let (store, _dir) = temp_store();
    let mut txn = store.begin(false).unwrap();

    txn.insert(make_record("acct-1", "Acme Corp", 50000.0))
        .unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.delete("acct-1").unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let result = txn.get_by_id("acct-1").unwrap();
    assert!(result.is_none());
}

#[test]
fn scan_multiple_records() {
    let (store, _dir) = temp_store();
    let mut txn = store.begin(false).unwrap();

    txn.insert(make_record("acct-1", "Acme Corp", 50000.0))
        .unwrap();
    txn.insert(make_record("acct-2", "Globex", 80000.0))
        .unwrap();
    txn.insert(make_record("acct-3", "Initech", 12000.0))
        .unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let records: Vec<Record> = txn.scan().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(records.len(), 3);

    let ids: Vec<&str> = records.iter().map(|r| r.id.as_str()).collect();
    assert!(ids.contains(&"acct-1"));
    assert!(ids.contains(&"acct-2"));
    assert!(ids.contains(&"acct-3"));
}

#[test]
fn scan_empty_store() {
    let (store, _dir) = temp_store();
    let txn = store.begin(true).unwrap();
    let records: Vec<Record> = txn.scan().unwrap().map(|r| r.unwrap()).collect();
    assert!(records.is_empty());
}

#[test]
fn read_only_transaction_rejects_writes() {
    let (store, _dir) = temp_store();
    let mut txn = store.begin(true).unwrap();

    let result = txn.insert(make_record("acct-1", "Acme Corp", 50000.0));
    assert!(result.is_err());

    let result = txn.delete("acct-1");
    assert!(result.is_err());
}

#[test]
fn all_value_types() {
    let (store, _dir) = temp_store();
    let mut txn = store.begin(false).unwrap();

    let mut fields = HashMap::new();
    fields.insert("name".to_string(), Value::String("Acme".to_string()));
    fields.insert("count".to_string(), Value::Int(42));
    fields.insert("revenue".to_string(), Value::Float(99.99));
    fields.insert("active".to_string(), Value::Bool(true));
    fields.insert("created_at".to_string(), Value::Date(1707900000));
    fields.insert(
        "tags".to_string(),
        Value::List(vec![
            Value::String("enterprise".to_string()),
            Value::String("priority".to_string()),
        ]),
    );

    let mut contact = HashMap::new();
    contact.insert("name".to_string(), Value::String("Alice".to_string()));
    contact.insert("phone".to_string(), Value::String("555-1234".to_string()));
    contact.insert("exhausted".to_string(), Value::Bool(false));
    fields.insert("primary_contact".to_string(), Value::Map(contact));

    let record = Record {
        id: "acct-1".to_string(),
        fields,
    };
    txn.insert(record).unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let result = txn.get_by_id("acct-1").unwrap().unwrap();

    assert_eq!(
        result.fields.get("name"),
        Some(&Value::String("Acme".to_string()))
    );
    assert_eq!(result.fields.get("count"), Some(&Value::Int(42)));
    assert_eq!(result.fields.get("revenue"), Some(&Value::Float(99.99)));
    assert_eq!(result.fields.get("active"), Some(&Value::Bool(true)));
    assert_eq!(
        result.fields.get("created_at"),
        Some(&Value::Date(1707900000))
    );
    assert_eq!(
        result.fields.get("tags"),
        Some(&Value::List(vec![
            Value::String("enterprise".to_string()),
            Value::String("priority".to_string()),
        ]))
    );

    if let Some(Value::Map(contact)) = result.fields.get("primary_contact") {
        assert_eq!(
            contact.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            contact.get("phone"),
            Some(&Value::String("555-1234".to_string()))
        );
        assert_eq!(contact.get("exhausted"), Some(&Value::Bool(false)));
    } else {
        panic!("expected Map for primary_contact");
    }
}

#[test]
fn nested_list_of_maps() {
    let (store, _dir) = temp_store();
    let mut txn = store.begin(false).unwrap();

    let mut contact1 = HashMap::new();
    contact1.insert("name".to_string(), Value::String("Alice".to_string()));
    contact1.insert("exhausted".to_string(), Value::Bool(false));

    let mut contact2 = HashMap::new();
    contact2.insert("name".to_string(), Value::String("Bob".to_string()));
    contact2.insert("exhausted".to_string(), Value::Bool(true));

    let mut fields = HashMap::new();
    fields.insert("name".to_string(), Value::String("Acme".to_string()));
    fields.insert(
        "contacts".to_string(),
        Value::List(vec![Value::Map(contact1), Value::Map(contact2)]),
    );

    let record = Record {
        id: "acct-1".to_string(),
        fields,
    };
    txn.insert(record).unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let result = txn.get_by_id("acct-1").unwrap().unwrap();

    if let Some(Value::List(contacts)) = result.fields.get("contacts") {
        assert_eq!(contacts.len(), 2);
        if let Value::Map(c1) = &contacts[0] {
            assert_eq!(c1.get("name"), Some(&Value::String("Alice".to_string())));
        } else {
            panic!("expected Map in contacts list");
        }
    } else {
        panic!("expected List for contacts");
    }
}

#[test]
fn overwrite_existing_record() {
    let (store, _dir) = temp_store();
    let mut txn = store.begin(false).unwrap();
    txn.insert(make_record("acct-1", "Acme Corp", 50000.0))
        .unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.insert(make_record("acct-1", "Acme Inc", 75000.0))
        .unwrap();
    txn.commit().unwrap();

    let txn = store.begin(true).unwrap();
    let result = txn.get_by_id("acct-1").unwrap().unwrap();
    assert_eq!(
        result.fields.get("name"),
        Some(&Value::String("Acme Inc".to_string()))
    );
    assert_eq!(result.fields.get("revenue"), Some(&Value::Float(75000.0)));
}

#[test]
fn rollback_discards_writes() {
    let (store, _dir) = temp_store();
    let mut txn = store.begin(false).unwrap();
    txn.insert(make_record("acct-1", "Acme Corp", 50000.0))
        .unwrap();
    txn.rollback().unwrap();

    let txn = store.begin(true).unwrap();
    let result = txn.get_by_id("acct-1").unwrap();
    assert!(result.is_none());
}

#[test]
fn rollback_does_not_affect_committed_data() {
    let (store, _dir) = temp_store();

    let mut txn = store.begin(false).unwrap();
    txn.insert(make_record("acct-1", "Acme Corp", 50000.0))
        .unwrap();
    txn.commit().unwrap();

    let mut txn = store.begin(false).unwrap();
    txn.insert(make_record("acct-2", "Globex", 80000.0))
        .unwrap();
    txn.delete("acct-1").unwrap();
    txn.rollback().unwrap();

    let txn = store.begin(true).unwrap();
    let acct1 = txn.get_by_id("acct-1").unwrap();
    assert!(acct1.is_some());
    let acct2 = txn.get_by_id("acct-2").unwrap();
    assert!(acct2.is_none());
}
