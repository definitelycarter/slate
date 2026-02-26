use super::*;

#[test]
fn delete_removes_records() {
    let txn = MockTransaction::new();
    let docs = vec![
        rawdoc! { "_id": "1", "name": "Alice" },
        rawdoc! { "_id": "2", "name": "Bob" },
    ];
    let plan = Plan::Delete {
        collection: mock_collection(vec![]),
        source: Node::Values(docs),
    };
    let exec = Executor::new(&txn);
    let iter = exec.execute(plan).unwrap();
    let mut deleted = 0u64;
    for result in iter {
        result.unwrap();
        deleted += 1;
    }
    assert_eq!(deleted, 2);

    let deletes = txn.deletes.borrow();
    assert_eq!(deletes.len(), 2);
    // Engine handles encoding — we just verify doc IDs were passed
}

#[test]
fn update_writes_merged_doc() {
    let txn = MockTransaction::new();
    let docs = vec![rawdoc! { "_id": "1", "name": "Alice", "score": 70 }];
    let plan = Plan::Update {
        collection: mock_collection(vec![]),
        mutation: slate_query::parse_mutation(&rawdoc! { "score": 100 }).unwrap(),
        source: Node::Values(docs),
    };
    let exec = Executor::new(&txn);
    let iter = exec.execute(plan).unwrap();
    let mut matched = 0u64;
    let mut modified = 0u64;
    for result in iter {
        let opt_val = result.unwrap();
        matched += 1;
        if opt_val.is_some() {
            modified += 1;
        }
    }
    assert_eq!(matched, 1);
    assert_eq!(modified, 1);

    let puts = txn.puts.borrow();
    assert_eq!(puts.len(), 1);
    let written = bson::RawDocument::from_bytes(puts[0].doc.as_bytes()).unwrap();
    assert_eq!(written.get_str("name").unwrap(), "Alice");
    assert_eq!(written.get_i32("score").unwrap(), 100);
}

#[test]
fn update_unchanged_skips_write() {
    let txn = MockTransaction::new();
    let docs = vec![rawdoc! { "_id": "1", "name": "Alice", "status": "active" }];
    let plan = Plan::Update {
        collection: mock_collection(vec![]),
        mutation: slate_query::parse_mutation(&rawdoc! { "status": "active" }).unwrap(),
        source: Node::Values(docs),
    };
    let exec = Executor::new(&txn);
    let iter = exec.execute(plan).unwrap();
    let mut matched = 0u64;
    let mut modified = 0u64;
    for result in iter {
        let opt_val = result.unwrap();
        matched += 1;
        if opt_val.is_some() {
            modified += 1;
        }
    }
    assert_eq!(matched, 1);
    assert_eq!(modified, 0); // unchanged → not modified

    assert!(txn.puts.borrow().is_empty());
}

#[test]
fn replace_writes_replacement() {
    let txn = MockTransaction::new();
    let docs = vec![rawdoc! { "_id": "1", "name": "Alice", "status": "active" }];
    let plan = Plan::Replace {
        collection: mock_collection(vec![]),
        replacement: rawdoc! { "replaced": true },
        source: Node::Values(docs),
    };
    let exec = Executor::new(&txn);
    let iter = exec.execute(plan).unwrap();
    let mut matched = 0u64;
    let mut modified = 0u64;
    for result in iter {
        let opt_val = result.unwrap();
        matched += 1;
        if opt_val.is_some() {
            modified += 1;
        }
    }
    assert_eq!(matched, 1);
    assert_eq!(modified, 1);

    let puts = txn.puts.borrow();
    assert_eq!(puts.len(), 1);
    let written: bson::Document = bson::deserialize_from_slice(puts[0].doc.as_bytes()).unwrap();
    assert_eq!(written.get_bool("replaced").unwrap(), true);
}

#[test]
fn insert_writes_records() {
    let txn = MockTransaction::new();
    let docs = vec![
        rawdoc! { "_id": "1", "status": "active" },
        rawdoc! { "_id": "2", "status": "inactive" },
    ];
    let plan = Plan::Insert {
        collection: mock_collection(vec!["status".into()]),
        source: Node::Values(docs),
    };
    let exec = Executor::new(&txn);
    let iter = exec.execute(plan).unwrap();
    let mut count = 0u64;
    for result in iter {
        result.unwrap();
        count += 1;
    }
    assert_eq!(count, 2);

    let puts = txn.puts.borrow();
    // Engine handles index maintenance internally — insert_record only calls put
    assert_eq!(puts.len(), 2);
}

#[test]
fn full_update_pipeline() {
    let txn = MockTransaction::new();
    let docs = vec![rawdoc! { "_id": "1", "status": "active" }];
    let plan = Plan::Update {
        collection: mock_collection(vec!["status".into()]),
        mutation: slate_query::parse_mutation(&rawdoc! { "status": "archived" }).unwrap(),
        source: Node::Values(docs),
    };
    let exec = Executor::new(&txn);
    let iter = exec.execute(plan).unwrap();
    let mut matched = 0u64;
    let mut modified = 0u64;
    for result in iter {
        let opt_val = result.unwrap();
        matched += 1;
        if opt_val.is_some() {
            modified += 1;
        }
    }
    assert_eq!(matched, 1);
    assert_eq!(modified, 1);

    // Engine handles index diff internally
    let puts = txn.puts.borrow();
    assert_eq!(puts.len(), 1);
    let written = bson::RawDocument::from_bytes(puts[0].doc.as_bytes()).unwrap();
    assert_eq!(written.get_str("status").unwrap(), "archived");
}
