use super::*;

#[test]
fn values_yields_all_docs() {
    let txn = NoopTransaction;
    let docs = vec![
        rawdoc! { "_id": "1", "name": "Alice", "status": "active" },
        rawdoc! { "_id": "2", "name": "Bob", "status": "inactive" },
        rawdoc! { "_id": "3", "name": "Charlie", "status": "active" },
    ];
    let plan = Plan::Find(Node::Values(docs));
    let rows = collect_docs(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "1");
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "2");
    assert_eq!(rows[2].as_ref().unwrap().get_str("_id").unwrap(), "3");
    assert_eq!(rows[0].as_ref().unwrap().get_str("name").unwrap(), "Alice");
    assert_eq!(rows[1].as_ref().unwrap().get_str("name").unwrap(), "Bob");
    assert_eq!(
        rows[2].as_ref().unwrap().get_str("name").unwrap(),
        "Charlie"
    );
}

#[test]
fn filter_on_values() {
    let txn = NoopTransaction;
    let docs = vec![
        rawdoc! { "_id": "1", "name": "Alice", "status": "active" },
        rawdoc! { "_id": "2", "name": "Bob", "status": "inactive" },
        rawdoc! { "_id": "3", "name": "Charlie", "status": "active" },
    ];
    let plan = Plan::Find(Node::Filter {
        predicate: Expression::Eq("status".into(), bson::Bson::String("active".into())),
        source: Box::new(Node::Values(docs)),
    });
    let rows = collect_docs(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "1");
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "3");
}

#[test]
fn filter_empty_result() {
    let txn = NoopTransaction;
    let docs = vec![
        rawdoc! { "_id": "1", "name": "Alice", "status": "active" },
        rawdoc! { "_id": "2", "name": "Bob", "status": "inactive" },
    ];
    let plan = Plan::Find(Node::Filter {
        predicate: Expression::Eq("status".into(), bson::Bson::String("nope".into())),
        source: Box::new(Node::Values(docs)),
    });
    let rows = collect_docs(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(rows.len(), 0);
}

#[test]
fn sort_on_values() {
    let txn = NoopTransaction;
    let docs = vec![
        rawdoc! { "_id": "1", "name": "Alice", "score": 70 },
        rawdoc! { "_id": "2", "name": "Bob", "score": 90 },
        rawdoc! { "_id": "3", "name": "Charlie", "score": 80 },
    ];
    let plan = Plan::Find(Node::Sort {
        sorts: vec![Sort {
            field: "score".into(),
            direction: SortDirection::Desc,
        }],
        source: Box::new(Node::Values(docs)),
    });
    let rows = collect_docs(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "2"); // Bob: 90
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "3"); // Charlie: 80
    assert_eq!(rows[2].as_ref().unwrap().get_str("_id").unwrap(), "1"); // Alice: 70
}

#[test]
fn limit_skip_take() {
    let txn = NoopTransaction;
    let docs = vec![
        rawdoc! { "_id": "1", "name": "Alice" },
        rawdoc! { "_id": "2", "name": "Bob" },
        rawdoc! { "_id": "3", "name": "Charlie" },
        rawdoc! { "_id": "4", "name": "Diana" },
    ];
    let plan = Plan::Find(Node::Limit {
        skip: 1,
        take: Some(2),
        source: Box::new(Node::Values(docs)),
    });
    let rows = collect_docs(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "2");
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "3");
}

#[test]
fn limit_take_only() {
    let txn = NoopTransaction;
    let docs = vec![
        rawdoc! { "_id": "1", "name": "Alice" },
        rawdoc! { "_id": "2", "name": "Bob" },
    ];
    let plan = Plan::Find(Node::Limit {
        skip: 0,
        take: Some(1),
        source: Box::new(Node::Values(docs)),
    });
    let rows = collect_docs(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "1");
}

#[test]
fn projection_on_values() {
    let txn = NoopTransaction;
    let docs = vec![
        rawdoc! { "_id": "1", "name": "Alice", "status": "active", "score": 70 },
        rawdoc! { "_id": "2", "name": "Bob", "status": "inactive", "score": 90 },
    ];
    let plan = Plan::Find(Node::Projection {
        columns: Some(vec!["name".into()]),
        source: Box::new(Node::Values(docs)),
    });
    let rows = collect_docs(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(rows.len(), 2);
    let doc0 = rows[0].as_ref().unwrap();
    assert_eq!(doc0.get_str("_id").unwrap(), "1");
    assert_eq!(doc0.get_str("name").unwrap(), "Alice");
    assert!(doc0.get("status").is_none());
    assert!(doc0.get("score").is_none());
}

#[test]
fn distinct_on_values() {
    let txn = NoopTransaction;
    let docs = vec![
        rawdoc! { "_id": "1", "status": "active" },
        rawdoc! { "_id": "2", "status": "inactive" },
        rawdoc! { "_id": "3", "status": "active" },
        rawdoc! { "_id": "4", "status": "inactive" },
    ];
    let plan = Plan::Find(Node::Distinct {
        field: "status".into(),
        source: Box::new(Node::Projection {
            columns: Some(vec!["status".into()]),
            source: Box::new(Node::Values(docs)),
        }),
    });
    let mut iter = Executor::new(&txn).execute(plan).unwrap();
    let val = iter.next().unwrap().unwrap().unwrap();
    let arr = match val {
        bson::RawBson::Array(a) => a,
        _ => panic!("expected array"),
    };
    let values: Vec<String> = arr
        .into_iter()
        .map(|v| match v.unwrap() {
            bson::raw::RawBsonRef::String(s) => s.to_string(),
            _ => panic!("expected string"),
        })
        .collect();
    assert_eq!(values.len(), 2);
    assert!(values.contains(&"active".to_string()));
    assert!(values.contains(&"inactive".to_string()));
}

#[test]
fn composed_filter_sort_limit() {
    let txn = NoopTransaction;
    let docs = vec![
        rawdoc! { "_id": "1", "name": "Alice", "status": "active", "score": 70 },
        rawdoc! { "_id": "2", "name": "Bob", "status": "inactive", "score": 95 },
        rawdoc! { "_id": "3", "name": "Charlie", "status": "active", "score": 90 },
        rawdoc! { "_id": "4", "name": "Diana", "status": "active", "score": 80 },
    ];
    let plan = Plan::Find(Node::Limit {
        skip: 0,
        take: Some(2),
        source: Box::new(Node::Sort {
            sorts: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            source: Box::new(Node::Filter {
                predicate: Expression::Eq("status".into(), bson::Bson::String("active".into())),
                source: Box::new(Node::Values(docs)),
            }),
        }),
    });
    let rows = collect_docs(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "3"); // Charlie: 90
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "4"); // Diana: 80
}
