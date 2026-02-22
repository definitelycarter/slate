use std::net::TcpListener;
use std::thread;

use bson::{Bson, doc};
use rand::Rng;
use slate_client::Client;
use slate_db::{CollectionConfig, Database, DatabaseConfig};
use slate_query::*;
use slate_server::Server;
use slate_store::RocksStore;

const COLLECTION: &str = "accounts";

fn start_server() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    let db = Database::open(store, DatabaseConfig::default());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let mut server = Server::new(db, &addr);
    thread::spawn(move || {
        server.serve().unwrap();
    });

    thread::sleep(std::time::Duration::from_millis(50));

    (addr, dir)
}

fn ensure_collection(client: &mut Client, name: &str) {
    client
        .create_collection(&CollectionConfig {
            name: name.to_string(),
            indexes: vec![],
        })
        .unwrap();
}

#[test]
fn insert_and_find_one() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    ensure_collection(&mut client, COLLECTION);

    client
        .insert_one(
            COLLECTION,
            doc! { "_id": "acct-1", "name": "Acme Corp", "status": "active" },
        )
        .unwrap();

    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "_id".into(),
                operator: Operator::Eq,
                value: Bson::String("acct-1".into()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: Some(1),
        columns: None,
    };
    let result = client.find_one(COLLECTION, &query).unwrap();
    assert!(result.is_some());
    let r = result.unwrap();
    assert_eq!(r.get_str("_id").unwrap(), "acct-1");
    assert_eq!(r.get_str("name").unwrap(), "Acme Corp");
}

#[test]
fn find_one_not_found() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    ensure_collection(&mut client, COLLECTION);

    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "_id".into(),
                operator: Operator::Eq,
                value: Bson::String("nonexistent".into()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: Some(1),
        columns: None,
    };
    let result = client.find_one(COLLECTION, &query).unwrap();
    assert!(result.is_none());
}

#[test]
fn insert_many_and_find() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    ensure_collection(&mut client, COLLECTION);

    client
        .insert_many(
            COLLECTION,
            vec![
                doc! { "_id": "acct-1", "name": "Acme", "status": "active" },
                doc! { "_id": "acct-2", "name": "Globex", "status": "rejected" },
                doc! { "_id": "acct-3", "name": "Initech", "status": "active" },
            ],
        )
        .unwrap();

    // Query all
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = client.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 3);

    // Query with filter
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".to_string(),
                operator: Operator::Eq,
                value: Bson::String("active".to_string()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = client.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn delete_one() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    ensure_collection(&mut client, COLLECTION);

    client
        .insert_one(
            COLLECTION,
            doc! { "_id": "acct-1", "name": "Acme", "status": "active" },
        )
        .unwrap();

    let filter = FilterGroup {
        logical: LogicalOp::And,
        children: vec![FilterNode::Condition(Filter {
            field: "_id".into(),
            operator: Operator::Eq,
            value: Bson::String("acct-1".into()),
        })],
    };
    let result = client.delete_one(COLLECTION, &filter).unwrap();
    assert_eq!(result.deleted, 1);

    let query = Query {
        filter: Some(filter),
        sort: vec![],
        skip: None,
        take: Some(1),
        columns: None,
    };
    let result = client.find_one(COLLECTION, &query).unwrap();
    assert!(result.is_none());
}

#[test]
fn collection_operations() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    ensure_collection(&mut client, "contacts");
    ensure_collection(&mut client, "accounts");

    // Insert into two collections
    client
        .insert_one("contacts", doc! { "_id": "c-1", "name": "Alice" })
        .unwrap();
    client
        .insert_one("accounts", doc! { "_id": "a-1", "name": "Acme" })
        .unwrap();

    // List collections
    let mut collections = client.list_collections().unwrap();
    collections.sort();
    assert_eq!(collections, vec!["accounts", "contacts"]);

    // Drop one
    client.drop_collection("contacts").unwrap();
    let collections = client.list_collections().unwrap();
    assert_eq!(collections, vec!["accounts"]);
}

#[test]
fn find_with_sort_and_pagination() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    ensure_collection(&mut client, COLLECTION);

    let mut docs = Vec::new();
    for i in 0..20 {
        docs.push(doc! {
            "_id": format!("r-{i}"),
            "name": format!("Company-{i}"),
            "score": i as i64,
        });
    }
    client.insert_many(COLLECTION, docs).unwrap();

    let query = Query {
        filter: None,
        sort: vec![Sort {
            field: "score".to_string(),
            direction: SortDirection::Desc,
        }],
        skip: Some(5),
        take: Some(3),
        columns: None,
    };
    let results = client.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 3);
    // Descending: 19,18,17,16,15,14,13,12... skip 5 → 14,13,12
    assert_eq!(results[0].get_str("_id").unwrap(), "r-14");
    assert_eq!(results[1].get_str("_id").unwrap(), "r-13");
    assert_eq!(results[2].get_str("_id").unwrap(), "r-12");
}

#[test]
fn index_operations() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    ensure_collection(&mut client, COLLECTION);

    // Create index
    client.create_index(COLLECTION, "status").unwrap();

    // List indexes
    let mut indexes = client.list_indexes(COLLECTION).unwrap();
    indexes.sort();
    assert_eq!(indexes, vec!["status", "ttl"]);

    // Drop index
    client.drop_index(COLLECTION, "status").unwrap();
    let indexes = client.list_indexes(COLLECTION).unwrap();
    assert_eq!(indexes, vec!["ttl"]);
}

#[test]
fn update_one_merge() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    ensure_collection(&mut client, COLLECTION);

    client
        .insert_one(
            COLLECTION,
            doc! { "_id": "acct-1", "name": "Acme", "status": "active" },
        )
        .unwrap();

    let filter = FilterGroup {
        logical: LogicalOp::And,
        children: vec![FilterNode::Condition(Filter {
            field: "_id".into(),
            operator: Operator::Eq,
            value: Bson::String("acct-1".into()),
        })],
    };
    let result = client
        .update_one(COLLECTION, &filter, doc! { "status": "rejected" }, false)
        .unwrap();
    assert_eq!(result.matched, 1);
    assert_eq!(result.modified, 1);

    let query = Query {
        filter: Some(filter),
        sort: vec![],
        skip: None,
        take: Some(1),
        columns: None,
    };
    let doc = client.find_one(COLLECTION, &query).unwrap().unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Acme"); // unchanged
    assert_eq!(doc.get_str("status").unwrap(), "rejected"); // updated
}

// ---------------------------------------------------------------------------
// Helpers for data-heavy tests
// ---------------------------------------------------------------------------

/// Extract an integer field that may be Int32 or Int64 after msgpack round-trip.
fn get_int(doc: &bson::Document, key: &str) -> i64 {
    match doc.get(key) {
        Some(Bson::Int64(v)) => *v,
        Some(Bson::Int32(v)) => *v as i64,
        other => panic!("expected int for {key}, got {other:?}"),
    }
}

const STATUSES: &[&str] = &["active", "rejected"];
const REC1: &[&str] = &["ProductA", "ProductB", "ProductC"];
const REC2: &[&str] = &["ProductX", "ProductY", "ProductZ"];
const REC3: &[&str] = &["Widget1", "Widget2", "Widget3"];

fn generate_doc(seq: usize) -> bson::Document {
    let mut rng = rand::thread_rng();
    let mut doc = doc! {
        "_id": format!("r-{seq}"),
        "name": format!("Company-{seq}"),
        "status": STATUSES[rng.gen_range(0..STATUSES.len())],
        "contacts_count": rng.gen_range(0_i64..100),
        "product_recommendation1": REC1[rng.gen_range(0..REC1.len())],
        "product_recommendation2": REC2[rng.gen_range(0..REC2.len())],
        "product_recommendation3": REC3[rng.gen_range(0..REC3.len())],
    };
    if rng.gen_ratio(7, 10) {
        let epoch_secs = rng.gen_range(1_700_000_000_i64..1_740_000_000);
        doc.insert(
            "last_contacted_at",
            bson::Bson::DateTime(bson::DateTime::from_millis(epoch_secs * 1000)),
        );
    }
    if rng.gen_bool(0.5) {
        doc.insert("notes", format!("Note for {seq}"));
    }
    doc
}

/// Start a server and seed it with 1,000 records in a collection with indexes.
fn start_server_with_data() -> (Client, tempfile::TempDir) {
    let (addr, dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    client
        .create_collection(&CollectionConfig {
            name: COLLECTION.to_string(),
            indexes: vec!["status".into(), "contacts_count".into()],
        })
        .unwrap();

    for batch in 0..10 {
        let start = batch * 100;
        let docs: Vec<_> = (start..start + 100).map(generate_doc).collect();
        client.insert_many(COLLECTION, docs).unwrap();
    }

    (client, dir)
}

// ---------------------------------------------------------------------------
// Data-heavy integration tests
// ---------------------------------------------------------------------------

#[test]
fn bulk_insert_and_count() {
    let (mut client, _dir) = start_server_with_data();
    let count = client.count(COLLECTION, None).unwrap();
    assert_eq!(count, 1000);
}

#[test]
fn query_indexed_filter() {
    let (mut client, _dir) = start_server_with_data();

    let filter = FilterGroup {
        logical: LogicalOp::And,
        children: vec![FilterNode::Condition(Filter {
            field: "status".into(),
            operator: Operator::Eq,
            value: Bson::String("active".into()),
        })],
    };

    let results = client
        .find(
            COLLECTION,
            &Query {
                filter: Some(filter.clone()),
                sort: vec![],
                skip: None,
                take: None,
                columns: None,
            },
        )
        .unwrap();

    assert!(!results.is_empty());
    for doc in &results {
        assert_eq!(doc.get_str("status").unwrap(), "active");
    }

    let count = client.count(COLLECTION, Some(&filter)).unwrap();
    assert_eq!(count, results.len() as u64);
}

#[test]
fn query_multi_condition_and() {
    let (mut client, _dir) = start_server_with_data();

    let results = client
        .find(
            COLLECTION,
            &Query {
                filter: Some(FilterGroup {
                    logical: LogicalOp::And,
                    children: vec![
                        FilterNode::Condition(Filter {
                            field: "status".into(),
                            operator: Operator::Eq,
                            value: Bson::String("active".into()),
                        }),
                        FilterNode::Condition(Filter {
                            field: "product_recommendation1".into(),
                            operator: Operator::Eq,
                            value: Bson::String("ProductA".into()),
                        }),
                        FilterNode::Condition(Filter {
                            field: "product_recommendation2".into(),
                            operator: Operator::Eq,
                            value: Bson::String("ProductX".into()),
                        }),
                    ],
                }),
                sort: vec![],
                skip: None,
                take: None,
                columns: None,
            },
        )
        .unwrap();

    for doc in &results {
        assert_eq!(doc.get_str("status").unwrap(), "active");
        assert_eq!(doc.get_str("product_recommendation1").unwrap(), "ProductA");
        assert_eq!(doc.get_str("product_recommendation2").unwrap(), "ProductX");
    }
}

#[test]
fn query_sort_and_take() {
    let (mut client, _dir) = start_server_with_data();

    let results = client
        .find(
            COLLECTION,
            &Query {
                filter: None,
                sort: vec![Sort {
                    field: "contacts_count".into(),
                    direction: SortDirection::Desc,
                }],
                skip: None,
                take: Some(10),
                columns: None,
            },
        )
        .unwrap();

    assert_eq!(results.len(), 10);
    for window in results.windows(2) {
        let a = get_int(&window[0], "contacts_count");
        let b = get_int(&window[1], "contacts_count");
        assert!(a >= b, "expected descending order: {a} >= {b}");
    }
}

#[test]
fn query_skip_take() {
    let (mut client, _dir) = start_server_with_data();

    let sort = vec![Sort {
        field: "contacts_count".into(),
        direction: SortDirection::Desc,
    }];

    // Get the full sorted set (first 20)
    let full = client
        .find(
            COLLECTION,
            &Query {
                filter: None,
                sort: sort.clone(),
                skip: None,
                take: Some(20),
                columns: None,
            },
        )
        .unwrap();

    // Get skip(5) + take(5)
    let page = client
        .find(
            COLLECTION,
            &Query {
                filter: None,
                sort,
                skip: Some(5),
                take: Some(5),
                columns: None,
            },
        )
        .unwrap();

    assert_eq!(page.len(), 5);
    for (i, doc) in page.iter().enumerate() {
        assert_eq!(
            doc.get_str("_id").unwrap(),
            full[5 + i].get_str("_id").unwrap(),
        );
    }
}

#[test]
fn query_take_no_sort() {
    let (mut client, _dir) = start_server_with_data();

    let results = client
        .find(
            COLLECTION,
            &Query {
                filter: Some(FilterGroup {
                    logical: LogicalOp::And,
                    children: vec![FilterNode::Condition(Filter {
                        field: "status".into(),
                        operator: Operator::Eq,
                        value: Bson::String("active".into()),
                    })],
                }),
                sort: vec![],
                skip: None,
                take: Some(50),
                columns: None,
            },
        )
        .unwrap();

    assert_eq!(results.len(), 50);
    for doc in &results {
        assert_eq!(doc.get_str("status").unwrap(), "active");
    }
}

#[test]
fn point_lookups() {
    let (mut client, _dir) = start_server_with_data();

    for i in (0..1000).step_by(10) {
        let id = format!("r-{i}");
        let doc = client.find_by_id(COLLECTION, &id, None).unwrap();
        assert!(doc.is_some(), "expected to find {id}");
        assert_eq!(doc.unwrap().get_str("_id").unwrap(), id);
    }
}

#[test]
fn concurrent_readers_and_writers() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    client
        .create_collection(&CollectionConfig {
            name: COLLECTION.to_string(),
            indexes: vec!["status".into()],
        })
        .unwrap();

    // Seed with 500 records so readers have something to query
    let docs: Vec<_> = (0..500).map(generate_doc).collect();
    client.insert_many(COLLECTION, docs).unwrap();

    let initial_count = client.count(COLLECTION, None).unwrap();
    assert_eq!(initial_count, 500);
    drop(client);

    let addr_owned = addr.clone();
    let mut handles = Vec::new();

    // 2 writer threads — each inserts 100 records
    for writer_id in 0..2 {
        let addr = addr_owned.clone();
        handles.push(thread::spawn(move || {
            let mut c = Client::connect(&addr).expect("writer connect");
            let base = 1000 + writer_id * 100;
            let docs: Vec<_> = (base..base + 100)
                .map(|seq| {
                    let mut rng = rand::thread_rng();
                    doc! {
                        "_id": format!("w-{seq}"),
                        "name": format!("Writer-{seq}"),
                        "status": STATUSES[rng.gen_range(0..STATUSES.len())],
                        "contacts_count": rng.gen_range(0_i64..100),
                    }
                })
                .collect();
            c.insert_many(COLLECTION, docs).expect("writer insert");
        }));
    }

    // 4 reader threads — each does 5 queries
    for _ in 0..4 {
        let addr = addr_owned.clone();
        handles.push(thread::spawn(move || {
            let mut c = Client::connect(&addr).expect("reader connect");
            for _ in 0..5 {
                let results = c
                    .find(
                        COLLECTION,
                        &Query {
                            filter: Some(FilterGroup {
                                logical: LogicalOp::And,
                                children: vec![FilterNode::Condition(Filter {
                                    field: "status".into(),
                                    operator: Operator::Eq,
                                    value: Bson::String("active".into()),
                                })],
                            }),
                            sort: vec![],
                            skip: None,
                            take: Some(50),
                            columns: None,
                        },
                    )
                    .expect("reader find");
                assert!(results.len() <= 50);
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    // Verify writers completed: 500 original + 200 from writers
    let mut client = Client::connect(&addr).unwrap();
    let final_count = client.count(COLLECTION, None).unwrap();
    assert_eq!(final_count, 700);
}
