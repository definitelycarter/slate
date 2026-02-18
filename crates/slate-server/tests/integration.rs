use std::net::TcpListener;
use std::thread;

use bson::doc;
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
                value: QueryValue::String("acct-1".into()),
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

    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "_id".into(),
                operator: Operator::Eq,
                value: QueryValue::String("nonexistent".into()),
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
                value: QueryValue::String("active".to_string()),
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
            value: QueryValue::String("acct-1".into()),
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
    let indexes = client.list_indexes(COLLECTION).unwrap();
    assert_eq!(indexes, vec!["status"]);

    // Drop index
    client.drop_index(COLLECTION, "status").unwrap();
    let indexes = client.list_indexes(COLLECTION).unwrap();
    assert!(indexes.is_empty());
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
            value: QueryValue::String("acct-1".into()),
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
