use std::collections::HashMap;
use std::net::TcpListener;
use std::thread;

use slate_client::Client;
use slate_db::{Database, Datasource, FieldDef, FieldType};
use slate_query::*;
use slate_server::Server;
use slate_store::{Record, RocksStore, Value};

fn start_server() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    let db = Database::new(store);

    // Find a free port
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let server = Server::new(db, &addr);
    thread::spawn(move || {
        server.serve().unwrap();
    });

    // Give server a moment to start listening
    thread::sleep(std::time::Duration::from_millis(50));

    (addr, dir)
}

fn make_record(id: &str, name: &str, status: &str) -> Record {
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), Value::String(name.to_string()));
    fields.insert("status".to_string(), Value::String(status.to_string()));
    Record {
        id: id.to_string(),
        fields,
    }
}

#[test]
fn insert_and_get_by_id() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    let record = make_record("acct-1", "Acme Corp", "active");
    client.insert(record).unwrap();

    let result = client.get_by_id("acct-1").unwrap();
    assert!(result.is_some());
    let r = result.unwrap();
    assert_eq!(r.id, "acct-1");
    assert_eq!(
        r.fields.get("name"),
        Some(&Value::String("Acme Corp".to_string()))
    );
}

#[test]
fn get_by_id_not_found() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    let result = client.get_by_id("nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn insert_batch_and_query() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    let records = vec![
        make_record("acct-1", "Acme", "active"),
        make_record("acct-2", "Globex", "rejected"),
        make_record("acct-3", "Initech", "active"),
    ];
    client.insert_batch(records).unwrap();

    // Query all
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = client.query(&query).unwrap();
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
    };
    let results = client.query(&query).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn delete_record() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    client
        .insert(make_record("acct-1", "Acme", "active"))
        .unwrap();
    client.delete("acct-1").unwrap();

    let result = client.get_by_id("acct-1").unwrap();
    assert!(result.is_none());
}

#[test]
fn datasource_crud() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    let ds = Datasource {
        id: "ds1".to_string(),
        name: "SBA".to_string(),
        fields: vec![
            FieldDef {
                name: "name".to_string(),
                field_type: FieldType::String,
            },
            FieldDef {
                name: "revenue".to_string(),
                field_type: FieldType::Float,
            },
        ],
    };

    // Save
    client.save_datasource(&ds).unwrap();

    // Get
    let result = client.get_datasource("ds1").unwrap();
    assert!(result.is_some());
    let fetched = result.unwrap();
    assert_eq!(fetched.name, "SBA");
    assert_eq!(fetched.fields.len(), 2);

    // List
    let list = client.list_datasources().unwrap();
    assert_eq!(list.len(), 1);

    // Delete
    client.delete_datasource("ds1").unwrap();
    let result = client.get_datasource("ds1").unwrap();
    assert!(result.is_none());
}

#[test]
fn query_with_sort_and_pagination() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    let mut records = Vec::new();
    for i in 0..20 {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::String(format!("Company-{i}")));
        fields.insert("score".to_string(), Value::Int(i));
        records.push(Record {
            id: format!("r-{i}"),
            fields,
        });
    }
    client.insert_batch(records).unwrap();

    let query = Query {
        filter: None,
        sort: vec![Sort {
            field: "score".to_string(),
            direction: SortDirection::Desc,
        }],
        skip: Some(5),
        take: Some(3),
    };
    let results = client.query(&query).unwrap();
    assert_eq!(results.len(), 3);
    // Descending: 19,18,17,16,15,14,13,12... skip 5 → 14,13,12
    assert_eq!(results[0].id, "r-14");
    assert_eq!(results[1].id, "r-13");
    assert_eq!(results[2].id, "r-12");
}
