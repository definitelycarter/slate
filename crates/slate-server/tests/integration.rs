use std::net::TcpListener;
use std::thread;

use slate_client::Client;
use slate_db::{CellWrite, Database, Datasource, FieldDef, FieldType, Value};
use slate_query::*;
use slate_server::Server;
use slate_store::RocksStore;

const DS_ID: &str = "ds1";
const PARTITION: &str = "test";

fn start_server() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    let db = Database::new(store);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let server = Server::new(db, &addr);
    thread::spawn(move || {
        server.serve().unwrap();
    });

    thread::sleep(std::time::Duration::from_millis(50));

    (addr, dir)
}

fn make_datasource() -> Datasource {
    Datasource {
        id: DS_ID.to_string(),
        name: "SBA".to_string(),
        fields: vec![
            FieldDef {
                name: "name".to_string(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "status".to_string(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "score".to_string(),
                field_type: FieldType::Int,
                ttl_seconds: None,
                indexed: false,
            },
        ],
        partition: PARTITION.to_string(),
    }
}

fn make_cells(name: &str, status: &str, ts: i64) -> Vec<CellWrite> {
    vec![
        CellWrite {
            column: "name".into(),
            value: Value::String(name.into()),
            timestamp: ts,
        },
        CellWrite {
            column: "status".into(),
            value: Value::String(status.into()),
            timestamp: ts,
        },
    ]
}

#[test]
fn insert_and_get_by_id() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    let ts = 1_700_000_000;

    client.save_datasource(&make_datasource()).unwrap();
    client
        .write_cells(DS_ID, "acct-1", make_cells("Acme Corp", "active", ts))
        .unwrap();

    let result = client.get_by_id(DS_ID, "acct-1", None).unwrap();
    assert!(result.is_some());
    let r = result.unwrap();
    assert_eq!(r.id, "acct-1");
    assert_eq!(
        r.cells.get("name").map(|c| &c.value),
        Some(&Value::String("Acme Corp".to_string()))
    );
}

#[test]
fn get_by_id_not_found() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    client.save_datasource(&make_datasource()).unwrap();
    let result = client.get_by_id(DS_ID, "nonexistent", None).unwrap();
    assert!(result.is_none());
}

#[test]
fn write_batch_and_query() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    let ts = 1_700_000_000;

    client.save_datasource(&make_datasource()).unwrap();
    client
        .write_batch(
            DS_ID,
            vec![
                ("acct-1".into(), make_cells("Acme", "active", ts)),
                ("acct-2".into(), make_cells("Globex", "rejected", ts)),
                ("acct-3".into(), make_cells("Initech", "active", ts)),
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
    let results = client.query(DS_ID, &query).unwrap();
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
    let results = client.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn delete_record() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    let ts = 1_700_000_000;

    client.save_datasource(&make_datasource()).unwrap();
    client
        .write_cells(DS_ID, "acct-1", make_cells("Acme", "active", ts))
        .unwrap();
    client.delete_record(DS_ID, "acct-1").unwrap();

    let result = client.get_by_id(DS_ID, "acct-1", None).unwrap();
    assert!(result.is_none());
}

#[test]
fn datasource_crud() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();

    let ds = Datasource {
        id: DS_ID.to_string(),
        name: "SBA".to_string(),
        fields: vec![
            FieldDef {
                name: "name".to_string(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "revenue".to_string(),
                field_type: FieldType::Float,
                ttl_seconds: None,
                indexed: false,
            },
        ],
        partition: PARTITION.to_string(),
    };

    // Save
    client.save_datasource(&ds).unwrap();

    // Get
    let result = client.get_datasource(DS_ID).unwrap();
    assert!(result.is_some());
    let fetched = result.unwrap();
    assert_eq!(fetched.name, "SBA");
    assert_eq!(fetched.fields.len(), 2);

    // List
    let list = client.list_datasources().unwrap();
    assert_eq!(list.len(), 1);

    // Delete
    client.delete_datasource(DS_ID).unwrap();
    let result = client.get_datasource(DS_ID).unwrap();
    assert!(result.is_none());
}

#[test]
fn query_with_sort_and_pagination() {
    let (addr, _dir) = start_server();
    let mut client = Client::connect(&addr).unwrap();
    let ts = 1_700_000_000;

    // Create datasource with score field
    client.save_datasource(&make_datasource()).unwrap();

    let mut writes = Vec::new();
    for i in 0..20 {
        writes.push((
            format!("r-{i}"),
            vec![
                CellWrite {
                    column: "name".into(),
                    value: Value::String(format!("Company-{i}")),
                    timestamp: ts,
                },
                CellWrite {
                    column: "score".into(),
                    value: Value::Int(i),
                    timestamp: ts,
                },
            ],
        ));
    }
    client.write_batch(DS_ID, writes).unwrap();

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
    let results = client.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 3);
    // Descending: 19,18,17,16,15,14,13,12... skip 5 → 14,13,12
    assert_eq!(results[0].id, "r-14");
    assert_eq!(results[1].id, "r-13");
    assert_eq!(results[2].id, "r-12");
}
