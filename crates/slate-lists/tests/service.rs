use std::collections::HashMap;
use std::net::TcpListener;
use std::thread;

use bson::doc;
use slate_client::{Client, ClientPool};
use slate_db::Database;
use slate_lists::*;
use slate_query::*;
use slate_server::Server;
use slate_store::MemoryStore;

const COLLECTION: &str = "accounts";

fn start_server() -> String {
    let store = MemoryStore::new();
    let db = Database::new(store);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let server = Server::new(db, &addr);
    thread::spawn(move || {
        server.serve().unwrap();
    });

    thread::sleep(std::time::Duration::from_millis(50));
    addr
}

fn seed_data(addr: &str) {
    let mut client = Client::connect(addr).unwrap();
    client
        .insert_many(
            COLLECTION,
            vec![
                doc! { "_id": "acct-1", "name": "Acme Corp", "status": "active", "revenue": 50000.0 },
                doc! { "_id": "acct-2", "name": "Globex", "status": "rejected", "revenue": 80000.0 },
                doc! { "_id": "acct-3", "name": "Initech", "status": "active", "revenue": 12000.0 },
                doc! { "_id": "acct-4", "name": "Umbrella", "status": "active", "revenue": 95000.0 },
                doc! { "_id": "acct-5", "name": "Stark Industries", "status": "snoozed", "revenue": 200000.0 },
            ],
        )
        .unwrap();
}

fn test_config() -> ListConfig {
    ListConfig {
        id: "list-1".into(),
        title: "Active Accounts".into(),
        collection: COLLECTION.into(),
        filters: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: QueryValue::String("active".into()),
            })],
        }),
        columns: vec![
            Column {
                field: "name".into(),
                header: "Name".into(),
                width: 200,
                pinned: true,
            },
            Column {
                field: "status".into(),
                header: "Status".into(),
                width: 100,
                pinned: false,
            },
            Column {
                field: "revenue".into(),
                header: "Revenue".into(),
                width: 120,
                pinned: false,
            },
        ],
    }
}

fn no_filter_config() -> ListConfig {
    ListConfig {
        id: "list-2".into(),
        title: "All Accounts".into(),
        collection: COLLECTION.into(),
        filters: None,
        columns: vec![
            Column {
                field: "name".into(),
                header: "Name".into(),
                width: 200,
                pinned: false,
            },
            Column {
                field: "status".into(),
                header: "Status".into(),
                width: 100,
                pinned: false,
            },
        ],
    }
}

// ── Basic query tests ───────────────────────────────────────────

#[test]
fn get_list_data_with_default_filters() {
    let addr = start_server();
    seed_data(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let service = ListService::new(pool, NoopLoader);
    let config = test_config();

    let response = service
        .get_list_data(&config, "user-1", &ListRequest::default(), &HashMap::new())
        .unwrap();

    // 3 active accounts
    assert_eq!(response.total, 3);
    assert_eq!(response.records.len(), 3);
}

#[test]
fn get_list_data_no_filters() {
    let addr = start_server();
    seed_data(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let service = ListService::new(pool, NoopLoader);
    let config = no_filter_config();

    let response = service
        .get_list_data(&config, "user-1", &ListRequest::default(), &HashMap::new())
        .unwrap();

    assert_eq!(response.total, 5);
    assert_eq!(response.records.len(), 5);
}

// ── User filters merged with list filters ───────────────────────

#[test]
fn get_list_data_with_user_filters() {
    let addr = start_server();
    seed_data(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let service = ListService::new(pool, NoopLoader);
    let config = test_config(); // default: status = active

    let request = ListRequest {
        filters: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "revenue".into(),
                operator: Operator::Gt,
                value: QueryValue::Float(50000.0),
            })],
        }),
        ..Default::default()
    };

    let response = service
        .get_list_data(&config, "user-1", &request, &HashMap::new())
        .unwrap();

    // active AND revenue > 50000 → Umbrella (95k)
    assert_eq!(response.total, 1);
    assert_eq!(response.records.len(), 1);
}

// ── Pagination ──────────────────────────────────────────────────

#[test]
fn get_list_data_with_pagination() {
    let addr = start_server();
    seed_data(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let service = ListService::new(pool, NoopLoader);
    let config = test_config(); // 3 active accounts

    let request = ListRequest {
        skip: Some(1),
        take: Some(1),
        ..Default::default()
    };

    let response = service
        .get_list_data(&config, "user-1", &request, &HashMap::new())
        .unwrap();

    // total is still 3 (before skip/take)
    assert_eq!(response.total, 3);
    // but only 1 record returned
    assert_eq!(response.records.len(), 1);
}

// ── Sort ────────────────────────────────────────────────────────

#[test]
fn get_list_data_with_sort() {
    let addr = start_server();
    seed_data(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let service = ListService::new(pool, NoopLoader);
    let config = test_config();

    let request = ListRequest {
        sort: vec![Sort {
            field: "revenue".into(),
            direction: SortDirection::Desc,
        }],
        ..Default::default()
    };

    let response = service
        .get_list_data(&config, "user-1", &request, &HashMap::new())
        .unwrap();

    assert_eq!(response.total, 3);
    // Descending by revenue: Umbrella (95k), Acme (50k), Initech (12k)
    assert_eq!(response.records[0].get_str("name").unwrap(), "Umbrella");
    assert_eq!(response.records[2].get_str("name").unwrap(), "Initech");
}

// ── Column projection ───────────────────────────────────────────

#[test]
fn get_list_data_projects_columns() {
    let addr = start_server();
    seed_data(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let service = ListService::new(pool, NoopLoader);
    let config = no_filter_config(); // columns: name, status only

    let response = service
        .get_list_data(&config, "user-1", &ListRequest::default(), &HashMap::new())
        .unwrap();

    for record in &response.records {
        assert!(record.contains_key("name"));
        assert!(record.contains_key("status"));
        assert!(!record.contains_key("revenue"));
    }
}

// ── Empty collection ────────────────────────────────────────────

#[test]
fn get_list_data_empty_collection() {
    let addr = start_server();
    // Don't seed data

    let pool = ClientPool::new(&addr, 2).unwrap();
    let service = ListService::new(pool, NoopLoader);
    let config = test_config();

    let response = service
        .get_list_data(&config, "user-1", &ListRequest::default(), &HashMap::new())
        .unwrap();

    assert_eq!(response.total, 0);
    assert!(response.records.is_empty());
}

// ── No matching filters ─────────────────────────────────────────

#[test]
fn get_list_data_no_matches() {
    let addr = start_server();
    seed_data(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let service = ListService::new(pool, NoopLoader);

    // List that filters on a status nobody has
    let config = ListConfig {
        id: "list-x".into(),
        title: "Archived".into(),
        collection: COLLECTION.into(),
        filters: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: QueryValue::String("archived".into()),
            })],
        }),
        columns: vec![Column {
            field: "name".into(),
            header: "Name".into(),
            width: 200,
            pinned: false,
        }],
    };

    let response = service
        .get_list_data(&config, "user-1", &ListRequest::default(), &HashMap::new())
        .unwrap();

    assert_eq!(response.total, 0);
    assert!(response.records.is_empty());
}
