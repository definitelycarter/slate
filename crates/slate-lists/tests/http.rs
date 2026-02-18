use std::net::TcpListener;
use std::thread;

use ::http::{Method, Request, StatusCode};
use bson::doc;
use slate_client::{Client, ClientPool};
use slate_db::{CollectionConfig, Database, DatabaseConfig};
use slate_lists::*;
use slate_query::*;
use slate_server::Server;
use slate_store::MemoryStore;

const COLLECTION: &str = "accounts";

fn start_server() -> String {
    let store = MemoryStore::new();
    let db = Database::open(store, DatabaseConfig::default());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let mut server = Server::new(db, &addr);
    thread::spawn(move || {
        server.serve().unwrap();
    });

    thread::sleep(std::time::Duration::from_millis(50));
    addr
}

fn seed_data(addr: &str) {
    let mut client = Client::connect(addr).unwrap();
    client
        .create_collection(&CollectionConfig {
            name: COLLECTION.to_string(),
            indexes: vec![],
        })
        .unwrap();
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

fn active_config() -> ListConfig {
    ListConfig {
        id: "active-accounts".into(),
        title: "Active Accounts".into(),
        collection: COLLECTION.into(),
        indexes: vec![],
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

fn all_config() -> ListConfig {
    ListConfig {
        id: "all-accounts".into(),
        title: "All Accounts".into(),
        collection: COLLECTION.into(),
        indexes: vec![],
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

fn build_handler(addr: &str, config: ListConfig) -> ListHttp<NoopLoader> {
    let pool = ClientPool::new(addr, 2).unwrap();
    let service = ListService::new(pool, NoopLoader);
    ListHttp::new(config, service)
}

// ── GET /config ─────────────────────────────────────────────────

#[test]
fn get_config_returns_list() {
    let addr = start_server();
    let handler = build_handler(&addr, active_config());

    let req = Request::builder()
        .method(Method::GET)
        .uri("/config")
        .body(Vec::new())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["id"], "active-accounts");
    assert_eq!(body["title"], "Active Accounts");
    assert_eq!(body["columns"].as_array().unwrap().len(), 3);
}

// ── POST /data ──────────────────────────────────────────────────

#[test]
fn get_data_returns_filtered_records() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr, active_config());

    let req = Request::builder()
        .method(Method::POST)
        .uri("/data")
        .header("x-list-key", "user-1")
        .body(Vec::new())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["total"], 3);
    assert_eq!(body["records"].as_array().unwrap().len(), 3);
}

#[test]
fn get_data_returns_all_records() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr, all_config());

    let req = Request::builder()
        .method(Method::POST)
        .uri("/data")
        .header("x-list-key", "user-1")
        .body(Vec::new())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["total"], 5);
    assert_eq!(body["records"].as_array().unwrap().len(), 5);
}

#[test]
fn get_data_with_user_filters() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr, active_config());

    let request_body = serde_json::json!({
        "filters": {
            "logical": "and",
            "children": [{
                "condition": {
                    "field": "revenue",
                    "operator": "gt",
                    "value": { "float": 50000.0 }
                }
            }]
        }
    });

    let req = Request::builder()
        .method(Method::POST)
        .uri("/data")
        .header("x-list-key", "user-1")
        .body(serde_json::to_vec(&request_body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    // active AND revenue > 50000 → Umbrella (95k)
    assert_eq!(body["total"], 1);
}

#[test]
fn get_data_with_pagination() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr, all_config());

    let request_body = serde_json::json!({
        "skip": 1,
        "take": 1
    });

    let req = Request::builder()
        .method(Method::POST)
        .uri("/data")
        .header("x-list-key", "user-1")
        .body(serde_json::to_vec(&request_body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["total"], 5); // total before pagination
    assert_eq!(body["records"].as_array().unwrap().len(), 1); // paginated
}

#[test]
fn get_data_bad_request_body() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr, active_config());

    let req = Request::builder()
        .method(Method::POST)
        .uri("/data")
        .header("x-list-key", "user-1")
        .body(b"not json".to_vec())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── Routing ─────────────────────────────────────────────────────

#[test]
fn unknown_route_returns_404() {
    let addr = start_server();
    let handler = build_handler(&addr, active_config());

    let req = Request::builder()
        .method(Method::GET)
        .uri("/unknown")
        .body(Vec::new())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[test]
fn wrong_method_returns_404() {
    let addr = start_server();
    let handler = build_handler(&addr, active_config());

    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/data")
        .body(Vec::new())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── Metadata ────────────────────────────────────────────────────

#[test]
fn metadata_headers_passed_through() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr, all_config());

    let req = Request::builder()
        .method(Method::POST)
        .uri("/data")
        .header("x-list-key", "user-1")
        .header("x-meta-tenant", "acme")
        .header("x-meta-region", "us-east")
        .body(Vec::new())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);
}
