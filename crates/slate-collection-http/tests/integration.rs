use std::net::TcpListener;
use std::thread;

use ::http::{Method, Request, StatusCode};
use bson::doc;
use slate_client::{Client, ClientPool};
use slate_collection::*;
use slate_db::{CollectionConfig, Database, DatabaseConfig};
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

fn build_handler(addr: &str) -> CollectionHttp {
    let pool = ClientPool::new(addr, 2).unwrap();
    CollectionHttp::new(COLLECTION.into(), pool)
}

// ── POST /query ─────────────────────────────────────────────────

#[test]
fn query_returns_records() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/query")
        .body(Vec::new())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["total"], 5);
    assert_eq!(body["records"].as_array().unwrap().len(), 5);
}

#[test]
fn query_with_filters() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let request_body = serde_json::json!({
        "filters": {
            "logical": "and",
            "children": [{
                "condition": {
                    "field": "revenue",
                    "operator": "gt",
                    "value": 50000.0
                }
            }]
        }
    });

    let req = Request::builder()
        .method(Method::POST)
        .uri("/query")
        .body(serde_json::to_vec(&request_body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["total"], 3);
}

#[test]
fn query_with_pagination() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let request_body = serde_json::json!({
        "skip": 1,
        "take": 2
    });

    let req = Request::builder()
        .method(Method::POST)
        .uri("/query")
        .body(serde_json::to_vec(&request_body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["total"], 5);
    assert_eq!(body["records"].as_array().unwrap().len(), 2);
}
