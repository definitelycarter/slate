use std::net::TcpListener;
use std::thread;

use bson::doc;
use http::{Method, Request, StatusCode};
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
fn query_returns_all_records() {
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
        "filters": { "status": "active" }
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
    assert_eq!(body["records"].as_array().unwrap().len(), 3);
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

#[test]
fn query_bad_body() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/query")
        .body(b"not json".to_vec())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── POST /data (insert) ─────────────────────────────────────────

#[test]
fn post_data_inserts_records() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let body = serde_json::json!([
        { "_id": "acct-10", "name": "NewCo", "status": "active" },
        { "_id": "acct-11", "name": "FreshCo", "status": "rejected" },
    ]);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/data")
        .body(serde_json::to_vec(&body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);
    let result: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(result["inserted"], 2);

    // Verify via query
    let req = Request::builder()
        .method(Method::POST)
        .uri("/query")
        .body(Vec::new())
        .unwrap();
    let resp = handler.handle(req);
    let result: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(result["total"], 7);
}

// ── PUT /data (upsert) ─────────────────────────────────────────

#[test]
fn put_data_inserts_new_records() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let body = serde_json::json!([
        { "_id": "acct-20", "name": "BrandNew", "status": "active" },
    ]);

    let req = Request::builder()
        .method(Method::PUT)
        .uri("/data")
        .body(serde_json::to_vec(&body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);
    let result: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(result["inserted"], 1);
    assert_eq!(result["updated"], 0);
}

#[test]
fn put_data_replaces_existing_records() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let body = serde_json::json!([
        { "_id": "acct-1", "name": "Acme Corp v2", "status": "rejected" },
    ]);

    let req = Request::builder()
        .method(Method::PUT)
        .uri("/data")
        .body(serde_json::to_vec(&body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);
    let result: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(result["inserted"], 0);
    assert_eq!(result["updated"], 1);

    // Verify total unchanged
    let req = Request::builder()
        .method(Method::POST)
        .uri("/query")
        .body(Vec::new())
        .unwrap();
    let resp = handler.handle(req);
    let result: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(result["total"], 5);
}

// ── PATCH /data (merge) ─────────────────────────────────────────

#[test]
fn patch_data_merges_existing_records() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let body = serde_json::json!([
        { "_id": "acct-1", "status": "rejected" },
    ]);

    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/data")
        .body(serde_json::to_vec(&body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);
    let result: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(result["inserted"], 0);
    assert_eq!(result["updated"], 1);
}

#[test]
fn patch_data_inserts_new_records() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let body = serde_json::json!([
        { "_id": "acct-30", "name": "MergeCo", "status": "active" },
    ]);

    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/data")
        .body(serde_json::to_vec(&body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);
    let result: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(result["inserted"], 1);
    assert_eq!(result["updated"], 0);
}

// ── DELETE /data ────────────────────────────────────────────────

#[test]
fn delete_data_removes_matching_records() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let body = serde_json::json!({
        "filter": { "status": "rejected" }
    });

    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/data")
        .body(serde_json::to_vec(&body).unwrap())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::OK);
    let result: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(result["deleted"], 1);

    // Verify total is now 4
    let req = Request::builder()
        .method(Method::POST)
        .uri("/query")
        .body(Vec::new())
        .unwrap();
    let resp = handler.handle(req);
    let result: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(result["total"], 4);
}

// ── Bad request bodies ──────────────────────────────────────────

#[test]
fn post_data_bad_body_returns_400() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/data")
        .body(b"not json".to_vec())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn delete_data_bad_body_returns_400() {
    let addr = start_server();
    seed_data(&addr);
    let handler = build_handler(&addr);

    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/data")
        .body(b"not json".to_vec())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── Routing ─────────────────────────────────────────────────────

#[test]
fn unknown_route_returns_404() {
    let addr = start_server();
    let handler = build_handler(&addr);

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
    let handler = build_handler(&addr);

    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/nonexistent")
        .body(Vec::new())
        .unwrap();

    let resp = handler.handle(req);
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
