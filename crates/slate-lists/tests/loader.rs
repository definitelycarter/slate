use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use bson::doc;
use slate_client::{Client, ClientPool};
use slate_db::{CollectionConfig, Database, DatabaseConfig};
use slate_lists::*;
use slate_query::*;
use slate_server::Server;
use slate_store::MemoryStore;

/// A fake loader that streams 10 BSON documents one at a time.
struct FakeLoader;

impl Loader for FakeLoader {
    fn load(
        &self,
        _collection: &str,
        _metadata: &HashMap<String, String>,
    ) -> Result<Box<dyn Iterator<Item = Result<bson::Document, ListError>> + '_>, ListError> {
        let docs = (0..10).map(|i| {
            Ok(doc! {
                "_id": format!("fake-{i}"),
                "name": format!("Company {i}"),
                "status": if i % 2 == 0 { "active" } else { "inactive" },
                "revenue": (i as f64 + 1.0) * 10000.0,
            })
        });
        Ok(Box::new(docs))
    }
}

/// A loader that counts how many times it's been called.
struct CountingLoader {
    call_count: AtomicUsize,
}

impl CountingLoader {
    fn new() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
        }
    }

    fn count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

impl Loader for CountingLoader {
    fn load(
        &self,
        _collection: &str,
        _metadata: &HashMap<String, String>,
    ) -> Result<Box<dyn Iterator<Item = Result<bson::Document, ListError>> + '_>, ListError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let docs = (0..10).map(|i| {
            Ok(doc! {
                "_id": format!("counted-{i}"),
                "name": format!("Company {i}"),
                "status": if i % 2 == 0 { "active" } else { "inactive" },
                "revenue": (i as f64 + 1.0) * 10000.0,
            })
        });
        Ok(Box::new(docs))
    }
}

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

fn create_collection(addr: &str) {
    let mut client = Client::connect(addr).unwrap();
    client
        .create_collection(&CollectionConfig {
            name: COLLECTION.to_string(),
            indexes: vec![],
        })
        .unwrap();
}

fn active_config() -> ListConfig {
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

fn all_config() -> ListConfig {
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

fn post_data(handler: &ListHttp<impl Loader>, body: &str) -> serde_json::Value {
    let req = ::http::Request::builder()
        .method(::http::Method::POST)
        .uri("/data")
        .body(body.as_bytes().to_vec())
        .unwrap();
    let resp = handler.handle(req);
    serde_json::from_slice(resp.body()).unwrap()
}

fn post_data_empty(handler: &ListHttp<impl Loader>) -> serde_json::Value {
    let req = ::http::Request::builder()
        .method(::http::Method::POST)
        .uri("/data")
        .body(Vec::new())
        .unwrap();
    let resp = handler.handle(req);
    serde_json::from_slice(resp.body()).unwrap()
}

// ── Loader populates empty collection ───────────────────────────

#[test]
fn loader_streams_documents_into_empty_collection() {
    let addr = start_server();
    create_collection(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let handler = ListHttp::new(all_config(), pool, FakeLoader);

    let body = post_data_empty(&handler);
    assert_eq!(body["total"], 10);
    assert_eq!(body["records"].as_array().unwrap().len(), 10);
}

#[test]
fn loader_with_list_filters() {
    let addr = start_server();
    create_collection(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let handler = ListHttp::new(active_config(), pool, FakeLoader);

    let body = post_data_empty(&handler);
    // FakeLoader inserts 10 docs, 5 are active (even indices)
    assert_eq!(body["total"], 5);
    assert_eq!(body["records"].as_array().unwrap().len(), 5);
}

#[test]
fn loader_with_user_filters_and_sort() {
    let addr = start_server();
    create_collection(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let handler = ListHttp::new(all_config(), pool, FakeLoader);

    let request = serde_json::json!({
        "filters": {
            "logical": "and",
            "children": [{
                "condition": {
                    "field": "revenue",
                    "operator": "gt",
                    "value": { "float": 50000.0 }
                }
            }]
        },
        "sort": [{ "field": "revenue", "direction": "desc" }]
    });

    let body = post_data(&handler, &request.to_string());
    assert_eq!(body["total"], 5);
    assert_eq!(body["records"][0]["name"], "Company 9");
    assert_eq!(body["records"][4]["name"], "Company 5");
}

// ── Loader skipped when data exists ─────────────────────────────

#[test]
fn loader_not_called_when_data_exists() {
    let addr = start_server();
    seed_data(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let handler = ListHttp::new(all_config(), pool, FakeLoader);

    let body = post_data_empty(&handler);
    // Should have the 5 seeded records, NOT the 10 from FakeLoader
    assert_eq!(body["total"], 5);
    assert_eq!(body["records"].as_array().unwrap().len(), 5);
}

#[test]
fn loader_called_once_across_multiple_requests() {
    let addr = start_server();
    create_collection(&addr);

    let pool = ClientPool::new(&addr, 2).unwrap();
    let handler = ListHttp::new(all_config(), pool, CountingLoader::new());

    // First request — collection empty, loader should fire
    let body1 = post_data_empty(&handler);
    assert_eq!(body1["total"], 10);
    assert_eq!(handler.loader().count(), 1);

    // Second request — data exists, loader should NOT fire
    let body2 = post_data_empty(&handler);
    assert_eq!(body2["total"], 10);
    assert_eq!(handler.loader().count(), 1);
}
