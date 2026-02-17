use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use slate_client::ClientPool;
use slate_db::Database;
use slate_lists::{ListConfig, ListHttp, ListService, NoopLoader};
use slate_server::Server;
use slate_store::MemoryStore;

fn load_config() -> ListConfig {
    let path =
        std::env::var("SLATE_LIST_CONFIG").unwrap_or_else(|_| "/etc/slate/list.json".to_string());

    let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        eprintln!("failed to read config from {path}: {e}");
        std::process::exit(1);
    });

    serde_json::from_str(&content).unwrap_or_else(|e| {
        eprintln!("failed to parse config from {path}: {e}");
        std::process::exit(1);
    })
}

fn start_tcp_server() -> String {
    let store = MemoryStore::new();
    let db = Database::new(store);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let server = Server::new(db, &addr);
    thread::spawn(move || {
        server.serve().expect("tcp server failed");
    });

    thread::sleep(std::time::Duration::from_millis(50));
    addr
}

async fn handle(
    req: Request<Incoming>,
    handler: Arc<ListHttp<NoopLoader>>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let (parts, body) = req.into_parts();
    let body_bytes = body.collect().await?.to_bytes().to_vec();
    let http_req = Request::from_parts(parts, body_bytes);

    let http_resp = handler.handle(http_req);

    let (parts, body_bytes) = http_resp.into_parts();
    let resp = Response::from_parts(parts, Full::new(Bytes::from(body_bytes)));
    Ok(resp)
}

#[tokio::main]
async fn main() {
    let config = load_config();
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let pool_size: usize = std::env::var("SLATE_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    eprintln!("loading list: {} ({})", config.title, config.id);

    let tcp_addr = start_tcp_server();
    eprintln!("in-process tcp server on {tcp_addr}");

    let pool = ClientPool::new(&tcp_addr, pool_size).unwrap_or_else(|e| {
        eprintln!("failed to create client pool: {e}");
        std::process::exit(1);
    });

    let service = ListService::new(pool, NoopLoader);
    let handler = Arc::new(ListHttp::new(config, service));

    let bind_addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to bind {bind_addr}: {e}");
            std::process::exit(1);
        });

    eprintln!("listening on {bind_addr}");

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        let handler = Arc::clone(&handler);

        tokio::task::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        let handler = Arc::clone(&handler);
                        handle(req, handler)
                    }),
                )
                .await
            {
                eprintln!("connection error: {e}");
            }
        });
    }
}
