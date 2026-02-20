use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use slate_client::ClientPool;
use slate_lists::{ListConfig, ListHttp};
use tokio::signal::unix::{SignalKind, signal};

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

async fn handle(
    req: Request<Incoming>,
    handler: Arc<ListHttp>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let (parts, body) = req.into_parts();
    let body_bytes = body.collect().await.unwrap().to_bytes().to_vec();
    let http_req = Request::from_parts(parts, body_bytes);
    let http_resp = handler.handle(http_req);
    let (parts, body_bytes) = http_resp.into_parts();
    Ok(Response::from_parts(
        parts,
        Full::new(Bytes::from(body_bytes)),
    ))
}

async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    sigterm.recv().await;
}

#[tokio::main]
async fn main() {
    let config = load_config();
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let pool_size: usize = std::env::var("SLATE_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    let server_addr = std::env::var("SLATE_SERVER_ADDR").unwrap_or_else(|_| {
        eprintln!("SLATE_SERVER_ADDR is required");
        std::process::exit(1);
    });

    eprintln!("loading list: {} ({})", config.title, config.id);
    eprintln!("connecting to slate-server at {server_addr}");

    let pool = ClientPool::new(&server_addr, pool_size).unwrap_or_else(|e| {
        eprintln!("failed to create client pool: {e}");
        std::process::exit(1);
    });

    let handler = Arc::new(ListHttp::new(config, pool));

    let bind_addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to bind {bind_addr}: {e}");
            std::process::exit(1);
        });

    eprintln!("listening on {bind_addr}");

    let http = http1::Builder::new();
    let graceful = GracefulShutdown::new();
    let mut signal = pin!(shutdown_signal());

    loop {
        tokio::select! {
            Ok((stream, _)) = listener.accept() => {
                let io = TokioIo::new(stream);
                let handler = Arc::clone(&handler);
                let conn = http.serve_connection(io, service_fn(move |req| {
                    let handler = Arc::clone(&handler);
                    handle(req, handler)
                }));
                let fut = graceful.watch(conn);
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        eprintln!("connection error: {e}");
                    }
                });
            }
            _ = &mut signal => {
                eprintln!("shutdown signal received");
                drop(listener);
                break;
            }
        }
    }

    tokio::select! {
        _ = graceful.shutdown() => {
            eprintln!("shutdown complete");
        }
        _ = tokio::time::sleep(Duration::from_secs(10)) => {
            eprintln!("shutdown timed out after 10s");
        }
    }
}
