use std::sync::Arc;

use slate_client::ClientPool;

use slate_api::routes;
use slate_api::state::AppState;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let tcp_addr = std::env::var("SLATE_TCP_ADDR").unwrap_or_else(|_| "127.0.0.1:9600".into());
    let api_addr = std::env::var("SLATE_API_ADDR").unwrap_or_else(|_| "0.0.0.0:9601".into());
    let pool_size: usize = std::env::var("SLATE_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    let pool = ClientPool::new(&tcp_addr, pool_size).unwrap_or_else(|e| {
        eprintln!("failed to connect to slate-server at {tcp_addr}: {e}");
        std::process::exit(1);
    });

    let state = AppState {
        pool: Arc::new(pool),
    };

    let app = routes::router().with_state(state);

    let listener = tokio::net::TcpListener::bind(&api_addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to bind {api_addr}: {e}");
            std::process::exit(1);
        });

    tracing::info!("slate-api listening on {api_addr} (upstream: {tcp_addr})");
    axum::serve(listener, app).await.unwrap();
}
