use slate_db::{Database, DatabaseConfig};
use slate_server::Server;
use slate_store::MemoryStore;

fn main() {
    let addr = std::env::var("SLATE_ADDR").unwrap_or_else(|_| "0.0.0.0:9600".to_string());

    let store = MemoryStore::new();
    let db = Database::open(store, DatabaseConfig::default());
    let mut server = Server::new(db, &addr);
    server.serve().expect("server failed");
}
