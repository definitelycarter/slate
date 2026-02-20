use std::path::Path;

use slate_db::{Database, DatabaseConfig};
use slate_server::Server;
use slate_store::{MemoryStore, RocksStore};

fn main() {
    let addr = std::env::var("SLATE_ADDR").unwrap_or_else(|_| "0.0.0.0:9600".to_string());

    match std::env::var("SLATE_STORE").as_deref() {
        Ok("rocksdb") => {
            let path = std::env::var("SLATE_DATA_DIR").unwrap_or_else(|_| "/data".to_string());
            let store = RocksStore::open(Path::new(&path)).expect("failed to open RocksDB");
            let db = Database::open(store, DatabaseConfig::default());
            let mut server = Server::new(db, &addr);
            server.serve().expect("server failed");
        }
        _ => {
            let store = MemoryStore::new();
            let db = Database::open(store, DatabaseConfig::default());
            let mut server = Server::new(db, &addr);
            server.serve().expect("server failed");
        }
    }
}
