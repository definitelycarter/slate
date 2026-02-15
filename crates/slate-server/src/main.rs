use std::path::PathBuf;

use slate_db::Database;
use slate_server::Server;
use slate_store::RocksStore;

fn main() {
    let data_dir = std::env::var("SLATE_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
    let addr = std::env::var("SLATE_ADDR").unwrap_or_else(|_| "0.0.0.0:9600".to_string());

    let path = PathBuf::from(&data_dir);
    let store = RocksStore::open(&path).expect("failed to open store");
    let db = Database::new(store);
    let server = Server::new(db, &addr);
    server.serve().expect("server failed");
}
