use std::time::Duration;
use std::{env, fs, thread};

use slate_client::Client;
use slate_db::CollectionConfig;

fn main() {
    let addr = env::var("SLATE_SERVER_ADDR").expect("SLATE_SERVER_ADDR must be set");
    let path = env::var("SLATE_INIT_CONFIG").expect("SLATE_INIT_CONFIG must be set");

    let data = fs::read_to_string(&path).expect("failed to read config file");
    let configs: Vec<CollectionConfig> =
        serde_json::from_str(&data).expect("failed to parse config");

    // Retry connection until the server is ready.
    let mut client = loop {
        match Client::connect(&addr) {
            Ok(c) => break c,
            Err(e) => {
                eprintln!("waiting for server at {addr}: {e}");
                thread::sleep(Duration::from_secs(1));
            }
        }
    };

    for config in &configs {
        match client.create_collection(config) {
            Ok(_) => eprintln!("created collection: {}", config.name),
            Err(e) => eprintln!("collection {}: {e}", config.name),
        }
    }
}
