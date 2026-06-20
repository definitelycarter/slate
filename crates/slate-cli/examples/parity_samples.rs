//! Dev tool: run the Azure-Samples/cosmos-db-nosql-query-samples corpus against
//! slate. For each `<dir>/query.sql` (+ optional `seed.json`), load the seed into
//! a fresh in-memory DB and run the (multi-line) query, emitting one JSONL record
//! `{dir, ok, items|error}`. `run_samples.py` diffs these against each folder's
//! authoritative `result.json`.
//!
//! Usage:
//!   cargo run -p slate-cli --example parity_samples -- <scripts-dir>

use std::fs;
use std::path::Path;

use bson::Bson;
use serde_json::{Value, json};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_store::MemoryStore;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: parity_samples <scripts-dir>");
        std::process::exit(2);
    }

    let mut folders: Vec<_> = fs::read_dir(&args[1])
        .expect("read scripts dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir() && p.join("query.sql").exists())
        .collect();
    folders.sort();

    for folder in folders {
        let name = folder
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let sql = fs::read_to_string(folder.join("query.sql")).expect("read query.sql");
        println!(
            "{}",
            serde_json::to_string(&run_one(&name, &sql, &folder)).expect("serialize")
        );
    }
}

fn run_one(name: &str, sql: &str, folder: &Path) -> Value {
    let db = match DatabaseBuilder::new().open(MemoryStore::new()) {
        Ok(db) => db,
        Err(e) => return err(name, &e.to_string()),
    };

    let seed = folder.join("seed.json");
    if seed.exists() {
        let docs: Vec<Value> = match fs::read_to_string(&seed).map(|s| serde_json::from_str(&s)) {
            Ok(Ok(d)) => d,
            Ok(Err(e)) => return err(name, &format!("seed parse: {e}")),
            Err(e) => return err(name, &e.to_string()),
        };
        let txn = match db.begin(false) {
            Ok(t) => t,
            Err(e) => return err(name, &e.to_string()),
        };
        let config = CollectionConfig {
            name: "c".into(),
            pk_path: "id".into(),
            ..Default::default()
        };
        if let Err(e) = txn.create_collection(&config) {
            return err(name, &e.to_string());
        }
        if let Err(e) = txn
            .insert_many(DEFAULT_CF, "c", docs)
            .and_then(|c| c.drain())
        {
            return err(name, &e.to_string());
        }
        if let Err(e) = txn.commit() {
            return err(name, &e.to_string());
        }
    }

    run_query(&db, name, sql)
}

fn run_query(db: &Database<MemoryStore>, name: &str, sql: &str) -> Value {
    let txn = match db.begin(true) {
        Ok(t) => t,
        Err(e) => return err(name, &e.to_string()),
    };
    let cursor = match txn.query(DEFAULT_CF, "c", sql) {
        Ok(c) => c,
        Err(e) => return err(name, &e.to_string()),
    };
    let iter = match cursor.iter_raw_values() {
        Ok(i) => i,
        Err(e) => return err(name, &e.to_string()),
    };
    let mut items = Vec::new();
    for it in iter {
        let raw = match it {
            Ok(r) => r,
            Err(e) => return err(name, &e.to_string()),
        };
        match Bson::try_from(raw) {
            Ok(b) => items.push(b.into_relaxed_extjson()),
            Err(e) => return err(name, &e.to_string()),
        }
    }
    json!({ "dir": name, "ok": true, "items": items })
}

fn err(name: &str, message: &str) -> Value {
    json!({ "dir": name, "ok": false, "error": message })
}
