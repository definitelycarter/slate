//! Dev tool for the Cosmos parity harness (`tools/cosmos-parity/`).
//!
//! Loads the shared fixture documents into an in-memory slate database, runs
//! each query in the matrix, and prints one JSON record per query to stdout
//! (JSONL) in the same shape the Cosmos side produces, so `run.py` can diff
//! slate against the Cosmos emulator.
//!
//! The collection's primary key is `id` (not the default `_id`) to match
//! Cosmos's system primary key, keeping the two sides' documents 1:1.
//!
//! Usage:
//!   cargo run -p slate-cli --example parity_dump -- <families.json> <queries.txt>

use std::fs;

use bson::Bson;
use serde_json::{Value, json};
use slate_db::{Database, DatabaseBuilder};
use slate_store::MemoryStore;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: parity_dump <fixtures.json> <queries.txt>");
        std::process::exit(2);
    }

    let docs: Vec<Value> =
        serde_json::from_str(&fs::read_to_string(&args[1]).expect("read fixtures"))
            .expect("parse fixtures json");
    let queries = fs::read_to_string(&args[2]).expect("read queries");

    let db = DatabaseBuilder::new()
        .open(MemoryStore::new())
        .expect("open db");
    {
        let txn = db.begin(false).expect("begin");
        db.collections()
            .create("c")
            .pk_path("id") // match Cosmos's system primary key
            .execute(&txn)
            .expect("create collection");
        db.collection("c")
            .insert_many(docs)
            .execute(&txn)
            .expect("insert");
        txn.commit().expect("commit");
    }

    for line in queries.lines() {
        let sql = line.trim();
        if sql.is_empty() || sql.starts_with('#') {
            continue;
        }
        let record = run_one(&db, sql);
        println!("{}", serde_json::to_string(&record).expect("serialize"));
    }
}

fn run_one(db: &Database<MemoryStore>, sql: &str) -> Value {
    let txn = match db.begin(true) {
        Ok(t) => t,
        Err(e) => return err(sql, &e.to_string()),
    };
    let iter = match db.collection("c").query(sql).iter_raw(&txn) {
        Ok(it) => it,
        Err(e) => return err(sql, &e.to_string()),
    };
    let mut items = Vec::new();
    for item in iter {
        let raw = match item {
            Ok(r) => r,
            Err(e) => return err(sql, &e.to_string()),
        };
        match Bson::try_from(raw) {
            Ok(b) => items.push(b.into_relaxed_extjson()),
            Err(e) => return err(sql, &e.to_string()),
        }
    }
    json!({ "sql": sql, "ok": true, "items": items })
}

fn err(sql: &str, message: &str) -> Value {
    json!({ "sql": sql, "ok": false, "error": message })
}
