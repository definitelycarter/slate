mod datagen;
mod report;
mod scenarios;
mod tcp_scenarios;

use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use slate_client::Client;
use slate_db::Database;
use slate_server::Server;
use slate_store::{MemoryStore, RocksStore, Store};

fn run_embedded<S: Store + Send + Sync + 'static>(
    backend: &str,
    cfg: &scenarios::BenchConfig,
    users: usize,
    all_results: &mut Vec<report::BenchResult>,
    make_db: impl Fn() -> Database<S>,
) {
    println!(
        "========== EMBEDDED ({backend}) — {} records/user ==========\n",
        cfg.label
    );

    for user in 0..users {
        println!("--- User {user} ---\n");

        let db = make_db();
        scenarios::setup_collection(&db);

        // Phase 1: Bulk Insert
        println!("[Phase 1] Bulk Insert");
        let insert_results = scenarios::bulk_insert(&db, user, cfg);
        if let Some(total) = insert_results.last() {
            total.print();
        }
        all_results.extend(insert_results);
        println!();

        // Phase 2: Data Integrity
        println!("[Phase 2] Data Integrity Verification");
        scenarios::verify_integrity(&db, user, cfg);
        println!();

        // Phase 3: Query Benchmarks
        println!("[Phase 3] Query Benchmarks");
        let query_results = scenarios::query_benchmarks(&db, user, cfg);
        for r in &query_results {
            r.print();
        }
        all_results.extend(query_results);
        println!();

        // Phase 4: Concurrency
        println!("[Phase 4] Concurrency Tests");
        let db_arc = Arc::new(db);
        let concurrency_results = scenarios::concurrency_tests(Arc::clone(&db_arc), user, cfg);
        for r in &concurrency_results {
            r.print();
        }
        all_results.extend(concurrency_results);
        println!();

        // Phase 5: Post-Concurrency Integrity
        println!("[Phase 5] Post-Concurrency Integrity");
        scenarios::verify_post_concurrency(&db_arc, user, cfg);
        println!();
    }
}

fn make_rocks_db() -> Database<RocksStore> {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let store = RocksStore::open(dir.path()).expect("failed to open store");
    std::mem::forget(dir);
    Database::new(store)
}

fn make_memory_db() -> Database<MemoryStore> {
    Database::new(MemoryStore::new())
}

fn main() {
    println!("=== Slate Benchmark Suite ===\n");

    let total_start = Instant::now();
    let mut all_results = Vec::new();

    // --- Embedded benchmarks: RocksStore ---

    run_embedded(
        "RocksStore",
        &scenarios::CONFIG_10K,
        3,
        &mut all_results,
        make_rocks_db,
    );
    run_embedded(
        "RocksStore",
        &scenarios::CONFIG_100K,
        3,
        &mut all_results,
        make_rocks_db,
    );

    // --- Embedded benchmarks: MemoryStore ---

    run_embedded(
        "MemoryStore",
        &scenarios::CONFIG_10K,
        3,
        &mut all_results,
        make_memory_db,
    );
    run_embedded(
        "MemoryStore",
        &scenarios::CONFIG_100K,
        3,
        &mut all_results,
        make_memory_db,
    );

    // --- TCP benchmarks ---

    println!("========== TCP (msgpack over localhost) ==========\n");

    for user in 0..3 {
        println!("--- TCP User {user} ---\n");

        let db = make_rocks_db();
        scenarios::setup_collection(&db);

        // Find a free port
        let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind");
        let addr = listener
            .local_addr()
            .expect("failed to get addr")
            .to_string();
        drop(listener);

        let server = Server::new(db, &addr);
        thread::spawn(move || {
            server.serve().expect("server failed");
        });
        thread::sleep(std::time::Duration::from_millis(50));

        let mut client = Client::connect(&addr).expect("client connect failed");

        // Phase 1: Bulk Insert
        println!("[Phase 1] TCP Bulk Insert");
        let insert_results = tcp_scenarios::bulk_insert(&mut client, user);
        if let Some(total) = insert_results.last() {
            total.print();
        }
        all_results.extend(insert_results);
        println!();

        // Phase 2: Data Integrity
        println!("[Phase 2] TCP Data Integrity Verification");
        tcp_scenarios::verify_integrity(&mut client, user);
        println!();

        // Phase 3: Query Benchmarks
        println!("[Phase 3] TCP Query Benchmarks");
        let query_results = tcp_scenarios::query_benchmarks(&mut client, user);
        for r in &query_results {
            r.print();
        }
        all_results.extend(query_results);
        println!();

        // Phase 4: Concurrency
        println!("[Phase 4] TCP Concurrency Tests");
        let concurrency_results = tcp_scenarios::concurrency_tests(&addr, user);
        for r in &concurrency_results {
            r.print();
        }
        all_results.extend(concurrency_results);
        println!();
    }

    let total_duration = total_start.elapsed();
    println!("=== Complete ===");
    println!("Total time: {:.2}s", total_duration.as_secs_f64());
}
