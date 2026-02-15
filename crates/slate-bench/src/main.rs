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
use slate_store::RocksStore;

fn main() {
    println!("=== Slate Benchmark Suite ===\n");

    let total_start = Instant::now();
    let mut all_results = Vec::new();

    // --- Embedded benchmarks ---

    println!("========== EMBEDDED (direct) ==========\n");

    for user in 0..3 {
        println!("--- User {user} ---\n");

        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let store = RocksStore::open(dir.path()).expect("failed to open store");
        let db = Database::new(store);

        // Phase 1: Bulk Insert
        println!("[Phase 1] Bulk Insert");
        let insert_results = scenarios::bulk_insert(&db, user);
        if let Some(total) = insert_results.last() {
            total.print();
        }
        all_results.extend(insert_results);
        println!();

        // Phase 2: Data Integrity
        println!("[Phase 2] Data Integrity Verification");
        scenarios::verify_integrity(&db, user);
        println!();

        // Phase 3: Query Benchmarks
        println!("[Phase 3] Query Benchmarks");
        let query_results = scenarios::query_benchmarks(&db, user);
        for r in &query_results {
            r.print();
        }
        all_results.extend(query_results);
        println!();

        // Phase 4: Concurrency
        println!("[Phase 4] Concurrency Tests");
        let db_arc = Arc::new(db);
        let concurrency_results = scenarios::concurrency_tests(Arc::clone(&db_arc), user);
        for r in &concurrency_results {
            r.print();
        }
        all_results.extend(concurrency_results);
        println!();

        // Phase 5: Post-Concurrency Integrity
        println!("[Phase 5] Post-Concurrency Integrity");
        scenarios::verify_post_concurrency(&db_arc, user);
        println!();
    }

    // --- Multi-prefix benchmarks ---

    println!("========== MULTI-PREFIX (shared DB) ==========\n");
    {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let store = RocksStore::open(dir.path()).expect("failed to open store");
        let db = Database::new(store);

        // Insert all 3 users into same DB
        for user in 0..3 {
            println!("[Setup] Inserting user {user} data");
            scenarios::bulk_insert(&db, user);
        }
        println!();

        println!("[Phase] Multi-Prefix Query Benchmarks");
        let multi_results = scenarios::multi_prefix_benchmarks(&db, 3);
        for r in &multi_results {
            r.print();
        }
        all_results.extend(multi_results);
        println!();
    }

    // --- TCP benchmarks ---

    println!("========== TCP (bincode over localhost) ==========\n");

    for user in 0..3 {
        println!("--- TCP User {user} ---\n");

        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let store = RocksStore::open(dir.path()).expect("failed to open store");
        let db = Database::new(store);

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
