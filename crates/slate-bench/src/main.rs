mod datagen;
mod report;
mod scenarios;

use std::sync::Arc;
use std::time::Instant;

use slate_db::Database;
use slate_store::RocksStore;

fn main() {
    println!("=== Slate Benchmark Suite ===\n");

    let total_start = Instant::now();
    let mut all_results = Vec::new();

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
        let query_results = scenarios::query_benchmarks(&db);
        for r in &query_results {
            r.print();
        }
        all_results.extend(query_results);
        println!();

        // Phase 4: Concurrency
        println!("[Phase 4] Concurrency Tests");
        let db_arc = Arc::new(db);
        let concurrency_results = scenarios::concurrency_tests(Arc::clone(&db_arc));
        for r in &concurrency_results {
            r.print();
        }
        all_results.extend(concurrency_results);
        println!();

        // Phase 5: Post-Concurrency Integrity
        println!("[Phase 5] Post-Concurrency Integrity");
        scenarios::verify_post_concurrency(&db_arc);
        println!();
    }

    let total_duration = total_start.elapsed();
    println!("=== Complete ===");
    println!("Total time: {:.2}s", total_duration.as_secs_f64());
}
