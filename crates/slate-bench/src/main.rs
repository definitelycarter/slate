mod alloc;
mod datagen;
mod report;
mod scenarios;

#[global_allocator]
static ALLOC: alloc::TrackingAllocator = alloc::TrackingAllocator;

use std::sync::Arc;
use std::time::Instant;

use slate_db::{Database, DatabaseConfig};
use slate_store::{MemoryStore, Store};

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

        // Phase 3b: Distinct Benchmarks
        println!("[Phase 3b] Distinct Benchmarks");
        let distinct_results = scenarios::distinct_benchmarks(&db, user, cfg);
        for r in &distinct_results {
            r.print();
        }
        all_results.extend(distinct_results);
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

fn make_memory_db() -> Database<MemoryStore> {
    Database::open(MemoryStore::new(), DatabaseConfig::default())
}

fn main() {
    println!("=== Slate Benchmark Suite ===\n");

    let total_start = Instant::now();
    let mut all_results = Vec::new();

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

    let total_duration = total_start.elapsed();
    println!("=== Complete ===");
    println!("Total time: {:.2}s", total_duration.as_secs_f64());
}
