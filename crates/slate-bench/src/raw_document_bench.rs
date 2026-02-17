//! Microbenchmark: RawDocument::from_bytes once vs per-node
//!
//! Simulates two approaches for accessing fields through a Filter → Sort → Projection pipeline:
//!
//! 1. **from_bytes once**: construct &RawDocument once, access 3 fields (Filter, Sort, Projection)
//! 2. **from_bytes per node**: construct &RawDocument 3 times, access 1 field each
//!
//! Run: cargo run --release -p slate-bench --bin raw-document-bench

use std::time::Instant;

use bson::{RawDocument, doc};

fn generate_raw_buffers(count: usize) -> Vec<Vec<u8>> {
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let doc = doc! {
                "name": format!("Company-{i}"),
                "status": if rand::Rng::gen_bool(&mut rng, 0.5) { "active" } else { "rejected" },
                "contacts_count": rand::Rng::gen_range(&mut rng, 0_i64..100),
                "product_recommendation1": "ProductA",
                "product_recommendation2": "ProductX",
                "product_recommendation3": "Widget1",
            };
            bson::to_vec(&doc).unwrap()
        })
        .collect()
}

fn bench_from_bytes_once(buffers: &[Vec<u8>]) -> usize {
    let mut count = 0;
    for buf in buffers {
        let raw = RawDocument::from_bytes(buf).unwrap();
        // Simulate Filter: access status
        let _ = raw.get("status").ok();
        // Simulate Sort: access contacts_count
        let _ = raw.get("contacts_count").ok();
        // Simulate Projection: access name
        let _ = raw.get("name").ok();
        count += 1;
    }
    count
}

fn bench_from_bytes_per_node(buffers: &[Vec<u8>]) -> usize {
    let mut count = 0;
    for buf in buffers {
        // Simulate Filter: from_bytes + access status
        let raw = RawDocument::from_bytes(buf).unwrap();
        let _ = raw.get("status").ok();

        // Simulate Sort: from_bytes + access contacts_count
        let raw = RawDocument::from_bytes(buf).unwrap();
        let _ = raw.get("contacts_count").ok();

        // Simulate Projection: from_bytes + access name
        let raw = RawDocument::from_bytes(buf).unwrap();
        let _ = raw.get("name").ok();

        count += 1;
    }
    count
}

fn main() {
    const N: usize = 100_000;
    const RUNS: usize = 5;

    println!("Generating {N} raw BSON buffers...");
    let buffers = generate_raw_buffers(N);
    let avg_size = buffers.iter().map(|b| b.len()).sum::<usize>() / buffers.len();
    println!("Average document size: {avg_size} bytes\n");

    println!("=== from_bytes ONCE + 3 field accesses ===");
    let mut times_once = Vec::new();
    for run in 0..RUNS {
        let start = Instant::now();
        let count = bench_from_bytes_once(&buffers);
        let elapsed = start.elapsed();
        let ms = elapsed.as_secs_f64() * 1000.0;
        println!("  run {}: {ms:.2}ms ({count} records)", run + 1);
        times_once.push(ms);
    }
    let avg_once = times_once.iter().sum::<f64>() / times_once.len() as f64;
    println!("  avg: {avg_once:.2}ms\n");

    println!("=== from_bytes PER NODE (3x) + 1 field access each ===");
    let mut times_per = Vec::new();
    for run in 0..RUNS {
        let start = Instant::now();
        let count = bench_from_bytes_per_node(&buffers);
        let elapsed = start.elapsed();
        let ms = elapsed.as_secs_f64() * 1000.0;
        println!("  run {}: {ms:.2}ms ({count} records)", run + 1);
        times_per.push(ms);
    }
    let avg_per = times_per.iter().sum::<f64>() / times_per.len() as f64;
    println!("  avg: {avg_per:.2}ms\n");

    let overhead = ((avg_per - avg_once) / avg_once) * 100.0;
    let sign = if overhead > 0.0 { "+" } else { "" };
    println!("=== Result ===");
    println!("  from_bytes once:     {avg_once:.2}ms");
    println!("  from_bytes per node: {avg_per:.2}ms");
    println!("  overhead:            {sign}{overhead:.1}%");
    println!(
        "  per-record cost:     {:.1}ns",
        (avg_per - avg_once) * 1_000_000.0 / N as f64
    );
}
