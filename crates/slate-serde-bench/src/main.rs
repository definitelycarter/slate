use std::time::Instant;

use bson::Bson;

const NUM_RECORDS: usize = 100_000;
const NUM_FIELDS: usize = 50;
const PROJECTED_FIELDS: &[&str] = &[
    "field_00", "field_05", "field_10", "field_15", "field_20", "field_25", "field_30", "field_35",
    "field_40", "field_45",
];

fn make_document(id: usize) -> bson::Document {
    let mut doc = bson::Document::new();
    for i in 0..NUM_FIELDS {
        let name = format!("field_{i:02}");
        let value = match i % 4 {
            0 => Bson::String(format!("value_{id}_{i}")),
            1 => Bson::Int64((id * 100 + i) as i64),
            2 => Bson::Double((id as f64) * 1.5 + (i as f64)),
            _ => Bson::Boolean(i % 2 == 0),
        };
        doc.insert(name, value);
    }
    doc
}

fn bench_messagepack(docs: &[bson::Document]) {
    println!("--- MessagePack ---\n");

    // Serialize
    let start = Instant::now();
    let serialized: Vec<Vec<u8>> = docs.iter().map(|d| rmp_serde::to_vec(d).unwrap()).collect();
    let ser_time = start.elapsed();
    let total_bytes: usize = serialized.iter().map(|v| v.len()).sum();
    println!(
        "  serialize {NUM_RECORDS} records:    {:>8.2}ms  ({:.0} bytes avg, {:.1} MB total)",
        ser_time.as_secs_f64() * 1000.0,
        total_bytes as f64 / NUM_RECORDS as f64,
        total_bytes as f64 / 1_000_000.0,
    );

    // Full deserialize + project 10 fields
    let start = Instant::now();
    let mut projected_count = 0u64;
    for bytes in &serialized {
        let doc: bson::Document = rmp_serde::from_slice(bytes).unwrap();
        for &field in PROJECTED_FIELDS {
            if doc.contains_key(field) {
                projected_count += 1;
            }
        }
    }
    let deser_time = start.elapsed();
    println!(
        "  deserialize + project 10: {:>8.2}ms  ({projected_count} fields found)",
        deser_time.as_secs_f64() * 1000.0,
    );

    // Full deserialize only (no projection overhead)
    let start = Instant::now();
    for bytes in &serialized {
        let _doc: bson::Document = rmp_serde::from_slice(bytes).unwrap();
    }
    let full_deser_time = start.elapsed();
    println!(
        "  deserialize only:         {:>8.2}ms",
        full_deser_time.as_secs_f64() * 1000.0,
    );

    println!();
}

fn bench_bson(docs: &[bson::Document]) {
    println!("--- BSON ---\n");

    // Serialize
    let start = Instant::now();
    let serialized: Vec<Vec<u8>> = docs.iter().map(|d| bson::to_vec(d).unwrap()).collect();
    let ser_time = start.elapsed();
    let total_bytes: usize = serialized.iter().map(|v| v.len()).sum();
    println!(
        "  serialize {NUM_RECORDS} records:    {:>8.2}ms  ({:.0} bytes avg, {:.1} MB total)",
        ser_time.as_secs_f64() * 1000.0,
        total_bytes as f64 / NUM_RECORDS as f64,
        total_bytes as f64 / 1_000_000.0,
    );

    // Full deserialize + project 10 fields
    let start = Instant::now();
    let mut projected_count = 0u64;
    for bytes in &serialized {
        let doc: bson::Document = bson::from_slice(bytes).unwrap();
        for &field in PROJECTED_FIELDS {
            if doc.contains_key(field) {
                projected_count += 1;
            }
        }
    }
    let full_deser_time = start.elapsed();
    println!(
        "  full deserialize + project 10: {:>8.2}ms  ({projected_count} fields found)",
        full_deser_time.as_secs_f64() * 1000.0,
    );

    // Raw BSON document: partial read (no full deserialize)
    let start = Instant::now();
    let mut projected_count = 0u64;
    for bytes in &serialized {
        let raw = bson::RawDocumentBuf::from_bytes(bytes.clone()).unwrap();
        for &field in PROJECTED_FIELDS {
            if let Ok(Some(_val)) = raw.get(field) {
                projected_count += 1;
            }
        }
    }
    let raw_time = start.elapsed();
    println!(
        "  raw doc partial read 10:       {:>8.2}ms  ({projected_count} fields found)",
        raw_time.as_secs_f64() * 1000.0,
    );

    // Full deserialize only (no projection overhead)
    let start = Instant::now();
    for bytes in &serialized {
        let _doc: bson::Document = bson::from_slice(bytes).unwrap();
    }
    let deser_only_time = start.elapsed();
    println!(
        "  full deserialize only:         {:>8.2}ms",
        deser_only_time.as_secs_f64() * 1000.0,
    );

    println!();
}

fn bench_bincode(docs: &[bson::Document]) {
    println!("--- Bincode (reference) ---\n");

    let start = Instant::now();
    let serialized: Vec<Vec<u8>> = docs
        .iter()
        .map(|d| bincode::serialize(d).unwrap())
        .collect();
    let ser_time = start.elapsed();
    let total_bytes: usize = serialized.iter().map(|v| v.len()).sum();
    println!(
        "  serialize {NUM_RECORDS} records:    {:>8.2}ms  ({:.0} bytes avg, {:.1} MB total)",
        ser_time.as_secs_f64() * 1000.0,
        total_bytes as f64 / NUM_RECORDS as f64,
        total_bytes as f64 / 1_000_000.0,
    );

    // bincode does not support deserialize_any, which bson::Document requires.
    // Deserialize is not possible for self-describing types like Bson.
    println!("  deserialize:              N/A (bincode does not support deserialize_any)");

    println!();
}

fn main() {
    println!("=== Serde Format Benchmark ===");
    println!(
        "  {NUM_RECORDS} records, {NUM_FIELDS} fields each, project {}\n",
        PROJECTED_FIELDS.len()
    );

    // Generate documents
    let start = Instant::now();
    let docs: Vec<bson::Document> = (0..NUM_RECORDS).map(make_document).collect();
    println!(
        "  generated documents in {:.2}ms\n",
        start.elapsed().as_secs_f64() * 1000.0
    );

    bench_messagepack(&docs);
    bench_bson(&docs);
    bench_bincode(&docs);
}
