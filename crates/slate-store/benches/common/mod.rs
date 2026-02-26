#![allow(dead_code)]

use slate_store::{Store, Transaction};

/// Generate `n` key-value pairs mimicking engine record keys + small doc values.
///
/// Keys: `r\x00test\x00{id}` (~12-16 bytes)
/// Values: deterministic ~200 byte payloads
pub fn generate_kv_pairs(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let key = format!("r\x00test\x00rec-{i}").into_bytes();
            // ~200 byte value simulating a small BSON doc
            let value = format!(
                "{{\"_id\":\"rec-{i}\",\"name\":\"User {i}\",\"status\":\"{}\",\"contacts_count\":{},\"padding\":\"{}\"}}",
                if i % 2 == 0 { "active" } else { "rejected" },
                i % 100,
                "x".repeat(120),
            )
            .into_bytes();
            (key, value)
        })
        .collect()
}

/// Seed a store's CF with `n` key-value pairs via a committed write transaction.
pub fn seed_store<S: Store>(store: &S, cf_name: &str, n: usize) {
    let pairs = generate_kv_pairs(n);
    let txn = store.begin(false).unwrap();
    let cf = txn.cf(cf_name).unwrap();
    let refs: Vec<(&[u8], &[u8])> = pairs.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
    txn.put_batch(&cf, &refs).unwrap();
    txn.commit().unwrap();
}
