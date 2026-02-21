use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use rand::Rng;
use slate_store::{MemoryStore, RedbStore, RocksStore, Store, Transaction};

const CF: &str = "data";
const TOTAL_RECORDS: usize = 500_000;
const BATCH_SIZE: usize = 1_000;
const VALUE_SIZE: usize = 10_000; // ~10 KB per document
const READER_THREADS: usize = 4;
const CONCURRENT_READ_ITERATIONS: usize = 50_000;

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

fn make_key(id: usize) -> Vec<u8> {
    format!("d:rec:{id:08}").into_bytes()
}

fn make_value(id: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut buf = vec![0u8; VALUE_SIZE];
    rng.fill(&mut buf[..]);
    // Stamp the id into the first 8 bytes so we can verify later
    buf[..8].copy_from_slice(&(id as u64).to_le_bytes());
    buf
}

fn verify_value(id: usize, data: &[u8]) -> bool {
    if data.len() != VALUE_SIZE {
        return false;
    }
    let stored_id = u64::from_le_bytes(data[..8].try_into().unwrap());
    stored_id == id as u64
}

// ---------------------------------------------------------------------------
// Bench helpers
// ---------------------------------------------------------------------------

struct BenchResult {
    name: String,
    write_time: std::time::Duration,
    read_all_time: std::time::Duration,
    scan_time: std::time::Duration,
    verified: usize,
    data_loss: usize,
    corruption: usize,
    total_bytes: usize,
}

impl BenchResult {
    fn print(&self) {
        let mb = self.total_bytes as f64 / (1024.0 * 1024.0);
        let gb = self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        println!("--- {} ---", self.name);
        println!();
        println!("  records:       {TOTAL_RECORDS} x {VALUE_SIZE} bytes = {mb:.1} MB ({gb:.2} GB)");
        println!(
            "  write:         {:>8.2}ms  ({:.0} records/sec)",
            self.write_time.as_secs_f64() * 1000.0,
            TOTAL_RECORDS as f64 / self.write_time.as_secs_f64(),
        );
        println!(
            "  read all:      {:>8.2}ms  ({:.0} records/sec)",
            self.read_all_time.as_secs_f64() * 1000.0,
            TOTAL_RECORDS as f64 / self.read_all_time.as_secs_f64(),
        );
        println!(
            "  scan prefix:   {:>8.2}ms",
            self.scan_time.as_secs_f64() * 1000.0,
        );
        println!();
        println!("  integrity:");
        println!("    verified:    {}", self.verified);
        println!("    data loss:   {}", self.data_loss);
        println!("    corruption:  {}", self.corruption);
        println!();
    }
}

// ---------------------------------------------------------------------------
// Generic bench over any Store
// ---------------------------------------------------------------------------

fn bench_store<S: Store>(store: &S, name: &str) -> BenchResult {
    store.create_cf(CF).unwrap();

    // -- Write phase: batch inserts --
    let write_start = Instant::now();
    let num_batches = TOTAL_RECORDS / BATCH_SIZE;
    let mut total_bytes = 0usize;

    for batch_idx in 0..num_batches {
        let base = batch_idx * BATCH_SIZE;
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..BATCH_SIZE)
            .map(|i| {
                let id = base + i;
                (make_key(id), make_value(id))
            })
            .collect();

        total_bytes += entries
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>();

        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let mut txn = store.begin(false).unwrap();
        let cf = txn.cf(CF).unwrap();
        txn.put_batch(&cf, &refs).unwrap();
        txn.commit().unwrap();
    }
    let write_time = write_start.elapsed();

    // -- Read-all phase: verify every record --
    let read_start = Instant::now();
    let mut verified = 0usize;
    let mut data_loss = 0usize;
    let mut corruption = 0usize;

    // Read in batches using multi_get (single txn + cf handle for the whole phase)
    let mut txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    for batch_idx in 0..num_batches {
        let base = batch_idx * BATCH_SIZE;
        let keys: Vec<Vec<u8>> = (0..BATCH_SIZE).map(|i| make_key(base + i)).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        let results = txn.multi_get(&cf, &key_refs).unwrap();

        for (i, result) in results.into_iter().enumerate() {
            let id = base + i;
            match result {
                Some(data) => {
                    if verify_value(id, &data) {
                        verified += 1;
                    } else {
                        corruption += 1;
                    }
                }
                None => {
                    data_loss += 1;
                }
            }
        }
    }
    let read_all_time = read_start.elapsed();

    // -- Scan phase: prefix scan a subset --
    let scan_start = Instant::now();
    let mut txn = store.begin(true).unwrap();
    let cf = txn.cf(CF).unwrap();
    let scan_count: usize = txn.scan_prefix(&cf, b"d:rec:").unwrap().count();
    assert_eq!(scan_count, TOTAL_RECORDS, "scan returned wrong count");
    let scan_time = scan_start.elapsed();

    BenchResult {
        name: name.to_string(),
        write_time,
        read_all_time,
        scan_time,
        verified,
        data_loss,
        corruption,
        total_bytes,
    }
}

// ---------------------------------------------------------------------------
// Concurrent read/write stress test
// ---------------------------------------------------------------------------

fn stress_concurrent<S: Store + Sync>(store: &S, name: &str) {
    println!("--- {name}: concurrent stress test ---");
    println!();
    println!(
        "  {READER_THREADS} reader threads x {CONCURRENT_READ_ITERATIONS} reads + 1 writer thread"
    );

    store.create_cf("stress").unwrap();

    // Pre-populate some data
    let pre_pop = 10_000;
    for batch_start in (0..pre_pop).step_by(BATCH_SIZE) {
        let count = BATCH_SIZE.min(pre_pop - batch_start);
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..count)
            .map(|i| {
                let id = batch_start + i;
                (make_key(id), make_value(id))
            })
            .collect();
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let mut txn = store.begin(false).unwrap();
        let cf = txn.cf("stress").unwrap();
        txn.put_batch(&cf, &refs).unwrap();
        txn.commit().unwrap();
    }

    let barrier = Arc::new(Barrier::new(READER_THREADS + 1)); // readers + writer
    let read_errors = Arc::new(AtomicU64::new(0));
    let read_successes = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));
    let corruption_count = Arc::new(AtomicU64::new(0));

    std::thread::scope(|s| {
        // Spawn reader threads
        for thread_id in 0..READER_THREADS {
            let barrier = barrier.clone();
            let read_errors = read_errors.clone();
            let read_successes = read_successes.clone();
            let corruption_count = corruption_count.clone();

            s.spawn(move || {
                let mut rng = rand::thread_rng();
                barrier.wait();

                for _ in 0..CONCURRENT_READ_ITERATIONS {
                    let id = rng.gen_range(0..pre_pop);
                    let key = make_key(id);

                    match store.begin(true) {
                        Ok(mut txn) => match txn.cf("stress") {
                            Ok(cf) => match txn.get(&cf, &key) {
                                Ok(Some(data)) => {
                                    if verify_value(id, &data) {
                                        read_successes.fetch_add(1, Ordering::Relaxed);
                                    } else {
                                        corruption_count.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Ok(None) => {
                                    read_errors.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    read_errors.fetch_add(1, Ordering::Relaxed);
                                }
                            },
                            Err(_) => {
                                read_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        },
                        Err(_) => {
                            read_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                let _ = thread_id; // used for spawn identity
            });
        }

        // Writer thread: overwrite existing records with new values
        {
            let barrier = barrier.clone();
            let write_count = write_count.clone();

            s.spawn(move || {
                let mut rng = rand::thread_rng();
                barrier.wait();

                // Write while readers are running
                for _ in 0..1_000 {
                    let id = rng.gen_range(0..pre_pop);
                    let key = make_key(id);
                    let value = make_value(id);

                    let mut txn = store.begin(false).unwrap();
                    let cf = txn.cf("stress").unwrap();
                    txn.put(&cf, &key, &value).unwrap();
                    txn.commit().unwrap();
                    write_count.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });

    let successes = read_successes.load(Ordering::Relaxed);
    let errors = read_errors.load(Ordering::Relaxed);
    let writes = write_count.load(Ordering::Relaxed);
    let corruptions = corruption_count.load(Ordering::Relaxed);

    println!("  reads:       {successes} ok, {errors} miss/err");
    println!("  writes:      {writes}");
    println!("  corruption:  {corruptions}");
    println!();

    assert_eq!(corruptions, 0, "data corruption detected!");
}

// ---------------------------------------------------------------------------
// Rollback integrity test
// ---------------------------------------------------------------------------

fn test_rollback_integrity<S: Store>(store: &S, name: &str) {
    println!("--- {name}: rollback integrity ---");

    store.create_cf("rollback").unwrap();

    // Write some baseline data
    let mut txn = store.begin(false).unwrap();
    let cf = txn.cf("rollback").unwrap();
    txn.put(&cf, b"key1", b"original").unwrap();
    txn.put(&cf, b"key2", b"original").unwrap();
    txn.commit().unwrap();

    // Start a write transaction, modify data, then rollback
    let mut txn = store.begin(false).unwrap();
    let cf = txn.cf("rollback").unwrap();
    txn.put(&cf, b"key1", b"modified").unwrap();
    txn.delete(&cf, b"key2").unwrap();
    txn.put(&cf, b"key3", b"new").unwrap();
    txn.rollback().unwrap();

    // Verify nothing changed
    let mut txn = store.begin(true).unwrap();
    let cf = txn.cf("rollback").unwrap();
    assert_eq!(&*txn.get(&cf, b"key1").unwrap().unwrap(), b"original");
    assert_eq!(&*txn.get(&cf, b"key2").unwrap().unwrap(), b"original");
    assert!(txn.get(&cf, b"key3").unwrap().is_none());

    println!("  PASSED: rollback preserved original state");
    println!();
}

// ---------------------------------------------------------------------------
// Snapshot isolation test
// ---------------------------------------------------------------------------

/// Snapshot isolation test — only valid for stores that snapshot on begin().
/// MemoryStore guarantees this. RocksDB optimistic transactions do not snapshot
/// read-only transactions by default, so this test is MemoryStore-specific.
fn test_snapshot_isolation(store: &MemoryStore, name: &str) {
    println!("--- {name}: snapshot isolation ---");

    store.create_cf("isolation").unwrap();

    // Write initial data
    let mut txn = store.begin(false).unwrap();
    let cf = txn.cf("isolation").unwrap();
    txn.put(&cf, b"key1", b"v1").unwrap();
    txn.commit().unwrap();

    // Reader takes a snapshot
    let mut reader = store.begin(true).unwrap();
    let cf = reader.cf("isolation").unwrap();
    assert_eq!(&*reader.get(&cf, b"key1").unwrap().unwrap(), b"v1");

    // Writer modifies data after reader started
    let mut writer = store.begin(false).unwrap();
    let wcf = writer.cf("isolation").unwrap();
    writer.put(&wcf, b"key1", b"v2").unwrap();
    writer.put(&wcf, b"key2", b"new").unwrap();
    writer.commit().unwrap();

    // Reader should still see old data (snapshot isolation)
    assert_eq!(&*reader.get(&cf, b"key1").unwrap().unwrap(), b"v1");
    assert!(reader.get(&cf, b"key2").unwrap().is_none());

    // New reader should see new data
    let mut reader2 = store.begin(true).unwrap();
    let cf2 = reader2.cf("isolation").unwrap();
    assert_eq!(&*reader2.get(&cf2, b"key1").unwrap().unwrap(), b"v2");
    assert_eq!(&*reader2.get(&cf2, b"key2").unwrap().unwrap(), b"new");

    println!("  PASSED: snapshot isolation verified");
    println!();
}

// ---------------------------------------------------------------------------
// Delete range integrity test
// ---------------------------------------------------------------------------

fn test_delete_range_integrity<S: Store>(store: &S, name: &str) {
    println!("--- {name}: delete_range integrity ---");

    store.create_cf("delrange").unwrap();

    // Write 1000 records
    let count = 1_000;
    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        (0..count).map(|i| (make_key(i), make_value(i))).collect();
    let refs: Vec<(&[u8], &[u8])> = entries
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    let mut txn = store.begin(false).unwrap();
    let cf = txn.cf("delrange").unwrap();
    txn.put_batch(&cf, &refs).unwrap();
    txn.commit().unwrap();

    // Delete range [200, 800)
    let from = make_key(200);
    let to = make_key(800);
    store
        .delete_range("delrange", from.clone()..to.clone())
        .unwrap();

    // Verify: 0-199 present, 200-799 gone, 800-999 present
    let mut txn = store.begin(true).unwrap();
    let cf = txn.cf("delrange").unwrap();
    let mut present = 0;
    let mut absent = 0;
    for i in 0..count {
        let key = make_key(i);
        match txn.get(&cf, &key).unwrap() {
            Some(data) => {
                assert!(i < 200 || i >= 800, "record {i} should have been deleted");
                assert!(verify_value(i, &data), "corruption at record {i}");
                present += 1;
            }
            None => {
                assert!((200..800).contains(&i), "record {i} should still exist");
                absent += 1;
            }
        }
    }

    println!("  PASSED: {present} present, {absent} deleted (expected 400/600)");
    println!();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    println!("=== Slate Store Benchmark ===");
    println!();
    println!(
        "  {TOTAL_RECORDS} records, ~{} KB each, {} KB batches",
        VALUE_SIZE / 1024,
        BATCH_SIZE * VALUE_SIZE / 1024,
    );
    println!();

    // -- MemoryStore --
    println!("============================================================");
    println!("  MemoryStore");
    println!("============================================================");
    println!();

    let mem_store = MemoryStore::new();
    let mem_result = bench_store(&mem_store, "MemoryStore");
    mem_result.print();

    let mem_store2 = MemoryStore::new();
    test_rollback_integrity(&mem_store2, "MemoryStore");
    test_snapshot_isolation(&mem_store2, "MemoryStore");
    test_delete_range_integrity(&mem_store2, "MemoryStore");
    stress_concurrent(&mem_store2, "MemoryStore");

    // -- RocksStore --
    println!("============================================================");
    println!("  RocksStore");
    println!("============================================================");
    println!();

    let dir = tempfile::tempdir().unwrap();
    let rocks_store = RocksStore::open(dir.path()).unwrap();
    let rocks_result = bench_store(&rocks_store, "RocksStore");
    rocks_result.print();

    let dir2 = tempfile::tempdir().unwrap();
    let rocks_store2 = RocksStore::open(dir2.path()).unwrap();
    test_rollback_integrity(&rocks_store2, "RocksStore");
    // Note: snapshot isolation test skipped for RocksDB — optimistic transactions
    // don't snapshot read-only transactions by default.
    test_delete_range_integrity(&rocks_store2, "RocksStore");
    stress_concurrent(&rocks_store2, "RocksStore");

    // -- RedbStore --
    println!("============================================================");
    println!("  RedbStore");
    println!("============================================================");
    println!();

    let redb_dir = tempfile::tempdir().unwrap();
    let redb_store = RedbStore::open(&redb_dir.path().join("bench.redb")).unwrap();
    let redb_result = bench_store(&redb_store, "RedbStore");
    redb_result.print();

    let redb_dir2 = tempfile::tempdir().unwrap();
    let redb_store2 = RedbStore::open(&redb_dir2.path().join("bench.redb")).unwrap();
    test_rollback_integrity(&redb_store2, "RedbStore");
    test_delete_range_integrity(&redb_store2, "RedbStore");
    // Note: concurrent stress test skipped for redb — single writer means
    // reader threads would block while writer holds the write transaction.

    // -- Comparison --
    println!("============================================================");
    println!("  Comparison");
    println!("============================================================");
    println!();
    println!(
        "  {:>12} {:>10} {:>10} {:>10}",
        "", "memory", "rocks", "redb"
    );
    println!(
        "  {:>12} {:>9.0}ms {:>9.0}ms {:>9.0}ms",
        "write",
        mem_result.write_time.as_secs_f64() * 1000.0,
        rocks_result.write_time.as_secs_f64() * 1000.0,
        redb_result.write_time.as_secs_f64() * 1000.0,
    );
    println!(
        "  {:>12} {:>9.0}ms {:>9.0}ms {:>9.0}ms",
        "read all",
        mem_result.read_all_time.as_secs_f64() * 1000.0,
        rocks_result.read_all_time.as_secs_f64() * 1000.0,
        redb_result.read_all_time.as_secs_f64() * 1000.0,
    );
    println!(
        "  {:>12} {:>9.0}ms {:>9.0}ms {:>9.0}ms",
        "scan",
        mem_result.scan_time.as_secs_f64() * 1000.0,
        rocks_result.scan_time.as_secs_f64() * 1000.0,
        redb_result.scan_time.as_secs_f64() * 1000.0,
    );
    println!();
    println!(
        "  memory data loss:  {}   corruption: {}",
        mem_result.data_loss, mem_result.corruption
    );
    println!(
        "  rocks data loss:   {}   corruption: {}",
        rocks_result.data_loss, rocks_result.corruption
    );
    println!(
        "  redb data loss:    {}   corruption: {}",
        redb_result.data_loss, redb_result.corruption
    );
    println!();
}
