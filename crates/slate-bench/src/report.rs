use std::time::{Duration, Instant};

use crate::alloc;

pub struct BenchResult {
    pub name: String,
    pub duration: Duration,
    pub record_count: usize,
    pub alloc: alloc::AllocStats,
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1_048_576 {
        format!("{:.1}MB", bytes as f64 / 1_048_576.0)
    } else {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    }
}

impl BenchResult {
    pub fn print(&self) {
        let ms = self.duration.as_secs_f64() * 1000.0;
        let mem = format!(
            "[peak: {}, allocs: {}]",
            format_bytes(self.alloc.peak_bytes),
            self.alloc.total_allocs
        );
        if self.record_count > 0 {
            let per_record = ms / self.record_count as f64;
            println!(
                "  {:<50} {:>10.2}ms  ({} records, {:.4}ms/record)  {}",
                self.name, ms, self.record_count, per_record, mem
            );
        } else {
            println!("  {:<50} {:>10.2}ms  {}", self.name, ms, mem);
        }
    }
}

pub fn bench<F>(name: &str, f: F) -> BenchResult
where
    F: FnOnce() -> usize,
{
    alloc::reset();
    let start = Instant::now();
    let record_count = f();
    let duration = start.elapsed();
    let alloc = alloc::snapshot();
    BenchResult {
        name: name.to_string(),
        duration,
        record_count,
        alloc,
    }
}
