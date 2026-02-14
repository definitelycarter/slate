use std::time::{Duration, Instant};

pub struct BenchResult {
    pub name: String,
    pub duration: Duration,
    pub record_count: usize,
}

impl BenchResult {
    pub fn print(&self) {
        let ms = self.duration.as_secs_f64() * 1000.0;
        if self.record_count > 0 {
            let per_record = ms / self.record_count as f64;
            println!(
                "  {:<50} {:>10.2}ms  ({} records, {:.4}ms/record)",
                self.name, ms, self.record_count, per_record
            );
        } else {
            println!("  {:<50} {:>10.2}ms", self.name, ms);
        }
    }
}

pub fn bench<F>(name: &str, f: F) -> BenchResult
where
    F: FnOnce() -> usize,
{
    let start = Instant::now();
    let record_count = f();
    let duration = start.elapsed();
    BenchResult {
        name: name.to_string(),
        duration,
        record_count,
    }
}
