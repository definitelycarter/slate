use rand::Rng;
use slate_db::{CellWrite, Value};

const STATUSES: &[&str] = &["active", "rejected"];
const REC1: &[&str] = &["ProductA", "ProductB", "ProductC"];
const REC2: &[&str] = &["ProductX", "ProductY", "ProductZ"];
const REC3: &[&str] = &["Widget1", "Widget2", "Widget3"];

pub fn generate_cells(user: usize, seq: usize, ts: i64) -> Vec<CellWrite> {
    let mut rng = rand::thread_rng();
    let mut cells = vec![
        CellWrite {
            column: "name".into(),
            value: Value::String(format!("Company-{user}-{seq}")),
            timestamp: ts,
        },
        CellWrite {
            column: "status".into(),
            value: Value::String(STATUSES[rng.gen_range(0..STATUSES.len())].to_string()),
            timestamp: ts,
        },
        CellWrite {
            column: "contacts_count".into(),
            value: Value::Int(rng.gen_range(0..100)),
            timestamp: ts,
        },
        CellWrite {
            column: "product_recommendation1".into(),
            value: Value::String(REC1[rng.gen_range(0..REC1.len())].to_string()),
            timestamp: ts,
        },
        CellWrite {
            column: "product_recommendation2".into(),
            value: Value::String(REC2[rng.gen_range(0..REC2.len())].to_string()),
            timestamp: ts,
        },
        CellWrite {
            column: "product_recommendation3".into(),
            value: Value::String(REC3[rng.gen_range(0..REC3.len())].to_string()),
            timestamp: ts,
        },
    ];

    // Nullable fields — omitted to represent null
    if rng.gen_ratio(7, 10) {
        cells.push(CellWrite {
            column: "last_contacted_at".into(),
            value: Value::Date(rng.gen_range(1_700_000_000..1_740_000_000)),
            timestamp: ts,
        });
    }

    if rng.gen_bool(0.5) {
        cells.push(CellWrite {
            column: "notes".into(),
            value: Value::String(format!("Note for {user}-{seq}")),
            timestamp: ts,
        });
    }

    cells
}

pub fn generate_record_id(user: usize, seq: usize) -> String {
    format!("user{user}-{seq}")
}

/// Generate a batch of (record_id, cells) tuples.
pub fn generate_batch(
    user: usize,
    start: usize,
    count: usize,
    ts: i64,
) -> Vec<(String, Vec<CellWrite>)> {
    (start..start + count)
        .map(|seq| (generate_record_id(user, seq), generate_cells(user, seq, ts)))
        .collect()
}
