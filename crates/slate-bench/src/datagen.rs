use std::collections::HashMap;

use rand::Rng;
use slate_store::{Record, Value};

const STATUSES: &[&str] = &["active", "rejected"];
const REC1: &[&str] = &["ProductA", "ProductB", "ProductC"];
const REC2: &[&str] = &["ProductX", "ProductY", "ProductZ"];
const REC3: &[&str] = &["Widget1", "Widget2", "Widget3"];

pub fn generate_record(user: usize, seq: usize) -> Record {
    let mut rng = rand::thread_rng();
    let mut fields = HashMap::new();

    fields.insert(
        "name".to_string(),
        Value::String(format!("Company-{user}-{seq}")),
    );
    fields.insert(
        "status".to_string(),
        Value::String(STATUSES[rng.gen_range(0..STATUSES.len())].to_string()),
    );
    fields.insert(
        "contacts_count".to_string(),
        Value::Int(rng.gen_range(0..100)),
    );
    fields.insert(
        "product_recommendation1".to_string(),
        Value::String(REC1[rng.gen_range(0..REC1.len())].to_string()),
    );
    fields.insert(
        "product_recommendation2".to_string(),
        Value::String(REC2[rng.gen_range(0..REC2.len())].to_string()),
    );
    fields.insert(
        "product_recommendation3".to_string(),
        Value::String(REC3[rng.gen_range(0..REC3.len())].to_string()),
    );

    // Nullable fields — omitted from the HashMap to represent null
    // last_contacted_at: ~70% present, ~30% null
    if rng.gen_ratio(7, 10) {
        fields.insert(
            "last_contacted_at".to_string(),
            Value::Date(rng.gen_range(1_700_000_000..1_740_000_000)),
        );
    }

    // notes: ~50% present, ~50% null
    if rng.gen_bool(0.5) {
        fields.insert(
            "notes".to_string(),
            Value::String(format!("Note for {user}-{seq}")),
        );
    }

    Record {
        id: format!("user{user}-{seq}"),
        fields,
    }
}

pub fn generate_batch(user: usize, start: usize, count: usize) -> Vec<Record> {
    (start..start + count)
        .map(|seq| generate_record(user, seq))
        .collect()
}
