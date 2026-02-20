use bson::doc;
use rand::Rng;

const STATUSES: &[&str] = &["active", "rejected"];
const REC1: &[&str] = &["ProductA", "ProductB", "ProductC"];
const REC2: &[&str] = &["ProductX", "ProductY", "ProductZ"];
const REC3: &[&str] = &["Widget1", "Widget2", "Widget3"];
const TAGS: &[&str] = &[
    "renewal_due",
    "high_value",
    "churning",
    "new_customer",
    "enterprise",
];

pub fn generate_doc(user: usize, seq: usize) -> bson::Document {
    let mut rng = rand::thread_rng();
    let mut doc = doc! {
        "name": format!("Company-{user}-{seq}"),
        "status": STATUSES[rng.gen_range(0..STATUSES.len())],
        "contacts_count": rng.gen_range(0_i64..100),
        "product_recommendation1": REC1[rng.gen_range(0..REC1.len())],
        "product_recommendation2": REC2[rng.gen_range(0..REC2.len())],
        "product_recommendation3": REC3[rng.gen_range(0..REC3.len())],
    };

    // Tags — 2-4 random tags per record
    let tag_count = rng.gen_range(2..=4);
    let tags: Vec<&str> = (0..tag_count)
        .map(|_| TAGS[rng.gen_range(0..TAGS.len())])
        .collect();
    doc.insert("tags", tags);

    // Nullable fields — omitted to represent null
    if rng.gen_ratio(7, 10) {
        let epoch_secs = rng.gen_range(1_700_000_000_i64..1_740_000_000);
        doc.insert(
            "last_contacted_at",
            bson::Bson::DateTime(bson::DateTime::from_millis(epoch_secs * 1000)),
        );
    }

    if rng.gen_bool(0.5) {
        doc.insert("notes", format!("Note for {user}-{seq}"));
    }

    doc
}

pub fn generate_record_id(user: usize, seq: usize) -> String {
    format!("user{user}-{seq}")
}

/// Generate a batch of documents with `_id` included (for insert_many).
pub fn generate_batch_docs(user: usize, start: usize, count: usize) -> Vec<bson::Document> {
    (start..start + count)
        .map(|seq| {
            let mut doc = generate_doc(user, seq);
            doc.insert("_id", generate_record_id(user, seq));
            doc
        })
        .collect()
}
