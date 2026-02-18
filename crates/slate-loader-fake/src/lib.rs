use std::collections::HashMap;

use bson::doc;
use slate_lists::{ListError, Loader};

pub struct FakeLoader;

impl Loader for FakeLoader {
    fn load(
        &self,
        _collection: &str,
        _metadata: &HashMap<String, String>,
    ) -> Result<Box<dyn Iterator<Item = Result<bson::Document, ListError>> + '_>, ListError> {
        Ok(Box::new((0..100).map(|i| Ok(generate_document(i)))))
    }
}

fn generate_document(i: u32) -> bson::Document {
    let first_names = [
        "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank", "Iris", "Jack", "Karen",
        "Leo", "Mona", "Nate", "Olivia", "Paul", "Quinn", "Rita", "Sam", "Tina",
    ];
    let last_names = [
        "Johnson",
        "Smith",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Anderson",
        "Taylor",
        "Thomas",
        "Moore",
        "Jackson",
        "White",
        "Harris",
        "Clark",
        "Lewis",
        "Young",
    ];
    let statuses = ["active", "inactive", "pending", "suspended"];
    let products = [
        "Enterprise Suite",
        "Starter Plan",
        "Growth Tier",
        "Platform Pro",
        "Developer Tools",
    ];
    let priorities = [("gold", 3), ("silver", 2), ("bronze", 1)];

    let recommendations: &[&[&str]] = &[
        &["Upgrade Plan", "Add SSO", "Enable Audit Logs"],
        &["Increase Seats", "Add Analytics", "Custom Domain"],
        &["Onboard Team", "API Access", "Priority Support"],
        &["Renew Early", "Bundle Discount", "Refer Partner"],
        &["Data Export", "Compliance Pack", "Dedicated Rep"],
    ];

    let trigger_pool = [
        "renewal_due",
        "usage_spike",
        "support_ticket",
        "contract_expiring",
        "onboarding_stalled",
        "expansion_signal",
        "churn_risk",
        "payment_overdue",
        "feature_request",
        "nps_drop",
    ];

    let first = first_names[i as usize % first_names.len()];
    let last = last_names[i as usize % last_names.len()];
    let status = statuses[i as usize % statuses.len()];
    let product = products[i as usize % products.len()];
    let (priority, priority_rank) = priorities[i as usize % priorities.len()];
    let recs = recommendations[i as usize % recommendations.len()];

    // Pick 1-4 triggers based on index
    let trigger_count = (i as usize % 4) + 1;
    let trigger_start = i as usize % trigger_pool.len();
    let triggers: Vec<&str> = (0..trigger_count)
        .map(|j| trigger_pool[(trigger_start + j) % trigger_pool.len()])
        .collect();

    let value = ((i as f64 + 1.0) * 1234.56).round();

    doc! {
        "_id": format!("user-{:03}", i),
        "name": format!("{first} {last}"),
        "status": status,
        "product": product,
        "recommendation_1": recs[0],
        "recommendation_2": recs[1],
        "recommendation_3": recs[2],
        "value": value,
        "priority": priority,
        "priority_rank": priority_rank,
        "triggers": {
            "items": triggers.iter().map(|s| bson::Bson::String(s.to_string())).collect::<Vec<_>>(),
            "total": trigger_count as i32,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_100_documents() {
        let loader = FakeLoader;
        let docs: Vec<_> = loader
            .load("test", &HashMap::new())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(docs.len(), 100);
    }

    #[test]
    fn document_has_expected_fields() {
        let doc = generate_document(0);

        assert_eq!(doc.get_str("_id").unwrap(), "user-000");
        assert_eq!(doc.get_str("name").unwrap(), "Alice Johnson");
        assert_eq!(doc.get_str("status").unwrap(), "active");
        assert_eq!(doc.get_str("product").unwrap(), "Enterprise Suite");
        assert_eq!(doc.get_str("recommendation_1").unwrap(), "Upgrade Plan");
        assert_eq!(doc.get_str("recommendation_2").unwrap(), "Add SSO");
        assert_eq!(
            doc.get_str("recommendation_3").unwrap(),
            "Enable Audit Logs"
        );
        assert!(doc.get_f64("value").is_ok());
        assert_eq!(doc.get_str("priority").unwrap(), "gold");
        assert_eq!(doc.get_i32("priority_rank").unwrap(), 3);

        let triggers = doc.get_document("triggers").unwrap();
        let items = triggers.get_array("items").unwrap();
        assert!(!items.is_empty());
        assert_eq!(triggers.get_i32("total").unwrap(), items.len() as i32);
    }

    #[test]
    fn priority_rank_matches_priority() {
        for i in 0..100 {
            let doc = generate_document(i);
            let priority = doc.get_str("priority").unwrap();
            let rank = doc.get_i32("priority_rank").unwrap();
            match priority {
                "gold" => assert_eq!(rank, 3),
                "silver" => assert_eq!(rank, 2),
                "bronze" => assert_eq!(rank, 1),
                _ => panic!("unexpected priority: {priority}"),
            }
        }
    }

    #[test]
    fn triggers_total_matches_items_len() {
        for i in 0..100 {
            let doc = generate_document(i);
            let triggers = doc.get_document("triggers").unwrap();
            let items = triggers.get_array("items").unwrap();
            let total = triggers.get_i32("total").unwrap();
            assert_eq!(total, items.len() as i32);
        }
    }
}
