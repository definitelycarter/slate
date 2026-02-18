use std::collections::HashMap;

use slate_client::ClientPool;
use slate_query::{FilterGroup, FilterNode, LogicalOp, Query};

use crate::config::ListConfig;
use crate::error::ListError;
use crate::loader::Loader;
use crate::request::{ListRequest, ListResponse};

pub struct ListService<L: Loader> {
    pool: ClientPool,
    loader: L,
}

impl<L: Loader> ListService<L> {
    const BATCH_SIZE: usize = 1000;

    pub fn new(pool: ClientPool, loader: L) -> Self {
        Self { pool, loader }
    }

    pub fn loader(&self) -> &L {
        &self.loader
    }

    fn batch_insert(
        &self,
        collection: &str,
        docs: Box<dyn Iterator<Item = Result<bson::Document, ListError>> + '_>,
    ) -> Result<(), ListError> {
        let mut batch = Vec::with_capacity(Self::BATCH_SIZE);
        for doc_result in docs {
            batch.push(doc_result?);
            if batch.len() >= Self::BATCH_SIZE {
                self.pool
                    .get()?
                    .insert_many(collection, std::mem::take(&mut batch))?;
                batch.reserve(Self::BATCH_SIZE);
            }
        }
        if !batch.is_empty() {
            self.pool.get()?.insert_many(collection, batch)?;
        }
        Ok(())
    }

    pub fn get_list_data(
        &self,
        config: &ListConfig,
        key: &str,
        request: &ListRequest,
        metadata: &HashMap<String, String>,
    ) -> Result<ListResponse, ListError> {
        // 1. Check if data exists for this key
        let count = self.pool.get()?.count(&config.collection, None)?;
        if count == 0 {
            let docs_iter = self.loader.load(&config.collection, key, metadata)?;
            self.batch_insert(&config.collection, docs_iter)?;
        }

        // 2. Merge list default filters with user filters
        let merged = merge_filters(config.filters.as_ref(), request.filters.as_ref());

        // 3. Build projection from column fields
        let columns: Vec<String> = config.columns.iter().map(|c| c.field.clone()).collect();

        // 4. Get total count with merged filters (before skip/take)
        let total = self
            .pool
            .get()?
            .count(&config.collection, merged.as_ref())?;

        // 5. Execute query
        let query = Query {
            filter: merged,
            sort: request.sort.clone(),
            skip: request.skip,
            take: request.take,
            columns: Some(columns),
        };
        let records = self.pool.get()?.find(&config.collection, &query)?;

        Ok(ListResponse { records, total })
    }
}

/// Merge two optional filter groups under a top-level AND.
///
/// - Both None → None
/// - One present → that one
/// - Both present → AND(list_filters, user_filters)
pub fn merge_filters(
    list_filters: Option<&FilterGroup>,
    user_filters: Option<&FilterGroup>,
) -> Option<FilterGroup> {
    match (list_filters, user_filters) {
        (None, None) => None,
        (Some(f), None) | (None, Some(f)) => Some(f.clone()),
        (Some(list), Some(user)) => Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![
                FilterNode::Group(list.clone()),
                FilterNode::Group(user.clone()),
            ],
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slate_query::*;

    fn eq_filter(field: &str, value: &str) -> FilterGroup {
        FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: field.into(),
                operator: Operator::Eq,
                value: QueryValue::String(value.into()),
            })],
        }
    }

    #[test]
    fn merge_both_none() {
        assert!(merge_filters(None, None).is_none());
    }

    #[test]
    fn merge_list_only() {
        let list = eq_filter("status", "active");
        let merged = merge_filters(Some(&list), None).unwrap();
        assert_eq!(merged, list);
    }

    #[test]
    fn merge_user_only() {
        let user = eq_filter("revenue", "50000");
        let merged = merge_filters(None, Some(&user)).unwrap();
        assert_eq!(merged, user);
    }

    #[test]
    fn merge_both() {
        let list = eq_filter("status", "active");
        let user = eq_filter("revenue", "50000");
        let merged = merge_filters(Some(&list), Some(&user)).unwrap();

        assert_eq!(merged.logical, LogicalOp::And);
        assert_eq!(merged.children.len(), 2);
        match &merged.children[0] {
            FilterNode::Group(g) => assert_eq!(g, &list),
            _ => panic!("expected group"),
        }
        match &merged.children[1] {
            FilterNode::Group(g) => assert_eq!(g, &user),
            _ => panic!("expected group"),
        }
    }
}
