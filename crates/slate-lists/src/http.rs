use std::collections::HashMap;

use http::{Method, Request, Response, StatusCode};
use slate_client::ClientPool;
use slate_query::{DistinctQuery, FilterGroup, FilterNode, LogicalOp, Query};

use crate::config::ListConfig;
use crate::error::ListError;
use crate::loader::Loader;
use crate::request::{DistinctRequest, DistinctResponse, ListRequest, ListResponse};

pub struct ListHttp<L: Loader> {
    config: ListConfig,
    pool: ClientPool,
    loader: L,
}

impl<L: Loader> ListHttp<L> {
    const BATCH_SIZE: usize = 1000;

    pub fn new(config: ListConfig, pool: ClientPool, loader: L) -> Self {
        Self {
            config,
            pool,
            loader,
        }
    }

    pub fn loader(&self) -> &L {
        &self.loader
    }

    pub fn handle(&self, req: Request<Vec<u8>>) -> Response<Vec<u8>> {
        let path = req.uri().path();
        let method = req.method();

        match (method, path.trim_end_matches('/')) {
            (&Method::GET, "/config") => self.get_config(),
            (&Method::POST, "/data") => self.get_data(&req),
            (&Method::POST, "/distinct") => self.get_distinct(&req),
            _ => json_response(StatusCode::NOT_FOUND, r#"{"error":"not found"}"#),
        }
    }

    fn get_config(&self) -> Response<Vec<u8>> {
        match serde_json::to_vec(&self.config) {
            Ok(body) => json_response(StatusCode::OK, body),
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    fn get_data(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let metadata = extract_metadata(req);

        let list_request: ListRequest = if req.body().is_empty() {
            ListRequest::default()
        } else {
            match serde_json::from_slice(req.body()) {
                Ok(r) => r,
                Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
            }
        };

        match self.get_list_data(&list_request, &metadata) {
            Ok(response) => match serde_json::to_vec(&response) {
                Ok(body) => json_response(StatusCode::OK, body),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(e.status_code(), &e.to_string()),
        }
    }

    fn get_distinct(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let metadata = extract_metadata(req);

        let distinct_request: DistinctRequest = match serde_json::from_slice(req.body()) {
            Ok(r) => r,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };

        match self.get_distinct_data(&distinct_request, &metadata) {
            Ok(response) => match serde_json::to_vec(&response) {
                Ok(body) => json_response(StatusCode::OK, body),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(e.status_code(), &e.to_string()),
        }
    }

    fn get_list_data(
        &self,
        request: &ListRequest,
        metadata: &HashMap<String, String>,
    ) -> Result<ListResponse, ListError> {
        let collection = &self.config.collection;

        // Load data if collection is empty
        let count = self.pool.get()?.count(collection, None)?;
        if count == 0 {
            let docs_iter = self.loader.load(collection, metadata)?;
            self.batch_insert(collection, docs_iter)?;
        }

        // Merge list default filters with user filters
        let merged = merge_filters(self.config.filters.as_ref(), request.filters.as_ref());

        // Build projection from column fields
        let columns: Vec<String> = self
            .config
            .columns
            .iter()
            .map(|c| c.field.clone())
            .collect();

        // Get total count with merged filters (before skip/take)
        let total = self.pool.get()?.count(collection, merged.as_ref())?;

        // Execute query
        let query = Query {
            filter: merged,
            sort: request.sort.clone(),
            skip: request.skip,
            take: request.take,
            columns: Some(columns),
        };
        let records = self.pool.get()?.find(collection, &query)?;

        Ok(ListResponse { records, total })
    }

    fn get_distinct_data(
        &self,
        request: &DistinctRequest,
        metadata: &HashMap<String, String>,
    ) -> Result<DistinctResponse, ListError> {
        let collection = &self.config.collection;

        // Load data if collection is empty
        let count = self.pool.get()?.count(collection, None)?;
        if count == 0 {
            let docs_iter = self.loader.load(collection, metadata)?;
            self.batch_insert(collection, docs_iter)?;
        }

        // Merge list default filters with user filters
        let merged = merge_filters(self.config.filters.as_ref(), request.filters.as_ref());

        let query = DistinctQuery {
            field: request.field.clone(),
            filter: merged,
            sort: request.sort,
        };
        let values = self.pool.get()?.distinct(collection, &query)?;

        Ok(DistinctResponse { values })
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

fn extract_metadata<T>(req: &Request<T>) -> HashMap<String, String> {
    req.headers()
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str();
            if let Some(key) = name.strip_prefix("x-meta-") {
                value
                    .to_str()
                    .ok()
                    .map(|v| (key.to_string(), v.to_string()))
            } else {
                None
            }
        })
        .collect()
}

fn json_response(status: StatusCode, body: impl Into<Vec<u8>>) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(body.into())
        .unwrap()
}

fn error_response(status: StatusCode, message: &str) -> Response<Vec<u8>> {
    let body = serde_json::json!({ "error": message });
    json_response(status, body.to_string().into_bytes())
}
