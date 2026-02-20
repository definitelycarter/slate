use http::{Method, Request, Response, StatusCode};
use serde::Deserialize;
use slate_client::ClientPool;
use slate_query::{DistinctQuery, FilterGroup, FilterNode, LogicalOp, Query};

use crate::config::ListConfig;
use crate::error::ListError;
use crate::request::{DistinctRequest, DistinctResponse, ListRequest, ListResponse};

pub struct ListHttp {
    config: ListConfig,
    pool: ClientPool,
}

impl ListHttp {
    pub fn new(config: ListConfig, pool: ClientPool) -> Self {
        Self { config, pool }
    }

    pub fn handle(&self, req: Request<Vec<u8>>) -> Response<Vec<u8>> {
        let path = req.uri().path();
        let method = req.method();

        match (method, path.trim_end_matches('/')) {
            (&Method::GET, "/config") => self.get_config(),
            (&Method::POST, "/query") => self.get_data(&req),
            (&Method::POST, "/query/distinct") => self.get_distinct(&req),
            (&Method::POST, "/data") => self.post_records(&req),
            (&Method::PUT, "/data") => self.put_records(&req),
            (&Method::PATCH, "/data") => self.patch_records(&req),
            (&Method::DELETE, "/data") => self.delete_records(&req),
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
        let list_request: ListRequest = if req.body().is_empty() {
            ListRequest::default()
        } else {
            match serde_json::from_slice(req.body()) {
                Ok(r) => r,
                Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
            }
        };

        match self.get_list_data(&list_request) {
            Ok(response) => match serde_json::to_vec(&response) {
                Ok(body) => json_response(StatusCode::OK, body),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(e.status_code(), &e.to_string()),
        }
    }

    fn get_distinct(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let distinct_request: DistinctRequest = match serde_json::from_slice(req.body()) {
            Ok(r) => r,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };

        match self.get_distinct_data(&distinct_request) {
            Ok(response) => match serde_json::to_vec(&response) {
                Ok(body) => json_response(StatusCode::OK, body),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(e.status_code(), &e.to_string()),
        }
    }

    fn get_list_data(&self, request: &ListRequest) -> Result<ListResponse, ListError> {
        let collection = &self.config.collection;

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

    fn get_distinct_data(&self, request: &DistinctRequest) -> Result<DistinctResponse, ListError> {
        let collection = &self.config.collection;

        // Merge list default filters with user filters
        let merged = merge_filters(self.config.filters.as_ref(), request.filters.as_ref());

        let query = DistinctQuery {
            field: request.field.clone(),
            filter: merged,
            sort: request.sort,
            skip: request.skip,
            take: request.take,
        };
        let values = self.pool.get()?.distinct(collection, &query)?;

        Ok(DistinctResponse { values })
    }

    // ── Data write routes ───────────────────────────────────────

    fn post_records(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let docs: Vec<bson::Document> = match serde_json::from_slice(req.body()) {
            Ok(b) => b,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };
        let collection = &self.config.collection;
        match self
            .pool
            .get()
            .and_then(|mut c| Ok(c.insert_many(collection, docs)?))
        {
            Ok(results) => {
                match serde_json::to_vec(&serde_json::json!({ "inserted": results.len() })) {
                    Ok(b) => json_response(StatusCode::OK, b),
                    Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
                }
            }
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    fn put_records(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let docs: Vec<bson::Document> = match serde_json::from_slice(req.body()) {
            Ok(b) => b,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };
        let collection = &self.config.collection;
        match self
            .pool
            .get()
            .and_then(|mut c| Ok(c.upsert_many(collection, docs)?))
        {
            Ok(result) => match serde_json::to_vec(&result) {
                Ok(b) => json_response(StatusCode::OK, b),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    fn patch_records(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let docs: Vec<bson::Document> = match serde_json::from_slice(req.body()) {
            Ok(b) => b,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };
        let collection = &self.config.collection;
        match self
            .pool
            .get()
            .and_then(|mut c| Ok(c.merge_many(collection, docs)?))
        {
            Ok(result) => match serde_json::to_vec(&result) {
                Ok(b) => json_response(StatusCode::OK, b),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    fn delete_records(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let body: DeleteBody = match serde_json::from_slice(req.body()) {
            Ok(b) => b,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };
        let collection = &self.config.collection;
        match self
            .pool
            .get()
            .and_then(|mut c| Ok(c.delete_many(collection, &body.filter)?))
        {
            Ok(result) => match serde_json::to_vec(&result) {
                Ok(b) => json_response(StatusCode::OK, b),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }
}

#[derive(Deserialize)]
struct DeleteBody {
    filter: FilterGroup,
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
