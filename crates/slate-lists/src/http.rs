use std::collections::HashMap;

use http::{Method, Request, Response, StatusCode};

use crate::config::ListConfig;
use crate::loader::Loader;
use crate::request::ListRequest;
use crate::service::ListService;

pub struct ListHttp<L: Loader> {
    config: ListConfig,
    service: ListService<L>,
}

impl<L: Loader> ListHttp<L> {
    pub fn new(config: ListConfig, service: ListService<L>) -> Self {
        Self { config, service }
    }

    pub fn handle(&self, req: Request<Vec<u8>>) -> Response<Vec<u8>> {
        let path = req.uri().path();
        let method = req.method();

        match (method, path.trim_end_matches('/')) {
            (&Method::GET, "/config") => self.get_config(),
            (&Method::POST, "/data") => self.get_data(&req),
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
        let key = req
            .headers()
            .get("x-list-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let metadata = extract_metadata(req);

        let list_request: ListRequest = if req.body().is_empty() {
            ListRequest::default()
        } else {
            match serde_json::from_slice(req.body()) {
                Ok(r) => r,
                Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
            }
        };

        match self
            .service
            .get_list_data(&self.config, key, &list_request, &metadata)
        {
            Ok(response) => match serde_json::to_vec(&response) {
                Ok(body) => json_response(StatusCode::OK, body),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(e.status_code(), &e.to_string()),
        }
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
