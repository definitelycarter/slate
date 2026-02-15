mod datasources;
mod health;

use axum::routing::{delete, get, post};
use axum::Router;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/v1/datasources", post(datasources::save))
        .route("/v1/datasources", get(datasources::list))
        .route("/v1/datasources/{id}", get(datasources::get))
        .route("/v1/datasources/{id}", delete(datasources::delete))
}
