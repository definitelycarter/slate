use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use slate_db::Datasource;

use crate::error::ApiError;
use crate::state::AppState;

pub async fn save(
    State(state): State<AppState>,
    Json(ds): Json<Datasource>,
) -> Result<(StatusCode, Json<Datasource>), ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut client = state.pool.get()?;
        client.save_datasource(&ds)?;
        Ok((StatusCode::CREATED, Json(ds)))
    })
    .await
    .unwrap()
}

pub async fn list(State(state): State<AppState>) -> Result<Json<Vec<Datasource>>, ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut client = state.pool.get()?;
        let datasources = client.list_datasources()?;
        Ok(Json(datasources))
    })
    .await
    .unwrap()
}

pub async fn get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Datasource>, ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut client = state.pool.get()?;
        match client.get_datasource(&id)? {
            Some(ds) => Ok(Json(ds)),
            None => Err(ApiError::not_found(&id)),
        }
    })
    .await
    .unwrap()
}

pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut client = state.pool.get()?;
        client.delete_datasource(&id)?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
    .unwrap()
}
