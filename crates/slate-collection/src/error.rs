use std::fmt;

#[derive(Debug)]
pub enum CollectionHttpError {
    Client(slate_client::ClientError),
}

impl fmt::Display for CollectionHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CollectionHttpError::Client(e) => write!(f, "client error: {e}"),
        }
    }
}

impl std::error::Error for CollectionHttpError {}

impl CollectionHttpError {
    pub fn status_code(&self) -> http::StatusCode {
        match self {
            CollectionHttpError::Client(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<slate_client::ClientError> for CollectionHttpError {
    fn from(e: slate_client::ClientError) -> Self {
        CollectionHttpError::Client(e)
    }
}
