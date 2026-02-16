use std::fmt;

#[derive(Debug)]
pub enum ListError {
    Client(slate_client::ClientError),
    NotFound(String),
    Loader(String),
}

impl fmt::Display for ListError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ListError::Client(e) => write!(f, "client error: {e}"),
            ListError::NotFound(id) => write!(f, "list not found: {id}"),
            ListError::Loader(msg) => write!(f, "loader error: {msg}"),
        }
    }
}

impl std::error::Error for ListError {}

impl From<slate_client::ClientError> for ListError {
    fn from(e: slate_client::ClientError) -> Self {
        ListError::Client(e)
    }
}
