#[derive(thiserror::Error, Debug)]
pub enum PrestinoError {
    #[error("http error")]
    HttpError(#[from] hyper::http::Error),
    #[error("hyper error")]
    HyperError(#[from] hyper::Error),
    #[error("Unexpected HTTP response code {0}")]
    StatusCodeError(u16),
    #[error("Could not parse JSON")]
    JsonParseError(#[from] serde_json::Error),
    #[error("Error in query")]
    QueryError(#[from] crate::results::QueryError),
}

impl PrestinoError {
    pub fn from_status_code(code: u16) -> Self {
        PrestinoError::StatusCodeError(code)
    }
}
