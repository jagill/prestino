
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("http error")]
    HttpError(#[from] hyper::http::Error),
    #[error("hyper error")]
    HyperError(#[from] hyper::Error),
    #[error("Unexpected HTTP response code {0}")]
    StatusCodeError(u16),
    #[error("Could not parse JSON")]
    JsonParseError(#[from] serde_json::Error),
    #[error("Query error {0:?}")]
    QueryError(crate::results::QueryError),
}

impl Error {
    pub fn from_status_code(code: u16) -> Self {
        Error::StatusCodeError(code)
    }
}
