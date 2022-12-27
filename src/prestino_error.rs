#[derive(thiserror::Error, Debug)]
pub enum PrestinoError {
    #[error("Http error")]
    HttpError(#[from] reqwest::Error),
    #[error("Unexpected HTTP response code {0}: {1}")]
    StatusCodeError(u16, String),
    #[error("Could not parse JSON")]
    JsonParseError(#[from] serde_json::Error),
    #[error("Error in query")]
    QueryError(#[from] crate::results::QueryError),
    #[error("Query {0} already finished")]
    QueryFinishedError(String),
    #[error("Header names and values must only contain visible ASCII characters")]
    HeaderParseError,
}

impl PrestinoError {
    pub fn from_status_code(code: u16, message: String) -> Self {
        PrestinoError::StatusCodeError(code, message)
    }
}

impl From<reqwest::header::ToStrError> for PrestinoError {
    fn from(_err: reqwest::header::ToStrError) -> Self {
        PrestinoError::HeaderParseError
    }
}

impl From<reqwest::header::InvalidHeaderName> for PrestinoError {
    fn from(_err: reqwest::header::InvalidHeaderName) -> Self {
        PrestinoError::HeaderParseError
    }
}

impl From<reqwest::header::InvalidHeaderValue> for PrestinoError {
    fn from(_err: reqwest::header::InvalidHeaderValue) -> Self {
        PrestinoError::HeaderParseError
    }
}
