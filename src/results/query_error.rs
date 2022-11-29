use serde::{Deserialize, Serialize};
use std::error::Error as StdError;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    pub sql_state: Option<String>,
    pub message: String,
    pub error_code: i64,
    pub error_name: String,
    pub error_type: String,
    pub error_location: Option<ErrorLocation>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ErrorLocation {
    pub line_number: u32,
    pub column_number: u32,
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)
    }
}

impl StdError for QueryError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}
