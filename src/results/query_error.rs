use serde::{Deserialize, Serialize};

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
