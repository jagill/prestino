use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    pub sql_state: String,
    pub message: String,
    pub error_code: i64,
    pub error_name: String,
    pub error_type: String,
}
