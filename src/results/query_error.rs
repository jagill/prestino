use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    sql_state: String,
    message: String,
    error_code: i64,
    error_name: String,
    error_type: String,
}
