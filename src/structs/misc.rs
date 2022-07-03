use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    sql_state: String,
    message: String,
    error_code: i64,
    error_name: String,
    error_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}
