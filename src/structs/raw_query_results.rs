use super::{Column, QueryError};
use crate::structs::misc::opt_uri_serde;
use http::uri::Uri;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Debug;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RawQueryResults {
    pub id: String,
    #[serde(with = "http_serde::uri")]
    pub info_uri: Uri,
    #[serde(default, with = "opt_uri_serde")]
    pub next_uri: Option<Uri>,
    pub columns: Vec<Column>,
    pub data: Vec<Value>,
    pub error: Option<QueryError>,
}
