use super::{Column, QueryError};
use crate::structs::misc::opt_uri_serde;
use crate::structs::RawQueryResults;
use http::uri::Uri;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryResults<T> {
    pub id: String,
    #[serde(with = "http_serde::uri")]
    pub info_uri: Uri,
    #[serde(default, with = "opt_uri_serde")]
    pub next_uri: Option<Uri>,
    pub columns: Vec<Column>,
    pub data: Vec<T>,
    pub error: Option<QueryError>,
}

// #[derive(Debug)]
// pub struct TypedColumn {
//     name: String,
//     type_name: String,
//     r#type: DataType,
// }

impl<T> QueryResults<T>
where
    T: DeserializeOwned + Debug,
{
    pub fn rows(&self) -> &[T] {
        &self.data
    }

    pub fn rows_mut(&mut self) -> &mut Vec<T> {
        &mut self.data
    }

    pub fn rows_owned(self) -> Vec<T> {
        self.data
    }
}

impl<T> From<RawQueryResults> for QueryResults<T>
where
    T: DeserializeOwned + Debug,
{
    fn from(raw: RawQueryResults) -> Self {
        let RawQueryResults {
            id,
            info_uri,
            next_uri,
            columns,
            data,
            error,
        } = raw;

        // let typed_columns: Vec<_> = columns.into_iter().map(|col| {
        //     let RawColumn {
        //         name, type_name
        //     } = col;
        //     let col_type = crate::parse_data_type::parse_data_type(&type_name).unwrap();
        //     TypedColumn {
        //         name,
        //         type_name,
        //         r#type: col_type,
        //     }
        // }).collect();

        let typed_data: Vec<T> = data
            .into_iter()
            .map(|val| serde_json::from_value(val).unwrap())
            .collect();

        Self {
            id,
            info_uri,
            next_uri,
            columns,
            data: typed_data,
            error,
        }
    }
}
