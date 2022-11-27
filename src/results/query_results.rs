use super::{Column, QueryError, QueryStats};
use crate::utils::opt_uri_serde;
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
    #[serde(default, with = "opt_uri_serde")]
    pub partial_cancel_uri: Option<Uri>,
    pub columns: Option<Vec<Column>>,
    pub data: Option<Vec<T>>,
    pub stats: QueryStats,
    pub error: Option<QueryError>,
}

impl<T> QueryResults<T>
where
    T: DeserializeOwned + Debug,
{
    pub fn rows(&self) -> Option<&[T]> {
        self.data.as_deref()
    }

    pub fn rows_mut(&mut self) -> Option<&mut Vec<T>> {
        self.data.as_mut()
    }

    pub fn rows_owned(self) -> Option<Vec<T>> {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::{json, Value};

    use super::*;

    static BASE_RESULTS_STR: &str = r#"
        {
          "id" : "20160128_214710_00012_rk68b",
          "infoUri": "http://localhost:8080/ui/query.html?20221102_163624_00001_ipi9h",
          "partialCancelUri": "http://localhost:8080/v1/statement/executing/partialCancel/20221102_163624_00001_ipi9h/0/y38e97f41c34994651e26a9a982cbaccb231a50e0/1",
          "nextUri": "http://localhost:8080/v1/statement/executing/20221102_163624_00001_ipi9h/y38e97f41c34994651e26a9a982cbaccb231a50e0/1",
          "columns" : [
            {
              "name" : "a_int",
              "type" : "bigint",
              "typeSignature" : {
                "rawType" : "bigint",
                "typeArguments" : [ ],
                "literalArguments" : [ ],
                "arguments" : [ ]
              }
            },
            {
              "name" : "a_bool",
              "type" : "boolean",
              "typeSignature" : {
                "rawType" : "boolean",
                "typeArguments" : [ ],
                "literalArguments" : [ ],
                "arguments" : [ ]
              }
            }
          ],
          "data" : [ [ 123, true ] ],
          "stats" : {
            "state" : "FINISHED",
            "queued" : false,
            "scheduled" : false,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "cpuTimeMillis": 1,
            "wallTimeMillis": 1,
            "queuedTimeMillis": 1,
            "elapsedTimeMillis": 46985,
            "processedRows": 0,
            "processedBytes": 0,
            "physicalInputBytes": 0,
            "peakMemoryBytes": 103,
            "spilledBytes": 0
          }
        }
        "#;

    #[test]
    fn deserialize_basic_value() {
        let deserialized: QueryResults<Value> = serde_json::from_str(BASE_RESULTS_STR).unwrap();
        println!("deserialized = {:?}", deserialized);
        assert_eq!(deserialized.data.unwrap(), [json!([123, true])]);
    }

    #[test]
    fn deserialize_basic_tuple() {
        let deserialized_tuple: QueryResults<(i64, bool)> =
            serde_json::from_str(BASE_RESULTS_STR).unwrap();
        println!("deserialized = {:?}", deserialized_tuple);
        assert_eq!(deserialized_tuple.data.unwrap(), [(123, true)]);
    }

    #[test]
    fn deserialize_basic_typed_row() {
        #[derive(Deserialize, Debug)]
        struct StructRow {
            a_int: i64,
            a_bool: bool,
        }

        // let deserialized_raw: RawQueryResults = serde_json::from_str(BASE_RESULTS_STR).unwrap();
        // let deserialized_tuple: QueryResults<StructRow> = QueryResults::from(deserialized_raw.clone());
        let deserialized_struct: QueryResults<StructRow> =
            serde_json::from_str(BASE_RESULTS_STR).unwrap();
        println!("deserialized_struct = {:?}", deserialized_struct);
        let row = &deserialized_struct.rows().unwrap()[0];
        assert_eq!(row.a_int, 123);
        assert_eq!(row.a_bool, true);
    }
}
