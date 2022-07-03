mod misc;
mod query_results;
mod raw_query_results;

pub use misc::{Column, QueryError};
pub use query_results::QueryResults;
pub use raw_query_results::RawQueryResults;

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    static BASE_RESULTS_STR: &str = r#"
        {
          "id" : "20160128_214710_00012_rk68b",
          "infoUri" : "http://localhost:54855/query.html?20160128_214710_00012_rk68b",
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
            "nodes" : 0,
            "totalSplits" : 0,
            "queuedSplits" : 0,
            "runningSplits" : 0,
            "completedSplits" : 0,
            "cpuTimeMillis" : 0,
            "wallTimeMillis" : 0,
            "queuedTimeMillis" : 0,
            "elapsedTimeMillis" : 0,
            "processedRows" : 0,
            "processedBytes" : 0,
            "peakMemoryBytes" : 0
          }
        }
        "#;

    #[test]
    fn deserialize_basic_raw() {
        let deserialized: RawQueryResults = serde_json::from_str(BASE_RESULTS_STR).unwrap();
        println!("deserialized = {:?}", deserialized);
    }

    #[test]
    fn deserialize_basic_typed_tuple() {
        let deserialized_raw: RawQueryResults = serde_json::from_str(BASE_RESULTS_STR).unwrap();

        let deserialized_tuple: QueryResults<(i64, bool)> =
            QueryResults::from(deserialized_raw.clone());
        println!("deserialized_tuple = {:?}", deserialized_tuple);
        let row = &deserialized_tuple.rows()[0];
        assert_eq!(row.0, 123);
        assert_eq!(row.1, true);
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
        let deserialized_tuple: QueryResults<StructRow> =
            serde_json::from_str(BASE_RESULTS_STR).unwrap();
        println!("deserialized_tuple = {:?}", deserialized_tuple);
        let row = &deserialized_tuple.rows()[0];
        assert_eq!(row.a_int, 123);
        assert_eq!(row.a_bool, true);
    }
}
