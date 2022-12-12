use super::{Column, QueryError, QueryStats};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryResults<T> {
    pub id: String,
    pub info_uri: String,
    pub next_uri: Option<String>,
    pub partial_cancel_uri: Option<String>,
    pub columns: Option<Vec<Column>>,
    pub data: Option<Vec<T>>,
    pub stats: QueryStats,
    pub error: Option<QueryError>,
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
        #[derive(Deserialize, Debug, PartialEq)]
        struct StructRow {
            a_int: i64,
            a_bool: bool,
        }

        // let deserialized_raw: RawQueryResults = serde_json::from_str(BASE_RESULTS_STR).unwrap();
        // let deserialized_tuple: QueryResults<StructRow> = QueryResults::from(deserialized_raw.clone());
        let mut deserialized_struct: QueryResults<StructRow> =
            serde_json::from_str(BASE_RESULTS_STR).unwrap();
        println!("deserialized_struct = {:?}", deserialized_struct);
        let rows = deserialized_struct.data.take().unwrap();
        assert_eq!(
            rows,
            vec![StructRow {
                a_int: 123,
                a_bool: true,
            }]
        );
    }

    #[test]
    fn deserialize_parse_error() {
        let response_str = r#"
            {"id":"20221128_035242_00004_educe","infoUri":"http://localhost:8080/ui/query.html?20221128_035242_00004_educe","stats":{"state":"FAILED","queued":false,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":0,"elapsedTimeMillis":0,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"error":{"message":"line 5:14: mismatched input \u0027BOOLEAN\u0027. Expecting: \u0027)\u0027, \u0027,\u0027","errorCode":1,"errorName":"SYNTAX_ERROR","errorType":"USER_ERROR","errorLocation":{"lineNumber":5,"columnNumber":14},"failureInfo":{"type":"io.trino.sql.parser.ParsingException","message":"line 5:14: mismatched input \u0027BOOLEAN\u0027. Expecting: \u0027)\u0027, \u0027,\u0027","suppressed":[],"stack":["io.trino.sql.parser.ErrorHandler.syntaxError(ErrorHandler.java:109)","org.antlr.v4.runtime.ProxyErrorListener.syntaxError(ProxyErrorListener.java:41)","org.antlr.v4.runtime.Parser.notifyErrorListeners(Parser.java:544)","org.antlr.v4.runtime.DefaultErrorStrategy.reportUnwantedToken(DefaultErrorStrategy.java:377)","org.antlr.v4.runtime.DefaultErrorStrategy.singleTokenDeletion(DefaultErrorStrategy.java:548)","org.antlr.v4.runtime.DefaultErrorStrategy.sync(DefaultErrorStrategy.java:266)","io.trino.sql.parser.SqlBaseParser.columnAliases(SqlBaseParser.java:9472)","io.trino.sql.parser.SqlBaseParser.aliasedRelation(SqlBaseParser.java:9413)","io.trino.sql.parser.SqlBaseParser.patternRecognition(SqlBaseParser.java:8634)","io.trino.sql.parser.SqlBaseParser.sampledRelation(SqlBaseParser.java:8258)","io.trino.sql.parser.SqlBaseParser.relation(SqlBaseParser.java:7929)","io.trino.sql.parser.SqlBaseParser.querySpecification(SqlBaseParser.java:6845)","io.trino.sql.parser.SqlBaseParser.queryPrimary(SqlBaseParser.java:6577)","io.trino.sql.parser.SqlBaseParser.queryTerm(SqlBaseParser.java:6377)","io.trino.sql.parser.SqlBaseParser.queryNoWith(SqlBaseParser.java:6023)","io.trino.sql.parser.SqlBaseParser.query(SqlBaseParser.java:5180)","io.trino.sql.parser.SqlBaseParser.statement(SqlBaseParser.java:2636)","io.trino.sql.parser.SqlBaseParser.singleStatement(SqlBaseParser.java:321)","io.trino.sql.parser.SqlParser.invokeParser(SqlParser.java:143)","io.trino.sql.parser.SqlParser.createStatement(SqlParser.java:85)","io.trino.execution.QueryPreparer.prepareQuery(QueryPreparer.java:55)","io.trino.dispatcher.DispatchManager.createQueryInternal(DispatchManager.java:179)","io.trino.dispatcher.DispatchManager.lambda$createQuery$0(DispatchManager.java:148)","io.trino.$gen.Trino_402____20221128_035215_2.run(Unknown Source)","java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)","java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)","java.base/java.lang.Thread.run(Thread.java:833)"],"errorLocation":{"lineNumber":5,"columnNumber":14}}},"warnings":[]}
        "#;
        let response: QueryResults<Value> = serde_json::from_str(response_str).unwrap();
        assert_eq!(response.stats.state, "FAILED");
        assert!(response.error.is_some());
        let error = response.error.clone().unwrap();
        assert_eq!(error.error_code, 1);
        assert_eq!(error.error_name, "SYNTAX_ERROR");
        assert_eq!(error.error_type, "USER_ERROR");
        assert!(error.error_location.is_some());
    }
}
