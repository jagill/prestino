use serde::{Deserialize, Serialize};
use serde_json::Value;
use http::uri::Uri;
use std::fmt::Debug;
use super::{Column, QueryError, QueryStats};
use crate::utils::opt_uri_serde;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryResultsValue {
    pub id: String,
    #[serde(with = "http_serde::uri")]
    pub info_uri: Uri,
    #[serde(default, with = "opt_uri_serde")]
    pub next_uri: Option<Uri>,
    #[serde(default, with = "opt_uri_serde")]
    pub partial_cancel_uri: Option<Uri>,
    pub columns: Option<Vec<Column>>,
    pub data: Option<Vec<Value>>,
    pub stats: QueryStats,
    pub error: Option<QueryError>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_response_first_response() {
        let results_str = r#"{"id":"20221102_184053_00002_ipi9h","infoUri":"http://localhost:8080/ui/query.html?20221102_184053_00002_ipi9h","nextUri":"http://localhost:8080/v1/statement/queued/20221102_184053_00002_ipi9h/y053673808954299dc4e6c9fbb6fb5fb331ea5dc4/1","stats":{"state":"QUEUED","queued":true,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":0,"elapsedTimeMillis":0,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"warnings":[]}"#;
        let results: QueryResultsValue = serde_json::from_str(results_str).unwrap();
        assert_eq!(&results.id, "20221102_184053_00002_ipi9h");
        assert_eq!(&results.stats.state, "QUEUED");
    }


    #[test]
    fn test_query_results_second_response() {
        let results_str = r#"{"id":"20221102_163350_00000_ipi9h","infoUri":"http://localhost:8080/ui/query.html?20221102_163350_00000_ipi9h","nextUri":"http://localhost:8080/v1/statement/queued/20221102_163350_00000_ipi9h/y29c391084624a1a8ce96c911d548cbc75a2962df/2","stats":{"state":"QUEUED","queued":true,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":10,"elapsedTimeMillis":11,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"warnings":[]}"#;
        let results: QueryResultsValue = serde_json::from_str(results_str).unwrap();
        assert_eq!(&results.id, "20221102_163350_00000_ipi9h");
        assert_eq!(&results.stats.state, "QUEUED");
    }

    #[test]
    fn test_query_results_third_response() {
        let results_str = r#"{"id":"20221102_163624_00001_ipi9h","infoUri":"http://localhost:8080/ui/query.html?20221102_163624_00001_ipi9h","partialCancelUri":"http://localhost:8080/v1/statement/executing/partialCancel/20221102_163624_00001_ipi9h/0/y38e97f41c34994651e26a9a982cbaccb231a50e0/1","nextUri":"http://localhost:8080/v1/statement/executing/20221102_163624_00001_ipi9h/y38e97f41c34994651e26a9a982cbaccb231a50e0/1","columns":[{"name":"a","type":"integer","typeSignature":{"rawType":"integer","arguments":[]}}],"data":[[1]],"stats":{"state":"RUNNING","queued":false,"scheduled":true,"nodes":1,"totalSplits":1,"queuedSplits":0,"runningSplits":0,"completedSplits":1,"cpuTimeMillis":1,"wallTimeMillis":1,"queuedTimeMillis":1,"elapsedTimeMillis":46985,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":103,"spilledBytes":0,"rootStage":{"stageId":"0","state":"RUNNING","done":false,"nodes":1,"totalSplits":1,"queuedSplits":0,"runningSplits":0,"completedSplits":1,"cpuTimeMillis":1,"wallTimeMillis":1,"processedRows":1,"processedBytes":0,"physicalInputBytes":0,"failedTasks":0,"coordinatorOnly":false,"subStages":[]},"progressPercentage":100.0},"warnings":[]}"#;
        let results: QueryResultsValue = serde_json::from_str(results_str).unwrap();
        assert_eq!(&results.id, "20221102_163624_00001_ipi9h");
        assert_eq!(&results.stats.state, "RUNNING");
        assert!(&results.columns.is_some());
    }
}
