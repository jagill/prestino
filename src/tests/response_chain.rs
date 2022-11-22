use serde::{Deserialize, Serialize};
use serde_json::Value;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub struct ResponseChain {
    pub base_uri: String,
    pub first_response: String,
    pub next_responses: Vec<String>,
    pub next_uris: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct NextUri {
    pub next_uri: Option<String>,
}

impl ResponseChain {
    pub fn new(responses: &[&str], base_uri: String) -> Self {
        let mut responses: Vec<String> = responses
            .iter()
            .map(|s| s.replace("http://localhost:8080", &base_uri))
            .collect();
        let next_uris: Vec<String> = responses
            .iter()
            .filter_map(|resp| Self::extract_next_uri(resp, &base_uri))
            .collect();
        let first_response = responses.remove(0).to_string();
        ResponseChain {
            base_uri,
            first_response,
            next_responses: responses,
            next_uris,
        }
    }

    fn extract_next_uri(response_str: &str, base_uri: &str) -> Option<String> {
        let NextUri { next_uri } = serde_json::from_str(response_str).unwrap();

        Some(next_uri?.strip_prefix(base_uri).unwrap().to_string())
    }

    pub async fn mock_flow(&self, mock_server: &MockServer) {
        let first_response = ResponseTemplate::new(200).set_body_string(&self.first_response);

        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(first_response)
            .mount(&mock_server)
            .await;

        for (next_uri, next_json) in self.next_uris.iter().zip(self.next_responses.iter()) {
            println!("Mocking {} with\n\t{}", next_uri, next_json);
            let next_response = ResponseTemplate::new(200).set_body_string(next_json);
            Mock::given(method("GET"))
                .and(path(next_uri))
                .respond_with(next_response)
                .mount(&mock_server)
                .await;
        }
    }

    pub fn make_response_set(columns: &[(&str, &str)], data: &[Value]) -> Vec<String> {
        let id = uuid::Uuid::new_v4();
        let column_str = columns.iter()
            .map(|(name, typ)| format!(r#"{{"name":"{name}","type":"{typ}","typeSignature":{{"rawType":"{typ}","arguments":[]}}}}"#))
            .collect::<Vec<String>>().join(",");
        println!("Column str: {column_str}");

        let mut responses = Vec::new();
        // Queued
        responses.push(format!(r#"{{"id":"{id}","infoUri":"http://localhost:8080/ui/query.html?{id}","nextUri":"http://localhost:8080/v1/statement/queued/{id}/ye6b7b80fe5070e687a6f31e87c69b2157bc560b6/1","stats":{{"state":"QUEUED","queued":true,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":0,"elapsedTimeMillis":0,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0}},"warnings":[]}}"#));
        // Running, columns but no data
        responses.push(format!(r#"{{"id":"{id}","infoUri":"http://localhost:8080/ui/query.html?{id}","partialCancelUri":"http://localhost:8080/v1/statement/executing/partialCancel/{id}/0/y6aab6ff9dc9b744da35e343b3ed6b6b3f5a1bc4d/1","nextUri":"http://localhost:8080/v1/statement/executing/{id}/y6aab6ff9dc9b744da35e343b3ed6b6b3f5a1bc4d/1","columns":[{column_str}],"stats":{{"state":"RUNNING","queued":false,"scheduled":true,"nodes":1,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":3,"elapsedTimeMillis":60,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0,"rootStage":{{"stageId":"0","state":"RUNNING","done":false,"nodes":1,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"failedTasks":0,"coordinatorOnly":false,"subStages":[]}}}},"warnings":[]}}"#));
        for (idx, rows) in data.iter().enumerate() {
            // Data row; can have empty ("[]") data.
            let data_str = serde_json::to_string(rows).unwrap();
            println!("Data str: {data_str}");
            responses.push(format!(r#"{{"id":"{id}","infoUri":"http://localhost:8080/ui/query.html?{id}","partialCancelUri":"http://localhost:8080/v1/statement/executing/partialCancel/{id}/0/y0f368c8309d5e2d7dd10875b57cb880e862ab0cc/2","nextUri":"http://localhost:8080/v1/statement/executing/{id}/y0f368c8309d5e2d7dd10875b57cb880e862ab0cc/{idx}","columns":[{column_str}],"data":{data_str},"stats":{{"state":"RUNNING","queued":false,"scheduled":true,"nodes":1,"totalSplits":1,"queuedSplits":1,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":3,"elapsedTimeMillis":85,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":103,"spilledBytes":0,"rootStage":{{"stageId":"0","state":"RUNNING","done":false,"nodes":1,"totalSplits":1,"queuedSplits":1,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"failedTasks":0,"coordinatorOnly":false,"subStages":[]}},"progressPercentage":0.0}},"warnings":[]}}"#));
        }
        // Finished, next_uri (stats still calculating)
        responses.push(format!(r#"{{"id":"{id}","infoUri":"http://localhost:8080/ui/query.html?{id}","nextUri":"http://localhost:8080/v1/statement/executing/{id}/y0566e949b2111366df34de9d56ebb7cbd3e9a45c/3","columns":[{column_str}],"stats":{{"state":"FINISHED","queued":false,"scheduled":true,"nodes":1,"totalSplits":1,"queuedSplits":1,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":3,"elapsedTimeMillis":89,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":103,"spilledBytes":0,"rootStage":{{"stageId":"0","state":"FINISHED","done":true,"nodes":1,"totalSplits":1,"queuedSplits":1,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"failedTasks":0,"coordinatorOnly":false,"subStages":[]}},"progressPercentage":0.0}},"warnings":[]}}"#));
        // Finished, no next_uri
        responses.push(format!(r#"{{"id":"{id}","infoUri":"http://localhost:8080/ui/query.html?{id}","columns":[{column_str}],"stats":{{"state":"FINISHED","queued":false,"scheduled":true,"nodes":1,"totalSplits":1,"queuedSplits":0,"runningSplits":0,"completedSplits":1,"cpuTimeMillis":1,"wallTimeMillis":1,"queuedTimeMillis":3,"elapsedTimeMillis":89,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":103,"spilledBytes":0,"rootStage":{{"stageId":"0","state":"FINISHED","done":true,"nodes":1,"totalSplits":1,"queuedSplits":0,"runningSplits":0,"completedSplits":1,"cpuTimeMillis":1,"wallTimeMillis":1,"processedRows":1,"processedBytes":0,"physicalInputBytes":0,"failedTasks":0,"coordinatorOnly":false,"subStages":[]}},"progressPercentage":100.0}},"warnings":[]}}"#));

        responses
    }
}
