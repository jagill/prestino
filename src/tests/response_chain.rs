use serde::{Deserialize, Serialize};
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

        Some(
            next_uri?
                .strip_prefix(base_uri)
                .unwrap()
                .to_string(),
        )
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
}
