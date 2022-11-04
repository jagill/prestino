use hyper::client::HttpConnector;
use hyper::{Client, Uri};
use crate::results::QueryResultsValue;
use crate::error::Error;
use crate::PrestoApi;


pub struct PrestoClient {
    base_url: String,
    http_client: Client<HttpConnector>,
}

impl PrestoClient {
    pub fn new(base_url: String) -> Self {
        PrestoClient {
            base_url,
            http_client: Client::new(),
        }
    }

    pub async fn query(&self, query: &str) -> Result<QueryResultsValue, Error> {
        let request = PrestoApi::post_query_request(&self.base_url, query)?;
        PrestoApi::get_results(request, &self.http_client).await
    }

    pub async fn next_results(&self, next_uri: Uri) -> Result<QueryResultsValue, Error> {
        let request = PrestoApi::get_results_request(next_uri)?;
        PrestoApi::get_results(request, &self.http_client).await
    }
}


