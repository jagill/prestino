use crate::error::Error;
use crate::PrestoApi;
use crate::StatementExecutor;
use hyper::Client;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone)]
pub struct PrestoClient {
    base_url: String,
}

impl PrestoClient {
    pub fn new(base_url: String) -> PrestoClient {
        PrestoClient { base_url }
    }

    pub async fn execute<T: DeserializeOwned>(
        &self,
        statement: String,
    ) -> Result<StatementExecutor<T>, Error> {
        let request = PrestoApi::post_statement_request(&self.base_url, statement)?;
        let http_client = Client::new();
        let results = PrestoApi::get_results(request, &http_client).await?;
        return Ok(StatementExecutor::new(http_client, results));
    }
}
