use crate::StatementExecutor;
use crate::{PrestinoError, PrestoApi};
use reqwest::Client;
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
    ) -> Result<StatementExecutor<T>, PrestinoError> {
        let http_client = Client::new();
        let request = PrestoApi::post_statement_request(&http_client, &self.base_url, statement);

        let results = PrestoApi::get_results(request).await?;
        return Ok(StatementExecutor::new(http_client, results));
    }
}
