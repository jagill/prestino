use crate::error::Error;
use crate::PrestoApi;
use crate::StatementExecutor;
use hyper::Client;

pub struct PrestoClient {
    base_url: String,
}

impl PrestoClient {
    pub fn new(base_url: String) -> PrestoClient {
        PrestoClient { base_url }
    }

    pub async fn execute(self, statement: String) -> Result<StatementExecutor, Error> {
        let request = PrestoApi::post_statement_request(&self.base_url, statement)?;
        let http_client = Client::new();
        let results = PrestoApi::get_results(request, &http_client).await?;
        return Ok(StatementExecutor::new(http_client, results));
    }
}
