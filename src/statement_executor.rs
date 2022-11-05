use crate::error::Error;
use crate::results::QueryResultsValue;
use crate::PrestoApi;
use hyper::client::HttpConnector;
use hyper::{Client, Uri};

pub struct StatementExecutor {
    id: String,
    http_client: Client<HttpConnector>,
    results: QueryResultsValue,
}

impl StatementExecutor {
    pub fn new(http_client: Client<HttpConnector>, results: QueryResultsValue) -> Self {
        Self {
            id: results.id.clone(),
            http_client,
            results,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        if let Some(data) = self.results.data.take() {
            println!("{:?}", data);
        }

        let mut uri_opt = self.results.next_uri.take();
        while let Some(uri) = uri_opt {
            println!("Getting next uri {uri}");
            self.fetch_next_results(uri).await?;
            uri_opt = self.results.next_uri.take();
            if let Some(data) = self.results.data.take() {
                println!("{:?}", data);
            }
        }

        Ok(())
    }

    // Fetch and store (not return) next results
    pub async fn fetch_next_results(&mut self, uri: Uri) -> Result<(), Error> {
        let request = PrestoApi::get_results_request(uri)?;
        let results = PrestoApi::get_results(request, &self.http_client).await?;
        println!("{:?}", results.stats);
        self.results = results;
        Ok(())
    }
}
