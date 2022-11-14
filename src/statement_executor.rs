use crate::error::Error;
use crate::results::QueryResultsJson;
use crate::PrestoApi;
use async_stream::try_stream;
use futures::Stream;
use futures_util::pin_mut;
use hyper::client::HttpConnector;
use hyper::{Client, Uri};
use serde_json::Value;

pub struct StatementExecutor {
    id: String,
    http_client: Client<HttpConnector>,
    results: QueryResultsJson,
}

impl StatementExecutor {
    pub fn new(http_client: Client<HttpConnector>, results: QueryResultsJson) -> Self {
        Self {
            id: results.id.clone(),
            http_client,
            results,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    // Fetch and store (not return) next results given a uri
    async fn _fetch_next_results(&mut self, uri: Uri) -> Result<(), Error> {
        let request = PrestoApi::get_results_request(uri)?;
        let results = PrestoApi::get_results(request, &self.http_client).await?;
        self.results = results;
        Ok(())
    }

    /// Fetch and store (not return) next QueryResultsValue
    pub async fn fetch_next_results(&mut self) -> Option<Result<(), Error>> {
        let next_uri = self.results.next_uri.take()?;
        Some(self._fetch_next_results(next_uri).await)
    }

    pub fn responses(mut self) -> impl Stream<Item = Result<Vec<Value>, Error>> {
        try_stream! {
            yield self.results.data.take().unwrap_or_default();

            while let Some(next_uri) = self.results.next_uri.take() {
                self._fetch_next_results(next_uri).await?;
                yield self.results.data.take().unwrap_or_default();
            }
        }
    }

    pub fn batches(self) -> impl Stream<Item = Result<Vec<Value>, Error>> {
        try_stream! {
            let query_results = self.responses();
            pin_mut!(query_results);
            for await rows_result in query_results {
                let rows = rows_result.unwrap();
                if !rows.is_empty() {
                    yield rows;
                }
            }
        }
    }

    pub fn rows(self) -> impl Stream<Item = Result<Value, Error>> {
        try_stream! {
            let batches = self.batches();
            pin_mut!(batches);
            for await batch in batches {
                for row in batch?.drain(..) {
                    yield row;
                }
            }
        }
    }
}
