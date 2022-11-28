use crate::PrestinoError;
use crate::results::{Column, QueryError, QueryResults, QueryStats};
use crate::PrestoApi;
use async_stream::try_stream;
use futures::Stream;
use futures_util::pin_mut;
use hyper::client::HttpConnector;
use hyper::{Client, Uri};
use serde::de::DeserializeOwned;

pub struct StatementExecutor<T: DeserializeOwned> {
    id: String,
    http_client: Client<HttpConnector>,
    results: QueryResults<T>,
}

impl<T: DeserializeOwned> StatementExecutor<T> {
    pub fn new(http_client: Client<HttpConnector>, results: QueryResults<T>) -> Self {
        Self {
            id: results.id.clone(),
            http_client,
            results,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn info_uri(&self) -> &Uri {
        &self.results.info_uri
    }

    pub fn columns(&self) -> Option<&[Column]> {
        self.results.columns.as_deref()
    }

    pub fn take_next_uri(&mut self) -> Option<Uri> {
        self.results.next_uri.take()
    }

    pub fn take_data(&mut self) -> Option<Vec<T>> {
        self.results.data.take()
    }

    pub fn take_error(&mut self) -> Option<QueryError> {
        self.results.error.take()
    }

    pub fn stats(&self) -> &QueryStats {
        &self.results.stats
    }

    pub fn take_result(&mut self) -> Option<Result<Vec<T>, PrestinoError>> {
        if let Some(err) = self.take_error() {
            Some(Err(err.into()))
        } else if let Some(rows) = self.take_data() {
            Some(Ok(rows))
        } else {
            None
        }
    }

    pub async fn next_response(&mut self) -> Option<Result<Vec<T>, PrestinoError>> {
        if let Some(result) = self.take_result() {
            return Some(result);
        }

        let Some(next_uri) = self.take_next_uri() else {
            return None;
        };

        let request = match PrestoApi::get_results_request(next_uri) {
            Err(err) => return Some(Err(err)),
            Ok(request) => request,
        };

        self.results = match PrestoApi::get_results(request, &self.http_client).await {
            Err(err) => return Some(Err(err)),
            Ok(results) => results,
        };

        // Return an empty vec as a placeholder meaning "try again"
        Some(Ok(Vec::new()))
    }

    pub fn responses(mut self) -> impl Stream<Item = Result<Vec<T>, PrestinoError>> {
        try_stream! {
            while let Some(response) = self.next_response().await {
                yield response?;
            }
        }
    }

    pub fn batches(self) -> impl Stream<Item = Result<Vec<T>, PrestinoError>> {
        try_stream! {
            let query_results = self.responses();
            pin_mut!(query_results);
            for await rows_result in query_results {
                let rows = rows_result?;
                if !rows.is_empty() {
                    yield rows;
                }
            }
        }
    }

    pub fn rows(self) -> impl Stream<Item = Result<T, PrestinoError>> {
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
