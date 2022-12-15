use crate::client_connection::ClientConnection;
use crate::results::{Column, QueryResults, QueryStats};
use crate::PrestinoError;
use async_stream::try_stream;
use futures::Stream;
use futures_util::pin_mut;
use serde::de::DeserializeOwned;

pub struct StatementExecutor<T: DeserializeOwned> {
    pub(crate) id: String,
    pub(crate) connection: ClientConnection,
    pub(crate) results: QueryResults<T>,
}

impl<T: DeserializeOwned> StatementExecutor<T> {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn info_uri(&self) -> &str {
        &self.results.info_uri
    }

    pub fn columns(&self) -> Option<&[Column]> {
        self.results.columns.as_deref()
    }

    pub fn stats(&self) -> &QueryStats {
        &self.results.stats
    }

    /// Cancel execution of this statement.  If the query is already finished,
    /// return PrestinoError::QueryFinishedError with the query id.
    pub async fn cancel(mut self) -> Result<(), PrestinoError> {
        let Some(next_uri) = self.results.next_uri.take() else {
            return Err(PrestinoError::QueryFinishedError(self.id().to_owned()));
        };

        // TODO: If this is an HTTP error, we should probably try again, or at least
        // allow the caller to try again.
        self.connection.cancel(&next_uri).await
    }

    pub async fn next_response(&mut self) -> Option<Result<Vec<T>, PrestinoError>> {
        // Clear out any data that we've saved.
        if let Some(err) = self.results.error.take() {
            return Some(Err(err.into()));
        } else if let Some(rows) = self.results.data.take() {
            return Some(Ok(rows));
        }

        // If there is no next_uri, we have finished iteration.
        let next_uri = self.results.next_uri.take()?;
        self.results = match self.connection.get_next_results(&next_uri).await {
            Err(err) => return Some(Err(err)),
            Ok(results) => results,
        };

        if let Some(err) = self.results.error.take() {
            return Some(Err(err.into()));
        }
        let rows = self.results.data.take().unwrap_or(Vec::new());
        Some(Ok(rows))
    }

    pub fn responses(mut self) -> impl Stream<Item = Result<(Vec<T>, QueryStats), PrestinoError>> {
        try_stream! {
            while let Some(response) = self.next_response().await {
                yield (response?, self.results.stats.clone());
            }
        }
    }

    pub fn batches(self) -> impl Stream<Item = Result<Vec<T>, PrestinoError>> {
        try_stream! {
            let query_results = self.responses();
            pin_mut!(query_results);
            for await rows_result in query_results {
                let rows = rows_result?.0;
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
