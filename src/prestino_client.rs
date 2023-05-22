use crate::client_connection::ClientConnection;
use crate::headers::Headers;
use crate::{PrestinoError, StatementExecutor};
use futures::pin_mut;
use futures::TryStreamExt;
use reqwest::Client;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone)]
pub struct PrestinoClient {
    base_url: String,
    headers: Headers,
    http_client: Client,
}

impl PrestinoClient {
    /// Create a Presto client with no headers set.
    pub fn presto(base_url: &str) -> Self {
        Self::with_headers(base_url, Headers::presto())
    }

    /// Create a Trino client with no headers set.
    pub fn trino(base_url: &str) -> Self {
        Self::with_headers(base_url, Headers::trino())
    }

    /// Create a client with the headers set.  The headers fork will determine the client's fork.
    pub fn with_headers(base_url: &str, headers: Headers) -> Self {
        Self {
            base_url: base_url.into(),
            headers,
            http_client: Client::new(),
        }
    }

    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers
    }

    /// Convenience function to set the user header.  Not needed if it's already set.
    pub fn user(mut self, user: &str) -> Self {
        self.headers.set_user(user);
        self
    }

    /// Begin execution of a statement, returning a StatementExecutor to continue execution.
    pub async fn execute<T: DeserializeOwned>(
        &self,
        statement: &str,
    ) -> Result<StatementExecutor<T>, PrestinoError> {
        let new_headers = self.headers.new_with_fork();
        self.execute_with_headers(statement, &new_headers).await
    }

    /// Begin execution of a statement, returning a StatementExecutor to continue execution.
    pub async fn execute_with_headers<T: DeserializeOwned>(
        &self,
        statement: &str,
        headers: &Headers,
    ) -> Result<StatementExecutor<T>, PrestinoError> {
        let mut connection_headers = self.headers.clone();
        connection_headers.update(headers);

        let mut connection = ClientConnection {
            headers: connection_headers,
            http_client: self.http_client.clone(),
        };

        let results = connection.post_statement(&self.base_url, statement).await?;

        Ok(StatementExecutor::new(
            results.id.clone(),
            connection,
            results,
        ))
    }

    /// A convenience function to retrieve all the rows for the statement into a single Vec.
    pub async fn execute_collect<T: DeserializeOwned>(
        &self,
        statement: &str,
    ) -> Result<Vec<T>, PrestinoError> {
        let new_headers = self.headers.new_with_fork();
        self.execute_collect_with_headers(statement, &new_headers)
            .await
    }

    /// A convenience function to retrieve all the rows for the statement into a single Vec.
    pub async fn execute_collect_with_headers<T: DeserializeOwned>(
        &self,
        statement: &str,
        headers: &Headers,
    ) -> Result<Vec<T>, PrestinoError> {
        let mut rows: Vec<T> = Vec::new();
        let executor = self.execute_with_headers::<T>(statement, headers).await?;
        let stream = executor.batches();
        pin_mut!(stream);
        while let Some(batch) = stream.try_next().await? {
            rows.extend(batch);
        }

        Ok(rows)
    }
}
