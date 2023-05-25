use crate::client_connection::{ClientConnection, ReqwestClientConnection};
use crate::Headers;
use crate::{PrestinoError, StatementExecutor};
use crate::results::QueryResults;
use futures::pin_mut;
use futures::TryStreamExt;
use serde::de::DeserializeOwned;

pub struct PrestinoClient<C> {
    base_url: String,
    headers: Headers,
    client_connection: C,
}

impl<C: ClientConnection> PrestinoClient<C> {
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
        let client_connection = ReqwestClientConnection { http_client: reqwest::Client::new() };
        Self {
            base_url: base_url.into(),
            headers,
            client_connection:  client_connection ,
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

        let mut connection = ReqwestClientConnection {
            http_client: self.http_client.clone(),
        };

        let statement_uri = format!("{}/v1/statement", &self.base_url);
        let result_bytes = connection.post_statement(&statement_uri, statement, &mut connection_headers).await?;
        let results: QueryResults<T> = serde_json::from_slice(&result_bytes)?;

        Ok(StatementExecutor::new(
            results.id.clone(),
            headers,
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
