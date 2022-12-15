use crate::client_connection::ClientConnection;
use crate::{Fork, PrestinoError, StatementExecutor};
use futures::pin_mut;
use futures::TryStreamExt;
use reqwest::header::{HeaderMap, HeaderName};
use reqwest::Client;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone)]
pub struct PrestinoClient {
    fork: Fork,
    base_url: String,
    headers: HeaderMap,
    http_client: Client,
}

impl PrestinoClient {
    pub fn presto(base_url: String) -> Self {
        Self {
            fork: Fork::Presto,
            base_url,
            headers: HeaderMap::new(),
            http_client: Client::new(),
        }
    }

    pub fn trino(base_url: String) -> Self {
        Self {
            fork: Fork::Trino,
            base_url,
            headers: HeaderMap::new(),
            http_client: Client::new(),
        }
    }

    fn name_for(&self, name: &str) -> HeaderName {
        // Since we control the input, we can ensure that it is always visible ASCII
        HeaderName::try_from(self.fork.name_for(name)).unwrap()
    }

    /// Specifies the session user. If not supplied, the session user is
    /// automatically determined via [User mapping](https://trino.io/docs/current/security/user-mapping.html).
    /// The `user` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn user(mut self, user: &str) -> Self {
        self.headers.insert(
            self.name_for("user"),
            user.to_ascii_lowercase().parse().unwrap(),
        );
        self
    }

    /// For reporting purposes, this supplies the name of the software that
    /// submitted the query.
    /// The `source` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn source(mut self, source: &str) -> Self {
        self.headers.insert(
            self.name_for("source"),
            source.to_ascii_lowercase().parse().unwrap(),
        );
        self
    }

    /// Supplies a trace token to the Trino engine to help identify log lines
    /// that originate with this query request.
    /// The `trace_token` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn trace_token(mut self, trace_token: &str) -> Self {
        self.headers.insert(
            self.name_for("trace-token"),
            trace_token.to_ascii_lowercase().parse().unwrap(),
        );
        self
    }

    /// Contains arbitrary information about the client program submitting the query.
    /// The `client_info` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn client_info(mut self, client_info: &str) -> Self {
        self.headers.insert(
            self.name_for("client-info"),
            client_info.to_ascii_lowercase().parse().unwrap(),
        );
        self
    }

    /// A convenience function to retrieve all the rows for the statement into a single Vec.
    pub async fn execute_collect<T: DeserializeOwned>(
        &self,
        statement: String,
    ) -> Result<Vec<T>, PrestinoError> {
        let mut rows: Vec<T> = Vec::new();
        let executor = self.execute::<T>(statement).await?;
        let stream = executor.batches();
        pin_mut!(stream);
        while let Some(batch) = stream.try_next().await? {
            rows.extend(batch);
        }

        Ok(rows)
    }

    /// Begin execution of a statement, returning a StatementExecutor to continue execution.
    pub async fn execute<T: DeserializeOwned>(
        &self,
        statement: String,
    ) -> Result<StatementExecutor<T>, PrestinoError> {
        let mut connection = ClientConnection {
            headers: self.headers.clone(),
            http_client: self.http_client.clone(),
        };

        let results = connection.post_statement(&self.base_url, statement).await?;

        Ok(StatementExecutor::new(
            results.id.clone(),
            connection,
            results,
        ))
    }
}
