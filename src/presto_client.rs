use crate::results::QueryResults;
use crate::{Fork, PrestinoError, StatementExecutor};
use futures::pin_mut;
use futures::TryStreamExt;
use reqwest::header::{HeaderMap, HeaderName};
use reqwest::{Client, RequestBuilder};
use serde::de::DeserializeOwned;

#[derive(Debug, Clone)]
pub struct PrestoClient {
    fork: Fork,
    base_url: String,
    headers: HeaderMap,
}

impl PrestoClient {
    pub fn presto(base_url: String) -> Self {
        Self {
            fork: Fork::Presto,
            base_url,
            headers: HeaderMap::new(),
        }
    }

    pub fn trino(base_url: String) -> Self {
        Self {
            fork: Fork::Trino,
            base_url,
            headers: HeaderMap::new(),
        }
    }

    fn name_for(&self, name: &str) -> HeaderName {
        HeaderName::try_from(self.fork.name_for(name)).unwrap()
    }

    /// Specifies the session user. If not supplied, the session user is automatically determined via [User mapping](https://trino.io/docs/current/security/user-mapping.html).
    pub fn user(mut self, user: &str) -> Self {
        self.headers.insert(
            self.name_for("user"),
            user.to_ascii_lowercase().parse().unwrap(),
        );
        self
    }

    /// For reporting purposes, this supplies the name of the software that submitted the query.
    pub fn source(mut self, source: &str) -> Self {
        self.headers.insert(
            self.name_for("source"),
            source.to_ascii_lowercase().parse().unwrap(),
        );
        self
    }

    /// Supplies a trace token to the Trino engine to help identify log lines that originate with this query request.
    pub fn trace_token(mut self, trace_token: &str) -> Self {
        self.headers.insert(
            self.name_for("trace-token"),
            trace_token.to_ascii_lowercase().parse().unwrap(),
        );
        self
    }

    /// Contains arbitrary information about the client program submitting the query.
    pub fn client_info(mut self, client_info: &str) -> Self {
        self.headers.insert(
            self.name_for("client-info"),
            client_info.to_ascii_lowercase().parse().unwrap(),
        );
        self
    }

    /// A convenience function to retrieve all the rows for the statement into a single Vec.
    pub async fn rows_from<T: DeserializeOwned>(
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
        let http_client = Client::new();
        let request = http_client
            .post(format!("{}/v1/statement", self.base_url))
            .headers(self.headers.clone())
            .body(statement);

        let results = Self::get_results(request).await?;
        return Ok(StatementExecutor::new(http_client, results));
    }

    pub async fn get_results<T: DeserializeOwned>(
        request: RequestBuilder,
    ) -> Result<QueryResults<T>, PrestinoError> {
        let response = request.send().await?;
        let status = response.status();
        if status != reqwest::StatusCode::OK {
            let message = response.text().await?;
            return Err(PrestinoError::from_status_code(status.as_u16(), message));
        }
        // TODO: Make better error messages on json deser.  In particular, if there's a type error,
        // can we print out the row that causes the error?
        Ok(response.json().await?)
    }

    pub fn get_results_request(client: &Client, next_uri: &str) -> RequestBuilder {
        println!("Getting next results: {}", next_uri);
        let request = client.get(next_uri).header("X-Trino-User", "jagill");
        request
    }
}
