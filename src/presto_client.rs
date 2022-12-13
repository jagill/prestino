use crate::results::QueryResults;
use crate::{PrestinoError, StatementExecutor};
use futures::pin_mut;
use futures::TryStreamExt;
use reqwest::{Client, RequestBuilder};
use serde::de::DeserializeOwned;

#[derive(Debug, Clone)]
pub struct PrestoClient {
    base_url: String,
}

impl PrestoClient {
    pub fn new(base_url: String) -> PrestoClient {
        PrestoClient { base_url }
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
            .header("X-Trino-User", "jagill")
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
            return Err(PrestinoError::from_status_code(
                status.as_u16(),
                message,
            ));
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
