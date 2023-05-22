use super::ClientConnection;
use crate::{Headers, PrestinoError};
use async_trait::async_trait;
use log::{debug, info};
use reqwest::{Client, Response};
use bytes::Bytes;

#[derive(Debug)]
pub struct ReqwestClientConnection {
    pub headers: Headers,
    pub http_client: Client,
}

impl ReqwestClientConnection {
    async fn parse_response(
        &mut self,
        response: Response,
    ) -> Result<Bytes, PrestinoError> {
        let status = response.status();
        if status != reqwest::StatusCode::OK {
            let message = response.text().await?;
            return Err(PrestinoError::from_status_code(status.as_u16(), message));
        }
        self.headers
            .update_from_response_headers(response.headers())?;
        // TODO: Make better error messages on json deser.  In particular, if there's a type error,
        // can we print out the row that causes the error?
        Ok(response.bytes().await?)
    }
}

#[async_trait]
impl ClientConnection for ReqwestClientConnection {
    async fn post_statement(
        &mut self,
        statement_uri: &str,
        statement: &str,
    ) -> Result<Bytes, PrestinoError> {
        let response = self
            .http_client
            .post(statement_uri)
            .headers(self.headers.build()?)
            .body(statement.to_owned())
            .send()
            .await?;

        self.parse_response(response).await
    }

    async fn get_next_results(
        &mut self,
        next_uri: &str,
    ) -> Result<Bytes, PrestinoError> {
        debug!("Getting next results: {}", next_uri);
        let response = self
            .http_client
            .get(next_uri)
            .headers(self.headers.build()?)
            .send()
            .await?;
        self.parse_response(response).await
    }

    async fn cancel(&mut self, next_uri: &str) -> Result<(), PrestinoError> {
        info!("Cancelling query with uri: {}", next_uri);
        let response = self
            .http_client
            .delete(next_uri)
            .headers(self.headers.build()?)
            .send()
            .await?;

        self.parse_response(response).await.map(|_| ())
    }
}
