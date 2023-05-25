use super::ClientConnection;
use crate::{Headers, PrestinoError};
use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, info};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Response};

#[derive(Clone, Debug)]
pub struct ReqwestClientConnection {
    pub http_client: Client,
}

impl ReqwestClientConnection {
    async fn parse_response(
        &self,
        response: Response,
        headers: &mut Headers,
    ) -> Result<Bytes, PrestinoError> {
        let status = response.status();
        if status != reqwest::StatusCode::OK {
            let message = response.text().await?;
            return Err(PrestinoError::from_status_code(status.as_u16(), message));
        }
        ReqwestClientConnection::update_from_response_headers(headers, response.headers())?;
        Ok(response.bytes().await?)
    }

    fn make_request_headers(headers: &Headers) -> Result<HeaderMap, PrestinoError> {
        let mut header_map = HeaderMap::new();
        for (name, value) in headers.get_headers() {
            // We define the header names, so this should never happen.
            let header_name: HeaderName =
                name.parse().expect("Poorly formatted header name: {name}");
            let header_value: HeaderValue =
                HeaderValue::from_str(&name).map_err(|_| PrestinoError::HeaderParseError)?;
            header_map.append(header_name, header_value);
        }
        Ok(header_map)
    }

    pub fn update_from_response_headers(
        headers: &mut Headers,
        response_headers: &HeaderMap,
    ) -> Result<(), PrestinoError> {
        for (name, value) in response_headers.iter() {
            let name_str = name.as_str();
            let value_str = value.to_str()?;
            headers.update_from_response_header(name_str, value_str)?;
        }
        Ok(())
    }
}

#[async_trait]
impl ClientConnection for ReqwestClientConnection {
    fn clone_boxed(&self) -> Box<dyn ClientConnection> {
        Box::new(self.clone())
    }

    async fn post_statement(
        &self,
        statement_uri: &str,
        statement: &str,
        headers: &mut Headers,
    ) -> Result<Bytes, PrestinoError> {
        let response = self
            .http_client
            .post(statement_uri)
            .headers(ReqwestClientConnection::make_request_headers(headers)?)
            .body(statement.to_owned())
            .send()
            .await?;

        self.parse_response(response, headers).await
    }

    async fn get_next_results(
        &self,
        next_uri: &str,
        headers: &mut Headers,
    ) -> Result<Bytes, PrestinoError> {
        debug!("Getting next results: {}", next_uri);
        let response = self
            .http_client
            .get(next_uri)
            .headers(ReqwestClientConnection::make_request_headers(headers)?)
            .send()
            .await?;
        self.parse_response(response, headers).await
    }

    async fn cancel(&self, next_uri: &str, headers: &mut Headers) -> Result<(), PrestinoError> {
        info!("Cancelling query with uri: {}", next_uri);
        let response = self
            .http_client
            .delete(next_uri)
            .headers(ReqwestClientConnection::make_request_headers(headers)?)
            .send()
            .await?;

        self.parse_response(response, headers).await.map(|_| ())
    }
}
