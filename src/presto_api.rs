use crate::results::QueryResults;
use crate::PrestinoError;
use reqwest::{Client, RequestBuilder, Response};
use serde::de::DeserializeOwned;

pub struct PrestoApi {}

impl PrestoApi {
    pub async fn get_results<T: DeserializeOwned>(
        request: RequestBuilder,
    ) -> Result<QueryResults<T>, PrestinoError> {
        let response = request.send().await?;
        Self::check_status(&response)?;
        // TODO: Make better error messages on json deser.  In particular, if there's a type error,
        // can we print out the row that causes the error?
        Ok(response.json().await?)
    }

    fn check_status(response: &Response) -> Result<(), PrestinoError> {
        match response.status().as_u16() {
            200 => Ok(()),
            503 => unimplemented!(),
            code => Err(PrestinoError::from_status_code(code)),
        }
    }

    pub fn post_statement_request(
        client: &Client,
        base_url: &str,
        statement: String,
    ) -> RequestBuilder {
        let uri_str = format!("{}/v1/statement", base_url);

        let request = client
            .post(uri_str)
            .header("X-Trino-User", "jagill")
            .body(statement);

        request
    }

    pub fn get_results_request(client: &Client, next_uri: &str) -> RequestBuilder {
        println!("Getting next results: {}", next_uri);
        let request = client.get(next_uri).header("X-Trino-User", "jagill");
        request
    }
}
