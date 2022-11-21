use crate::error::Error;
use crate::results::QueryResultsJson;
use hyper::body::HttpBody as _;
use hyper::client::HttpConnector;
use hyper::Client;
use hyper::{Body, Method, Request, Response, Uri};

pub struct PrestoApi {}

impl PrestoApi {
    pub async fn get_results(
        request: Request<Body>,
        http_client: &Client<HttpConnector>,
    ) -> Result<QueryResultsJson, Error> {
        let response = http_client.request(request).await?;
        Self::parse_response(response).await
    }

    async fn parse_response(mut response: Response<Body>) -> Result<QueryResultsJson, Error> {
        let status = response.status();
        match status.as_u16() {
            200 => (),
            503 => unimplemented!(),
            code => return Err(Error::from_status_code(code)),
        }

        let mut data = Vec::new();
        while let Some(chunk) = response.body_mut().data().await {
            data.extend(&chunk?);
        }

        println!(
            "Response Body: {}",
            String::from_utf8(data.clone()).unwrap()
        );

        Ok(serde_json::from_slice(&data)?)
    }

    pub fn post_statement_request(
        base_url: &str,
        statement: String,
    ) -> Result<Request<Body>, Error> {
        let uri_str = format!("{}/v1/statement", base_url);

        let request = Request::builder()
            .method(Method::POST)
            .uri(uri_str)
            .header("X-Trino-User", "jagill")
            // .header("content-type", "application/json")
            .body(Body::from(statement))?;

        Ok(request)
    }

    pub fn get_results_request(next_uri: Uri) -> Result<Request<Body>, Error> {
        println!("Getting next results: {}", next_uri);
        let request = Request::builder()
            .method(Method::GET)
            .uri(next_uri)
            .header("X-Trino-User", "jagill")
            .body(Body::empty())?;

        Ok(request)
    }
}
