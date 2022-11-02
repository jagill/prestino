pub mod error;
pub mod results;
mod utils;

pub use error::Error;
use hyper::body::HttpBody as _;
use hyper::client::HttpConnector;
use hyper::Client;
use hyper::{Body, Method, Request, Response, Uri};

pub struct PrestoClient {
    base_url: String,
    http_client: Client<HttpConnector>,
}

impl PrestoClient {
    pub fn new(base_url: String) -> Self {
        PrestoClient {
            base_url,
            http_client: Client::new(),
        }
    }

    pub async fn post_query(&self, query: &str) -> Result<Response<Body>, Error> {
        let uri_str = format!("{}/v1/statement", &self.base_url);

        let req = Request::builder()
            .method(Method::POST)
            .uri(uri_str)
            .header("X-Trino-User", "jagill")
            // .header("content-type", "application/json")
            .body(Body::from(query.to_owned()))?;

        let result = self.http_client.request(req).await?;
        Ok(result)
    }

    // pub async fn get_next_results(&self, next_uri: &str) -> Result<Response<Body>, Error> {
    //     let req = Request::builder()
    //         .method(Method::POST)
    //         .uri(next_uri)
    //         .header("X-Trino-User", "jagill")
    //         // .header("content-type", "application/json")
    //         .body(Body::from(query.to_owned()))?;

    //     let result = self.http_client.request(req).await?;
    //     Ok(result)
    // }
}
