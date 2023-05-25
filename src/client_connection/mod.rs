use crate::Headers;
use crate::PrestinoError;
use async_trait::async_trait;
use bytes::Bytes;

mod reqwest_client_connection;
pub use reqwest_client_connection::ReqwestClientConnection;

#[async_trait]
pub trait ClientConnection {
    fn clone_boxed(&self) -> Box<dyn ClientConnection>;

    async fn post_statement(
        &self,
        statement_uri: &str,
        statement: &str,
        headers: &mut Headers,
    ) -> Result<Bytes, PrestinoError>;

    async fn get_next_results(
        &self,
        next_uri: &str,
        headers: &mut Headers,
    ) -> Result<Bytes, PrestinoError>;

    async fn cancel(&self, next_uri: &str, headers: &mut Headers) -> Result<(), PrestinoError>;
}
