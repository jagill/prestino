use crate::PrestinoError;
use async_trait::async_trait;
use bytes::Bytes;

mod reqwest_client_connection;
pub use reqwest_client_connection::ReqwestClientConnection;

#[async_trait]
pub trait ClientConnection {
    async fn post_statement(
        &mut self,
        statement_uri: &str,
        statement: &str,
    ) -> Result<Bytes, PrestinoError>;

    async fn get_next_results(&mut self, next_uri: &str) -> Result<Bytes, PrestinoError>;

    async fn cancel(&mut self, next_uri: &str) -> Result<(), PrestinoError>;
}
