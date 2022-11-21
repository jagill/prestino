mod response_chain;
mod response_set_1;

#[cfg(test)]
mod tests {
    use super::response_chain::ResponseChain;
    use crate::{Error, PrestoClient};
    use futures::TryStreamExt;
    use futures_util::pin_mut;
    use serde_json::{json, Value};
    use wiremock::MockServer;

    #[tokio::test]
    async fn test_basic_flow_1() {
        let mock_server = MockServer::start().await;
        let base_uri = mock_server.uri();
        println!("{base_uri}");
        let responses = ResponseChain::new(super::response_set_1::RESPONSES, base_uri);
        responses.mock_flow(&mock_server).await;

        let presto_client = PrestoClient::new(mock_server.uri());
        let executor = presto_client.execute("test".to_string()).await.unwrap();

        let stream = executor.rows();
        pin_mut!(stream);
        let rows: Result<Vec<Value>, Error> = stream.try_collect().await;
        assert_eq!(rows.unwrap(), vec![json!([1])],);
    }
}
