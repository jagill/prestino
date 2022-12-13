mod response_chain;
mod response_set_1;

use crate::{PrestinoError, PrestoClient};
use futures::TryStreamExt;
use futures_util::pin_mut;
use response_chain::ResponseChain;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use wiremock::MockServer;

async fn get_rows<T: DeserializeOwned>(response_strs: &[&str]) -> Result<Vec<T>, PrestinoError> {
    let mock_server = MockServer::start().await;
    let base_uri = mock_server.uri();
    println!("{base_uri}");
    let responses = ResponseChain::new(response_strs, base_uri);
    responses.mock_flow(&mock_server).await;

    let presto_client = PrestoClient::trino(mock_server.uri()).user("me");
    let executor = presto_client.execute("test".to_string()).await.unwrap();

    let stream = executor.rows();
    pin_mut!(stream);
    let rows: Result<Vec<T>, PrestinoError> = stream.try_collect().await;
    rows
}

#[tokio::test]
async fn test_basic_flow_1() {
    let rows = get_rows::<Value>(response_set_1::RESPONSES).await;
    assert_eq!(rows.unwrap(), vec![json!([1])],);
}

#[tokio::test]
async fn test_basic_types() {
    let response_strs: Vec<String> = ResponseChain::make_response_set(
        &[
            ("a_bool", "boolean"),
            ("a_int8", "tinyint"),
            ("a_int16", "smallint"),
            ("a_int32", "integer"),
            ("a_int64", "bigint"),
            ("a_float32", "real"),
            ("a_float64", "double"),
            ("a_str", "varchar"),
        ],
        &[
            json!([
                [true, 1, 2, 3, 4, 1.1, 2.2, "a"],
                [false, -1, -2, -3, -4, -1.1, -2.2, "b"]
            ]),
            json!([[null, null, null, null, null, null, null, null]]),
        ],
    );
    let response_ref: Vec<&str> = response_strs.iter().map(AsRef::as_ref).collect();
    let rows: Result<Vec<Value>, PrestinoError> = get_rows(&response_ref).await;
    assert_eq!(
        rows.unwrap(),
        vec![
            json!([true, 1, 2, 3, 4, 1.1, 2.2, "a"]),
            json!([false, -1, -2, -3, -4, -1.1, -2.2, "b"]),
            json!([null, null, null, null, null, null, null, null]),
        ],
    );
}
