use futures::TryStreamExt;
use futures_util::pin_mut;
use prestino::{PrestinoClient, PrestinoError};
use serde::de::DeserializeOwned;

pub async fn get_rows<T: DeserializeOwned>(sql: &str) -> Result<Vec<T>, PrestinoError> {
    let client = PrestinoClient::trino("http://localhost:8080".to_owned()).user("me");
    let executor = client.execute(sql.to_string()).await?;
    let stream = executor.rows();
    pin_mut!(stream);
    let rows: Result<Vec<T>, PrestinoError> = stream.try_collect().await;
    rows
}
