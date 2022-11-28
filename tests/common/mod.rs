use futures::TryStreamExt;
use futures_util::pin_mut;
use prestino::{Error, PrestoClient};
use serde::de::DeserializeOwned;

pub async fn get_rows<T: DeserializeOwned>(sql: &str) -> Result<Vec<T>, Error> {
    let client = PrestoClient::new("http://localhost:8080".to_owned());
    let executor = client.execute(sql.to_string()).await?;
    let stream = executor.rows();
    pin_mut!(stream);
    let rows: Result<Vec<T>, Error> = stream.try_collect().await;
    rows
}

