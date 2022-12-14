use prestino::{PrestinoClient, PrestinoError};
use serde::de::DeserializeOwned;

pub async fn get_rows<T: DeserializeOwned>(sql: &str) -> Result<Vec<T>, PrestinoError> {
    PrestinoClient::trino("http://localhost:8080".to_owned())
        .user("me")
        .execute_collect(sql.to_string())
        .await
}
