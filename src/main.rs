use prestino::PrestoClient;
use tokio::io::{stdout, AsyncWriteExt as _};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut client = PrestoClient::new("http://localhost:8080".to_owned());
    let mut results = client.query("SELECT 1 AS a").await?;

    Ok(())
}
