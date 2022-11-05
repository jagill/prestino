use prestino::PrestoClient;
use tokio::io::{stdout, AsyncWriteExt as _};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let client = PrestoClient::new("http://localhost:8080".to_owned());
    let mut executor = client.execute("SELECT 1 AS a".to_owned()).await?;
    executor.run().await?;
    Ok(())
}
