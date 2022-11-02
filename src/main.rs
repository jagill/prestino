use hyper::body::HttpBody as _;
use hyper::body::HttpBody as _;
use prestino::results::QueryResultsValue;
use prestino::PrestoClient;
use tokio::io::{stdout, AsyncWriteExt as _};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let client = PrestoClient::new("http://localhost:8080".to_owned());
    let mut resp = client.post_query("SELECT 1 AS a").await?;

    let status = resp.status();
    println!("Response: {}", status);
    match status.as_u16() {
        200 => (),
        503 => unimplemented!(),
        code => return Err(anyhow::anyhow!("Unexpected response code {code}")),
    }

    let mut data = Vec::new();
    while let Some(chunk) = resp.body_mut().data().await {
        data.extend(&chunk?);
    }

    let parsed: QueryResultsValue = serde_json::from_slice(&data)?;
    println!("{:?}", parsed);

    Ok(())
}
