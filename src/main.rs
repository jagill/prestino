use futures::StreamExt;
use prestino::PrestoClient;
use tokio::io::AsyncWriteExt as _;
use futures_util::pin_mut;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let client = PrestoClient::new("http://localhost:8080".to_owned());

    println!("Iterate results");
    let executor = client.execute("SELECT 1 AS a".to_owned()).await?;
    let query_results = executor.query_results();
    pin_mut!(query_results);
    while let Some(result) = query_results.next().await {
        println!("Rows {:?}", result);
    }

    println!("Iterate batches");
    let executor = client.execute("SELECT 1 AS a".to_owned()).await?;
    let batches = executor.batches();
    pin_mut!(batches);
    while let Some(batch) = batches.next().await {
        println!("Batch {:?}", batch);
    }

    println!("Iterate rows");
    let executor = client.execute("SELECT 1 AS a".to_owned()).await?;
    let rows = executor.rows();
    pin_mut!(rows);
    while let Some(row) = rows.next().await {
        println!("Row {:?}", row);
    }
    Ok(())
}
