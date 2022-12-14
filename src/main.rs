use futures::StreamExt;
use futures_util::pin_mut;
use prestino::PrestinoClient;
use serde_json::Value;
use tokio::io::AsyncWriteExt as _;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let host = "http://localhost:8080";
    let query = "SELECT 1 AS a";

    run(host, query, StreamMode::Response).await?;
    run(host, query, StreamMode::Batch).await?;
    run(host, query, StreamMode::Row).await?;
    Ok(())
}

#[derive(Debug, Copy, Clone)]
enum StreamMode {
    Response,
    Batch,
    Row,
}

struct Outputter {}

impl Outputter {
    pub fn output(&self, val: &Value) {
        println!(">>> Row {:?}", val);
    }
}

async fn run(host: &str, query: &str, stream_mode: StreamMode) -> Result<(), anyhow::Error> {
    let client = PrestinoClient::trino(host.to_owned()).user("jagill");
    let executor = client.execute(query.to_owned()).await?;
    let outputter = Outputter {};

    match stream_mode {
        StreamMode::Response => {
            println!("Iterate responses");
            let stream = executor.responses();
            pin_mut!(stream);
            while let Some(items) = stream.next().await {
                for item in items? {
                    outputter.output(&item);
                }
            }
        }
        StreamMode::Batch => {
            println!("Iterate batches");
            let stream = executor.batches();
            pin_mut!(stream);
            while let Some(items) = stream.next().await {
                for item in items? {
                    outputter.output(&item);
                }
            }
        }
        StreamMode::Row => {
            println!("Iterate rows");
            let executor = client.execute(query.to_owned()).await?;
            let stream = executor.rows();
            pin_mut!(stream);
            while let Some(item) = stream.next().await {
                outputter.output(&item?);
            }
        }
    }

    Ok(())
}
