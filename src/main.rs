use futures::StreamExt;
use futures_util::pin_mut;
use prestino::{PrestinoClient, StatementExecutor};
use serde_json::Value;

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
    pub fn output(&self, kind: &str, val: impl std::fmt::Debug) {
        println!(">>> {} {:?}", kind, val);
    }
}

async fn run(host: &str, query: &str, stream_mode: StreamMode) -> Result<(), anyhow::Error> {
    let client = PrestinoClient::trino(host).user("jagill");
    let executor: StatementExecutor<Value> = client.execute(query).await?;
    let outputter = Outputter {};

    match stream_mode {
        StreamMode::Response => {
            println!(">> Iterate responses");
            let stream = executor.responses();
            pin_mut!(stream);
            while let Some(response_res) = stream.next().await {
                let (rows, stats) = response_res?;
                outputter.output("STATS ", stats);
                for row in rows {
                    outputter.output("ROW ", row);
                }
            }
        }
        StreamMode::Batch => {
            println!(">> Iterate batches");
            let stream = executor.batches();
            pin_mut!(stream);
            while let Some(items) = stream.next().await {
                for item in items? {
                    outputter.output("ROW", &item);
                }
            }
        }
        StreamMode::Row => {
            println!(">> Iterate rows");
            let stream = executor.rows();
            pin_mut!(stream);
            while let Some(item) = stream.next().await {
                outputter.output("ROW", &item?);
            }
        }
    }

    Ok(())
}
