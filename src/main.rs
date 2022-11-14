use std::marker::PhantomData;

use futures::StreamExt;
use futures_util::pin_mut;
use prestino::{results::Column, PrestoClient};
use serde_json::Value;
use tokio::io::AsyncWriteExt as _;

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

enum OutputFormat {
    JSONL,
    XSV { header: bool, delimiter: String },
}

enum OutputMode {
    Response,
    Batch,
    Row,
}

struct Outputter {
    output_format: OutputFormat,
    columns: Vec<Column>,
}

impl Outputter {
    pub fn maybe_output_header(&self) {
        if let OutputFormat::XSV {
            header: true,
            delimiter,
        } = self.output_format
        {
            println!(
                "{}",
                self.columns
                    .iter()
                    .map(|col| col.name())
                    .collect::<Vec<&str>>()
                    .join(&delimiter)
            );
        }
    }
    pub fn output(&self, val: Value) {}
}

async fn run(
    host: &str,
    query: &str,
    output_format: OutputFormat,
    output_mode: OutputMode,
) -> Result<(), anyhow::Error> {
    let client = PrestoClient::new(host.to_owned());
    let executor = client.execute(query.to_owned()).await?;
    let stream = match output_mode {
        OutputMode::Response => executor.query_results(),
        OutputMode::Batch => executor.batches(),
        OutputMode::Row => executor.rows(),
    };
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
        println!("Rows {:?}", result);
    }

    Ok(())
}
