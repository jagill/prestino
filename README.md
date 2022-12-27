# Prestino: Rust Presto/Trino client

**Status**: Experimental, but under active development.  Not yet ready for production use.  Bug reports or help on issues welcome.

Prestino intends to be a strongly-typed 100% Safe Rust client for [Presto](https://prestodb.io/) and [Trino](https://trino.io/).
Here, strongly-typed means that executed statements (in particular queries) will return a Stream of either Tuple or Struct rows.
A schema mismatch between the statement results and the Tuple/Struct will result in a run-time error, but code using the results
can rely on Rust's compile-time typing guarantees. For example, the following code would give a Stream of user-defined Structs,
including implementations.

```rs
use futures::TryStreamExt;
use futures_util::pin_mut;
use prestino::PrestoClient;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
struct MyRow {
    a: i64,
    b: Vec<String>.
    c: HashMap<String, Vec<bool>>
}

impl MyRow {
    pub fn foo(&self) -> &str {
        ...
    }
}

let client = PrestoClient::new("http://localhost:8080");

// If you want to stream the results by row, create a StatementExecutor
let executor = client.execute(r#"
    SELECT a, b, c
    FROM (...)
"#).await.unwrap();
// To stream row-by-row, use `.rows()`. You can also stream by batches with
// `.batches()`, or get detailed progress statistics with `.responses()`
stream = executor.rows();
pin_mut!(stream);
let rows: Vec<MyRow> = stream.try_collect().await.unwrap();
for my_row in rows {
    println!("{}", my_row.foo());
}


// Or if you just want all the results at once
let rows: Vec<MyRow> = client.execute_collect(r#"
    SELECT a, b, c
    FROM (...)
"#).await.unwrap();
```

It will also be able to be used as a CLI that is a drop-in replacement for the Presto/Trino CLI tool.

## Testing

To run the integration tests, or test your own library against a real Trino instance, use the
supplied docker-compose file (this will be noisy, so do it in a separate terminal):

```
prestino$ docker compose up
```

This creates a Trino instance in a Docker container, accessible via localhost:8080.  It has an
in-memory catalog, so you can create tables and select them, as well as a TPCH catalog to test
against standardized data.
