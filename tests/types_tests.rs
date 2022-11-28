use futures::TryStreamExt;
use futures_util::pin_mut;
use prestino::PrestoClient;

#[tokio::test]
async fn test_basic_types() {
    let sql = r#"
    SELECT
        b,
        CAST(i8 AS TINYINT) AS i8,
        CAST(i16 AS SMALLINT) AS i16,
        CAST(i32 AS INTEGER) AS i32,
        CAST(i64 AS BIGINT) AS i64,
        CAST(f32 AS REAL) AS f32,
        CAST(f64 AS DOUBLE) AS f64,
        str
    FROM (
        VALUES (TRUE, 1, 2, 3, 4, 5.1, 6.1, 'cat')
    ) AS t(b, i8, i16, i32, i64, f32, f64, str)
    "#;

    let client = PrestoClient::new("http://localhost:8080".to_owned());
    let executor = client.execute(sql.to_string()).await.unwrap();
    let stream = executor.rows();
    pin_mut!(stream);
    let mut rows: Vec<(bool, i8, i16, i32, i64, f32, f64, String)> =
        stream.try_collect().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.pop().unwrap(),
        (true, 1, 2, 3, 4, 5.1, 6.1, "cat".to_string())
    );
}
