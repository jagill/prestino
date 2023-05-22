use prestino::PrestinoClient;
use test_log::test;

#[test(tokio::test)]
async fn test_create_select_delete_drop() {
    // Do a full loop of creating a table with data, selecting from it,
    // deleting rows, and dropping the table

    let client = PrestinoClient::trino("http://localhost:8080").user("me");

    client
        .execute_collect::<()>("DROP TABLE IF EXISTS memory.default.my_table")
        .await
        .unwrap();

    let rows: Vec<(i64,)> = client
        .execute_collect(
            r#"
    CREATE TABLE memory.default.my_table AS
    SELECT * FROM (
        VALUES
            (1, 'a'),
            (2, 'b'),
            (3, 'c')
    ) AS t (id, name)
    "#,
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![(3i64,)]);

    let rows: Vec<(i64, String)> = client
        .execute_collect("SELECT id, name FROM memory.default.my_table")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, "a".to_owned()),
            (2, "b".to_owned()),
            (3, "c".to_owned())
        ]
    );

    let rows: Vec<()> = client
        .execute_collect("DROP TABLE memory.default.my_table")
        .await
        .unwrap();
    assert_eq!(rows, Vec::new());
}

#[test(tokio::test)]
async fn test_str_ref_statement() {
    let client = PrestinoClient::trino("http://localhost:8080").user("me");
    let rows: Vec<(i64,)> = client.execute_collect("SELECT 1 AS a").await.unwrap();
    assert_eq!(rows, vec![(1i64,)]);
}

#[test(tokio::test)]
async fn test_string_statement() {
    let client = PrestinoClient::trino("http://localhost:8080").user("me");
    let rows: Vec<(i64,)> = client
        .execute_collect("SELECT 1 AS a")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1i64,)]);
}
