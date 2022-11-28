mod common;
use common::get_rows;
use prestino::PrestinoError;

#[tokio::test]
async fn test_syntax_error() {
    let result: Result<Vec<()>, PrestinoError> = get_rows("not good sql").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    match err {
        PrestinoError::QueryError(e) => {
            assert_eq!(e.error_name, "SYNTAX_ERROR");
            assert_eq!(e.error_type, "USER_ERROR");
        }
        _ => panic!("Unexpected error type: {err:?}"),
    }
}

#[tokio::test]
async fn test_missing_col_error() {
    let result: Result<Vec<()>, PrestinoError> = get_rows(
        r#"
        SELECT a FROM (VALUES 1) AS t(b)
    "#,
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    match err {
        PrestinoError::QueryError(e) => {
            assert_eq!(e.error_name, "COLUMN_NOT_FOUND");
            assert_eq!(e.error_type, "USER_ERROR");
        }
        _ => panic!("Unexpected error type: {err:?}"),
    }
}
