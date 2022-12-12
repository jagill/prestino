mod common;
use common::get_rows;
use maplit::hashmap;
use prestino::PrestinoError;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Deserialize)]
struct BasicTypes {
    b: bool,
    int8: i8,
    int16: i16,
    int32: i32,
    int64: i64,
    float32: f32,
    float64: f64,
    string: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct BasicTypesOption {
    b: Option<bool>,
    int8: Option<i8>,
    int16: Option<i16>,
    int32: Option<i32>,
    int64: Option<i64>,
    float32: Option<f32>,
    float64: Option<f64>,
    string: Option<String>,
}

#[tokio::test]
async fn test_basic_types() {
    let sql = r#"
    SELECT
        b,
        CAST(int8 AS TINYINT) AS int8,
        CAST(int16 AS SMALLINT) AS int16,
        CAST(int32 AS INTEGER) AS int32,
        CAST(int64 AS BIGINT) AS int64,
        CAST(float32 AS REAL) AS float32,
        CAST(float64 AS DOUBLE) AS float64,
        string
    FROM (
        VALUES (TRUE, 1, 2, 3, 4, 5.1, 6.1, 'cat')
    ) AS t(b, int8, int16, int32, int64, float32, float64, string)
    "#;

    let rows: Vec<(bool, i8, i16, i32, i64, f32, f64, String)> = get_rows(sql).await.unwrap();
    assert_eq!(rows, vec![(true, 1, 2, 3, 4, 5.1, 6.1, "cat".to_string())]);

    let rows2: Vec<BasicTypes> = get_rows(sql).await.unwrap();
    assert_eq!(
        rows2,
        vec![BasicTypes {
            b: true,
            int8: 1,
            int16: 2,
            int32: 3,
            int64: 4,
            float32: 5.1,
            float64: 6.1,
            string: "cat".to_owned(),
        }]
    );
}

#[tokio::test]
async fn test_basic_types_nullable() {
    let sql = r#"
    SELECT
        b,
        CAST(int8 AS TINYINT) AS int8,
        CAST(int16 AS SMALLINT) AS int16,
        CAST(int32 AS INTEGER) AS int32,
        CAST(int64 AS BIGINT) AS int64,
        CAST(float32 AS REAL) AS float32,
        CAST(float64 AS DOUBLE) AS float64,
        string
    FROM (
        VALUES
            (TRUE, 1, 2, 3, 4, 5.1, 6.1, 'cat'),
            (null, null, null, null, null, null, null, null)
    ) AS t(b, int8, int16, int32, int64, float32, float64, string)
    "#;

    let rows: Vec<(
        Option<bool>,
        Option<i8>,
        Option<i16>,
        Option<i32>,
        Option<i64>,
        Option<f32>,
        Option<f64>,
        Option<String>,
    )> = get_rows(sql).await.unwrap();
    assert_eq!(
        rows,
        vec![
            (
                Some(true),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5.1),
                Some(6.1),
                Some("cat".to_string())
            ),
            (None, None, None, None, None, None, None, None,)
        ]
    );

    let rows2: Vec<BasicTypesOption> = get_rows(sql).await.unwrap();
    assert_eq!(
        rows2,
        vec![
            BasicTypesOption {
                b: Some(true),
                int8: Some(1),
                int16: Some(2),
                int32: Some(3),
                int64: Some(4),
                float32: Some(5.1),
                float64: Some(6.1),
                string: Some("cat".to_owned()),
            },
            BasicTypesOption {
                b: None,
                int8: None,
                int16: None,
                int32: None,
                int64: None,
                float32: None,
                float64: None,
                string: None,
            },
        ]
    );
}

#[tokio::test]
async fn test_basic_types_nullable_non_option() {
    let sql = r#"
    SELECT
        b,
        CAST(int8 AS TINYINT) AS int8,
        CAST(int16 AS SMALLINT) AS int16,
        CAST(int32 AS INTEGER) AS int32,
        CAST(int64 AS BIGINT) AS int64,
        CAST(float32 AS REAL) AS float32,
        CAST(float64 AS DOUBLE) AS float64,
        string
    FROM (
        VALUES
            (TRUE, 1, 2, 3, 4, 5.1, 6.1, 'cat'),
            (null, null, null, null, null, null, null, null)
    ) AS t(b, int8, int16, int32, int64, float32, float64, string)
    "#;

    let result: Result<Vec<(bool, i8, i16, i32, i64, f32, f64, String)>, PrestinoError> =
        get_rows(sql).await;
    match result {
        Ok(_) => panic!("Failed to error on incorrect type deserialization."),
        Err(PrestinoError::HttpError(e)) => println!("Found right error: {e:?}"),
        Err(err) => panic!("Unexpected error: {err:?}"),
    }

    let result2: Result<Vec<BasicTypes>, PrestinoError> = get_rows(sql).await;
    match result2 {
        Ok(_) => panic!("Failed to error on incorrect type deserialization."),
        Err(PrestinoError::HttpError(e)) => println!("Found right error: {e:?}"),
        Err(err) => panic!("Unexpected error: {err:?}"),
    }
}

#[tokio::test]
async fn test_array_types() {
    let sql = r#"
        SELECT
            ARRAY[true, false] AS bool_arr,
            ARRAY[true, null] AS bool_opt_arr,
            ARRAY[ARRAY[1, 2], ARRAY[], ARRAY[3]] as int_arr_arr
    "#;

    let rows: Vec<(Vec<bool>, Vec<Option<bool>>, Vec<Vec<i32>>)> = get_rows(sql).await.unwrap();
    assert_eq!(
        rows,
        vec![(
            vec![true, false],
            vec![Some(true), None],
            vec![vec![1, 2], vec![], vec![3]],
        )]
    );

    #[derive(Debug, PartialEq, Deserialize)]
    struct ArrayRow {
        bool_arr: Vec<bool>,
        bool_opt_arr: Vec<Option<bool>>,
        int_arr_arr: Vec<Vec<i32>>,
    }
    let rows2: Vec<ArrayRow> = get_rows(sql).await.unwrap();
    assert_eq!(
        rows2,
        vec![ArrayRow {
            bool_arr: vec![true, false],
            bool_opt_arr: vec![Some(true), None],
            int_arr_arr: vec![vec![1, 2], vec![], vec![3]],
        }]
    );
}

#[tokio::test]
async fn test_map_types() {
    let sql = r#"
        SELECT
            MAP(ARRAY[1, 2], ARRAY[true, false]) AS map_,
            MAP(ARRAY[1, 2], ARRAY[true, null]) AS map_opt,
            MAP(ARRAY[1, 2], ARRAY[
                MAP(ARRAY['a', 'b'], ARRAY[true, false]), MAP()
            ]) AS map_map
    "#;

    let rows: Vec<(
        HashMap<i32, bool>,
        HashMap<i32, Option<bool>>,
        HashMap<i32, HashMap<String, bool>>,
    )> = get_rows(sql).await.unwrap();
    let expected: Vec<(
        HashMap<i32, bool>,
        HashMap<i32, Option<bool>>,
        HashMap<i32, HashMap<String, bool>>,
    )> = vec![(
        hashmap! {1 => true, 2 => false},
        hashmap! {1 => Some(true), 2 => None},
        hashmap! {1 => hashmap!{"a".to_owned() => true, "b".to_owned() => false}, 2 => hashmap!{}},
    )];
    assert_eq!(rows, expected);

    #[derive(Debug, PartialEq, Deserialize)]
    struct MapRow {
        map_: HashMap<i32, bool>,
        map_opt: HashMap<i32, Option<bool>>,
        map_map: HashMap<i32, HashMap<String, bool>>,
    }

    let rows2: Vec<MapRow> = get_rows(sql).await.unwrap();
    assert_eq!(
        rows2,
        vec![MapRow {
            map_: hashmap! {1 => true, 2 => false},
            map_opt: hashmap! {1 => Some(true), 2 => None},
            map_map: hashmap! {1 => hashmap!{"a".to_owned() => true, "b".to_owned() => false}, 2 => hashmap!{}},
        }]
    );
}

#[tokio::test]
async fn test_struct_types() {
    let sql = r#"
        SELECT
            CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)) AS s1,
            CAST(ROW('cat', null) AS ROW(s VARCHAR, b BOOLEAN)) AS s2
    "#;

    #[derive(Debug, PartialEq, Deserialize)]
    struct Struct1 {
        x: i64,
        y: f64,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Struct2 {
        s: String,
        b: Option<bool>,
    }

    let rows: Vec<(Struct1, Struct2)> = get_rows(sql).await.unwrap();
    let expected: Vec<(Struct1, Struct2)> = vec![(
        Struct1 { x: 1, y: 2.0 },
        Struct2 {
            s: "cat".to_string(),
            b: None,
        },
    )];
    assert_eq!(rows, expected);

    #[derive(Debug, PartialEq, Deserialize)]
    struct StructRow {
        s1: Struct1,
        s2: Struct2,
    }

    let rows2: Vec<StructRow> = get_rows(sql).await.unwrap();
    assert_eq!(
        rows2,
        vec![StructRow {
            s1: Struct1 { x: 1, y: 2.0 },
            s2: Struct2 {
                s: "cat".to_string(),
                b: None
            }
        }]
    );
}
