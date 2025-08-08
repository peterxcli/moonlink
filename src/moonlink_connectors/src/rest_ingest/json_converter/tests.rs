use super::*;
use arrow::datatypes;
use arrow_schema::{DataType, Field, Schema};
use moonlink::row::RowValue;
use serde_json::json;
use std::sync::Arc;

fn make_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("field1", DataType::Int32, false),
        Field::new("field2", DataType::Utf8, false),
    ]))
}

fn make_date_time_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("date_field", DataType::Date32, false),
        Field::new(
            "time_field",
            DataType::Time64(datatypes::TimeUnit::Microsecond),
            false,
        ),
        Field::new(
            "timestamp_field",
            DataType::Timestamp(datatypes::TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "timestamptz_field",
            DataType::Timestamp(datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]))
}

#[test]
fn test_successful_conversion() {
    let schema = make_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);
    let input = json!({
        "field1": 42,
        "field2": "hello"
    });
    let row = converter.convert(&input).unwrap();
    assert_eq!(row.values.len(), 2);
    assert_eq!(row.values[0], RowValue::Int32(42));
    assert_eq!(row.values[1], RowValue::ByteArray(b"hello".to_vec()));
}

#[test]
fn test_missing_field() {
    let schema = make_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);
    let input = json!({
        "field1": 42
    });
    let err = converter.convert(&input).unwrap_err();
    match err {
        JsonToMoonlinkRowError::MissingField(field) => assert_eq!(field, "field2"),
        _ => panic!("unexpected error: {err:?}"),
    }
}

#[test]
fn test_type_mismatch() {
    let schema = make_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);
    let input = json!({
        "field1": "not a number",
        "field2": "hello"
    });
    let err = converter.convert(&input).unwrap_err();
    match err {
        JsonToMoonlinkRowError::TypeMismatch(field) => assert_eq!(field, "field1"),
        _ => panic!("unexpected error: {err:?}"),
    }
}

#[test]
fn test_date_conversion() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);
    let input = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15T14:30:25Z",
        "timestamptz_field": "2023-01-15T14:30:25.123Z"
    });
    let row = converter.convert(&input).unwrap();
    assert_eq!(row.values.len(), 4);

    assert_eq!(row.values[0], RowValue::Int32(19372));
    assert_eq!(row.values[1], RowValue::Int64(52225000000));
    assert_eq!(row.values[2], RowValue::Int64(1673793025000000));
    assert_eq!(row.values[3], RowValue::Int64(1673793025123000));
}

#[test]
fn test_time_with_fraction() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);
    let input = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25.123456",
        "timestamp_field": "2023-01-15T14:30:25Z",
        "timestamptz_field": "2023-01-15T14:30:25.123Z"
    });
    let row = converter.convert(&input).unwrap();
    assert_eq!(row.values[1], RowValue::Int64(52225123456));
}

#[test]
fn test_timestamp_formats() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);

    let input1 = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15T14:30:25Z",
        "timestamptz_field": "2023-01-15T14:30:25Z"
    });
    let row1 = converter.convert(&input1).unwrap();
    assert_eq!(row1.values[2], RowValue::Int64(1673793025000000));
    assert_eq!(row1.values[3], RowValue::Int64(1673793025000000));

    let input2 = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15T14:30:25",
        "timestamptz_field": "2023-01-15T14:30:25"
    });
    let row2 = converter.convert(&input2).unwrap();
    assert_eq!(row2.values[2], RowValue::Int64(1673793025000000));
    assert_eq!(row2.values[3], RowValue::Int64(1673793025000000));

    let input3 = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15 14:30:25+00:00",
        "timestamptz_field": "2023-01-15 14:30:25+00:00"
    });
    let row3 = converter.convert(&input3).unwrap();
    assert_eq!(row3.values[2], RowValue::Int64(1673793025000000));
    assert_eq!(row3.values[3], RowValue::Int64(1673793025000000));
}

#[test]
fn test_invalid_date_format() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);
    let input = json!({
        "date_field": "2023/01/15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15T14:30:25Z",
        "timestamptz_field": "2023-01-15T14:30:25.123Z"
    });
    let err = converter.convert(&input).unwrap_err();
    match err {
        JsonToMoonlinkRowError::InvalidDateFormat(field, _) => assert_eq!(field, "date_field"),
        _ => panic!("unexpected error: {err:?}"),
    }
}

#[test]
fn test_invalid_time_format() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);
    let input = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30",
        "timestamp_field": "2023-01-15T14:30:25Z",
        "timestamptz_field": "2023-01-15T14:30:25.123Z"
    });
    let err = converter.convert(&input).unwrap_err();
    match err {
        JsonToMoonlinkRowError::InvalidTimeFormat(field, _) => assert_eq!(field, "time_field"),
        _ => panic!("unexpected error: {err:?}"),
    }
}

#[test]
fn test_invalid_timestamp_format() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);
    let input = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "invalid-timestamp",
        "timestamptz_field": "2023-01-15T14:30:25.123Z"
    });
    let err = converter.convert(&input).unwrap_err();
    match err {
        JsonToMoonlinkRowError::InvalidTimestampFormat(field, _) => {
            assert_eq!(field, "timestamp_field")
        }
        _ => panic!("unexpected error: {err:?}"),
    }
}

#[test]
fn test_additional_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("int64_field", DataType::Int64, false),
        Field::new("float32_field", DataType::Float32, false),
        Field::new("float64_field", DataType::Float64, false),
        Field::new("bool_field", DataType::Boolean, false),
    ]));
    let converter = JsonToMoonlinkRowConverter::new(schema);
    let input = json!({
        "int64_field": 9223372036854775807i64,
        "float32_field": 42.5,
        "float64_field": 123.456,
        "bool_field": true
    });
    let row = converter.convert(&input).unwrap();
    assert_eq!(row.values.len(), 4);
    assert_eq!(row.values[0], RowValue::Int64(9223372036854775807i64));
    assert_eq!(row.values[1], RowValue::Float32(42.5));
    assert_eq!(row.values[2], RowValue::Float64(123.456));
    assert_eq!(row.values[3], RowValue::Bool(true));
}

#[test]
fn test_timestamp_with_timezone_offsets() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);

    let input1 = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15T14:30:25+05:00",
        "timestamptz_field": "2023-01-15T14:30:25+05:00"
    });
    let row1 = converter.convert(&input1).unwrap();
    assert_eq!(row1.values[2], RowValue::Int64(1673775025000000));
    assert_eq!(row1.values[3], RowValue::Int64(1673775025000000));

    let input2 = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15T14:30:25-08:00",
        "timestamptz_field": "2023-01-15T14:30:25-08:00"
    });
    let row2 = converter.convert(&input2).unwrap();
    assert_eq!(row2.values[2], RowValue::Int64(1673821825000000));
    assert_eq!(row2.values[3], RowValue::Int64(1673821825000000));
}

#[test]
fn test_timestamp_without_t_separator() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);

    let input = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15 14:30:25",
        "timestamptz_field": "2023-01-15 14:30:25.123"
    });
    let row = converter.convert(&input).unwrap();
    assert_eq!(row.values[2], RowValue::Int64(1673793025000000));
    assert_eq!(row.values[3], RowValue::Int64(1673793025123000));
}

#[test]
fn test_date_edge_cases() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);

    let input1 = json!({
        "date_field": "1970-01-01",
        "time_field": "00:00:00",
        "timestamp_field": "1970-01-01T00:00:00Z",
        "timestamptz_field": "1970-01-01T00:00:00Z"
    });
    let row1 = converter.convert(&input1).unwrap();
    assert_eq!(row1.values[0], RowValue::Int32(0));
    assert_eq!(row1.values[1], RowValue::Int64(0));
    assert_eq!(row1.values[2], RowValue::Int64(0));
    assert_eq!(row1.values[3], RowValue::Int64(0));

    let input2 = json!({
        "date_field": "2024-02-29",
        "time_field": "23:59:59.999999",
        "timestamp_field": "2024-02-29T23:59:59.999999Z",
        "timestamptz_field": "2024-02-29T23:59:59.999999Z"
    });
    let row2 = converter.convert(&input2).unwrap();
    assert_eq!(row2.values[0], RowValue::Int32(19782));
    assert_eq!(row2.values[1], RowValue::Int64(86399999999));
}

#[test]
fn test_time_edge_cases() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);

    let input1 = json!({
        "date_field": "2023-01-15",
        "time_field": "00:00:00",
        "timestamp_field": "2023-01-15T00:00:00Z",
        "timestamptz_field": "2023-01-15T00:00:00Z"
    });
    let row1 = converter.convert(&input1).unwrap();
    assert_eq!(row1.values[1], RowValue::Int64(0));

    let input2 = json!({
        "date_field": "2023-01-15",
        "time_field": "23:59:59.999999",
        "timestamp_field": "2023-01-15T23:59:59.999999Z",
        "timestamptz_field": "2023-01-15T23:59:59.999999Z"
    });
    let row2 = converter.convert(&input2).unwrap();
    assert_eq!(row2.values[1], RowValue::Int64(86399999999));

    let input3 = json!({
        "date_field": "2023-01-15",
        "time_field": "12:00:00",
        "timestamp_field": "2023-01-15T12:00:00Z",
        "timestamptz_field": "2023-01-15T12:00:00Z"
    });
    let row3 = converter.convert(&input3).unwrap();
    assert_eq!(row3.values[1], RowValue::Int64(43200000000));
}

#[test]
fn test_timestamp_millisecond_precision() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);

    let input = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25.001",
        "timestamp_field": "2023-01-15T14:30:25.001Z",
        "timestamptz_field": "2023-01-15T14:30:25.001Z"
    });
    let row = converter.convert(&input).unwrap();
    assert_eq!(row.values[1], RowValue::Int64(52225001000));
    assert_eq!(row.values[2], RowValue::Int64(1673793025001000));
    assert_eq!(row.values[3], RowValue::Int64(1673793025001000));
}

#[test]
fn test_timestamp_microsecond_precision() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);

    let input = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25.123456",
        "timestamp_field": "2023-01-15T14:30:25.123456Z",
        "timestamptz_field": "2023-01-15T14:30:25.123456Z"
    });
    let row = converter.convert(&input).unwrap();
    assert_eq!(row.values[1], RowValue::Int64(52225123456));
    assert_eq!(row.values[2], RowValue::Int64(1673793025123456));
    assert_eq!(row.values[3], RowValue::Int64(1673793025123456));
}

#[test]
fn test_timestamp_traditional_format_with_offset() {
    let schema = make_date_time_schema();
    let converter = JsonToMoonlinkRowConverter::new(schema);

    let input1 = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15 14:30:25.123+0530",
        "timestamptz_field": "2023-01-15 14:30:25.123+0530"
    });
    let row1 = converter.convert(&input1).unwrap();
    assert_eq!(row1.values[2], RowValue::Int64(1673773225123000));
    assert_eq!(row1.values[3], RowValue::Int64(1673773225123000));

    let input2 = json!({
        "date_field": "2023-01-15",
        "time_field": "14:30:25",
        "timestamp_field": "2023-01-15 14:30:25.123-05:30",
        "timestamptz_field": "2023-01-15 14:30:25.123-05:30"
    });
    let row2 = converter.convert(&input2).unwrap();
    assert_eq!(row2.values[2], RowValue::Int64(1673812825123000));
    assert_eq!(row2.values[3], RowValue::Int64(1673812825123000));
}