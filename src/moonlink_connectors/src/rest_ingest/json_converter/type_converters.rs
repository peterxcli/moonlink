use moonlink::row::RowValue;
use serde_json::Value;
use super::error::JsonToMoonlinkRowError;

pub fn convert_int32(value: &Value, field_name: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    value.as_i64()
        .map(|i| RowValue::Int32(i as i32))
        .ok_or_else(|| JsonToMoonlinkRowError::TypeMismatch(field_name.to_string()))
}

pub fn convert_int64(value: &Value, field_name: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    value.as_i64()
        .map(RowValue::Int64)
        .ok_or_else(|| JsonToMoonlinkRowError::TypeMismatch(field_name.to_string()))
}

pub fn convert_float32(value: &Value, field_name: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    value.as_f64()
        .map(|f| RowValue::Float32(f as f32))
        .ok_or_else(|| JsonToMoonlinkRowError::TypeMismatch(field_name.to_string()))
}

pub fn convert_float64(value: &Value, field_name: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    value.as_f64()
        .map(RowValue::Float64)
        .ok_or_else(|| JsonToMoonlinkRowError::TypeMismatch(field_name.to_string()))
}

pub fn convert_bool(value: &Value, field_name: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    value.as_bool()
        .map(RowValue::Bool)
        .ok_or_else(|| JsonToMoonlinkRowError::TypeMismatch(field_name.to_string()))
}

pub fn convert_string(value: &Value, field_name: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    value.as_str()
        .map(|s| RowValue::ByteArray(s.as_bytes().to_vec()))
        .ok_or_else(|| JsonToMoonlinkRowError::TypeMismatch(field_name.to_string()))
}

pub fn convert_date(value: &Value, field_name: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    let date_str = value.as_str()
        .ok_or_else(|| JsonToMoonlinkRowError::TypeMismatch(field_name.to_string()))?;
    
    super::datetime_parser::parse_date(field_name, date_str)
}

pub fn convert_time(value: &Value, field_name: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    let time_str = value.as_str()
        .ok_or_else(|| JsonToMoonlinkRowError::TypeMismatch(field_name.to_string()))?;
    
    super::datetime_parser::parse_time(field_name, time_str)
}

pub fn convert_timestamp(value: &Value, field_name: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    let timestamp_str = value.as_str()
        .ok_or_else(|| JsonToMoonlinkRowError::TypeMismatch(field_name.to_string()))?;
    
    super::datetime_parser::parse_timestamp(field_name, timestamp_str)
}