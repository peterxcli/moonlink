use arrow_schema::{DataType, Field, Schema};
use moonlink::row::{MoonlinkRow, RowValue};
use serde_json::Value;
use std::sync::Arc;

use super::{
    error::JsonToMoonlinkRowError,
    type_converters,
};

pub struct JsonToMoonlinkRowConverter {
    schema: Arc<Schema>,
}

impl JsonToMoonlinkRowConverter {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }

    pub fn convert(&self, json: &Value) -> Result<MoonlinkRow, JsonToMoonlinkRowError> {
        let mut values = Vec::with_capacity(self.schema.fields.len());
        for field in &self.schema.fields {
            let field_name = field.name();
            let value = json
                .get(field_name)
                .ok_or_else(|| JsonToMoonlinkRowError::MissingField(field_name.clone()))?;
            let row_value = Self::convert_value(field, value)?;
            values.push(row_value);
        }
        Ok(MoonlinkRow::new(values))
    }

    fn convert_value(field: &Field, value: &Value) -> Result<RowValue, JsonToMoonlinkRowError> {
        use DataType::*;
        
        let field_name = field.name();
        
        match field.data_type() {
            Int32 => type_converters::convert_int32(value, field_name),
            Int64 => type_converters::convert_int64(value, field_name),
            Float32 => type_converters::convert_float32(value, field_name),
            Float64 => type_converters::convert_float64(value, field_name),
            Boolean => type_converters::convert_bool(value, field_name),
            Utf8 => type_converters::convert_string(value, field_name),
            Date32 => type_converters::convert_date(value, field_name),
            Time64(_) => type_converters::convert_time(value, field_name),
            Timestamp(_, _) => type_converters::convert_timestamp(value, field_name),
            _ => Err(JsonToMoonlinkRowError::TypeMismatch(field_name.clone())),
        }
    }
}