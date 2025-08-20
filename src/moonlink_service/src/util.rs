use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;

// Convert field schemas to Arrow schema with proper field IDs (like PostgreSQL)
pub fn field_schemas_to_arrow_schema(
    schema_fields: &[moonlink_rpc::FieldSchema],
) -> Result<Schema, String> {
    let mut fields = Vec::with_capacity(schema_fields.len());

    for (field_id, field) in (0_i32..).zip(schema_fields.iter()) {
        let data_type = match field.data_type.as_str() {
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "string" | "text" => DataType::Utf8,
            "boolean" | "bool" => DataType::Boolean,
            "float32" => DataType::Float32,
            "float64" => DataType::Float64,
            other => return Err(format!("Unsupported data type: {other}")),
        };

        // Create field with metadata (like PostgreSQL does)
        let mut metadata = HashMap::new();
        metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());

        fields.push(Field::new(&field.name, data_type, field.nullable).with_metadata(metadata));
    }

    Ok(Schema::new(fields))
}
