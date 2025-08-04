use crate::error::Error;
use crate::row::RowValue;
use arrow::array::builder::{
    BinaryBuilder, BooleanBuilder, ListBuilder, NullBufferBuilder, PrimitiveBuilder, StringBuilder,
    StructBuilder,
};
use arrow::array::types::{Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type};
use arrow::array::{ArrayBuilder, ArrayRef, FixedSizeBinaryBuilder, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::kernels::cast;
use arrow::datatypes::DataType;
use std::any::Any;
use std::mem::take;
use std::sync::Arc;

pub(crate) struct ArrayBuilderHelper {
    offset_builder: Vec<i32>,
    null_builder: NullBufferBuilder,
}

impl ArrayBuilderHelper {
    fn push(&mut self, value: i32) {
        self.offset_builder.push(value);
        self.null_builder.append_non_null();
    }
    fn push_null(&mut self) {
        self.offset_builder
            .push(self.offset_builder.last().copied().unwrap_or(0));
        self.null_builder.append_null();
    }
}
/// A column array builder that can handle different types
pub(crate) enum ColumnArrayBuilder {
    Boolean(BooleanBuilder, Option<ArrayBuilderHelper>),
    Int32(PrimitiveBuilder<Int32Type>, Option<ArrayBuilderHelper>),
    Int64(PrimitiveBuilder<Int64Type>, Option<ArrayBuilderHelper>),
    Float32(PrimitiveBuilder<Float32Type>, Option<ArrayBuilderHelper>),
    Float64(PrimitiveBuilder<Float64Type>, Option<ArrayBuilderHelper>),
    Decimal128(PrimitiveBuilder<Decimal128Type>, Option<ArrayBuilderHelper>),
    Utf8(StringBuilder, Option<ArrayBuilderHelper>),
    FixedSizeBinary(FixedSizeBinaryBuilder, Option<ArrayBuilderHelper>),
    Binary(BinaryBuilder, Option<ArrayBuilderHelper>),
    Struct(StructBuilder, Option<ArrayBuilderHelper>),
}

impl ColumnArrayBuilder {
    /// Create a new column array builder for a specific data type
    pub(crate) fn new(data_type: &DataType, capacity: usize, is_list: bool) -> Self {
        let array_builder = if is_list {
            Some(ArrayBuilderHelper {
                offset_builder: Vec::with_capacity(capacity),
                null_builder: NullBufferBuilder::new(capacity),
            })
        } else {
            None
        };
        match data_type {
            DataType::Boolean => {
                ColumnArrayBuilder::Boolean(BooleanBuilder::with_capacity(capacity), array_builder)
            }
            DataType::Int16 | DataType::Int32 | DataType::Date32 => ColumnArrayBuilder::Int32(
                PrimitiveBuilder::<Int32Type>::with_capacity(capacity),
                array_builder,
            ),
            DataType::Timestamp(_, _) | DataType::Int64 | DataType::Time64(_) => {
                ColumnArrayBuilder::Int64(
                    PrimitiveBuilder::<Int64Type>::with_capacity(capacity),
                    array_builder,
                )
            }
            DataType::Float32 => ColumnArrayBuilder::Float32(
                PrimitiveBuilder::<Float32Type>::with_capacity(capacity),
                array_builder,
            ),
            DataType::Float64 => ColumnArrayBuilder::Float64(
                PrimitiveBuilder::<Float64Type>::with_capacity(capacity),
                array_builder,
            ),
            DataType::Utf8 => ColumnArrayBuilder::Utf8(
                StringBuilder::with_capacity(capacity, capacity * 10),
                array_builder,
            ),
            DataType::FixedSizeBinary(_size) => {
                assert_eq!(*_size, 16);
                ColumnArrayBuilder::FixedSizeBinary(
                    FixedSizeBinaryBuilder::with_capacity(capacity, 16),
                    array_builder,
                )
            }
            DataType::Decimal128(precision, scale) => ColumnArrayBuilder::Decimal128(
                PrimitiveBuilder::<Decimal128Type>::with_capacity(capacity)
                    .with_precision_and_scale(*precision, *scale)
                    .expect("Failed to create Decimal128Type"),
                array_builder,
            ),
            DataType::Binary => ColumnArrayBuilder::Binary(
                BinaryBuilder::with_capacity(capacity, capacity * 10),
                array_builder,
            ),
            DataType::List(inner) => ColumnArrayBuilder::new(inner.data_type(), capacity, true),
            DataType::Struct(fields) => {
                // Use Arrow's built-in field builder creation for better compatibility
                ColumnArrayBuilder::Struct(
                    StructBuilder::from_fields(fields.clone(), capacity),
                    array_builder,
                )
            }
            _ => panic!("data type: {data_type:?}"),
        }
    }
    /// Append a value to this builder
    pub(crate) fn append_value(&mut self, value: &RowValue) -> Result<(), Error> {
        match self {
            ColumnArrayBuilder::Boolean(builder, array_helper) => {
                match value {
                    RowValue::Bool(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Bool(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Bool expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Bool expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Int32(builder, array_helper) => {
                match value {
                    RowValue::Int32(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Int32(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Int32 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Int32 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Int64(builder, array_helper) => {
                match value {
                    RowValue::Int64(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Int64(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Int64 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Int64 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Float32(builder, array_helper) => {
                match value {
                    RowValue::Float32(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Float32(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Float32 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Float32 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Float64(builder, array_helper) => {
                match value {
                    RowValue::Float64(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Float64(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Float64 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Float64 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Decimal128(builder, array_helper) => {
                match value {
                    RowValue::Decimal(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Decimal(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Decimal128 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Decimal128 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Utf8(builder, array_helper) => {
                match value {
                    RowValue::ByteArray(v) => {
                        builder.append_value(unsafe { std::str::from_utf8_unchecked(v) })
                    }
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::ByteArray(v) => builder
                                    .append_value(unsafe { std::str::from_utf8_unchecked(v) }),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("ByteArray expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("ByteArray expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::FixedSizeBinary(builder, array_helper) => {
                match value {
                    RowValue::FixedLenByteArray(v) => builder.append_value(v)?,
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::FixedLenByteArray(v) => builder.append_value(v)?,
                                RowValue::Null => builder.append_null(),
                                _ => {
                                    unreachable!("FixedLenByteArray expected from well-typed input")
                                }
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("FixedLenByteArray expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Binary(builder, array_helper) => {
                match value {
                    RowValue::ByteArray(v) => builder.append_value(v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::ByteArray(v) => builder.append_value(v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("ByteArray expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("ByteArray expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Struct(builder, array_helper) => {
                match value {
                    RowValue::Struct(fields) => {
                        // Append each field value to the corresponding field builder
                        for i in 0..builder.num_fields() {
                            if i < fields.len() {
                                // Try to append the field value based on the field type
                                if let Some(int_builder) =
                                    builder.field_builder::<PrimitiveBuilder<Int32Type>>(i)
                                {
                                    match &fields[i] {
                                        RowValue::Int32(v) => int_builder.append_value(*v),
                                        RowValue::Null => int_builder.append_null(),
                                        _ => {
                                            return Err(Error::TypeError(
                                                "Expected Int32".to_string(),
                                            ))
                                        }
                                    }
                                } else if let Some(string_builder) =
                                    builder.field_builder::<StringBuilder>(i)
                                {
                                    match &fields[i] {
                                        RowValue::ByteArray(v) => {
                                            string_builder.append_value(unsafe {
                                                std::str::from_utf8_unchecked(v)
                                            });
                                        }
                                        RowValue::Null => string_builder.append_null(),
                                        _ => {
                                            return Err(Error::TypeError(
                                                "Expected ByteArray".to_string(),
                                            ))
                                        }
                                    }
                                } else if let Some(bool_builder) =
                                    builder.field_builder::<BooleanBuilder>(i)
                                {
                                    match &fields[i] {
                                        RowValue::Bool(v) => bool_builder.append_value(*v),
                                        RowValue::Null => bool_builder.append_null(),
                                        _ => {
                                            return Err(Error::TypeError(
                                                "Expected Bool".to_string(),
                                            ))
                                        }
                                    }
                                } else if let Some(int64_builder) =
                                    builder.field_builder::<PrimitiveBuilder<Int64Type>>(i)
                                {
                                    match &fields[i] {
                                        RowValue::Int64(v) => int64_builder.append_value(*v),
                                        RowValue::Null => int64_builder.append_null(),
                                        _ => {
                                            return Err(Error::TypeError(
                                                "Expected Int64".to_string(),
                                            ))
                                        }
                                    }
                                } else if let Some(float32_builder) =
                                    builder.field_builder::<PrimitiveBuilder<Float32Type>>(i)
                                {
                                    match &fields[i] {
                                        RowValue::Float32(v) => float32_builder.append_value(*v),
                                        RowValue::Null => float32_builder.append_null(),
                                        _ => {
                                            return Err(Error::TypeError(
                                                "Expected Float32".to_string(),
                                            ))
                                        }
                                    }
                                } else if let Some(float64_builder) =
                                    builder.field_builder::<PrimitiveBuilder<Float64Type>>(i)
                                {
                                    match &fields[i] {
                                        RowValue::Float64(v) => float64_builder.append_value(*v),
                                        RowValue::Null => float64_builder.append_null(),
                                        _ => {
                                            return Err(Error::TypeError(
                                                "Expected Float64".to_string(),
                                            ))
                                        }
                                    }
                                } else if let Some(nested_struct_builder) =
                                    builder.field_builder::<StructBuilder>(i)
                                {
                                    // Handle nested structs
                                    match &fields[i] {
                                        RowValue::Struct(nested_fields) => {
                                            // Append each nested field value
                                            for j in 0..nested_struct_builder.num_fields() {
                                                if j < nested_fields.len() {
                                                    // Handle nested field types
                                                    if let Some(nested_int_builder) = nested_struct_builder.field_builder::<PrimitiveBuilder<Int32Type>>(j) {
                                                    match &nested_fields[j] {
                                                        RowValue::Int32(v) => nested_int_builder.append_value(*v),
                                                        RowValue::Null => nested_int_builder.append_null(),
                                                        _ => return Err(Error::TypeError("Expected nested Int32".to_string())),
                                                    }
                                                } else if let Some(nested_string_builder) = nested_struct_builder.field_builder::<StringBuilder>(j) {
                                                    match &nested_fields[j] {
                                                        RowValue::ByteArray(v) => {
                                                            nested_string_builder.append_value(unsafe { std::str::from_utf8_unchecked(v) });
                                                        }
                                                        RowValue::Null => nested_string_builder.append_null(),
                                                        _ => return Err(Error::TypeError("Expected nested ByteArray".to_string())),
                                                    }
                                                } else if let Some(nested_bool_builder) = nested_struct_builder.field_builder::<BooleanBuilder>(j) {
                                                    match &nested_fields[j] {
                                                        RowValue::Bool(v) => nested_bool_builder.append_value(*v),
                                                        RowValue::Null => nested_bool_builder.append_null(),
                                                        _ => return Err(Error::TypeError("Expected nested Bool".to_string())),
                                                    }
                                                } else {
                                                    // For unsupported nested field types, append null
                                                    println!("Unsupported nested field type at index {}", j);
                                                }
                                                } else {
                                                    // If we don't have enough nested fields, append null
                                                    if let Some(nested_int_builder) = nested_struct_builder.field_builder::<PrimitiveBuilder<Int32Type>>(j) {
                                                    nested_int_builder.append_null();
                                                } else if let Some(nested_string_builder) = nested_struct_builder.field_builder::<StringBuilder>(j) {
                                                    nested_string_builder.append_null();
                                                } else if let Some(nested_bool_builder) = nested_struct_builder.field_builder::<BooleanBuilder>(j) {
                                                    nested_bool_builder.append_null();
                                                }
                                                }
                                            }
                                            nested_struct_builder.append(true);
                                        }
                                        RowValue::Null => {
                                            // Append null to all nested field builders
                                            for j in 0..nested_struct_builder.num_fields() {
                                                if let Some(nested_int_builder) = nested_struct_builder.field_builder::<PrimitiveBuilder<Int32Type>>(j) {
                                                nested_int_builder.append_null();
                                            } else if let Some(nested_string_builder) = nested_struct_builder.field_builder::<StringBuilder>(j) {
                                                nested_string_builder.append_null();
                                            } else if let Some(nested_bool_builder) = nested_struct_builder.field_builder::<BooleanBuilder>(j) {
                                                nested_bool_builder.append_null();
                                            }
                                            }
                                            nested_struct_builder.append(false);
                                        }
                                        _ => {
                                            return Err(Error::TypeError(
                                                "Expected nested Struct".to_string(),
                                            ))
                                        }
                                    }
                                } else if let Some(list_builder) =
                                    builder.field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(i)
                                {
                                    // Handle list fields
                                    match &fields[i] {
                                        RowValue::Array(elements) => {
                                            // Append each element to the list
                                            for element in elements {
                                                match element {
                                                    RowValue::Int32(v) => {
                                                        // Downcast the values builder to Int32Builder
                                                        if let Some(int_builder) = list_builder.values().as_any_mut().downcast_mut::<PrimitiveBuilder<Int32Type>>() {
                                                        int_builder.append_value(*v);
                                                    } else {
                                                        return Err(Error::TypeError("Expected Int32Builder in list".to_string()));
                                                    }
                                                    }
                                                    RowValue::Null => {
                                                        // Downcast the values builder to Int32Builder
                                                        if let Some(int_builder) = list_builder.values().as_any_mut().downcast_mut::<PrimitiveBuilder<Int32Type>>() {
                                                        int_builder.append_null();
                                                    } else {
                                                        return Err(Error::TypeError("Expected Int32Builder in list".to_string()));
                                                    }
                                                    }
                                                    _ => {
                                                        return Err(Error::TypeError(
                                                            "Expected Int32 in list".to_string(),
                                                        ))
                                                    }
                                                }
                                            }
                                            list_builder.append(true);
                                        }
                                        RowValue::Null => {
                                            list_builder.append(false);
                                        }
                                        _ => {
                                            return Err(Error::TypeError(
                                                "Expected Array".to_string(),
                                            ))
                                        }
                                    }
                                } else {
                                    // For unsupported field types, append null
                                    // TODO: Add support for more field types like List, Struct, etc.
                                }
                            } else {
                                // If we don't have enough fields, append null to all remaining field builders
                                if let Some(int_builder) =
                                    builder.field_builder::<PrimitiveBuilder<Int32Type>>(i)
                                {
                                    int_builder.append_null();
                                } else if let Some(string_builder) =
                                    builder.field_builder::<StringBuilder>(i)
                                {
                                    string_builder.append_null();
                                } else if let Some(bool_builder) =
                                    builder.field_builder::<BooleanBuilder>(i)
                                {
                                    bool_builder.append_null();
                                } else if let Some(int64_builder) =
                                    builder.field_builder::<PrimitiveBuilder<Int64Type>>(i)
                                {
                                    int64_builder.append_null();
                                } else if let Some(float32_builder) =
                                    builder.field_builder::<PrimitiveBuilder<Float32Type>>(i)
                                {
                                    float32_builder.append_null();
                                } else if let Some(float64_builder) =
                                    builder.field_builder::<PrimitiveBuilder<Float64Type>>(i)
                                {
                                    float64_builder.append_null();
                                } else if let Some(nested_struct_builder) =
                                    builder.field_builder::<StructBuilder>(i)
                                {
                                    // Append null to all nested field builders
                                    for j in 0..nested_struct_builder.num_fields() {
                                        if let Some(nested_int_builder) =
                                            nested_struct_builder
                                                .field_builder::<PrimitiveBuilder<Int32Type>>(j)
                                        {
                                            nested_int_builder.append_null();
                                        } else if let Some(nested_string_builder) =
                                            nested_struct_builder.field_builder::<StringBuilder>(j)
                                        {
                                            nested_string_builder.append_null();
                                        } else if let Some(nested_bool_builder) =
                                            nested_struct_builder.field_builder::<BooleanBuilder>(j)
                                        {
                                            nested_bool_builder.append_null();
                                        }
                                    }
                                    nested_struct_builder.append(false);
                                } else if let Some(list_builder) =
                                    builder.field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(i)
                                {
                                    list_builder.append(false);
                                } else {
                                    // For any other unsupported field types, we can't append null directly
                                    // This will cause the struct to fail, but it's better than panicking
                                    println!("Cannot append null to field at index {} - unsupported type", i);
                                }
                            }
                        }
                        // Finish the struct row - append true to indicate a non-null struct
                        builder.append(true);
                    }
                    RowValue::Null => {
                        // For null structs, we need to append null to all field builders
                        for i in 0..builder.num_fields() {
                            if let Some(int_builder) =
                                builder.field_builder::<PrimitiveBuilder<Int32Type>>(i)
                            {
                                int_builder.append_null();
                            } else if let Some(string_builder) =
                                builder.field_builder::<StringBuilder>(i)
                            {
                                string_builder.append_null();
                            } else if let Some(bool_builder) =
                                builder.field_builder::<BooleanBuilder>(i)
                            {
                                bool_builder.append_null();
                            } else if let Some(int64_builder) =
                                builder.field_builder::<PrimitiveBuilder<Int64Type>>(i)
                            {
                                int64_builder.append_null();
                            } else if let Some(float32_builder) =
                                builder.field_builder::<PrimitiveBuilder<Float32Type>>(i)
                            {
                                float32_builder.append_null();
                            } else if let Some(float64_builder) =
                                builder.field_builder::<PrimitiveBuilder<Float64Type>>(i)
                            {
                                float64_builder.append_null();
                            } else if let Some(nested_struct_builder) =
                                builder.field_builder::<StructBuilder>(i)
                            {
                                // Append null to all nested field builders
                                for j in 0..nested_struct_builder.num_fields() {
                                    if let Some(nested_int_builder) =
                                        nested_struct_builder
                                            .field_builder::<PrimitiveBuilder<Int32Type>>(j)
                                    {
                                        nested_int_builder.append_null();
                                    } else if let Some(nested_string_builder) =
                                        nested_struct_builder.field_builder::<StringBuilder>(j)
                                    {
                                        nested_string_builder.append_null();
                                    } else if let Some(nested_bool_builder) =
                                        nested_struct_builder.field_builder::<BooleanBuilder>(j)
                                    {
                                        nested_bool_builder.append_null();
                                    }
                                }
                                nested_struct_builder.append(false);
                            } else if let Some(list_builder) =
                                builder.field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(i)
                            {
                                list_builder.append(false);
                            } else {
                                // TODO: Add support for more field types like List, Struct, etc.
                            }
                        }
                        // Append false to indicate a null struct
                        builder.append(false);
                    }
                    _ => unreachable!("Struct expected from well-typed input"),
                };
                Ok(())
            }
        }
    }
    /// Finish building and return the array
    pub(crate) fn finish(&mut self, logical_type: &DataType) -> ArrayRef {
        let (array, array_helper): (ArrayRef, &mut Option<ArrayBuilderHelper>) = match self {
            ColumnArrayBuilder::Boolean(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Int32(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Int64(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Float32(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Float64(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Decimal128(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Utf8(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::FixedSizeBinary(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Binary(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Struct(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
        };
        if let Some(helper) = array_helper.as_mut() {
            let mut offset_array = take(&mut helper.offset_builder);
            offset_array.push(array.len() as i32);
            let null_array = helper.null_builder.finish();
            let inner_field = match logical_type {
                DataType::List(inner) => inner,
                _ => panic!("List expected from well-typed input"),
            };
            let list_array = ListArray::new(
                inner_field.clone(),
                OffsetBuffer::new(offset_array.into()),
                array,
                null_array,
            );
            Arc::new(list_array)
        } else {
            cast(&array, logical_type).unwrap_or_else(|_| {
                panic!(
                    "Fail to cast to correct type in ColumnArrayBuilder::finish for {logical_type:?}"
                )
            })
        }
    }
}

impl ArrayBuilder for ColumnArrayBuilder {
    fn len(&self) -> usize {
        match self {
            ColumnArrayBuilder::Boolean(b, _) => b.len(),
            ColumnArrayBuilder::Int32(b, _) => b.len(),
            ColumnArrayBuilder::Int64(b, _) => b.len(),
            ColumnArrayBuilder::Float32(b, _) => b.len(),
            ColumnArrayBuilder::Float64(b, _) => b.len(),
            ColumnArrayBuilder::Decimal128(b, _) => b.len(),
            ColumnArrayBuilder::Utf8(b, _) => b.len(),
            ColumnArrayBuilder::FixedSizeBinary(b, _) => b.len(),
            ColumnArrayBuilder::Binary(b, _) => b.len(),
            ColumnArrayBuilder::Struct(b, _) => b.len(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        // We need to determine the logical type for finish
        // For now, we'll use a placeholder - this should be improved
        let logical_type = match self {
            ColumnArrayBuilder::Boolean(_, _) => DataType::Boolean,
            ColumnArrayBuilder::Int32(_, _) => DataType::Int32,
            ColumnArrayBuilder::Int64(_, _) => DataType::Int64,
            ColumnArrayBuilder::Float32(_, _) => DataType::Float32,
            ColumnArrayBuilder::Float64(_, _) => DataType::Float64,
            ColumnArrayBuilder::Decimal128(_, _) => DataType::Decimal128(38, 0), // Default precision/scale
            ColumnArrayBuilder::Utf8(_, _) => DataType::Utf8,
            ColumnArrayBuilder::FixedSizeBinary(_, _) => DataType::FixedSizeBinary(16),
            ColumnArrayBuilder::Binary(_, _) => DataType::Binary,
            ColumnArrayBuilder::Struct(_, _) => {
                DataType::Struct(Vec::<arrow::datatypes::Field>::new().into())
            } // Placeholder
        };
        self.finish(&logical_type)
    }

    fn finish_cloned(&self) -> ArrayRef {
        // This is a simplified implementation - in practice you might want to clone the builder
        panic!("finish_cloned not implemented for ColumnArrayBuilder")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray,
    };
    use arrow::datatypes::DataType;
    #[test]
    fn test_column_array_builder() {
        // Test Int32 type
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, 2, false);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Int32(2)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 2);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert_eq!(int32_array.value(1), 2);

        // Test Int64 type
        let mut builder = ColumnArrayBuilder::new(&DataType::Int64, 2, false);
        builder.append_value(&RowValue::Int64(100)).unwrap();
        builder.append_value(&RowValue::Int64(200)).unwrap();
        let array = builder.finish(&DataType::Int64);
        assert_eq!(array.len(), 2);
        let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 100);
        assert_eq!(int64_array.value(1), 200);

        // Test Float32 type
        let mut builder = ColumnArrayBuilder::new(&DataType::Float32, 2, false);
        builder
            .append_value(&RowValue::Float32(std::f32::consts::PI))
            .unwrap();
        builder
            .append_value(&RowValue::Float32(std::f32::consts::E))
            .unwrap();
        let array = builder.finish(&DataType::Float32);
        assert_eq!(array.len(), 2);
        let float32_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((float32_array.value(0) - std::f32::consts::PI).abs() < 0.0001);
        assert!((float32_array.value(1) - std::f32::consts::E).abs() < 0.0001);

        // Test Float64 type
        let mut builder = ColumnArrayBuilder::new(&DataType::Float64, 2, false);
        builder
            .append_value(&RowValue::Float64(std::f64::consts::PI))
            .unwrap();
        builder
            .append_value(&RowValue::Float64(std::f64::consts::E))
            .unwrap();
        let array = builder.finish(&DataType::Float64);
        assert_eq!(array.len(), 2);
        let float64_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((float64_array.value(0) - std::f64::consts::PI).abs() < 0.00001);
        assert!((float64_array.value(1) - std::f64::consts::E).abs() < 0.00001);

        // Test Boolean type
        let mut builder = ColumnArrayBuilder::new(&DataType::Boolean, 2, false);
        builder.append_value(&RowValue::Bool(true)).unwrap();
        builder.append_value(&RowValue::Bool(false)).unwrap();
        let array = builder.finish(&DataType::Boolean);
        assert_eq!(array.len(), 2);
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));

        // Test Utf8 (ByteArray) type
        let mut builder = ColumnArrayBuilder::new(&DataType::Utf8, 2, false);
        builder
            .append_value(&RowValue::ByteArray("hello".as_bytes().to_vec()))
            .unwrap();
        builder
            .append_value(&RowValue::ByteArray("world".as_bytes().to_vec()))
            .unwrap();
        let array = builder.finish(&DataType::Utf8);
        assert_eq!(array.len(), 2);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "hello");
        assert_eq!(string_array.value(1), "world");

        // Test FixedSizeBinary type
        let mut builder = ColumnArrayBuilder::new(&DataType::FixedSizeBinary(16), 2, false);
        let bytes1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let bytes2 = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
        builder
            .append_value(&RowValue::FixedLenByteArray(bytes1))
            .unwrap();
        builder
            .append_value(&RowValue::FixedLenByteArray(bytes2))
            .unwrap();
        let array = builder.finish(&DataType::FixedSizeBinary(16));
        assert_eq!(array.len(), 2);
        let binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(binary_array.value(0), bytes1);
        assert_eq!(binary_array.value(1), bytes2);

        // Test null values
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, 3, false);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Null).unwrap();
        builder.append_value(&RowValue::Int32(3)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 3);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert!(int32_array.is_null(1));
        assert_eq!(int32_array.value(2), 3);

        // Test using null values directly from RowValue::Null
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, 3, false);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Null).unwrap();
        builder.append_value(&RowValue::Int32(3)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 3);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert!(int32_array.is_null(1));
        assert_eq!(int32_array.value(2), 3);
    }

    #[test]
    fn test_column_array_builder_list() {
        // Test List<Int32> type
        let mut builder = ColumnArrayBuilder::new(
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            2,
            true,
        );

        // Add a list of integers [1, 2, 3]
        builder
            .append_value(&RowValue::Array(vec![
                RowValue::Int32(1),
                RowValue::Int32(2),
                RowValue::Int32(3),
            ]))
            .unwrap();

        // Add another list of integers [4, 5]
        builder
            .append_value(&RowValue::Array(vec![
                RowValue::Int32(4),
                RowValue::Int32(5),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int32,
            true,
        ))));

        assert_eq!(array.len(), 2);
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();

        // Check first list [1, 2, 3]
        let first_list = list_array.value(0);
        let first_int_array = first_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(first_int_array.len(), 3);
        assert_eq!(first_int_array.value(0), 1);
        assert_eq!(first_int_array.value(1), 2);
        assert_eq!(first_int_array.value(2), 3);

        // Check second list [4, 5]
        let second_list = list_array.value(1);
        let second_int_array = second_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(second_int_array.len(), 2);
        assert_eq!(second_int_array.value(0), 4);
        assert_eq!(second_int_array.value(1), 5);
    }

    #[test]
    fn test_column_array_builder_struct() {
        // Test struct with primitive types
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new("id", DataType::Int32, true)),
            Arc::new(arrow::datatypes::Field::new("name", DataType::Utf8, true)),
            Arc::new(arrow::datatypes::Field::new(
                "active",
                DataType::Boolean,
                true,
            )),
        ];

        let mut builder =
            ColumnArrayBuilder::new(&DataType::Struct(struct_fields.clone().into()), 2, false);

        // Add a struct with values
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(1),
                RowValue::ByteArray(b"Alice".to_vec()),
                RowValue::Bool(true),
            ]))
            .unwrap();

        // Add another struct
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(2),
                RowValue::ByteArray(b"Bob".to_vec()),
                RowValue::Bool(false),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        assert_eq!(array.len(), 2);

        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Check first struct
        let id_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let active_array = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert_eq!(id_array.value(0), 1);
        assert_eq!(name_array.value(0), "Alice");
        assert!(active_array.value(0));

        assert_eq!(id_array.value(1), 2);
        assert_eq!(name_array.value(1), "Bob");
        assert!(!active_array.value(1));
    }

    #[test]
    fn test_column_array_builder_nested_struct() {
        // Test nested struct: {id: int32, info: {name: string, age: int32}}
        let inner_struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new("name", DataType::Utf8, true)),
            Arc::new(arrow::datatypes::Field::new("age", DataType::Int32, true)),
        ];

        let outer_struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new("id", DataType::Int32, true)),
            Arc::new(arrow::datatypes::Field::new(
                "info",
                DataType::Struct(inner_struct_fields.clone().into()),
                true,
            )),
        ];

        let mut builder = ColumnArrayBuilder::new(
            &DataType::Struct(outer_struct_fields.clone().into()),
            2,
            false,
        );

        // Add a nested struct
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(1),
                RowValue::Struct(vec![
                    RowValue::ByteArray(b"Alice".to_vec()),
                    RowValue::Int32(25),
                ]),
            ]))
            .unwrap();

        // Add another nested struct
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(2),
                RowValue::Struct(vec![
                    RowValue::ByteArray(b"Bob".to_vec()),
                    RowValue::Int32(30),
                ]),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::Struct(outer_struct_fields.into()));
        assert_eq!(array.len(), 2);

        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Check outer struct fields
        let id_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let info_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        // Check inner struct fields
        let name_array = info_array
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let age_array = info_array
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(age_array.value(0), 25);
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(age_array.value(1), 30);
    }

    #[test]
    fn test_column_array_builder_struct_with_list() {
        // Test struct with list field: {id: int32, scores: list<int32>}
        let list_field = Arc::new(arrow::datatypes::Field::new(
            "scores",
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            true,
        ));

        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new("id", DataType::Int32, true)),
            list_field,
        ];

        let mut builder =
            ColumnArrayBuilder::new(&DataType::Struct(struct_fields.clone().into()), 2, false);

        // Add a struct with list field
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(1),
                RowValue::Array(vec![
                    RowValue::Int32(85),
                    RowValue::Int32(90),
                    RowValue::Int32(88),
                ]),
            ]))
            .unwrap();

        // Add another struct with list field
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(2),
                RowValue::Array(vec![RowValue::Int32(92), RowValue::Int32(87)]),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        assert_eq!(array.len(), 2);

        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Check id field
        let id_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        // Check list field
        let scores_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // Check first list [85, 90, 88]
        let first_scores = scores_array.value(0);
        let first_scores_int = first_scores.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(first_scores_int.len(), 3);
        assert_eq!(first_scores_int.value(0), 85);
        assert_eq!(first_scores_int.value(1), 90);
        assert_eq!(first_scores_int.value(2), 88);

        // Check second list [92, 87]
        let second_scores = scores_array.value(1);
        let second_scores_int = second_scores.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(second_scores_int.len(), 2);
        assert_eq!(second_scores_int.value(0), 92);
        assert_eq!(second_scores_int.value(1), 87);
    }

    #[test]
    fn test_column_array_builder_struct_with_null() {
        // Test struct with null values
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new("id", DataType::Int32, true)),
            Arc::new(arrow::datatypes::Field::new("name", DataType::Utf8, true)),
            Arc::new(arrow::datatypes::Field::new("age", DataType::Int32, true)),
        ];

        let mut builder =
            ColumnArrayBuilder::new(&DataType::Struct(struct_fields.clone().into()), 3, false);

        // Add a struct with some null values
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(1),
                RowValue::ByteArray(b"Alice".to_vec()),
                RowValue::Null, // null age
            ]))
            .unwrap();

        // Add a struct with all values
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(2),
                RowValue::ByteArray(b"Bob".to_vec()),
                RowValue::Int32(30),
            ]))
            .unwrap();

        // Add a null struct
        builder.append_value(&RowValue::Null).unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        assert_eq!(array.len(), 3);

        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        let id_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let age_array = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Check first row
        assert_eq!(id_array.value(0), 1);
        assert_eq!(name_array.value(0), "Alice");
        assert!(age_array.is_null(0)); // age is null

        // Check second row
        assert_eq!(id_array.value(1), 2);
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(age_array.value(1), 30);

        // Check third row (null struct)
        assert!(id_array.is_null(2));
        assert!(name_array.is_null(2));
        assert!(age_array.is_null(2));
    }
}
