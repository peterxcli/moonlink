use std::sync::Arc;

use arrow::array::builder::{
    BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, NullBufferBuilder, PrimitiveBuilder,
    StringBuilder,
};
use arrow::array::types::{Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type};
use arrow::array::{ArrayBuilder, ArrayRef, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::kernels::cast;
use arrow::datatypes::DataType;

use crate::error::Error;
use crate::row::RowValue;

pub(crate) struct ArrayBuilderHelper {
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
}

impl ArrayBuilderHelper {
    fn with_capacity(cap: usize) -> Self {
        let mut offsets = Vec::with_capacity(cap + 1);
        offsets.push(0);
        Self {
            offsets,
            nulls: NullBufferBuilder::new(cap),
        }
    }

    fn push(&mut self, current_len: i32) {
        self.offsets.push(current_len);
        self.nulls.append_non_null();
    }
    fn push_null(&mut self) {
        self.offsets.push(*self.offsets.last().unwrap_or(&0));
        self.nulls.append_null();
    }
}

trait ScalarAdapter {
    type Builder: ArrayBuilder;
    const ROW_KIND: &'static str;

    fn try_extract(v: &RowValue) -> Option<Self::Value>;
    fn append_scalar(builder: &mut Self::Builder, v: Self::Value);

    type Value;
}

macro_rules! impl_scalar_adapter {
    ($row:path, $builder:ty, $field:ident, $kind:literal, $val_ty:ty) => {
        impl ScalarAdapter for $builder {
            type Builder = $builder;
            type Value = $val_ty;
            const ROW_KIND: &'static str = $kind;

            #[inline]
            fn try_extract(v: &RowValue) -> Option<Self::Value> {
                if let $row(x) = v {
                    Some(*x)
                } else {
                    None
                }
            }

            #[inline]
            fn append_scalar(builder: &mut Self::Builder, v: Self::Value) {
                builder.append_value(v);
            }
        }
    };
}

impl_scalar_adapter!(RowValue::Bool, BooleanBuilder, value, "Bool", bool);
impl_scalar_adapter!(
    RowValue::Int32,
    PrimitiveBuilder<Int32Type>,
    value,
    "Int32",
    i32
);
impl_scalar_adapter!(
    RowValue::Int64,
    PrimitiveBuilder<Int64Type>,
    value,
    "Int64",
    i64
);
impl_scalar_adapter!(
    RowValue::Float32,
    PrimitiveBuilder<Float32Type>,
    value,
    "Float32",
    f32
);
impl_scalar_adapter!(
    RowValue::Float64,
    PrimitiveBuilder<Float64Type>,
    value,
    "Float64",
    f64
);
impl_scalar_adapter!(
    RowValue::Decimal,
    PrimitiveBuilder<Decimal128Type>,
    value,
    "Decimal128",
    i128
);

// Special‑case adapters (Utf8, Binary, Fixed‑size) -------------------------

trait Utf8Adapter {
    fn append_utf8(&mut self, v: &RowValue) -> Result<(), Error>;
}

impl Utf8Adapter for StringBuilder {
    fn append_utf8(&mut self, v: &RowValue) -> Result<(), Error> {
        match v {
            RowValue::ByteArray(bytes) => {
                // SAFETY: upstream code guarantees valid UTF‑8 for Utf8 field.
                self.append_value(unsafe { std::str::from_utf8_unchecked(bytes) });
                Ok(())
            }
            RowValue::Null => {
                self.append_null();
                Ok(())
            }
            _ => unreachable!("ByteArray expected from well‑typed input"),
        }
    }
}

impl Utf8Adapter for BinaryBuilder {
    fn append_utf8(&mut self, v: &RowValue) -> Result<(), Error> {
        match v {
            RowValue::ByteArray(bytes) => {
                self.append_value(bytes);
                Ok(())
            }
            RowValue::Null => {
                self.append_null();
                Ok(())
            }
            _ => unreachable!("ByteArray expected from well‑typed input"),
        }
    }
}

impl Utf8Adapter for FixedSizeBinaryBuilder {
    fn append_utf8(&mut self, v: &RowValue) -> Result<(), Error> {
        match v {
            RowValue::FixedLenByteArray(b) => {
                self.append_value(*b)?;
                Ok(())
            }
            RowValue::Null => {
                self.append_null();
                Ok(())
            }
            _ => unreachable!("FixedLenByteArray expected from well‑typed input"),
        }
    }
}

pub enum NodeBuilder<'a> {
    Scalar(Box<dyn DynScalarBuilder + 'a>),
    List {
        helper: ArrayBuilderHelper,
        item: Box<NodeBuilder<'a>>,
    },
    Struct {
        nulls: NullBufferBuilder,
        fields: Vec<NodeBuilder<'a>>,
    },
}

/// Trait‑object wrapper so Scalar builders share a single enum variant.
pub(crate) trait DynScalarBuilder: Send + Sync {
    fn append(&mut self, v: &RowValue) -> Result<(), Error>;
    fn len(&self) -> usize;
    fn finish(&mut self) -> ArrayRef;
}

macro_rules! impl_dyn_scalar {
    ($builder:ty) => {
        impl DynScalarBuilder for $builder {
            fn append(&mut self, v: &RowValue) -> Result<(), Error> {
                if let Some(val) = <$builder as ScalarAdapter>::try_extract(v) {
                    <$builder as ScalarAdapter>::append_scalar(self, val);
                    Ok(())
                } else {
                    match v {
                        RowValue::Null => {
                            self.append_null();
                            Ok(())
                        }
                        _ => unreachable!(
                            "{} expected from well‑typed input",
                            <$builder as ScalarAdapter>::ROW_KIND
                        ),
                    }
                }
            }
            fn len(&self) -> usize {
                ArrayBuilder::len(self)
            }
            fn finish(&mut self) -> ArrayRef {
                Arc::new(self.finish())
            }
        }
    };
}

impl_dyn_scalar!(BooleanBuilder);
impl_dyn_scalar!(PrimitiveBuilder<Int32Type>);
impl_dyn_scalar!(PrimitiveBuilder<Int64Type>);
impl_dyn_scalar!(PrimitiveBuilder<Float32Type>);
impl_dyn_scalar!(PrimitiveBuilder<Float64Type>);
impl_dyn_scalar!(PrimitiveBuilder<Decimal128Type>);

impl DynScalarBuilder for StringBuilder {
    fn append(&mut self, v: &RowValue) -> Result<(), Error> {
        self.append_utf8(v)
    }
    fn len(&self) -> usize {
        ArrayBuilder::len(self)
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
impl DynScalarBuilder for BinaryBuilder {
    fn append(&mut self, v: &RowValue) -> Result<(), Error> {
        self.append_utf8(v)
    }
    fn len(&self) -> usize {
        ArrayBuilder::len(self)
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
impl DynScalarBuilder for FixedSizeBinaryBuilder {
    fn append(&mut self, v: &RowValue) -> Result<(), Error> {
        self.append_utf8(v)
    }
    fn len(&self) -> usize {
        ArrayBuilder::len(self)
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<'a> NodeBuilder<'a> {
    /// Build a tree that mirrors `DataType` exactly.
    pub fn new(dt: &DataType, capacity: usize) -> Self {
        match dt {
            // Primitive / binary / string -----------------------------------
            DataType::Boolean => Self::scalar(BooleanBuilder::with_capacity(capacity)),
            DataType::Int16 | DataType::Int32 | DataType::Date32 => {
                Self::scalar(PrimitiveBuilder::<Int32Type>::with_capacity(capacity))
            }
            DataType::Int64 | DataType::Timestamp(_, _) | DataType::Time64(_) => {
                Self::scalar(PrimitiveBuilder::<Int64Type>::with_capacity(capacity))
            }
            DataType::Float32 => {
                Self::scalar(PrimitiveBuilder::<Float32Type>::with_capacity(capacity))
            }
            DataType::Float64 => {
                Self::scalar(PrimitiveBuilder::<Float64Type>::with_capacity(capacity))
            }
            DataType::Decimal128(p, s) => Self::scalar(
                PrimitiveBuilder::<Decimal128Type>::with_capacity(capacity)
                    .with_precision_and_scale(*p, *s)
                    .expect("decimal builder"),
            ),
            DataType::Utf8 => Self::scalar(StringBuilder::with_capacity(capacity, capacity * 8)),
            DataType::Binary => Self::scalar(BinaryBuilder::with_capacity(capacity, capacity * 8)),
            DataType::FixedSizeBinary(size) => {
                assert_eq!(*size, 16, "only 16‑byte fixed binaries supported");
                Self::scalar(FixedSizeBinaryBuilder::with_capacity(capacity, *size))
            }

            // List -----------------------------------------------------------
            DataType::List(inner) => Self::List {
                helper: ArrayBuilderHelper::with_capacity(capacity),
                item: Box::new(Self::new(inner.data_type(), capacity)),
            },

            // Struct ---------------------------------------------------------
            DataType::Struct(fields) => {
                let field_builders: Vec<NodeBuilder> = fields
                    .iter()
                    .map(|f| Self::new(f.data_type(), capacity))
                    .collect();
                let nulls = NullBufferBuilder::new(capacity);
                Self::Struct {
                    nulls,
                    fields: field_builders,
                }
            }

            _ => unimplemented!("{dt:?} not yet supported in NodeBuilder::new"),
        }
    }

    #[inline]
    fn scalar<B: DynScalarBuilder + 'a>(b: B) -> Self {
        Self::Scalar(Box::new(b))
    }

    /// Append one `RowValue` (scalar, list, struct, null).
    pub fn append(&mut self, v: &RowValue) -> Result<(), Error> {
        match (self, v) {
            // Scalar ----------------------------------------------------------
            (NodeBuilder::Scalar(ref mut b), _) => b.append(v),

            // List ------------------------------------------------------------
            (NodeBuilder::List { helper, item }, RowValue::Array(elems)) => {
                for elem in elems {
                    item.append(elem)?
                }
                helper.push(item.len() as i32);
                Ok(())
            }
            (NodeBuilder::List { helper, .. }, RowValue::Null) => {
                helper.push_null();
                Ok(())
            }

            // Struct ----------------------------------------------------------
            (NodeBuilder::Struct { nulls, fields }, RowValue::Struct(vals)) => {
                for (child, val) in fields.iter_mut().zip(vals) {
                    child.append(val)?;
                }
                nulls.append_non_null();
                Ok(())
            }
            (NodeBuilder::Struct { nulls, fields }, RowValue::Null) => {
                for child in fields {
                    child.append(&RowValue::Null)?
                }
                nulls.append_null();
                Ok(())
            }

            _ => unreachable!("type mismatch between builder and RowValue"),
        }
    }

    /// Number of scalar items produced so far (for List offset calculation).
    fn len(&self) -> usize {
        match self {
            NodeBuilder::Scalar(b) => b.len(),
            NodeBuilder::List { item, .. } => item.len(),
            NodeBuilder::Struct { nulls, .. } => nulls.len(),
        }
    }

    /// Finalise the whole tree, returning an `ArrayRef` that matches the original DataType.
    pub fn finish(&mut self, logical_type: &DataType) -> ArrayRef {
        match self {
            // Scalar ---------------------------------------------------------
            NodeBuilder::Scalar(b) => {
                let arr = b.finish();
                cast(&arr, logical_type)
                    .unwrap_or_else(|_| panic!("cast failed for {logical_type:?}"))
            }

            // List -----------------------------------------------------------
            NodeBuilder::List { helper, item } => {
                let inner_field = match logical_type {
                    DataType::List(inner) => inner,
                    _ => unreachable!(),
                };
                let mut offsets = std::mem::take(&mut helper.offsets);
                // We need n+1 offsets for n lists
                // If nulls.len() == offsets.len() - 1, we're good
                // Otherwise we need to add the final offset
                if helper.nulls.len() == offsets.len() {
                    // Missing the final offset
                    offsets.push(item.len() as i32);
                }
                let null_buf = helper.nulls.finish();
                let values = item.finish(inner_field.data_type());
                Arc::new(ListArray::new(
                    inner_field.clone(),
                    OffsetBuffer::new(offsets.into()),
                    values,
                    null_buf,
                ))
            }

            // Struct ---------------------------------------------------------
            NodeBuilder::Struct { nulls, fields } => {
                // Get the logical struct fields
                let struct_fields = match logical_type {
                    DataType::Struct(fields) => fields,
                    _ => unreachable!(),
                };
                // Finish children arrays
                let child_arrays: Vec<ArrayRef> = fields
                    .iter_mut()
                    .zip(struct_fields.iter())
                    .map(|(b, f)| b.finish(f.data_type()))
                    .collect();
                // Get validity bitmap
                let validity = nulls.finish();
                // Create the final struct array
                Arc::new(arrow::array::StructArray::new(
                    struct_fields.clone(),
                    child_arrays,
                    validity,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array,
        Int64Array, ListArray, StringArray, StructArray,
    };
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_column_array_builder() {
        // Test Int32 type
        let mut builder = NodeBuilder::new(&DataType::Int32, 2);
        builder.append(&RowValue::Int32(1)).unwrap();
        builder.append(&RowValue::Int32(2)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 2);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert_eq!(int32_array.value(1), 2);

        // Test Int64 type
        let mut builder = NodeBuilder::new(&DataType::Int64, 2);
        builder.append(&RowValue::Int64(100)).unwrap();
        builder.append(&RowValue::Int64(200)).unwrap();
        let array = builder.finish(&DataType::Int64);
        assert_eq!(array.len(), 2);
        let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 100);
        assert_eq!(int64_array.value(1), 200);

        // Test Float32 type
        let mut builder = NodeBuilder::new(&DataType::Float32, 2);
        builder
            .append(&RowValue::Float32(std::f32::consts::PI))
            .unwrap();
        builder
            .append(&RowValue::Float32(std::f32::consts::E))
            .unwrap();
        let array = builder.finish(&DataType::Float32);
        assert_eq!(array.len(), 2);
        let float32_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((float32_array.value(0) - std::f32::consts::PI).abs() < 0.0001);
        assert!((float32_array.value(1) - std::f32::consts::E).abs() < 0.0001);

        // Test Float64 type
        let mut builder = NodeBuilder::new(&DataType::Float64, 2);
        builder
            .append(&RowValue::Float64(std::f64::consts::PI))
            .unwrap();
        builder
            .append(&RowValue::Float64(std::f64::consts::E))
            .unwrap();
        let array = builder.finish(&DataType::Float64);
        assert_eq!(array.len(), 2);
        let float64_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((float64_array.value(0) - std::f64::consts::PI).abs() < 0.00001);
        assert!((float64_array.value(1) - std::f64::consts::E).abs() < 0.00001);

        // Test Boolean type
        let mut builder = NodeBuilder::new(&DataType::Boolean, 2);
        builder.append(&RowValue::Bool(true)).unwrap();
        builder.append(&RowValue::Bool(false)).unwrap();
        let array = builder.finish(&DataType::Boolean);
        assert_eq!(array.len(), 2);
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));

        // Test Utf8 (ByteArray) type
        let mut builder = NodeBuilder::new(&DataType::Utf8, 2);
        builder
            .append(&RowValue::ByteArray("hello".as_bytes().to_vec()))
            .unwrap();
        builder
            .append(&RowValue::ByteArray("world".as_bytes().to_vec()))
            .unwrap();
        let array = builder.finish(&DataType::Utf8);
        assert_eq!(array.len(), 2);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "hello");
        assert_eq!(string_array.value(1), "world");

        // Test FixedSizeBinary type
        let mut builder = NodeBuilder::new(&DataType::FixedSizeBinary(16), 2);
        let bytes1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let bytes2 = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
        builder
            .append(&RowValue::FixedLenByteArray(bytes1))
            .unwrap();
        builder
            .append(&RowValue::FixedLenByteArray(bytes2))
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
        let mut builder = NodeBuilder::new(&DataType::Int32, 3);
        builder.append(&RowValue::Int32(1)).unwrap();
        builder.append(&RowValue::Null).unwrap();
        builder.append(&RowValue::Int32(3)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 3);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert!(int32_array.is_null(1));
        assert_eq!(int32_array.value(2), 3);

        // Test using null values directly from RowValue::Null
        let mut builder = NodeBuilder::new(&DataType::Int32, 3);
        builder.append(&RowValue::Int32(1)).unwrap();
        builder.append(&RowValue::Null).unwrap();
        builder.append(&RowValue::Int32(3)).unwrap();
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
        let mut builder = NodeBuilder::new(
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            2,
        );

        // Add a list of integers [1, 2, 3]
        builder
            .append(&RowValue::Array(vec![
                RowValue::Int32(1),
                RowValue::Int32(2),
                RowValue::Int32(3),
            ]))
            .unwrap();

        // Add another list of integers [4, 5]
        builder
            .append(&RowValue::Array(vec![
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
        use arrow::array::StructArray;

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

        let mut builder = NodeBuilder::new(&DataType::Struct(struct_fields.clone().into()), 2);

        // Add a struct with values
        builder
            .append(&RowValue::Struct(vec![
                RowValue::Int32(1),
                RowValue::ByteArray(b"Alice".to_vec()),
                RowValue::Bool(true),
            ]))
            .unwrap();

        // Add another struct
        builder
            .append(&RowValue::Struct(vec![
                RowValue::Int32(2),
                RowValue::ByteArray(b"Bob".to_vec()),
                RowValue::Bool(false),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        assert_eq!(array.len(), 2);

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.num_columns(), 3);

        // Check the id column
        let id_column = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 1);
        assert_eq!(id_column.value(1), 2);

        // Check the name column
        let name_column = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");

        // Check the active column
        let active_column = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active_column.value(0));
        assert!(!active_column.value(1));
    }

    #[test]
    fn test_column_array_builder_struct_with_nulls() {
        use arrow::array::StructArray;

        // Test struct with nulls
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new("id", DataType::Int32, true)),
            Arc::new(arrow::datatypes::Field::new(
                "score",
                DataType::Float64,
                true,
            )),
        ];

        let mut builder = NodeBuilder::new(&DataType::Struct(struct_fields.clone().into()), 3);

        // Add struct with all values
        builder
            .append(&RowValue::Struct(vec![
                RowValue::Int32(1),
                RowValue::Float64(95.5),
            ]))
            .unwrap();

        // Add struct with null field
        builder
            .append(&RowValue::Struct(vec![RowValue::Int32(2), RowValue::Null]))
            .unwrap();

        // Add null struct
        builder.append(&RowValue::Null).unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        assert_eq!(array.len(), 3);

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

        // Check struct validity
        assert!(!struct_array.is_null(0));
        assert!(!struct_array.is_null(1));
        assert!(struct_array.is_null(2));

        // Check values
        let id_column = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 1);
        assert_eq!(id_column.value(1), 2);
        assert!(id_column.is_null(2));

        let score_column = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(score_column.value(0), 95.5);
        assert!(score_column.is_null(1));
        assert!(score_column.is_null(2));
    }

    #[test]
    fn test_column_array_builder_struct_all_primitive_types() {
        use arrow::array::StructArray;

        // Test struct with all primitive types
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new(
                "bool_field",
                DataType::Boolean,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "int32_field",
                DataType::Int32,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "int64_field",
                DataType::Int64,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "float32_field",
                DataType::Float32,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "float64_field",
                DataType::Float64,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "decimal_field",
                DataType::Decimal128(10, 2),
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "string_field",
                DataType::Utf8,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "binary_field",
                DataType::Binary,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "fixed_binary_field",
                DataType::FixedSizeBinary(16),
                true,
            )),
        ];

        let mut builder = NodeBuilder::new(&DataType::Struct(struct_fields.clone().into()), 1);

        // Add struct with all types
        builder
            .append(&RowValue::Struct(vec![
                RowValue::Bool(true),
                RowValue::Int32(42),
                RowValue::Int64(1000),
                RowValue::Float32(std::f32::consts::PI),
                RowValue::Float64(std::f64::consts::E),
                RowValue::Decimal(12345),
                RowValue::ByteArray(b"test string".to_vec()),
                RowValue::ByteArray(b"binary data".to_vec()),
                RowValue::FixedLenByteArray([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                ]),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

        // Verify all fields
        assert_eq!(struct_array.num_columns(), 9);
        let bool_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0);
        assert!(bool_array);
        let int32_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(int32_array, 42);
        let int64_array = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(int64_array, 1000);
        let float32_array = struct_array
            .column(3)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0);
        assert_eq!(float32_array, std::f32::consts::PI);
        let float64_array = struct_array
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(float64_array, std::f64::consts::E);
        let decimal_array = struct_array
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap()
            .value(0);
        assert_eq!(decimal_array, 12345);
        let string_array = struct_array
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(string_array, "test string");
        let binary_array = struct_array
            .column(7)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap()
            .value(0);
        assert_eq!(binary_array, b"binary data");
        let fixed_binary_array = struct_array
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0);
        assert_eq!(
            fixed_binary_array,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
    }

    #[test]
    fn test_column_array_builder_struct_list() {
        use arrow::array::{ListArray, StructArray};

        // Test List<Struct> type
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new("id", DataType::Int32, true)),
            Arc::new(arrow::datatypes::Field::new("name", DataType::Utf8, true)),
            Arc::new(arrow::datatypes::Field::new(
                "active",
                DataType::Boolean,
                true,
            )),
        ];

        let mut builder = NodeBuilder::new(
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Struct(struct_fields.clone().into()),
                true,
            ))),
            2,
        );

        // Add a list of structs
        builder
            .append(&RowValue::Array(vec![
                RowValue::Struct(vec![
                    RowValue::Int32(1),
                    RowValue::ByteArray(b"Alice".to_vec()),
                    RowValue::Bool(true),
                ]),
                RowValue::Struct(vec![
                    RowValue::Int32(2),
                    RowValue::ByteArray(b"Bob".to_vec()),
                    RowValue::Bool(false),
                ]),
                RowValue::Null, // null struct in the list
            ]))
            .unwrap();

        // Add another list with mixed content
        builder
            .append(&RowValue::Array(vec![RowValue::Struct(vec![
                RowValue::Int32(3),
                RowValue::ByteArray(b"Charlie".to_vec()),
                RowValue::Bool(true),
            ])]))
            .unwrap();

        let array = builder.finish(&DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Struct(struct_fields.into()),
            true,
        ))));

        assert_eq!(array.len(), 2);
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();

        // Check first list [struct1, struct2, null]
        let first_list = list_array.value(0);
        let first_struct_array = first_list.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(first_struct_array.len(), 3);

        // Check struct validity in first list
        assert!(!first_struct_array.is_null(0)); // first struct
        assert!(!first_struct_array.is_null(1)); // second struct
        assert!(first_struct_array.is_null(2)); // null struct

        // Check values in first list
        let id_column = first_struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 1);
        assert_eq!(id_column.value(1), 2);
        assert!(id_column.is_null(2));

        let name_column = first_struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");
        assert!(name_column.is_null(2));

        let active_column = first_struct_array
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active_column.value(0));
        assert!(!active_column.value(1));
        assert!(active_column.is_null(2));

        // Check second list [struct3]
        let second_list = list_array.value(1);
        let second_struct_array = second_list.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(second_struct_array.len(), 1);

        // Check struct validity in second list
        assert!(!second_struct_array.is_null(0));

        // Check values in second list
        let id_column = second_struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 3);

        let name_column = second_struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Charlie");

        let active_column = second_struct_array
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active_column.value(0));
    }

    #[test]
    fn struct_with_array_field() {
        // Struct { id: Int32, scores: List<Int32> }
        let struct_dt = DataType::Struct(
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new(
                    "scores",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                ),
            ]
            .into(),
        );

        let mut builder = NodeBuilder::new(&struct_dt, 3);

        // { id: 1, scores: [10,20] }
        builder
            .append(&RowValue::Struct(vec![
                RowValue::Int32(1),
                RowValue::Array(vec![RowValue::Int32(10), RowValue::Int32(20)]),
            ]))
            .unwrap();

        // { id: 2, scores: null }
        builder
            .append(&RowValue::Struct(vec![RowValue::Int32(2), RowValue::Null]))
            .unwrap();

        // null struct
        builder.append(&RowValue::Null).unwrap();

        let array = builder.finish(&struct_dt);
        let sa = array.as_any().downcast_ref::<StructArray>().unwrap();

        assert_eq!(sa.len(), 3);
        assert!(sa.is_null(2));

        let id_col = sa.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);

        let list_col = sa.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        let first_scores = list_col.value(0);
        let int_arr = first_scores.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_arr.values(), &[10, 20]);
        assert!(list_col.is_null(1));
    }
}
