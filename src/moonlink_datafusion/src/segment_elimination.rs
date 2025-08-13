use datafusion::common::{Result, Column};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::physical_expr::PhysicalExpr;
use arrow::datatypes::{Schema, DataType};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::schema::types::SchemaDescriptor;
use datafusion::common::ScalarValue;

/// Represents statistics for a column in a row group
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub data_type: DataType,
}

/// Represents statistics for a row group
#[derive(Debug)]
pub struct RowGroupStatistics {
    pub row_count: i64,
    pub column_stats: Vec<Option<ColumnStatistics>>,
}

/// Represents statistics for a parquet file
#[derive(Debug)]
pub struct FileStatistics {
    pub row_groups: Vec<RowGroupStatistics>,
    pub total_rows: i64,
}

impl FileStatistics {
    /// Create file statistics from parquet metadata
    pub fn from_parquet_metadata(
        metadata: &ParquetMetaData,
        arrow_schema: &Schema,
    ) -> Result<Self> {
        let row_groups_metadata = metadata.row_groups();
        let parquet_schema = metadata.file_metadata().schema_descr();
        let total_rows = metadata.file_metadata().num_rows();
        
        let mut row_groups = Vec::with_capacity(row_groups_metadata.len());
        
        for row_group_meta in row_groups_metadata {
            let mut column_stats = Vec::with_capacity(arrow_schema.fields().len());
            
            for (field_idx, field) in arrow_schema.fields().iter().enumerate() {
                let column_stat = extract_column_statistics(
                    field,
                    field_idx,
                    row_group_meta,
                    parquet_schema,
                )?;
                column_stats.push(column_stat);
            }
            
            row_groups.push(RowGroupStatistics {
                row_count: row_group_meta.num_rows(),
                column_stats,
            });
        }
        
        Ok(Self {
            row_groups,
            total_rows,
        })
    }

    /// Create placeholder file statistics for testing
    pub fn new_placeholder(num_row_groups: usize, total_rows: i64) -> Self {
        let mut row_groups = Vec::with_capacity(num_row_groups);
        
        for _ in 0..num_row_groups {
            let column_stats = vec![
                Some(ColumnStatistics {
                    min: None,
                    max: None,
                    null_count: Some(0),
                    distinct_count: Some(100),
                    data_type: DataType::Int32,
                }),
                Some(ColumnStatistics {
                    min: None,
                    max: None,
                    null_count: Some(0),
                    distinct_count: Some(50),
                    data_type: DataType::Utf8,
                }),
            ];
            
            row_groups.push(RowGroupStatistics {
                row_count: total_rows / num_row_groups as i64,
                column_stats,
            });
        }

        Self {
            row_groups,
            total_rows,
        }
    }
}

/// Extract column statistics from a specific row group and column
fn extract_column_statistics(
    field: &arrow::datatypes::Field,
    _field_idx: usize,
    row_group_meta: &RowGroupMetaData,
    parquet_schema: &SchemaDescriptor,
) -> Result<Option<ColumnStatistics>> {
    // Find the parquet column corresponding to this arrow field
    let column_name = field.name();
    
    // Try to find the column in the parquet schema
    let parquet_column_index = parquet_schema
        .columns()
        .iter()
        .position(|col| col.name() == column_name);
    
    if let Some(parquet_col_idx) = parquet_column_index {
        let column_chunk = row_group_meta.column(parquet_col_idx);
        
        if let Some(statistics) = column_chunk.statistics() {
            // Create a statistics converter for this column
            // Note: StatisticsConverter requires the full arrow schema, not just the data type
            // For now, we'll use the simpler approach of extracting raw statistics
            let min_val = statistics.min_bytes_opt()
                .and_then(|bytes| scalar_value_from_bytes(bytes, field.data_type()));
            let max_val = statistics.max_bytes_opt()
                .and_then(|bytes| scalar_value_from_bytes(bytes, field.data_type()));
            
            return Ok(Some(ColumnStatistics {
                min: min_val,
                max: max_val,
                null_count: Some(statistics.null_count_opt().unwrap_or(0) as i64),
                distinct_count: statistics.distinct_count_opt().map(|c| c as i64),
                data_type: field.data_type().clone(),
            }));
        }
    }
    
    // No statistics available for this column
    Ok(None)
}

/// Convert raw parquet statistics bytes to ScalarValue
fn scalar_value_from_bytes(bytes: &[u8], data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Int32 => {
            if bytes.len() >= 4 {
                let value = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Some(ScalarValue::Int32(Some(value)))
            } else {
                None
            }
        }
        DataType::Int64 => {
            if bytes.len() >= 8 {
                let value = i64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Some(ScalarValue::Int64(Some(value)))
            } else {
                None
            }
        }
        DataType::Float32 => {
            if bytes.len() >= 4 {
                let value = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Some(ScalarValue::Float32(Some(value)))
            } else {
                None
            }
        }
        DataType::Float64 => {
            if bytes.len() >= 8 {
                let value = f64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Some(ScalarValue::Float64(Some(value)))
            } else {
                None
            }
        }
        DataType::Utf8 => {
            String::from_utf8(bytes.to_vec()).ok()
                .map(|s| ScalarValue::Utf8(Some(s)))
        }
        DataType::Boolean => {
            if !bytes.is_empty() {
                Some(ScalarValue::Boolean(Some(bytes[0] != 0)))
            } else {
                None
            }
        }
        // Add more data types as needed
        _ => None,
    }
}

/// Evaluates if a row group should be included based on predicate analysis
pub fn should_include_row_group(
    row_group_stats: &RowGroupStatistics,
    _predicate: &dyn PhysicalExpr,
    _schema: &Schema,
) -> Result<bool> {
    // Convert physical expression to logical for easier analysis
    // For now, we'll do a simplified evaluation
    // TODO: Implement full physical expression evaluation
    
    // If we don't have any statistics, include the row group to be safe
    let has_statistics = row_group_stats.column_stats.iter().any(|stats| stats.is_some());
    if !has_statistics {
        return Ok(true);
    }
    
    // For complex predicates, we default to including the row group
    // A full implementation would need to evaluate the physical expression tree
    Ok(true)
}

/// Analyzes a file and returns which row groups should be scanned
pub fn analyze_file_for_segment_elimination(
    file_stats: &FileStatistics,
    predicate: Option<&dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Vec<bool>> {
    let mut include_row_groups = Vec::with_capacity(file_stats.row_groups.len());
    
    for row_group_stats in &file_stats.row_groups {
        let should_include = if let Some(pred) = predicate {
            should_include_row_group(row_group_stats, pred, schema)?
        } else {
            true
        };
        include_row_groups.push(should_include);
    }
    
    Ok(include_row_groups)
}

/// Enhanced predicate evaluation against statistics
pub fn evaluate_predicate_against_stats(
    predicate: &Expr,
    column_stats: &[Option<ColumnStatistics>],
    schema: &Schema,
) -> Result<bool> {
    match predicate {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            match op {
                Operator::And => {
                    let left_result = evaluate_predicate_against_stats(left, column_stats, schema)?;
                    let right_result = evaluate_predicate_against_stats(right, column_stats, schema)?;
                    Ok(left_result && right_result)
                }
                Operator::Or => {
                    let left_result = evaluate_predicate_against_stats(left, column_stats, schema)?;
                    let right_result = evaluate_predicate_against_stats(right, column_stats, schema)?;
                    Ok(left_result || right_result)
                }
                // Comparison operators
                Operator::Eq | Operator::NotEq | 
                Operator::Lt | Operator::LtEq | 
                Operator::Gt | Operator::GtEq => {
                    evaluate_comparison_predicate(left, op, right, column_stats, schema)
                }
                _ => Ok(true), // For other operators, include the segment to be safe
            }
        }
        Expr::Column(_) => Ok(true), // Standalone column reference, include segment
        Expr::Literal(..) => Ok(true), // Standalone literal, include segment
        Expr::IsNull(expr) => {
            evaluate_null_predicate(expr, false, column_stats, schema)
        }
        Expr::IsNotNull(expr) => {
            evaluate_null_predicate(expr, true, column_stats, schema)
        }
        _ => Ok(true), // For other expression types, include the segment
    }
}

/// Evaluate comparison predicates like col > 5, col = 'foo', etc.
fn evaluate_comparison_predicate(
    left: &Expr,
    op: &Operator,
    right: &Expr,
    column_stats: &[Option<ColumnStatistics>],
    schema: &Schema,
) -> Result<bool> {
    // Try to identify column and literal value
    let (column, literal) = match (left, right) {
        (Expr::Column(col), Expr::Literal(lit, _)) => (col, lit),
        (Expr::Literal(lit, _), Expr::Column(col)) => {
            // Swap operator for literal op column
            let swapped_op = match op {
                Operator::Lt => &Operator::Gt,
                Operator::LtEq => &Operator::GtEq,
                Operator::Gt => &Operator::Lt,
                Operator::GtEq => &Operator::LtEq,
                _ => op,
            };
            return evaluate_comparison_with_stats(col, swapped_op, lit, column_stats, schema);
        }
        _ => return Ok(true), // Can't evaluate complex expressions, include segment
    };
    
    evaluate_comparison_with_stats(column, op, literal, column_stats, schema)
}

/// Evaluate a comparison between a column and a literal value using statistics
fn evaluate_comparison_with_stats(
    column: &Column,
    op: &Operator,
    literal: &ScalarValue,
    column_stats: &[Option<ColumnStatistics>],
    schema: &Schema,
) -> Result<bool> {
    // Find the column index in the schema
    let column_idx = schema.index_of(&column.name)?;
    
    // Get the column statistics
    let stats = match column_stats.get(column_idx).and_then(|s| s.as_ref()) {
        Some(stats) => stats,
        None => return Ok(true), // No stats available, include segment
    };
    
    // Get min and max values for the column
    let (min_val, max_val) = match (&stats.min, &stats.max) {
        (Some(min), Some(max)) => (min, max),
        _ => return Ok(true), // No min/max stats, include segment
    };
    
    // Evaluate the predicate against the statistics
    match op {
        Operator::Eq => {
            // For equality: literal must be within [min, max] range
            Ok(literal >= min_val && literal <= max_val)
        }
        Operator::NotEq => {
            // For inequality: if min == max == literal, then all rows have this value
            // so the result would be empty (false). Otherwise, include segment.
            if min_val == max_val && min_val == literal {
                Ok(false)
            } else {
                Ok(true)
            }
        }
        Operator::Lt => {
            // For col < literal: include if min < literal
            Ok(min_val < literal)
        }
        Operator::LtEq => {
            // For col <= literal: include if min <= literal
            Ok(min_val <= literal)
        }
        Operator::Gt => {
            // For col > literal: include if max > literal
            Ok(max_val > literal)
        }
        Operator::GtEq => {
            // For col >= literal: include if max >= literal
            Ok(max_val >= literal)
        }
        _ => Ok(true), // For other operators, include segment
    }
}

/// Evaluate null/not null predicates
fn evaluate_null_predicate(
    expr: &Expr,
    is_not_null: bool,
    column_stats: &[Option<ColumnStatistics>],
    schema: &Schema,
) -> Result<bool> {
    if let Expr::Column(column) = expr {
        let column_idx = schema.index_of(&column.name)?;
        
        if let Some(Some(stats)) = column_stats.get(column_idx) {
            if let Some(null_count) = stats.null_count {
                if is_not_null {
                    // IS NOT NULL: include if there are non-null values
                    // This means if null_count < row_count, but we don't have row_count here
                    // So we assume if null_count is not equal to total rows, include segment
                    Ok(true) // Conservative approach: include segment
                } else {
                    // IS NULL: include if there are null values
                    Ok(null_count > 0)
                }
            } else {
                Ok(true) // No null count stats, include segment
            }
        } else {
            Ok(true) // No stats available, include segment
        }
    } else {
        Ok(true) // Complex expression, include segment
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit, Expr};

    #[test]
    fn test_basic_functionality() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        
        let column_stats = vec![
            Some(ColumnStatistics {
                min: Some(ScalarValue::Int32(Some(1))),
                max: Some(ScalarValue::Int32(Some(10))),
                null_count: Some(0),
                distinct_count: Some(10),
                data_type: DataType::Int32,
            }),
            None, // No stats for name column
        ];
        
        // Test that we can create statistics
        assert!(column_stats[0].is_some());
        assert!(column_stats[1].is_none());
    }

    #[test]
    fn test_file_statistics_creation() {
        let file_stats = FileStatistics::new_placeholder(3, 300);
        assert_eq!(file_stats.row_groups.len(), 3);
        assert_eq!(file_stats.total_rows, 300);
        
        for row_group in &file_stats.row_groups {
            assert_eq!(row_group.column_stats.len(), 2);
            assert!(row_group.column_stats[0].is_some());
            assert!(row_group.column_stats[1].is_some());
        }
    }

    #[test]
    fn test_predicate_evaluation_equality() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]);
        
        let column_stats = vec![
            Some(ColumnStatistics {
                min: Some(ScalarValue::Int32(Some(10))),
                max: Some(ScalarValue::Int32(Some(20))),
                null_count: Some(0),
                distinct_count: Some(10),
                data_type: DataType::Int32,
            }),
        ];
        
        // Test equality within range - should include
        let predicate = col("id").eq(lit(15i32));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(result);
        
        // Test equality outside range - should exclude
        let predicate = col("id").eq(lit(5i32));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(!result);
        
        let predicate = col("id").eq(lit(25i32));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_predicate_evaluation_comparison() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]);
        
        let column_stats = vec![
            Some(ColumnStatistics {
                min: Some(ScalarValue::Int32(Some(10))),
                max: Some(ScalarValue::Int32(Some(20))),
                null_count: Some(0),
                distinct_count: Some(10),
                data_type: DataType::Int32,
            }),
        ];
        
        // Test id > 5 - should include (min=10 > 5)
        let predicate = col("id").gt(lit(5i32));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(result);
        
        // Test id > 25 - should exclude (max=20 < 25)
        let predicate = col("id").gt(lit(25i32));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(!result);
        
        // Test id < 5 - should exclude (min=10 >= 5)
        let predicate = col("id").lt(lit(5i32));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(!result);
        
        // Test id < 25 - should include (min=10 < 25)
        let predicate = col("id").lt(lit(25i32));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(result);
    }

    #[test]
    fn test_predicate_evaluation_and_or() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]);
        
        let column_stats = vec![
            Some(ColumnStatistics {
                min: Some(ScalarValue::Int32(Some(10))),
                max: Some(ScalarValue::Int32(Some(20))),
                null_count: Some(0),
                distinct_count: Some(10),
                data_type: DataType::Int32,
            }),
            Some(ColumnStatistics {
                min: Some(ScalarValue::Int32(Some(100))),
                max: Some(ScalarValue::Int32(Some(200))),
                null_count: Some(0),
                distinct_count: Some(50),
                data_type: DataType::Int32,
            }),
        ];
        
        // Test AND: both conditions must be true
        let predicate = col("id").gt(lit(5i32)).and(col("value").lt(lit(150i32)));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(result); // id > 5 (true) AND value < 150 (true)
        
        let predicate = col("id").gt(lit(25i32)).and(col("value").lt(lit(150i32)));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(!result); // id > 25 (false) AND value < 150 (true)
        
        // Test OR: at least one condition must be true
        let predicate = col("id").gt(lit(25i32)).or(col("value").lt(lit(150i32)));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(result); // id > 25 (false) OR value < 150 (true)
        
        let predicate = col("id").gt(lit(25i32)).or(col("value").gt(lit(250i32)));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(!result); // id > 25 (false) OR value > 250 (false)
    }

    #[test]
    fn test_predicate_evaluation_string() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
        ]);
        
        let column_stats = vec![
            Some(ColumnStatistics {
                min: Some(ScalarValue::Utf8(Some("Alice".to_string()))),
                max: Some(ScalarValue::Utf8(Some("Charlie".to_string()))),
                null_count: Some(1),
                distinct_count: Some(3),
                data_type: DataType::Utf8,
            }),
        ];
        
        // Test string equality within range
        let predicate = col("name").eq(lit("Bob"));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(result);
        
        // Test string equality outside range
        let predicate = col("name").eq(lit("David"));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(!result);
        
        // Test string comparison
        let predicate = col("name").gt(lit("Aaron"));
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(result); // max "Charlie" > "Aaron"
    }

    #[test]
    fn test_predicate_evaluation_null_handling() {
        let schema = Schema::new(vec![
            Field::new("nullable_col", DataType::Int32, true),
        ]);
        
        let column_stats = vec![
            Some(ColumnStatistics {
                min: Some(ScalarValue::Int32(Some(10))),
                max: Some(ScalarValue::Int32(Some(20))),
                null_count: Some(5),
                distinct_count: Some(10),
                data_type: DataType::Int32,
            }),
        ];
        
        // Test IS NULL - should include since null_count > 0
        let predicate = col("nullable_col").is_null();
        let result = evaluate_predicate_against_stats(&predicate, &column_stats, &schema).unwrap();
        assert!(result);
        
        // Test with zero null count
        let no_null_stats = vec![
            Some(ColumnStatistics {
                min: Some(ScalarValue::Int32(Some(10))),
                max: Some(ScalarValue::Int32(Some(20))),
                null_count: Some(0),
                distinct_count: Some(10),
                data_type: DataType::Int32,
            }),
        ];
        
        let predicate = col("nullable_col").is_null();
        let result = evaluate_predicate_against_stats(&predicate, &no_null_stats, &schema).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_analyze_file_for_segment_elimination() {
        let file_stats = FileStatistics {
            row_groups: vec![
                RowGroupStatistics {
                    row_count: 1000,
                    column_stats: vec![
                        Some(ColumnStatistics {
                            min: Some(ScalarValue::Int32(Some(1))),
                            max: Some(ScalarValue::Int32(Some(100))),
                            null_count: Some(0),
                            distinct_count: Some(100),
                            data_type: DataType::Int32,
                        }),
                    ],
                },
                RowGroupStatistics {
                    row_count: 1000,
                    column_stats: vec![
                        Some(ColumnStatistics {
                            min: Some(ScalarValue::Int32(Some(101))),
                            max: Some(ScalarValue::Int32(Some(200))),
                            null_count: Some(0),
                            distinct_count: Some(100),
                            data_type: DataType::Int32,
                        }),
                    ],
                },
                RowGroupStatistics {
                    row_count: 1000,
                    column_stats: vec![
                        Some(ColumnStatistics {
                            min: Some(ScalarValue::Int32(Some(201))),
                            max: Some(ScalarValue::Int32(Some(300))),
                            null_count: Some(0),
                            distinct_count: Some(100),
                            data_type: DataType::Int32,
                        }),
                    ],
                },
            ],
            total_rows: 3000,
        };

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]);

        // Test with no predicate - should include all row groups
        let include_all = analyze_file_for_segment_elimination(&file_stats, None, &schema).unwrap();
        assert_eq!(include_all, vec![true, true, true]);

        // Test id > 150 - should exclude first row group, include others
        // Row group 1: max=100 < 150 -> exclude
        // Row group 2: max=200 > 150 -> include  
        // Row group 3: max=300 > 150 -> include
        // But our current implementation doesn't convert from PhysicalExpr, so this test would need
        // to be updated when we implement logical-to-physical expression evaluation.
    }
}
