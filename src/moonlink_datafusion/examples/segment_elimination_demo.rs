use moonlink_datafusion::segment_elimination::{
    analyze_file_for_segment_elimination, evaluate_predicate_against_stats, 
    ColumnStatistics, FileStatistics, RowGroupStatistics
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{col, lit};

/// Demonstrates the segment elimination functionality with various predicates
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Moonlink DataFusion Segment Elimination Demo ===\n");

    // Create a sample schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("active", DataType::Boolean, false),
    ]);

    // Create sample file statistics with 3 row groups
    let file_stats = create_sample_file_statistics();
    
    println!("Sample Data Statistics:");
    println!("Total rows: {}", file_stats.total_rows);
    println!("Number of row groups: {}\n", file_stats.row_groups.len());
    
    for (idx, rg) in file_stats.row_groups.iter().enumerate() {
        println!("Row Group {}:", idx + 1);
        println!("  Rows: {}", rg.row_count);
        
        if let Some(Some(id_stats)) = rg.column_stats.get(0) {
            println!("  ID range: {:?} - {:?}", id_stats.min, id_stats.max);
        }
        
        if let Some(Some(name_stats)) = rg.column_stats.get(1) {
            println!("  Name range: {:?} - {:?}", name_stats.min, name_stats.max);
        }
        
        if let Some(Some(value_stats)) = rg.column_stats.get(2) {
            println!("  Value range: {:?} - {:?}", value_stats.min, value_stats.max);
        }
        println!();
    }

    // Test various predicates
    test_predicates(&file_stats, &schema)?;

    Ok(())
}

fn create_sample_file_statistics() -> FileStatistics {
    FileStatistics {
        row_groups: vec![
            // Row Group 1: IDs 1-1000, Names A-F, Values 10.0-50.0
            RowGroupStatistics {
                row_count: 1000,
                column_stats: vec![
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Int32(Some(1))),
                        max: Some(ScalarValue::Int32(Some(1000))),
                        null_count: Some(0),
                        distinct_count: Some(1000),
                        data_type: DataType::Int32,
                    }),
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Utf8(Some("Alice".to_string()))),
                        max: Some(ScalarValue::Utf8(Some("Frank".to_string()))),
                        null_count: Some(10),
                        distinct_count: Some(990),
                        data_type: DataType::Utf8,
                    }),
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Float64(Some(10.0))),
                        max: Some(ScalarValue::Float64(Some(50.0))),
                        null_count: Some(5),
                        distinct_count: Some(995),
                        data_type: DataType::Float64,
                    }),
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Boolean(Some(false))),
                        max: Some(ScalarValue::Boolean(Some(true))),
                        null_count: Some(0),
                        distinct_count: Some(2),
                        data_type: DataType::Boolean,
                    }),
                ],
            },
            // Row Group 2: IDs 1001-2000, Names G-M, Values 40.0-80.0
            RowGroupStatistics {
                row_count: 1000,
                column_stats: vec![
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Int32(Some(1001))),
                        max: Some(ScalarValue::Int32(Some(2000))),
                        null_count: Some(0),
                        distinct_count: Some(1000),
                        data_type: DataType::Int32,
                    }),
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Utf8(Some("George".to_string()))),
                        max: Some(ScalarValue::Utf8(Some("Mary".to_string()))),
                        null_count: Some(15),
                        distinct_count: Some(985),
                        data_type: DataType::Utf8,
                    }),
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Float64(Some(40.0))),
                        max: Some(ScalarValue::Float64(Some(80.0))),
                        null_count: Some(8),
                        distinct_count: Some(992),
                        data_type: DataType::Float64,
                    }),
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Boolean(Some(true))),
                        max: Some(ScalarValue::Boolean(Some(true))),
                        null_count: Some(0),
                        distinct_count: Some(1),
                        data_type: DataType::Boolean,
                    }),
                ],
            },
            // Row Group 3: IDs 2001-3000, Names N-Z, Values 70.0-100.0
            RowGroupStatistics {
                row_count: 1000,
                column_stats: vec![
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Int32(Some(2001))),
                        max: Some(ScalarValue::Int32(Some(3000))),
                        null_count: Some(0),
                        distinct_count: Some(1000),
                        data_type: DataType::Int32,
                    }),
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Utf8(Some("Nancy".to_string()))),
                        max: Some(ScalarValue::Utf8(Some("Zoe".to_string()))),
                        null_count: Some(20),
                        distinct_count: Some(980),
                        data_type: DataType::Utf8,
                    }),
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Float64(Some(70.0))),
                        max: Some(ScalarValue::Float64(Some(100.0))),
                        null_count: Some(12),
                        distinct_count: Some(988),
                        data_type: DataType::Float64,
                    }),
                    Some(ColumnStatistics {
                        min: Some(ScalarValue::Boolean(Some(false))),
                        max: Some(ScalarValue::Boolean(Some(true))),
                        null_count: Some(0),
                        distinct_count: Some(2),
                        data_type: DataType::Boolean,
                    }),
                ],
            },
        ],
        total_rows: 3000,
    }
}

fn test_predicates(file_stats: &FileStatistics, schema: &Schema) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Testing Predicate Evaluation ===\n");

    // Test cases: (predicate_description, predicate, expected_results)
    let test_cases = vec![
        (
            "id > 1500",
            col("id").gt(lit(1500i32)),
            "Should exclude RG1 (max=1000), include RG2 (max=2000) and RG3 (max=3000)"
        ),
        (
            "id < 500",
            col("id").lt(lit(500i32)),
            "Should include RG1 (min=1), exclude RG2 (min=1001) and RG3 (min=2001)"
        ),
        (
            "id = 1500",
            col("id").eq(lit(1500i32)),
            "Should exclude RG1 (range 1-1000), include RG2 (range 1001-2000), exclude RG3 (range 2001-3000)"
        ),
        (
            "value > 85.0",
            col("value").gt(lit(85.0f64)),
            "Should exclude RG1 (max=50.0), exclude RG2 (max=80.0), include RG3 (max=100.0)"
        ),
        (
            "name >= 'Paul'",
            col("name").gt_eq(lit("Paul")),
            "Should exclude RG1 (max='Frank'), exclude RG2 (max='Mary'), include RG3 (max='Zoe')"
        ),
        (
            "active = true",
            col("active").eq(lit(true)),
            "Should include all RGs (all have true in their range)"
        ),
        (
            "id > 1500 AND value < 75.0",
            col("id").gt(lit(1500i32)).and(col("value").lt(lit(75.0f64))),
            "Should exclude RG1 (id fails), include RG2 (both pass), exclude RG3 (value fails)"
        ),
        (
            "id < 500 OR id > 2500",
            col("id").lt(lit(500i32)).or(col("id").gt(lit(2500i32))),
            "Should include RG1 (first condition), exclude RG2 (both fail), include RG3 (second condition)"
        ),
        (
            "name IS NULL",
            col("name").is_null(),
            "Should include all RGs (all have null_count > 0)"
        ),
    ];

    for (description, predicate, explanation) in test_cases {
        println!("Testing: {}", description);
        println!("Expected: {}", explanation);
        
        // Evaluate predicate for each row group
        let mut results = Vec::new();
        for (rg_idx, row_group) in file_stats.row_groups.iter().enumerate() {
            let result = evaluate_predicate_against_stats(
                &predicate,
                &row_group.column_stats,
                schema,
            )?;
            results.push(result);
            println!("  Row Group {}: {}", rg_idx + 1, if result { "INCLUDE" } else { "EXCLUDE" });
        }
        
        println!("  Summary: {} row groups would be scanned", results.iter().filter(|&&x| x).count());
        println!();
    }

    // Test file-level analysis (placeholder for when we have physical expressions)
    println!("=== File-Level Analysis ===");
    let include_all = analyze_file_for_segment_elimination(file_stats, None, schema)?;
    println!("No predicate: {:?} (all row groups included)", include_all);
    
    println!("\nDemo completed successfully!");
    Ok(())
}
