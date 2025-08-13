# Moonlink DataFusion Integration Architecture

## Table of Contents
1. [Overview](#overview)
2. [Architecture Components](#architecture-components)
3. [Data Flow](#data-flow)
4. [Integration Points](#integration-points)
5. [Query Execution Pipeline](#query-execution-pipeline)
6. [Optimization Features](#optimization-features)
7. [Union Read: Original vs DataFusion](#union-read-original-vs-datafusion)
8. [Implementation Details](#implementation-details)
9. [Examples](#examples)

## Overview

Moonlink DataFusion is a query engine integration layer that bridges Apache DataFusion with Moonlink's real-time CDC and Iceberg storage capabilities. It enables SQL query execution on Moonlink tables while maintaining real-time data consistency and leveraging advanced query optimizations.

### Key Features
- **SQL Query Interface**: Full SQL support via DataFusion
- **Real-time Data Access**: Queries see the latest CDC changes
- **Segment Elimination**: Row group pruning based on statistics
- **Deletion Vector Support**: Efficient handling of deleted rows
- **Unix Socket RPC**: Low-latency communication with Moonlink core

## Architecture Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         Query Layer                              │
│  ┌────────────────────────────────────────────────────────┐     │
│  │                    DataFusion CLI                       │     │
│  │                 (SQL Query Interface)                   │     │
│  └──────────────────────┬──────────────────────────────────┘     │
│                         │                                        │
│  ┌──────────────────────▼──────────────────────────────────┐     │
│  │           MooncakeCatalogProvider                       │     │
│  │         (Catalog/Schema Discovery)                      │     │
│  └──────────────────────┬──────────────────────────────────┘     │
│                         │                                        │
│  ┌──────────────────────▼──────────────────────────────────┐     │
│  │           MooncakeTableProvider                         │     │
│  │      (Table Scan & Query Planning)                      │     │
│  └──────────────────────┬──────────────────────────────────┘     │
└─────────────────────────┼────────────────────────────────────────┘
                          │
                          │ Unix Socket RPC
                          │ (moonlink_rpc)
                          │
┌─────────────────────────▼────────────────────────────────────────┐
│                      Moonlink Core                               │
│  ┌────────────────────────────────────────────────────────┐     │
│  │              MooncakeTable                              │     │
│  │         (In-memory Arrow Buffer)                        │     │
│  └────────────────────┬───────────────────────────────────┘     │
│                       │                                          │
│  ┌────────────────────▼───────────────────────────────────┐     │
│  │            Storage Layer                                │     │
│  │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │     │
│  │   │  Parquet    │  │  Deletion   │  │   Puffin    │   │     │
│  │   │   Files     │  │  Vectors    │  │    Files    │   │     │
│  │   └─────────────┘  └─────────────┘  └─────────────┘   │     │
│  └──────────────────────────────────────────────────────────┘     │
└───────────────────────────────────────────────────────────────────┘
```

### Component Descriptions

#### 1. **MooncakeCatalogProvider**
- Implements DataFusion's `CatalogProvider` trait
- Manages database discovery and schema resolution
- Creates schema providers for database access

#### 2. **MooncakeSchemaProvider**
- Implements DataFusion's `SchemaProvider` trait
- Lists tables within a database
- Creates table providers for table access

#### 3. **MooncakeTableProvider**
- Core query execution component
- Implements DataFusion's `TableProvider` trait
- Handles:
  - Schema retrieval
  - Predicate pushdown
  - Scan planning
  - Segment elimination
  - Deletion vector processing

#### 4. **RPC Communication Layer**
- Unix socket-based communication
- Binary protocol using bincode serialization
- Operations:
  - `get_table_schema`: Retrieve table Arrow schema
  - `scan_table_begin`: Initialize table scan with LSN
  - `scan_table_end`: Cleanup scan resources
  - Table metadata exchange

## Data Flow

### Query Execution Flow

```
┌──────────────┐
│  SQL Query   │
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────┐
│   DataFusion Parser/Planner      │
│   - Parse SQL                     │
│   - Create Logical Plan           │
│   - Optimize Plan                 │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│   MooncakeTableProvider::scan()  │
│   - Connect via Unix Socket       │
│   - Request Table Metadata        │
│   - Apply Predicates              │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│   Segment Elimination             │
│   - Extract Row Group Statistics  │
│   - Evaluate Predicates           │
│   - Skip Non-matching Segments    │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│   Deletion Vector Processing      │
│   - Load Deletion Vectors         │
│   - Create Row Selectors          │
│   - Filter Deleted Rows           │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│   Parquet File Reading            │
│   - Read Selected Row Groups      │
│   - Apply Row Selection           │
│   - Return Arrow RecordBatches    │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────┐
│Query Results │
└──────────────┘
```

### Table Scan Lifecycle

```
┌─────────────┐     scan_table_begin     ┌──────────────┐
│ DataFusion  │──────────────────────────▶│   Moonlink   │
│             │                           │              │
│             │◀──────────────────────────│              │
│             │    Table Metadata         │              │
│             │    - Data files           │              │
│             │    - Deletion vectors     │              │
│             │    - Position deletes     │              │
│             │                           │              │
│             │     Read Parquet          │              │
│             │──────────────────────────▶│  Storage     │
│             │                           │              │
│             │◀──────────────────────────│              │
│             │     RecordBatches         │              │
│             │                           │              │
│             │    scan_table_end         │              │
│             │──────────────────────────▶│   Moonlink   │
│             │                           │              │
│             │◀──────────────────────────│              │
│             │         ACK               │              │
└─────────────┘                           └──────────────┘
```

## Integration Points

### 1. Unix Socket RPC Interface
The communication between DataFusion and Moonlink core happens through Unix domain sockets:

```rust
// Connection establishment
let mut stream = UnixStream::connect(uri).await?;

// Schema retrieval
let schema = get_table_schema(&mut stream, database_id, table_id).await?;

// Scan initialization with LSN
let metadata = scan_table_begin(&mut stream, database_id, table_id, lsn).await?;

// Scan cleanup
scan_table_end(&mut stream, database_id, table_id).await?;
```

### 2. Table Metadata Exchange
Moonlink provides comprehensive metadata for query planning:

```rust
pub struct MooncakeTableMetadata {
    pub data_files: Vec<String>,      // Parquet data files
    pub puffin_files: Vec<String>,     // Puffin files for deletion vectors
    pub deletion_vectors: Vec<DeletionVector>,
    pub position_deletes: Vec<PositionDelete>,
}
```

### 3. Arrow Schema Integration
DataFusion and Moonlink share Arrow schemas for seamless data exchange:

```rust
// Convert Moonlink schema to DataFusion schema
let schema = StreamReader::try_new(schema_bytes, None)?.schema();
```

## Query Execution Pipeline

### Stage 1: Query Planning
```
SQL Query → Logical Plan → Physical Plan → Execution Plan
```

### Stage 2: Predicate Pushdown
DataFusion pushes predicates to the table provider:
```rust
fn supports_filters_pushdown(&self, filters: &[&Expr]) 
    -> Result<Vec<TableProviderFilterPushDown>>
```

### Stage 3: Segment Elimination

```
┌─────────────────────────────────┐
│     Parquet File Statistics      │
├─────────────────────────────────┤
│ Row Group 1: id [1-1000]        │
│ Row Group 2: id [1001-2000]     │
│ Row Group 3: id [2001-3000]     │
└─────────────────────────────────┘
             │
             │ WHERE id > 1500
             ▼
┌─────────────────────────────────┐
│    Segment Elimination Logic     │
├─────────────────────────────────┤
│ RG1: max(1000) < 1500 → SKIP    │
│ RG2: overlaps → SCAN            │
│ RG3: min(2001) > 1500 → SCAN    │
└─────────────────────────────────┘
```

The segment elimination process:
1. Extract column statistics from Parquet metadata
2. Evaluate predicates against min/max values
3. Skip row groups that cannot contain matching rows
4. Create `ParquetAccessPlan` with selected row groups

### Stage 4: Deletion Vector Application

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Row Group   │────▶│  Deletion    │────▶│   Selected   │
│   (1000)     │     │   Vector     │     │    Rows      │
│              │     │   {5,10,15}  │     │    (997)     │
└──────────────┘     └──────────────┘     └──────────────┘

Row Selection:
- Rows 1-4:   SELECT
- Row 5:      SKIP (deleted)
- Rows 6-9:   SELECT
- Row 10:     SKIP (deleted)
- Rows 11-14: SELECT
- Row 15:     SKIP (deleted)
- Rows 16+:   SELECT
```

## Optimization Features

### 1. Segment Elimination
- **Purpose**: Reduce I/O by skipping irrelevant row groups
- **Mechanism**: Compare query predicates with row group statistics
- **Supported Predicates**:
  - Equality: `col = value`
  - Comparison: `col > value`, `col < value`, etc.
  - Range: `col BETWEEN x AND y`
  - Null checks: `col IS NULL`, `col IS NOT NULL`
  - Combinations: `AND`, `OR`

### 2. Deletion Vector Optimization
- **Roaring Bitmaps**: Efficient storage of deleted row positions
- **Puffin Files**: Iceberg-standard format for deletion vectors
- **Row Selection**: Create `RowSelector` arrays for Parquet reader

### 3. Parallel Scan Execution
- Multiple Parquet files scanned in parallel
- Row groups within files processed concurrently
- DataFusion's execution engine handles parallelism

### 4. Resource Management
- **Scan Lifecycle**: Proper cleanup with `scan_table_end`
- **Connection Pooling**: Reuse Unix socket connections
- **Memory Management**: Stream processing with bounded buffers

## Union Read: Original vs DataFusion

### Original Union Read Flow (Direct Moonlink)

The original union read implementation in Moonlink combines in-memory and on-disk data directly:

```
┌─────────────────────────────────────────────────────────┐
│                 Original Union Read                      │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  Client Application                                      │
│       │                                                  │
│       ▼                                                  │
│  Moonlink Union Read API                                │
│       │                                                  │
│       ├──────────────┬──────────────┐                  │
│       ▼              ▼              ▼                  │
│  Arrow Buffer   Iceberg Files  Deletion Log            │
│  (Hot Data)     (Cold Data)    (Deletions)             │
│       │              │              │                  │
│       └──────────────┴──────────────┘                  │
│                      │                                  │
│                      ▼                                  │
│              Merge & Filter                            │
│                      │                                  │
│                      ▼                                  │
│              RecordBatches                             │
│              (Final Result)                            │
└─────────────────────────────────────────────────────────┘
```

**Characteristics:**
- **Direct Access**: Client directly calls Moonlink's union read API
- **In-Process Merging**: Data merging happens within Moonlink process
- **Custom Query Logic**: Applications implement their own filtering/aggregation
- **Lower Latency**: No RPC overhead for local clients
- **Limited Query Capabilities**: No SQL, manual query construction

### DataFusion-Based Flow

The DataFusion integration provides SQL query capabilities:

```
┌─────────────────────────────────────────────────────────┐
│              DataFusion Query Flow                       │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  SQL Query                                               │
│       │                                                  │
│       ▼                                                  │
│  DataFusion Engine                                       │
│       │                                                  │
│       ▼                                                  │
│  MooncakeTableProvider                                   │
│       │                                                  │
│       │ Unix Socket RPC                                  │
│       ▼                                                  │
│  Moonlink Process                                        │
│       │                                                  │
│       ├─────────────────┐                               │
│       ▼                 ▼                               │
│  scan_table_begin   scan_table_end                      │
│       │                                                  │
│       ▼                                                  │
│  Table Metadata                                          │
│  (file paths, deletion vectors)                          │
│       │                                                  │
│       ▼                                                  │
│  DataFusion reads files directly                         │
│       │                                                  │
│       ├──────────────┬──────────────┐                  │
│       ▼              ▼              ▼                  │
│  Parquet Files  Puffin Files  Statistics               │
│       │              │              │                  │
│       └──────────────┴──────────────┘                  │
│                      │                                  │
│                      ▼                                  │
│          Segment Elimination &                          │
│          Deletion Vector Application                    │
│                      │                                  │
│                      ▼                                  │
│              RecordBatches                             │
│              (Query Result)                            │
└─────────────────────────────────────────────────────────┘
```

**Characteristics:**
- **SQL Interface**: Full SQL query capabilities
- **Query Optimization**: Predicate pushdown, segment elimination
- **File-Based Access**: DataFusion reads files directly
- **RPC Overhead**: Unix socket communication for metadata
- **Rich Query Features**: Joins, aggregations, window functions

### Comparison Table

| Aspect | Original Union Read | DataFusion Integration |
|--------|-------------------|----------------------|
| **Interface** | Programmatic API | SQL + API |
| **Query Complexity** | Simple scans | Complex SQL queries |
| **Data Access** | Direct in-memory + files | File-based with metadata RPC |
| **Hot Data Handling** | Direct Arrow buffer access | Through flushed Parquet files |
| **Optimization** | Manual | Automatic (segment elimination) |
| **Latency** | Lower (no RPC) | Higher (RPC + file I/O) |
| **Scalability** | Limited by single process | DataFusion parallelism |
| **Use Case** | Real-time operational queries | Analytical queries |

### Key Differences

#### 1. **Data Access Pattern**

**Original:**
```rust
// Direct access to in-memory buffer
let hot_data = table.arrow_buffer.read();
let cold_data = iceberg_reader.read();
let merged = union_read::merge(hot_data, cold_data);
```

**DataFusion:**
```rust
// Metadata-based file access
let metadata = scan_table_begin(...);
for file in metadata.data_files {
    let batches = parquet_reader.read(file);
    // Apply deletion vectors
}
```

#### 2. **Deletion Handling**

**Original:**
- Positional deletion log maintained in memory
- Applied during union read merge

**DataFusion:**
- Deletion vectors loaded from Puffin files
- Position deletes merged with deletion vectors
- Applied as RowSelector during Parquet scan

#### 3. **Query Execution**

**Original:**
```rust
// Manual filtering
let results = union_read(table)
    .filter(|row| row.id > 1000)
    .collect();
```

**DataFusion:**
```sql
-- Automatic optimization
SELECT * FROM table WHERE id > 1000;
-- Segment elimination automatically applied
```

#### 4. **Performance Trade-offs**

**Original Union Read:**
- ✅ Lower latency for simple queries
- ✅ Direct access to hot data
- ✅ No file I/O for recent data
- ❌ Limited query optimization
- ❌ Manual implementation required

**DataFusion Integration:**
- ✅ Advanced query optimization
- ✅ SQL interface
- ✅ Parallel execution
- ✅ Segment elimination
- ❌ RPC overhead
- ❌ File I/O for all data (including recent)

### When to Use Which

**Use Original Union Read when:**
- Building real-time applications
- Need lowest possible latency
- Simple point queries or scans
- Direct integration with Moonlink

**Use DataFusion Integration when:**
- Need SQL query interface
- Complex analytical queries
- Batch processing workloads
- Integration with BI tools
- Need query optimization features

### Future Convergence

Potential improvements to combine benefits:
1. **Hybrid Execution**: Use original union read for hot data, DataFusion for cold
2. **Arrow Flight**: Direct Arrow buffer transfer to DataFusion
3. **Shared Memory**: Avoid RPC for local DataFusion instances
4. **Pushdown to Moonlink**: Let Moonlink handle hot data filtering

## Implementation Details

### Table Provider Implementation

```rust
impl TableProvider for MooncakeTableProvider {
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. Create physical predicate from filters
        let predicate = conjunction(filters.to_vec())
            .map(|p| state.create_physical_expr(p, &schema))?;
        
        // 2. Get table metadata via RPC
        let metadata = scan_table_begin(...).await?;
        
        // 3. Process each data file
        for data_file in metadata.data_files {
            // Apply segment elimination
            let include_row_groups = analyze_file_for_segment_elimination(...)?;
            
            // Apply deletion vectors
            let row_selection = create_row_selection(...)?;
            
            // Configure file scan
            let file = PartitionedFile::new(data_file)
                .with_extensions(Arc::new(access_plan));
        }
        
        // 4. Build execution plan
        Ok(DataSourceExec::from_data_source(config.build()))
    }
}
```

### Segment Elimination Algorithm

```rust
fn evaluate_predicate_against_stats(
    predicate: &Expr,
    column_stats: &[Option<ColumnStatistics>],
    schema: &Schema,
) -> Result<bool> {
    match predicate {
        Expr::BinaryExpr { left, op, right } => {
            match op {
                Operator::Eq => {
                    // Check if value is within [min, max] range
                    Ok(literal >= min_val && literal <= max_val)
                }
                Operator::Lt => {
                    // Include if min < literal
                    Ok(min_val < literal)
                }
                Operator::And => {
                    // Both conditions must be true
                    Ok(eval(left) && eval(right))
                }
                // ... other operators
            }
        }
        // ... other expression types
    }
}
```

### Deletion Vector Processing

```rust
// Load deletion vector from Puffin file
let deleted_rows = RoaringTreemap::deserialize_from(buffer)?;

// Create row selectors for Parquet reader
let mut selectors = vec![];
for row in 0..row_group.num_rows() {
    if deleted_rows.contains(row) {
        selectors.push(RowSelector::skip(1));
    } else {
        selectors.push(RowSelector::select(1));
    }
}

// Apply selection to row group
access_plan.scan_selection(row_group_idx, RowSelection::from(selectors));
```

## Examples

### Basic Query Execution

```sql
-- Connect to Moonlink via DataFusion CLI
CREATE EXTERNAL TABLE customers 
STORED AS MOONCAKE 
LOCATION 'unix:///tmp/moonlink.sock/1/100/12345';

-- Query with predicate pushdown and segment elimination
SELECT * FROM customers 
WHERE customer_id > 1000 
  AND country = 'USA'
  AND created_at > '2024-01-01';
```

### Segment Elimination in Action

```sql
-- Table with 1M rows across 100 row groups
-- Each row group contains 10K rows with ID ranges

-- This query will skip 50% of row groups
SELECT count(*) FROM large_table WHERE id > 500000;

-- Execution statistics:
-- Row groups scanned: 50/100
-- Rows processed: 500K/1M
-- I/O reduction: 50%
```

### Handling Deletions

```sql
-- Query on table with active deletions
SELECT * FROM orders WHERE status = 'PENDING';

-- Behind the scenes:
-- 1. Load deletion vectors for each data file
-- 2. Create row selectors excluding deleted rows
-- 3. Return only non-deleted pending orders
```

## Performance Considerations

### Benefits
1. **I/O Reduction**: Skip entire row groups based on statistics
2. **CPU Efficiency**: Process only relevant data
3. **Memory Optimization**: Stream processing with bounded buffers
4. **Scalability**: Performance gains increase with data size

### Trade-offs
1. **Statistics Overhead**: Small overhead for extracting statistics
2. **Deletion Vector Loading**: Additional I/O for deleted row tracking
3. **RPC Latency**: Unix socket communication overhead (minimal)

### Best Practices
1. **Predicate Design**: Use selective predicates that leverage statistics
2. **Data Organization**: Cluster data by commonly filtered columns
3. **Row Group Size**: Balance between granularity and overhead
4. **Deletion Compaction**: Periodically compact files with many deletions

## Future Enhancements

### Planned Features
1. **Bloom Filter Support**: Additional filtering for high-cardinality columns
2. **Zone Maps**: More granular statistics within row groups
3. **Adaptive Execution**: Runtime optimization based on statistics
4. **Caching Layer**: Cache frequently accessed statistics and metadata
5. **Distributed Execution**: Scale across multiple Moonlink instances

### Integration Improvements
1. **Direct Arrow Transfer**: Bypass Parquet for hot data
2. **Incremental Queries**: Leverage CDC for incremental processing
3. **Materialized Views**: Pre-computed aggregations
4. **Query Result Caching**: Cache common query results

## Conclusion

Moonlink DataFusion provides a powerful query engine integration that combines:
- Real-time CDC data visibility
- Advanced query optimizations (segment elimination)
- Efficient deletion handling
- Standard SQL interface via DataFusion

This architecture enables analytical queries on operational data with minimal latency while maintaining ACID guarantees and optimizing resource usage through intelligent data skipping and pruning techniques.

The modular design allows for future enhancements while maintaining compatibility with the broader Apache Arrow and DataFusion ecosystems.