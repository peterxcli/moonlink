# Moonlink REST API: File Upload Design

## Overview

This document outlines the design for extending Moonlink's REST API with comprehensive file upload capabilities. The design builds upon the existing REST API architecture while adding support for bulk data ingestion through various file formats.

## Current Architecture Analysis

The existing REST API provides:
- **Health check**: `GET /health`
- **Table creation**: `POST /tables/{table}`  
- **Data ingestion**: `POST /ingest/{table}`
- **JSON→Arrow conversion** via `JsonToMoonlinkRowConverter`
- **Single-request processing** with immediate queuing

## File Upload API Design

### 1. New Endpoints

```
POST /upload/{table}           # Upload file with data for ingestion
POST /upload/{table}/batch     # Batch upload with multiple operations
GET  /upload/{upload_id}/status # Check upload status
```

### 2. Supported File Formats

**Primary Formats:**
- **CSV** (RFC 4180 compliant)
- **JSONL** (newline-delimited JSON)
- **Parquet** (native Arrow compatibility)
- **Arrow IPC** (native format)

**Secondary Formats:**
- **TSV** (tab-separated values)
- **JSON Array** (single array of objects)

### 3. File Processing Pipeline

```
Upload → Validation → Format Detection → Schema Inference/Validation → 
Conversion → Batch Processing → Commit → Response
```

### 4. Authentication & Security

- **Bearer Token Authentication**: Consistent with existing API auth design
- **File Size Limits**: Configurable (default 100MB)
- **Rate Limiting**: Per-client upload quotas
- **Content Type Validation**: Strict MIME type checking
- **Malicious Content Detection**: Basic file header validation

### 5. API Specification

## Single File Upload

```http
POST /upload/{table}
Content-Type: multipart/form-data
Authorization: Bearer <token>

Form fields:
- file: binary file data
- operation: "insert" | "update" | "upsert" | "replace" (optional, default: "insert") 
- format: "csv" | "jsonl" | "parquet" | "arrow" | "tsv" | "json" (optional, auto-detect)
- wait_for_commit: true | false (optional, default: false)
- schema_validation: "strict" | "coerce" | "ignore" (optional, default: "coerce")
- delimiter: string (for CSV/TSV, optional)
- has_headers: true | false (for CSV/TSV, optional, default: true)
```

**Response:**
```json
{
  "status": "success" | "error",
  "message": "File uploaded and processed successfully",
  "upload_id": "uuid-string",
  "table": "table_name", 
  "format": "detected_format",
  "rows_processed": 1500,
  "rows_failed": 0,
  "processing_time_ms": 2340,
  "commit_lsn": 12345,
  "errors": []
}
```

## Batch File Upload

```http
POST /upload/{table}/batch  
Content-Type: multipart/form-data
Authorization: Bearer <token>

Form fields:
- files[]: multiple binary files
- operations[]: array of operations per file
- wait_for_commit: true | false
- batch_mode: "atomic" | "best_effort" (optional, default: "best_effort")
```

**Response:**
```json
{
  "status": "success" | "partial" | "error",
  "batch_id": "uuid-string", 
  "table": "table_name",
  "total_files": 3,
  "successful_files": 2,
  "failed_files": 1,
  "total_rows_processed": 5000,
  "commit_lsn": 12346,
  "files": [
    {
      "filename": "data1.csv",
      "status": "success",
      "rows_processed": 2000,
      "format": "csv"
    },
    {
      "filename": "data2.jsonl", 
      "status": "success",
      "rows_processed": 3000,
      "format": "jsonl"
    },
    {
      "filename": "data3.csv",
      "status": "error", 
      "error": "Schema validation failed: column 'age' expects int32, got string"
    }
  ]
}
```

## Upload Status Check

```http
GET /upload/{upload_id}/status
Authorization: Bearer <token>
```

**Response:**
```json
{
  "upload_id": "uuid-string",
  "status": "processing" | "completed" | "failed",
  "table": "table_name",
  "progress": {
    "rows_processed": 1200,
    "total_rows": 2000,
    "percentage": 60.0
  },
  "started_at": "2025-01-15T10:30:00Z",
  "completed_at": "2025-01-15T10:32:15Z",
  "result": {
    "commit_lsn": 12345,
    "rows_processed": 2000,
    "errors": []
  }
}
```

### 6. Enhanced Error Handling

```json
{
  "error": "file_processing_failed",
  "message": "Failed to process uploaded file",
  "details": {
    "filename": "data.csv",
    "line": 42,
    "column": "age", 
    "expected_type": "int32",
    "actual_value": "not_a_number",
    "suggestion": "Use schema_validation: 'coerce' to automatically convert compatible values"
  }
}
```

### 7. Configuration & Limits

```rust
pub struct FileUploadConfig {
    pub max_file_size_mb: usize,           // Default: 100MB
    pub max_batch_files: usize,            // Default: 10
    pub max_concurrent_uploads: usize,     // Default: 5  
    pub temp_dir: PathBuf,                 // Default: /tmp/moonlink-uploads
    pub cleanup_interval_secs: u64,        // Default: 3600 (1 hour)
    pub supported_formats: Vec<FileFormat>,
    pub enable_async_processing: bool,     // Default: true for large files
}
```

### 8. Integration with Existing Architecture

The file upload API will:

1. **Extend `ApiState`** to include upload configuration and temp storage management
2. **Reuse `RestSource` and `RestSink`** for processing converted data
3. **Leverage existing JSON converter** patterns for format-specific converters
4. **Follow the same authentication** middleware as other endpoints
5. **Use the same error response** structures for consistency

### 9. Implementation Components

**New modules to add:**
- `src/moonlink_connectors/src/rest_ingest/file_converters/`
  - `csv_converter.rs`
  - `jsonl_converter.rs` 
  - `parquet_converter.rs`
  - `arrow_converter.rs`
- `src/moonlink_service/src/file_upload.rs` - Upload handlers
- `src/moonlink_service/src/upload_manager.rs` - Async processing

**Enhanced existing:**
- `rest_api.rs` - Add upload endpoints
- `json_converter.rs` - Extract common conversion traits

## Example Usage

### Upload CSV File

```bash
curl -X POST http://localhost:3030/upload/users \
  -H "Authorization: Bearer your-token" \
  -F "file=@users.csv" \
  -F "operation=insert" \
  -F "wait_for_commit=true"
```

### Batch Upload Multiple Files

```bash
curl -X POST http://localhost:3030/upload/users/batch \
  -H "Authorization: Bearer your-token" \
  -F "files[]=@users1.csv" \
  -F "files[]=@users2.jsonl" \
  -F "operations[]=insert" \
  -F "operations[]=upsert" \
  -F "batch_mode=atomic"
```

### Check Upload Status

```bash
curl -X GET http://localhost:3030/upload/550e8400-e29b-41d4-a716-446655440000/status \
  -H "Authorization: Bearer your-token"
```

## Implementation Roadmap

### Phase 1: Core Infrastructure
- [ ] File upload handling with multipart/form-data
- [ ] Basic CSV and JSONL format support
- [ ] Simple single-file upload endpoint
- [ ] Integration with existing RestSource/RestSink

### Phase 2: Advanced Features
- [ ] Batch upload support
- [ ] Async processing for large files
- [ ] Upload status tracking
- [ ] Enhanced error handling and validation

### Phase 3: Format Extensions
- [ ] Parquet format support
- [ ] Arrow IPC format support
- [ ] Schema inference and validation
- [ ] Advanced CSV parsing options

### Phase 4: Production Features
- [ ] Authentication and authorization
- [ ] Rate limiting and quotas
- [ ] Monitoring and metrics
- [ ] Performance optimizations

This design provides production-ready file upload capabilities while maintaining consistency with Moonlink's existing REST API architecture and data processing pipeline.