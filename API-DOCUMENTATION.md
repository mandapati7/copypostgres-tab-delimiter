# PostgreSQL CSV Loader - API Documentation

## Overview

The PostgreSQL CSV Loader provides a comprehensive REST API for processing predefined Title D application file types (PM1-PM7, IM1-IM3) with fixed schemas, ZIP file processing with parent-child batch tracking, configurable validation rules, data transformation capabilities, and concurrent processing support.

## Supported File Types

The system processes tab-delimited files with predefined schemas for Title D applications:

### Property Master Files (PM1-PM7)
- **PM1**: Property Master Table (17 columns) - Core property information
- **PM2**: Property-Instrument Relationships (5 columns) - Links properties to instruments  
- **PM3**: Alternative Party Information (7 columns) - Additional party details
- **PM4**: Parent Property Relationships (4 columns) - Property hierarchies
- **PM5**: Property Descriptions (3 columns) - Thumbnail/property descriptions
- **PM6**: Property Comments (3 columns) - Additional property comments
- **PM7**: Property Legal Descriptions (10 columns) - Legal property descriptions

### Instrument Master Files (IM1-IM3)
- **IM1**: Instrument Master Table (13 columns) - Core instrument information
- **IM2**: Instrument Details (9 columns) - Detailed instrument data with date transformations
- **IM3**: Instrument Comments (3 columns) - Instrument-related comments

### File Validation Features
- Configurable validation rules per file pattern
- Auto-fix capabilities for common data issues
- Data transformation (character replacement, date formatting)
- Comprehensive issue tracking and reporting

## Base URL

```
http://localhost:8081
```

## Content Types

### Request Content Types
- `multipart/form-data` - For file uploads
- `application/json` - For JSON payloads

### Response Content Types
- `application/json` - All API responses

## API Endpoints

### 1. ZIP Operations

#### Analyze ZIP File
```http
POST /api/v1/ingest/zip/analyze
```

**Description**: Extract and analyze ZIP file contents without processing data.

**Request**:
- **Content-Type**: `multipart/form-data`
- **Body**: Form data with ZIP file

**cURL Example**:
```bash
curl -X POST \
  -F "file=@data-export.zip" \
  http://localhost:8081/api/v1/ingest/zip/analyze
```

**Response (200 OK)**:
```json
{
  "zip_filename": "data-export.zip",
  "total_files_extracted": 5,
  "csv_files_found": 3,
  "extraction_status": "SUCCESS",
  "extraction_path": "/tmp/csv-enterprise/extract-550e8400",
  "extracted_files": [
    {
      "filename": "orders.csv",
      "file_size": 1048576,
      "file_type": "CSV",
      "estimated_rows": 5000,
      "headers_detected": ["order_id", "customer_id", "product_name", "quantity", "price"],
      "suggested_table_name": "orders",
      "staging_table_exists": false
    }
  ],
  "processing_recommendations": [
    "3 new tables will be created in staging schema",
    "Estimated total rows to process: 15,000",
    "Review table structures before proceeding to production migration",
    "Consider processing in batches for optimal performance"
  ],
  "estimated_processing_time_seconds": 45
}
```

#### Process ZIP File to Staging
```http
POST /api/v1/ingest/zip/process
```

**Description**: Extract ZIP file and process all CSV files to staging environment with parent-child batch tracking.

**Request**:
- **Content-Type**: `multipart/form-data`
- **Body**: Form data with ZIP file

**cURL Example**:
```bash
curl -X POST \
  -F "file=@data-export.zip" \
  http://localhost:8081/api/v1/ingest/zip/process
```

**Response (200 OK)**:
```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "zip_filename": "data-export.zip",
  "processing_status": "SUCCESS",
  "total_files_processed": 3,
  "successful_files": 3,
  "failed_files": 0,
  "total_rows_loaded": 15000,
  "processing_start_time": "2025-10-27T23:15:00",
  "processing_end_time": "2025-10-27T23:15:45",
  "processing_duration_ms": 45000,
  "staging_schema": "staging",
  "extraction_path": "/tmp/csv-enterprise/extract-550e8400",
  "file_results": [
    {
      "filename": "orders.csv",
      "table_name": "orders",
      "status": "SUCCESS",
      "rows_loaded": 5000,
      "columns_created": 5,
      "processing_time_ms": 15000,
      "error_message": null,
      "child_batch_id": "a1b2c3d4-e29b-41d4-a716-446655440001",
      "parent_batch_id": "550e8400-e29b-41d4-a716-446655440000"
    }
  ],
  "validation_summary": {
    "tables_ready_for_production": 3,
    "tables_requiring_review": 0,
    "data_quality_issues": [],
    "schema_conflicts": []
  }
}
```

**Parent-Child Relationship Fields**:
- `batch_id`: Parent batch ID for the entire ZIP processing operation
- `child_batch_id`: Individual batch ID for each CSV file processed from the ZIP
- `parent_batch_id`: References the parent ZIP batch (same as `batch_id` for child records)

**Query Relationships**:
```sql
-- Find all child batches for a ZIP file
SELECT * FROM ingestion_manifest
WHERE parent_batch_id = '550e8400-e29b-41d4-a716-446655440000';

-- Get parent ZIP information for a child batch
SELECT * FROM ingestion_manifest
WHERE batch_id = '550e8400-e29b-41d4-a716-446655440000';
```

### 2. Delimited File Operations

#### Upload Delimited File (TSV/CSV)
```http
POST /api/v1/ingest/delimited/upload
```

**Description**: Upload a single delimited file (TSV or CSV) for processing into predefined Title D tables based on filename pattern routing.

**Request**:
- **Content-Type**: `multipart/form-data`
- **Body**: Form data with file parameter
- **Query Parameters**:
  - `format`: File format (csv or tsv, default: csv)
  - `hasHeaders`: Whether file has headers (default: true for CSV, false for TSV)
  - `routeByFilename`: Enable filename-based routing (default: true)

**Supported File Patterns**:
- `PM1xx` → `title_d_app.pm1` table
- `PM2xx` → `title_d_app.pm2` table
- `PM3xx` → `title_d_app.pm3` table
- `PM4xx` → `title_d_app.pm4` table
- `PM5xx` → `title_d_app.pm5` table
- `PM6xx` → `title_d_app.pm6` table
- `PM7xx` → `title_d_app.pm7` table
- `IM1xx` → `title_d_app.im1` table
- `IM2xx` → `title_d_app.im2` table
- `IM3xx` → `title_d_app.im3` table

**cURL Example**:
```bash
# Upload TSV file with filename routing (default)
curl -X POST \
  -F "file=@PM162" \
  http://localhost:8081/api/v1/ingest/delimited/upload

# Upload CSV file with explicit parameters
curl -X POST \
  -F "file=@data.csv" \
  "http://localhost:8081/api/v1/ingest/delimited/upload?format=csv&hasHeaders=true&routeByFilename=false"
```

**Response (200 OK)**:
```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "file_name": "PM162",
  "table_name": "pm1",
  "file_size_bytes": 1048576,
  "status": "SUCCESS",
  "total_records": 5000,
  "processed_records": 5000,
  "failed_records": 0,
  "corrected_records": 0,
  "warning_count": 0,
  "error_count": 0,
  "data_quality_status": "CLEAN",
  "processing_start_time": "2025-11-12T10:30:00",
  "processing_end_time": "2025-11-12T10:30:15",
  "processing_duration_ms": 15000,
  "message": "File processed successfully. Format: TSV, Headers: false, Routing: true. Loaded 5000 rows to table: pm1"
}
```

**Response (200 OK) - Already Processed (Idempotency)**:
```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "file_name": "PM162",
  "tableName": "staging_orders_550e8400",
  "fileSizeBytes": 1048576,
  "status": "ALREADY_PROCESSED",
  "message": "File already processed previously. Original processing completed on 2025-11-12T10:25:15. Batch ID: 550e8400-e29b-41d4-a716-446655440000. No duplicate data was inserted."
}
```

#### Get Delimited File Processing Status
```http
GET /api/v1/ingest/delimited/status/{batchId}
```

**Description**: Get the processing status and details for a specific batch.

**Path Parameters**:
- `batchId` (UUID, required): Batch ID returned from upload operation

**Response (200 OK)**:
```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "file_name": "PM162",
  "table_name": "pm1",
  "status": "SUCCESS",
  "total_records": 5000,
  "processed_records": 5000,
  "failed_records": 0,
  "corrected_records": 0,
  "warning_count": 0,
  "error_count": 0,
  "data_quality_status": "CLEAN",
  "processing_start_time": "2025-11-12T10:30:00",
  "processing_end_time": "2025-11-12T10:30:15",
  "processing_duration_ms": 15000,
  "error_message": null
}
```

### 3. Watch Folder Operations
```http
POST /api/v1/ingest/zip/analyze
```

**Description**: Extract and analyze ZIP file contents without processing data.

**Request**:
- **Content-Type**: `multipart/form-data`
- **Body**: Form data with ZIP file

**cURL Example**:
```bash
curl -X POST \
  -F "file=@data-export.zip" \
  http://localhost:8081/api/v1/ingest/zip/analyze
```

**Response (200 OK)**:
```json
{
  "zip_filename": "data-export.zip",
  "total_files_extracted": 5,
  "csv_files_found": 3,
  "extraction_status": "SUCCESS",
  "extraction_path": "/tmp/csv-enterprise/extract-550e8400",
  "extracted_files": [
    {
      "filename": "orders.csv",
      "file_size": 1048576,
      "file_type": "CSV",
      "estimated_rows": 5000,
      "headers_detected": ["order_id", "customer_id", "product_name", "quantity", "price"],
      "suggested_table_name": "orders",
      "staging_table_exists": false
    },
    {
      "filename": "customers.csv",
      "file_size": 524288,
      "file_type": "CSV",
      "estimated_rows": 2500,
      "headers_detected": ["customer_id", "name", "email", "created_date"],
      "suggested_table_name": "customers",
      "staging_table_exists": false
    }
  ],
  "processing_recommendations": [
    "3 new tables will be created in staging schema",
    "Estimated total rows to process: 15,000",
    "Review table structures before proceeding to production migration",
    "Consider processing in batches for optimal performance"
  ],
  "estimated_processing_time_seconds": 45
}
```

#### Process ZIP File to Staging
```http
POST /api/v1/ingest/zip/process
```

**Description**: Extract ZIP file and process all CSV files to staging environment with parent-child batch tracking.

**Request**:
- **Content-Type**: `multipart/form-data`
- **Body**: Form data with ZIP file

**cURL Example**:
```bash
curl -X POST \
  -F "file=@data-export.zip" \
  http://localhost:8081/api/v1/ingest/zip/process-to-staging
```

**Response (200 OK)**:
```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "zip_filename": "data-export.zip",
  "processing_status": "SUCCESS",
  "total_files_processed": 3,
  "successful_files": 3,
  "failed_files": 0,
  "total_rows_loaded": 15000,
  "processing_start_time": "2025-10-27T23:15:00",
  "processing_end_time": "2025-10-27T23:15:45",
  "processing_duration_ms": 45000,
  "staging_schema": "staging",
  "extraction_path": "/tmp/csv-enterprise/extract-550e8400",
  "file_results": [
    {
      "filename": "orders.csv",
      "table_name": "orders",
      "status": "SUCCESS",
      "rows_loaded": 5000,
      "columns_created": 5,
      "processing_time_ms": 15000,
      "error_message": null,
      "child_batch_id": "a1b2c3d4-e29b-41d4-a716-446655440001",
      "parent_batch_id": "550e8400-e29b-41d4-a716-446655440000"
    },
    {
      "filename": "customers.csv",
      "table_name": "customers",
      "status": "SUCCESS",
      "rows_loaded": 2500,
      "columns_created": 4,
      "processing_time_ms": 12000,
      "error_message": null,
      "child_batch_id": "b2c3d4e5-e29b-41d4-a716-446655440002",
      "parent_batch_id": "550e8400-e29b-41d4-a716-446655440000"
    }
  ],
  "validation_summary": {
    "tables_ready_for_production": 3,
    "tables_requiring_review": 0,
    "data_quality_issues": [],
    "schema_conflicts": []
  }
}
```

**Parent-Child Relationship Fields**:
- `batch_id`: Parent batch ID for the entire ZIP processing operation
- `child_batch_id`: Individual batch ID for each CSV file processed from the ZIP
- `parent_batch_id`: References the parent ZIP batch (same as `batch_id` for child records)

**Query Relationships**:
```sql
-- Find all child batches for a ZIP file
SELECT * FROM ingestion_manifest
WHERE parent_batch_id = '550e8400-e29b-41d4-a716-446655440000';

-- Get parent ZIP information for a child batch
SELECT * FROM ingestion_manifest
}
```

### 4. Status Monitoring

#### Get Processing Status
```http
GET /api/v1/ingest/delimited/status/{batchId}
```

**Description**: Get detailed status of a delimited file processing operation by batch ID.

**Path Parameters**:
- `batchId` (string, required): Batch ID returned from processing operations

**cURL Example**:
```bash
curl -X GET http://localhost:8081/api/v1/ingest/csv/status/550e8400-e29b-41d4-a716-446655440000
```

**Response (200 OK)**:
```json
{
  "batchId": "550e8400-e29b-41d4-a716-446655440000",
  "fileName": "orders.csv",
  "status": "COMPLETED",
  "totalRecords": 5000,
  "processedRecords": 5000,
  "failedRecords": 0,
  "completionPercentage": 100.0,
  "startedAt": "2025-10-30T14:20:00",
  "completedAt": "2025-10-30T14:20:15",
  "processingDurationMs": 15000
}
```

**Response (404 Not Found)**:
When batch ID doesn't exist or is invalid.

## Request/Response Examples
```http
GET /api/v1/ingest/watch-folder/status
```

**Description**: Get comprehensive status of the watch folder system including file counts in each folder and service status.

**Response (200 OK)**:
```json
{
  "uploadCount": 5,
  "wipCount": 2,
  "errorCount": 1,
  "archiveCount": 25,
  "watchEnabled": true,
  "watchRunning": true,
  "lastCheck": "2025-11-03T14:30:00",
  "totalProcessedToday": 0
}
```

**Fields**:
- `uploadCount`: Number of files waiting to be processed
- `wipCount`: Number of files currently being processed
- `errorCount`: Number of files that failed processing
- `archiveCount`: Number of successfully processed files
- `watchEnabled`: Whether watch folder feature is enabled in configuration
- `watchRunning`: Whether the watch service is currently running
- `lastCheck`: Timestamp of last status check
- `totalProcessedToday`: Total files processed today (feature for future implementation)

#### List All Files in Watch Folders
```http
GET /api/v1/ingest/watch-folder/files
```

**Description**: List all files in all watch folders (upload, wip, error, archive).

**Response (200 OK)**:
```json
{
  "upload": ["orders.csv", "customers.csv.done"],
  "wip": ["processing_orders.csv"],
  "error": ["invalid_data.csv", "invalid_data.csv.error.json"],
  "archive": ["orders_2025-11-03_14-20-00.csv", "customers_2025-11-03_14-25-00.csv"]
}
```

#### List Files in Specific Folder
```http
GET /api/v1/ingest/watch-folder/files/{folder}
```

**Description**: List files in a specific watch folder.

**Path Parameters**:
- `folder` (string, required): Folder name (upload, wip, error, or archive)

**cURL Example**:
```bash
curl -X GET http://localhost:8081/api/v1/ingest/watch-folder/files/upload
```

**Response (200 OK)**:
```json
{
  "folder": "upload",
  "count": 3,
  "files": ["orders.csv", "customers.csv.done", "products.csv"]
}
```

**Response (400 Bad Request)**:
```json
{
  "error": "Invalid folder name. Use: upload, wip, error, or archive"
}
```

#### List Error Reports
```http
GET /api/v1/ingest/watch-folder/errors
```

**Description**: List all error report JSON files with details.

**Response (200 OK)**:
```json
{
  "total_errors": 2,
  "errors": [
    {
      "file": "invalid_data_2025-11-03_14-30-00.csv.error.json",
      "size": 2048,
      "created": "2025-11-03T14:30:15"
    },
    {
      "file": "malformed_csv_2025-11-03_14-35-00.csv.error.json",
      "size": 1536,
      "created": "2025-11-03T14:35:22"
    }
  ]
}
```

#### Retry Failed File
```http
POST /api/v1/ingest/watch-folder/retry/{filename}
```

**Description**: Move a failed file from error folder back to upload folder for reprocessing.

**Path Parameters**:
- `filename` (string, required): Name of the file to retry (without .error.json extension)

**cURL Example**:
```bash
curl -X POST http://localhost:8081/api/v1/ingest/watch-folder/retry/invalid_data.csv
```

**Response (200 OK)**:
```json
{
  "status": "SUCCESS",
  "message": "File moved to upload folder and will be processed automatically",
  "filename": "invalid_data.csv"
}
```

**Response (404 Not Found)**:
```json
{
  "error": "File not found in error folder: invalid_data.csv"
}
```

## Request/Response Examples

#### Get All Validation Rules
```http
GET /api/validation/rules
```

**Description**: Retrieve all file validation rules configured in the system.

**Response (200 OK)**:
```json
[
  {
    "id": 1,
    "filePattern": "PM1",
    "tableName": "pm1",
    "expectedTabCount": 16,
    "validationEnabled": true,
    "autoFixEnabled": true,
    "rejectOnViolation": false,
    "replaceControlChars": false,
    "replaceNonLatinChars": false,
    "collapseConsecutiveReplaced": false,
    "enableDataTransformation": false,
    "transformerClassName": null,
    "description": "PM1 files should have 16 tabs per row (17 columns)",
    "createdAt": "2025-11-12T10:00:00",
    "updatedAt": "2025-11-12T10:00:00",
    "createdBy": "system"
  }
]
```

#### Get Validation Rule by Pattern
```http
GET /api/validation/rules/{filePattern}
```

**Description**: Get validation rule for a specific file pattern.

**Path Parameters**:
- `filePattern` (string, required): File pattern (e.g., "PM1", "IM2")

#### Get Validation Issues by Batch
```http
GET /api/validation/issues/batch/{batchId}
```

**Description**: Retrieve all validation issues for a specific processing batch.

**Path Parameters**:
- `batchId` (UUID, required): Batch ID from processing operation

**Response (200 OK)**:
```json
[
  {
    "id": 1,
    "batchId": "550e8400-e29b-41d4-a716-446655440000",
    "fileName": "PM162",
    "lineNumber": 5,
    "issueType": "TAB_COUNT_MISMATCH",
    "severity": "ERROR",
    "expectedValue": "16 tabs",
    "actualValue": "15 tabs",
    "description": "Row has incorrect number of tab delimiters",
    "autoFixed": false,
    "fixDescription": null,
    "originalLine": "block1\tprop1\t...\tvalue",
    "correctedLine": null,
    "createdAt": "2025-11-12T10:30:05"
  }
]
```

#### Create/Update Validation Rule
```http
POST /api/validation/rules
```

**Description**: Create or update a file validation rule.

**Request Body**:
```json
{
  "filePattern": "PM1",
  "tableName": "pm1",
  "expectedTabCount": 16,
  "validationEnabled": true,
  "autoFixEnabled": true,
  "rejectOnViolation": false,
  "replaceControlChars": false,
  "replaceNonLatinChars": false,
  "collapseConsecutiveReplaced": false,
  "enableDataTransformation": false,
  "transformerClassName": null,
  "description": "PM1 files should have 16 tabs per row (17 columns)"
}
```

#### Toggle Validation for Pattern
```http
PUT /api/validation/rules/{filePattern}/enabled
```

**Description**: Enable or disable validation for a specific file pattern.

**Path Parameters**:
- `filePattern` (string, required): File pattern to toggle

**Query Parameters**:
- `enabled` (boolean, required): Whether to enable or disable validation

## Error Handling

### Error Response Format

All API endpoints return errors in a consistent format:

```json
{
  "timestamp": "2025-10-27T23:35:00",
  "status": 400,
  "error": "Bad Request",
  "message": "File validation failed: Invalid CSV format",
  "path": "/api/v1/ingest/csv/upload",
  "trace_id": "550e8400-e29b-41d4-a716-446655440000",
  "details": {
    "validation_errors": [
      "Missing required headers",
      "Invalid column names detected"
    ]
  }
}
```

### HTTP Status Codes

| Status Code | Description |
|-------------|-------------|
| 200 | Success - Request completed successfully |
| 201 | Created - Resource created successfully |
| 400 | Bad Request - Invalid input parameters |
| 401 | Unauthorized - Authentication required |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Resource not found |
| 409 | Conflict - Resource already exists |
| 413 | Payload Too Large - File size exceeds limits |
| 422 | Unprocessable Entity - Valid format but semantic errors |
| 500 | Internal Server Error - Server-side error |
| 503 | Service Unavailable - Service temporarily unavailable |

### Common Error Scenarios

#### File Upload Errors

**File Too Large (413)**:
```json
{
  "timestamp": "2025-10-27T23:40:00",
  "status": 413,
  "error": "Payload Too Large",
  "message": "File size 600MB exceeds maximum allowed size of 500MB",
  "path": "/api/v1/ingest/staging/upload"
}
```

**Invalid File Type (422)**:
```json
{
  "timestamp": "2025-10-27T23:40:00",
  "status": 422,
  "error": "Unprocessable Entity",
  "message": "Invalid file type. Only CSV and ZIP files are supported",
  "path": "/api/v1/ingest/staging/upload"
}
```

#### Database Errors

**Connection Failed (503)**:
```json
{
  "timestamp": "2025-10-27T23:40:00",
  "status": 503,
  "error": "Service Unavailable",
  "message": "Database connection failed. Please try again later",
  "path": "/api/v1/ingest/connection/info"
}
```

#### Processing Errors

**Batch Not Found (404)**:
```json
{
  "timestamp": "2025-10-27T23:40:00",
  "status": 404,
  "error": "Not Found",
  "message": "Batch ID not found: 550e8400-e29b-41d4-a716-446655440000",
  "path": "/api/v1/ingest/status/550e8400-e29b-41d4-a716-446655440000"
}
```

## Request/Response Examples

### Complete ZIP Processing Workflow

#### 1. Analyze ZIP
```bash
curl -X POST \
  -F "file=@title-d-export.zip" \
  http://localhost:8081/api/v1/ingest/zip/analyze
```

#### 2. Process ZIP to Staging
```bash
curl -X POST \
  -F "file=@title-d-export.zip" \
  http://localhost:8081/api/v1/ingest/zip/process
```

#### 3. Monitor Processing Status
```bash
curl -X GET http://localhost:8081/api/v1/ingest/delimited/status/{batch_id}
```

#### 4. Check Validation Issues
```bash
curl -X GET http://localhost:8081/api/validation/issues/batch/{batch_id}
```

## Best Practices

### File Upload Guidelines
- **File Size**: Keep individual files under 500MB for optimal performance
- **ZIP Archives**: Limit ZIP files to 1GB total extracted size
- **CSV Format**: Ensure proper UTF-8 encoding and consistent delimiters
- **Headers**: Use descriptive, SQL-safe column names

### Performance Optimization
- **ZIP Processing**: Use ZIP upload for multiple related files with parent-child tracking
- **Watch Folder**: Enable automated processing for high-volume scenarios
- **Status Monitoring**: Use status endpoints to track long-running operations
- **Error Handling**: Implement retry logic for transient failures

### Data Quality
- **Validation Rules**: Configure file validation rules for data quality assurance
- **Issue Tracking**: Monitor validation issues and auto-fix capabilities
- **File Patterns**: Use consistent filename patterns for automatic routing

### Security Considerations
- **Input Validation**: Always validate file contents and structure
- **File Scanning**: Consider virus scanning for uploaded files
- **Access Control**: Implement authentication in production environments
- **Audit Logging**: Monitor all file processing activities

## SDK and Client Libraries

### cURL Examples
All endpoints include cURL examples for easy testing and integration.

### Postman Collection
Use the provided `Enterprise-PostgreSQL-CSV-Loader.postman_collection.json` for comprehensive API testing.

### Future SDKs
- **Java SDK**: Native Spring Boot integration
- **Python SDK**: For data science workflows
- **JavaScript SDK**: For web applications
- **Go SDK**: For microservices integration

---

**For more information, see the main [README.md](README.md) documentation.**