# PostgreSQL CSV Loader - API Documentation

## Overview

The PostgreSQL CSV Loader provides a comprehensive REST API for advanced CSV data ingestion with staging capabilities, ZIP file processing, and dynamic schema evolution.

## Base URL

```
http://localhost:8081
```

## Authentication

Currently, the API does not require authentication. In production environments, consider implementing:
- API Keys
- JWT tokens
- OAuth 2.0
- Basic Authentication

## Content Types

### Request Content Types
- `multipart/form-data` - For file uploads
- `application/json` - For JSON payloads

### Response Content Types
- `application/json` - All API responses

## API Endpoints

### 1. Database Operations

#### Get Database Connection Information
```http
GET /api/v1/ingest/database/info
```

**Description**: Retrieves comprehensive database connection details and system status.

**Response (200 OK)**:
```json
{
  "database_url": "jdbc:postgresql://localhost:5432/postgres",
  "database_name": "postgres",
  "username": "postgres",
  "connection_pool_size": 20,
  "connection_status": "HEALTHY",
  "schemas_available": ["public", "staging", "audit"],
  "staging_schema_exists": true,
  "server_time": "2025-10-27T23:07:30"
}
```

**Fields**:
- `database_url`: Full JDBC connection URL
- `database_name`: Target database name
- `username`: Database username in use
- `connection_pool_size`: Current HikariCP pool size
- `connection_status`: Connection health (HEALTHY/UNHEALTHY)
- `schemas_available`: List of available database schemas
- `staging_schema_exists`: Whether staging schema is ready
- `server_time`: Current server timestamp

### 2. Health Check

#### Get Application Health Status
```http
GET /api/v1/ingest/database/health
```

**Description**: Comprehensive health check for application components.

**Response (200 OK)**:
```json
{
  "status": "HEALTHY",
  "database_connection": "HEALTHY",
  "staging_ready": true,
  "timestamp": "2025-10-30T15:30:00"
}
```

### 3. Single File Processing

#### Upload CSV to Staging
```http
POST /api/v1/ingest/csv/upload
```

**Description**: Upload a single CSV file to create a staging table with automatic schema detection.

**Request**:
- **Content-Type**: `multipart/form-data`
- **Body**: Form data with file parameter

**cURL Example**:
```bash
curl -X POST \
  -F "file=@orders.csv" \
  http://localhost:8081/api/v1/ingest/staging/upload
```

**Response (200 OK)**:
```json
{
  "batchId": "550e8400-e29b-41d4-a716-446655440000",
  "fileName": "orders.csv",
  "tableName": "staging_orders_550e8400",
  "fileSizeBytes": 1048576,
  "status": "COMPLETED",
  "message": "CSV file processed to staging area successfully"
}
```

**Response (200 OK) - Already Processed (Idempotency)**:
```json
{
  "batchId": "550e8400-e29b-41d4-a716-446655440000",
  "fileName": "orders.csv",
  "tableName": "staging_orders_550e8400",
  "fileSizeBytes": 1048576,
  "status": "ALREADY_PROCESSED",
  "message": "File already processed previously. Original processing completed on 2025-10-30T14:20:00. Batch ID: 550e8400-e29b-41d4-a716-446655440000. No duplicate data was inserted."
}
```

### 4. ZIP File Processing

#### Analyze ZIP File Contents
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

**Description**: Extract ZIP file and process all CSV files to staging environment.

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
      "error_message": null
    },
    {
      "filename": "customers.csv",
      "table_name": "customers",
      "status": "SUCCESS",
      "rows_loaded": 2500,
      "columns_created": 4,
      "processing_time_ms": 12000,
      "error_message": null
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

### 5. Batch Processing

#### Process Multiple Files to Staging
```http
POST /api/v1/ingest/batch/process
```

**Description**: Process multiple CSV files simultaneously to staging environment.

**Request**:
- **Content-Type**: `multipart/form-data`
- **Body**: Form data with multiple file parameters

**cURL Example**:
```bash
curl -X POST \
  -F "files=@orders.csv" \
  -F "files=@customers.csv" \
  -F "files=@products.csv" \
  http://localhost:8081/api/v1/ingest/batch/process-to-staging
```

**Response (200 OK)**:
```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "processing_status": "PARTIAL_SUCCESS",
  "total_files_submitted": 3,
  "successful_files": 2,
  "failed_files": 1,
  "total_rows_loaded": 7500,
  "processing_start_time": "2025-10-27T23:20:00",
  "processing_end_time": "2025-10-27T23:20:30",
  "processing_duration_ms": 30000,
  "staging_schema": "staging",
  "file_results": [
    {
      "filename": "orders.csv",
      "table_name": "orders",
      "status": "SUCCESS",
      "rows_loaded": 5000,
      "columns_created": 5,
      "processing_time_ms": 15000,
      "error_message": null
    },
    {
      "filename": "customers.csv",
      "table_name": "customers",
      "status": "SUCCESS",
      "rows_loaded": 2500,
      "columns_created": 4,
      "processing_time_ms": 12000,
      "error_message": null
    },
    {
      "filename": "products.csv",
      "table_name": "products",
      "status": "FAILED",
      "rows_loaded": 0,
      "columns_created": 0,
      "processing_time_ms": 3000,
      "error_message": "Invalid CSV format: Missing required headers"
    }
  ],
  "validation_summary": {
    "tables_ready_for_production": 2,
    "tables_requiring_review": 0,
    "data_quality_issues": ["products.csv: Header validation failed"],
    "schema_conflicts": []
  }
}
```

### 6. Staging Management

#### List Staging Tables
```http
GET /api/v1/ingest/staging/tables
```

**Description**: Get list of all staging tables filtered by the configured table prefix.

**Response (200 OK)**:
```json
{
  "schema": "default",
  "table_count": 3,
  "tables": [
    "staging_orders_550e8400",
    "staging_customers_661f9511",
    "staging_products_772fa622"
  ]
}
```

#### Delete All Staging Tables
```http
DELETE /api/v1/ingest/staging/tables
```

**Description**: Drop all staging tables filtered by the configured table prefix. **WARNING: This action is irreversible!**

**cURL Example**:
```bash
curl -X DELETE http://localhost:8081/api/v1/ingest/staging/tables
```

**Response (200 OK)**:
```json
{
  "status": "SUCCESS",
  "message": "All staging tables dropped successfully",
  "tables_dropped": 3,
  "dropped_tables": [
    "staging_orders_550e8400",
    "staging_customers_661f9511",
    "staging_products_772fa622"
  ]
}
```

### 7. Status Monitoring

#### Get Processing Status
```http
GET /api/v1/ingest/csv/status/{batchId}
```

**Description**: Get detailed status of a processing operation by batch ID.

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

### 8. Watch Folder Operations

#### Get Watch Folder Status
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
| 429 | Too Many Requests - Rate limit exceeded |
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

## Rate Limiting

The API implements rate limiting to ensure fair usage:

### Limits
- **File Upload**: 10 requests per minute per IP
- **Status Queries**: 100 requests per minute per IP
- **Connection Info**: 60 requests per minute per IP

### Rate Limit Headers

```http
X-RateLimit-Limit: 10
X-RateLimit-Remaining: 8
X-RateLimit-Reset: 1698441600
```

### Rate Limit Exceeded Response

```json
{
  "timestamp": "2025-10-27T23:45:00",
  "status": 429,
  "error": "Too Many Requests",
  "message": "Rate limit exceeded. Try again in 60 seconds",
  "retry_after_seconds": 60
}
```

## Request/Response Examples

### Complete ZIP Processing Workflow

#### 1. Check Connection
```bash
curl -X GET http://localhost:8081/api/v1/ingest/database/info
```

#### 2. Check Health
```bash
curl -X GET http://localhost:8081/api/v1/ingest/database/health
```

#### 3. Analyze ZIP
```bash
curl -X POST \
  -F "file=@enterprise-data.zip" \
  http://localhost:8081/api/v1/ingest/zip/analyze
```

#### 4. Process to Staging
```bash
curl -X POST \
  -F "file=@enterprise-data.zip" \
  http://localhost:8081/api/v1/ingest/zip/process
```

#### 5. Review Staging Tables
```bash
curl -X GET http://localhost:8081/api/v1/ingest/staging/tables
```

#### 6. Monitor Processing
```bash
curl -X GET http://localhost:8081/api/v1/ingest/csv/status/{batch_id}
```

#### 7. Clean Up (Delete All Staging Tables)
```bash
curl -X DELETE http://localhost:8081/api/v1/ingest/staging/tables
```

## Best Practices

### File Upload Guidelines
- **File Size**: Keep individual files under 500MB for optimal performance
- **ZIP Archives**: Limit ZIP files to 1GB total extracted size
- **CSV Format**: Ensure proper UTF-8 encoding and consistent delimiters
- **Headers**: Use descriptive, SQL-safe column names

### Performance Optimization
- **Batch Processing**: Use batch endpoints for multiple files
- **Staging Review**: Always review data in staging before production migration
- **Monitoring**: Use status endpoints to track long-running operations
- **Error Handling**: Implement retry logic for transient failures

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