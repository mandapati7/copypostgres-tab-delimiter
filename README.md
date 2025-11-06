# PostgreSQL CSV Loader

Enterprise-grade CSV data ingestion platform for PostgreSQL with staging capabilities, batch processing, ZIP file support, and automated watch folder monitoring.

## 🚀 Features

### Core Capabilities
- **High-Performance Loading**: Uses PostgreSQL COPY command for optimal throughput
- **Dynamic Table Creation**: Automatically creates staging tables from CSV files
- **Staging Area Management**: Dedicated staging schema for data review and validation
- **ZIP File Processing**: Extract and process multiple CSV files from ZIP archives
- **Batch Processing**: Handle multiple CSV files simultaneously with detailed reporting
- **Watch Folder Automation**: Automated file monitoring with 4-folder lifecycle
- **Idempotency**: SHA-256 checksum-based duplicate detection prevents reprocessing
- **Dynamic Schema Detection**: Automatically detects and creates tables based on CSV structure
- **Real-time Status**: Track processing progress with comprehensive manifest data

### Features
- **Comprehensive Validation**: File type, size, and format validation
- **Error Handling**: Detailed error reporting and retry mechanisms
- **Audit Trail**: Complete processing history and manifest tracking
- **Health Monitoring**: Database connection and system health checks
- **RESTful API**: Full REST API with OpenAPI/Swagger documentation

## 📋 Prerequisites

- **Java**: 21 or higher (OpenJDK or Oracle JDK)
- **Maven**: 3.8+ for build automation
- **PostgreSQL**: 12+ with connection credentials
- **Memory**: Minimum 2GB RAM (4GB+ recommended for large files)

## 🛠️ Installation

### 1. Clone the Repository
```bash
# TODO: Replace with actual repository URL (GitHub/GitLab/etc.)
# git clone <repository-url>
cd <project-directory>
```

### 2. Configure Database Connection
Update `src/main/resources/application.properties`:
```properties
# Database Configuration
spring.datasource.url=jdbc:postgresql://localhost:5432/your_database
spring.datasource.username=your_username
spring.datasource.password=your_password

# Server Configuration
server.port=8081

# File Upload Limits
spring.servlet.multipart.max-file-size=500MB
spring.servlet.multipart.max-request-size=500MB
```

### 3. Build the Application
```bash
mvn clean package
```

### 4. Run the Application
```bash
java -jar target/postgres-csv-loader-0.0.1-SNAPSHOT.jar
```

Application starts on: `http://localhost:8081`

### 5. Verify Installation
- **Web UI**: Visit `http://localhost:8081`
- **Swagger UI**: Visit `http://localhost:8081/swagger-ui/index.html`
- **API Docs**: Visit `http://localhost:8081/v3/api-docs`

## 🎯 API Endpoints

### Database Operations
```http
GET  /api/v1/ingest/database/info       # Database connection details
GET  /api/v1/ingest/database/health     # Health check
```

### CSV File Operations
```http
POST /api/v1/ingest/csv/upload          # Upload single CSV to staging
GET  /api/v1/ingest/csv/status/{batchId} # Get processing status
```

### ZIP File Operations
```http
POST /api/v1/ingest/zip/analyze         # Analyze ZIP contents
POST /api/v1/ingest/zip/process         # Process ZIP to staging
```

### Batch Operations
```http
POST /api/v1/ingest/batch/process       # Process multiple CSV files
```

### Staging Management
```http
GET    /api/v1/ingest/staging/tables    # List staging tables
DELETE /api/v1/ingest/staging/tables    # Delete all staging tables
```

### Watch Folder Operations
```http
GET  /api/v1/ingest/watch-folder/status         # Get watch folder status
GET  /api/v1/ingest/watch-folder/files          # List all files
GET  /api/v1/ingest/watch-folder/files/{folder} # List files in specific folder
GET  /api/v1/ingest/watch-folder/errors         # List error reports
POST /api/v1/ingest/watch-folder/retry/{filename} # Retry failed file
```

## 🔄 Watch Folder Feature

The Watch Folder feature provides fully automated CSV/ZIP file processing by monitoring a designated folder structure.

### 4-Folder Architecture
```
C:/data/csv-loader/
├── upload/          # Drop files here with .done marker
│   ├── orders.csv
│   └── orders.csv.done
├── wip/             # Files being processed (Work In Progress)
├── error/           # Failed files with error reports
│   ├── invalid_data.csv
│   └── invalid_data.csv.error.json
└── archive/         # Successfully processed files
    └── orders_2025-11-03_14-20-00.csv
```

### Configuration
Add to `application.properties`:
```properties
# Watch Folder Configuration
watch.folder.enabled=true
watch.folder.root=C:/data/csv-loader
watch.folder.upload=${watch.folder.root}/upload
watch.folder.wip=${watch.folder.root}/wip
watch.folder.error=${watch.folder.root}/error
watch.folder.archive=${watch.folder.root}/archive

# Marker file settings
watch.folder.use-marker-files=true
watch.folder.marker-extension=.done

# Processing settings
watch.folder.polling-interval=5000
watch.folder.max-concurrent-files=5
watch.folder.supported-extensions=.csv,.zip

# File stability checks
watch.folder.stability-check-delay=2000
watch.folder.stability-check-retries=3

# Retention policies
watch.folder.archive.retention-days=90
watch.folder.error.retention-days=30

# Cleanup settings
watch.folder.cleanup.enabled=true
watch.folder.cleanup.cron=0 0 2 * * *
```

### How It Works

**Automated Workflow:**
1. **Upload**: Place CSV/ZIP file in `upload/` folder
2. **Mark Ready**: Create marker file (e.g., `orders.csv.done`)
3. **Auto-Detect**: System automatically detects marker file
4. **Process**: File moves to `wip/` and processing begins
5. **Success**: File moves to `archive/` with timestamp
6. **Failure**: File moves to `error/` with detailed error report

### Usage Example
```bash
# Copy file to upload folder
copy orders.csv C:\data\csv-loader\upload\

# Create marker file to trigger processing
type nul > C:\data\csv-loader\upload\orders.csv.done
```

## 📊 Quick Start Examples

### 1. Check Database Connection
```bash
curl -X GET http://localhost:8081/api/v1/ingest/database/info
```

### 2. Upload Single CSV File
```bash
curl -X POST http://localhost:8081/api/v1/ingest/csv/upload \
  -F "file=@orders.csv"
```

**Response:**
```json
{
  "batchId": "a1b2c3d4-1234-5678-90ab-cdef12345678",
  "fileName": "orders.csv",
  "tableName": "staging_orders_a1b2c3d4",
  "fileSizeBytes": 15420,
  "status": "COMPLETED",
  "message": "CSV file processed to staging area successfully"
}
```

### 3. Upload ZIP File (Multiple CSVs)
```bash
curl -X POST http://localhost:8081/api/v1/ingest/zip/process \
  -F "file=@data-export.zip"
```

**Response:**
```json
{
  "batch_id": "b2c3d4e5-5678-90ab-cdef-1234567890ab",
  "processing_status": "SUCCESS",
  "total_files_processed": 3,
  "successful_files": 3,
  "failed_files": 0,
  "total_rows_loaded": 15420,
  "file_results": [
    {
      "filename": "orders.csv",
      "table_name": "staging_orders_b2c3d4e5",
      "status": "SUCCESS",
      "rows_loaded": 5420
    }
  ]
}
```

### 4. Check Processing Status
```bash
curl -X GET http://localhost:8081/api/v1/ingest/csv/status/a1b2c3d4-1234-5678-90ab-cdef12345678
```

### 5. Query Your Staging Data
```sql
-- Connect to PostgreSQL
psql -h localhost -U postgres -d your_database

-- List all staging tables
SELECT table_name FROM information_schema.tables
WHERE table_name LIKE 'staging_%'
ORDER BY table_name;

-- Query data from staging table
SELECT * FROM staging_orders_a1b2c3d4 LIMIT 10;

-- Check row counts
SELECT COUNT(*) as row_count FROM staging_orders_a1b2c3d4;
```

## 🏗️ Architecture

### System Components

**Controllers:**
- `DatabaseController` - Database connection and health monitoring
- `CsvController` - Single CSV file processing
- `ZipController` - ZIP file analysis and processing
- `BatchController` - Multiple file batch processing
- `StagingController` - Staging table management
- `WatchFolderController` - Watch folder monitoring and management

**Services:**
- `CsvProcessingService` - Core CSV processing and staging table creation
- `BatchProcessingService` - Batch operations coordination
- `ZipProcessingService` - ZIP extraction and analysis
- `WatchFolderService` - Automated file monitoring
- `WatchFolderManager` - File lifecycle management across 4 folders
- `DatabaseConnectionService` - Database operations and schema management
- `IngestionManifestService` - Processing audit and tracking

**Data Layer:**
- PostgreSQL COPY command for bulk data loading
- Staging tables with TEXT columns for all data
- Ingestion manifest for processing metadata and status tracking

### Data Flow

```
CSV/ZIP Files → Validation → Checksum → Table Creation → COPY → Manifest → Staging
```

### Table Creation Process

1. **Upload CSV File**: File is uploaded via API or watch folder
2. **Generate Batch ID**: UUID v4 is generated (e.g., `a1b2c3d4`)
3. **Checksum Calculation**: SHA-256 hash computed for idempotency
4. **Duplicate Check**: System checks if file was previously processed
5. **Table Name Generation**: `staging_{filename}_{batch_id_8chars}`
   - Example: `orders.csv` → `staging_orders_a1b2c3d4`
6. **Schema Detection**: CSV headers become column names (sanitized for SQL)
7. **Table Creation**: `CREATE TABLE` with all columns as `TEXT` type
8. **Data Loading**: PostgreSQL COPY command loads data efficiently
9. **Manifest Recording**: Processing metadata saved to `ingestion_manifest`

### Idempotency

The system uses SHA-256 file checksums to prevent duplicate processing:

1. **First Upload**: File is processed, checksum stored in `ingestion_manifest`
2. **Duplicate Upload**: Same file uploaded again
3. **Checksum Match**: System detects duplicate via checksum
4. **Response**: Returns original batch ID with `ALREADY_PROCESSED` status
5. **No Duplicate Data**: No new table created, no data reinserted

## 🔐 Validation & Security

### File Validation (FileValidationUtil)
- **Not Empty Check**: File must not be null or empty
- **Extension Check**: Must match allowed extensions (.csv, .zip) and have a valid filename
- **Size Check**: Enforced by Spring Boot configuration

### Data Validation
- **CSV Format**: Proper UTF-8 encoding and consistent delimiters
- **Headers**: Descriptive, SQL-safe column names
- **Content**: Data type validation and malformed content detection

### Security Considerations
- **Input Validation**: All inputs validated at client and server side
- **File Scanning**: Consider virus scanning for uploaded files
- **Access Control**: Authentication can be added in production
- **Audit Logging**: All file processing activities are logged

## 📚 API Documentation

For detailed API endpoint documentation, see [API-DOCUMENTATION.md](./API-DOCUMENTATION.md)

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## � Support

For support, please contact the development team or create an issue in the repository.

---

**Built with Spring Boot 3.4.0, Java 21, and PostgreSQL**
