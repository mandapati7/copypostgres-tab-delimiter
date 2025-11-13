# PostgreSQL CSV Loader

Enterprise-grade Title D data ingestion platform for PostgreSQL with fixed schema processing, ZIP file support, automated watch folder monitoring, parent-child relationship tracking, and configurable validation rules.

## 🚀 Features

### Core Capabilities
- **Fixed Schema Processing**: Processes predefined Title D application file types (PM1-PM7, IM1-IM3)
- **File Validation Rules**: Configurable validation rules per file pattern with auto-fix capabilities
- **Data Transformation**: Custom transformers for data cleaning and format conversion
- **Schema Management**: Dedicated title_d_app schema for Title D data processing and validation
- **ZIP File Processing**: Extract and process multiple Title D files from ZIP archives with parent-child batch tracking
- **Parent-Child Relationship Tracking**: ZIP files become parent batches, extracted files become child batches
- **Watch Folder Automation**: Automated file monitoring with 4-folder lifecycle and concurrent processing support
- **Idempotency**: SHA-256 checksum-based duplicate detection prevents reprocessing
- **Data Quality Tracking**: Comprehensive validation issue tracking and reporting
- **Real-time Status**: Track processing progress with comprehensive manifest data
- **Concurrent Processing**: Safe concurrent ZIP file processing without file collisions

### Features
- **Title D File Processing**: Specialized processing for PM1-PM7 and IM1-IM3 file types
- **Tab-Delimited Format**: Native support for tab-separated values without headers
- **File Pattern Routing**: Automatic routing based on filename patterns (PM162 → pm1 table)
- **Comprehensive Validation**: File type, size, and format validation with configurable rules
- **Auto-Fix Capabilities**: Automatic correction of common data issues
- **Data Transformation**: Custom transformers for date formatting and data cleaning
- **Error Handling**: Detailed error reporting and retry mechanisms
- **Audit Trail**: Complete processing history and manifest tracking
- **Health Monitoring**: Database connection and system health checks
- **RESTful API**: Full REST API with OpenAPI/Swagger documentation

## � Supported File Types & Tables

The system processes predefined Title D application file types with fixed schemas:

### Property Master Files (PM)
| File Pattern | Target Table | Description | Columns |
|-------------|-------------|-------------|---------|
| `PM1` | `title_d_app.pm1` | Property Master Table | 17 columns (block_num, property_id_num, registration_system_code, etc.) |
| `PM2` | `title_d_app.pm2` | Property-Instrument Relationship | 5 columns (block_num, property_id_num, lro_num, instrument_num, rule_out_ind) |
| `PM3` | `title_d_app.pm3` | Alternative Party Information | 7 columns (block_num, property_id_num, alt_party_id_code, alt_party_name, etc.) |
| `PM4` | `title_d_app.pm4` | Parent Property Relationships | 4 columns (block_num, property_id_num, parent_block_num, parent_property_id_num) |
| `PM5` | `title_d_app.pm5` | Property Thumbnail/Description | 3 columns (block_num, property_id_num, thumbnail_desc) |
| `PM6` | `title_d_app.pm6` | Property Comments | 3 columns (block_num, property_id_num, comment_desc) |
| `PM7` | `title_d_app.pm7` | Property Legal Description | 10 columns (block_num, property_id_num, legal_desc, etc.) |

### Instrument Master Files (IM)
| File Pattern | Target Table | Description | Columns |
|-------------|-------------|-------------|---------|
| `IM1` | `title_d_app.im1` | Instrument Master Table | 13 columns (block_num, property_id_num, instrument_num, etc.) |
| `IM2` | `title_d_app.im2` | Instrument Details with Date Transformation | 9 columns (includes date formatting) |
| `IM3` | `title_d_app.im3` | Instrument Comments | 3 columns (block_num, property_id_num, comment_desc) |

### Validation & Tracking Tables
| Table | Purpose |
|-------|---------|
| `title_d_app.ingestion_manifest` | Processing history and status tracking |
| `title_d_app.file_validation_rules` | Configurable validation rules per file pattern |
| `title_d_app.file_validation_issues` | Detailed validation issues and auto-fix tracking |

## �📋 Prerequisites

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

### ZIP Operations
```http
POST /api/v1/ingest/zip/analyze         # Analyze ZIP file contents
POST /api/v1/ingest/zip/process         # Process ZIP file with parent-child batch tracking
```

### Delimited File Operations
```http
POST /api/v1/ingest/delimited/upload    # Upload single TSV/CSV file with filename routing
GET  /api/v1/ingest/delimited/status/{batchId}  # Get processing status
```

### Watch Folder Operations
```http
GET  /api/v1/ingest/watch-folder/status         # Get watch folder status
GET  /api/v1/ingest/watch-folder/files          # List all files in watch folders
GET  /api/v1/ingest/watch-folder/files/{folder} # List files in specific folder
GET  /api/v1/ingest/watch-folder/errors         # List error reports
POST /api/v1/ingest/watch-folder/retry/{filename} # Retry failed file
```

### File Validation Operations
```http
GET  /api/validation/rules                    # Get all validation rules
GET  /api/validation/rules/{filePattern}      # Get rule by file pattern
POST /api/validation/rules                    # Create/update validation rule
DELETE /api/validation/rules/{id}             # Delete validation rule
PUT  /api/validation/rules/{filePattern}/enabled # Enable/disable validation for pattern
GET  /api/validation/issues/batch/{batchId}   # Get validation issues by batch
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

### 1. Analyze ZIP File
```bash
curl -X POST http://localhost:8081/api/v1/ingest/zip/analyze \
  -F "file=@title-d-export.zip"
```

**Response:**
```json
{
  "zip_filename": "title-d-export.zip",
  "total_files_extracted": 3,
  "csv_files_found": 3,
  "extraction_status": "SUCCESS",
  "extracted_files": [
    {
      "filename": "PM162",
      "file_type": "TSV",
      "estimated_rows": 5000
    }
  ]
}
```

### 2. Upload Single Delimited File
```bash
# Upload TSV file (default format for Title D files)
curl -X POST http://localhost:8081/api/v1/ingest/delimited/upload \
  -F "file=@PM162"

# Upload CSV file with explicit parameters
curl -X POST "http://localhost:8081/api/v1/ingest/delimited/upload?format=csv&hasHeaders=true" \
  -F "file=@data.csv"
```

**Response:**
```json
{
  "batch_id": "a1b2c3d4-1234-5678-90ab-cdef12345678",
  "file_name": "PM162",
  "table_name": "pm1",
  "file_size_bytes": 15420,
  "status": "SUCCESS",
  "total_records": 5000,
  "processed_records": 5000,
  "data_quality_status": "CLEAN",
  "message": "File processed successfully. Loaded 5000 rows to table: pm1"
}
```

### 3. Upload ZIP File (Multiple Title D Files)
```bash
curl -X POST http://localhost:8081/api/v1/ingest/zip/process \
  -F "file=@title-d-export.zip"
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
      "filename": "PM162",
      "table_name": "pm1",
      "status": "SUCCESS",
      "rows_loaded": 5420,
      "child_batch_id": "c3d4e5f6-7890-abcd-ef12-34567890abcd",
      "parent_batch_id": "b2c3d4e5-5678-90ab-cdef-1234567890ab"
    },
    {
      "filename": "IM262",
      "table_name": "im2",
      "status": "SUCCESS",
      "rows_loaded": 5200,
      "child_batch_id": "d4e5f678-9012-bcde-f123-4567890abcde",
      "parent_batch_id": "b2c3d4e5-5678-90ab-cdef-1234567890ab"
    }
  ]
}
```

**Parent-Child Relationship:**
- ZIP file gets a `batch_id` (parent batch)
- Each extracted Title D file gets its own `batch_id` (child batch)
- Child manifests include `parent_batch_id` linking back to ZIP
- Query relationships: `SELECT * FROM ingestion_manifest WHERE parent_batch_id = 'zip-batch-id'`
- Query relationships: `SELECT * FROM ingestion_manifest WHERE parent_batch_id = 'zip-batch-id'`

### 4. Check Processing Status
```bash
curl -X GET http://localhost:8081/api/v1/ingest/delimited/status/a1b2c3d4-1234-5678-90ab-cdef12345678
```

### 5. Query Your Processed Data
```sql
-- Connect to PostgreSQL
psql -h localhost -U postgres -d your_database

-- List Title D tables
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'title_d_app'
ORDER BY table_name;

-- Query data from processed table
SELECT * FROM title_d_app.pm1 LIMIT 10;

-- Check row counts for a specific file batch
SELECT COUNT(*) as row_count FROM title_d_app.pm1
WHERE batch_id = 'a1b2c3d4-1234-5678-90ab-cdef12345678';

-- View processing manifest
SELECT * FROM title_d_app.ingestion_manifest
WHERE batch_id = 'a1b2c3d4-1234-5678-90ab-cdef12345678';
```

## 🏗️ Architecture

### System Components

**Controllers:**
- `DelimitedFileController` - Single delimited file processing with filename routing
- `ZipController` - ZIP file analysis and processing with parent-child batch tracking
- `WatchFolderController` - Watch folder monitoring and management

**Services:**
- `DelimitedFileProcessingService` - Core delimited file processing and routing
- `ZipProcessingService` - ZIP extraction and analysis with parent-child batch tracking
- `WatchFolderService` - Automated file monitoring and lifecycle management
- `WatchFolderManager` - File movement across 4-folder architecture
- `DatabaseConnectionService` - Database operations and schema management
- `IngestionManifestService` - Processing audit and tracking
- `FileValidationService` - Configurable validation rules and issue tracking
- `FilenameRouterService` - Automatic routing based on file patterns
- `FileChecksumService` - SHA-256 checksum calculation for idempotency
- `PostgresCopyService` - Efficient bulk data loading using PostgreSQL COPY
- `CsvParsingService` - CSV/TSV parsing and data transformation
- `TableNamingService` - Consistent table naming for fixed schema processing

**Data Layer:**
- PostgreSQL COPY command for bulk data loading into fixed schema tables
- title_d_app schema with predefined PM1-PM7 and IM1-IM3 tables
- Ingestion manifest for processing metadata and status tracking
- File validation rules and issues tracking for data quality assurance

### Data Flow

```
Title D Files → Filename Pattern Routing → Fixed Schema Validation → Checksum → Table Routing → COPY → Manifest → title_d_app Schema
```

### Fixed Schema Processing

1. **Upload Title D File**: File is uploaded via API or watch folder
2. **Filename Pattern Recognition**: System recognizes file pattern (PM1xx → pm1 table, IM2xx → im2 table)
3. **Checksum Calculation**: SHA-256 hash computed for idempotency
4. **Duplicate Check**: System checks if file was previously processed
5. **Schema Validation**: File structure validated against predefined schema for target table
6. **Data Transformation**: Character replacement and date formatting applied if configured
7. **Data Loading**: PostgreSQL COPY command loads data into fixed title_d_app schema tables
8. **Manifest Recording**: Processing metadata saved to `ingestion_manifest` table
9. **Issue Tracking**: Any validation issues recorded in `file_validation_issues` table

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
