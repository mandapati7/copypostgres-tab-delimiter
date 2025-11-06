package teranet.mapdev.ingest.service;

import teranet.mapdev.ingest.config.CsvProcessingConfig;
import teranet.mapdev.ingest.model.IngestionManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.sql.DataSource;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

/**
 * High-performance CSV processing service with dynamic table creation
 * Features:
 * - Auto-creates tables based on CSV file names
 * - Dynamic schema detection from CSV headers
 * - Automatic column addition for schema evolution
 * - Fallback table creation if database tables are missing
 */
@Service
public class CsvProcessingService {
    
    private static final Logger logger = LoggerFactory.getLogger(CsvProcessingService.class);
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private IngestionManifestService manifestService;
    
    @Autowired
    private CsvProcessingConfig csvConfig;
    
    @Autowired
    private FileChecksumService fileChecksumService;
    
    @Autowired
    private CsvParsingService csvParsingService;
    
    @Autowired
    private TableNamingService tableNamingService;
    
    @Autowired
    private PostgresCopyService postgresCopyService;
    
    // Note: DatabaseConnectionService no longer needed after removing staging schema
    
    @Value("${csv.processing.batch-size:1000}")
    private int batchSize;
    
    @Value("${csv.processing.max-file-size:100MB}")
    private String maxFileSize;
    
    @Value("${csv.processing.temp-directory:${java.io.tmpdir}/csv-loader}")
    private String tempDirectory;
    
    /**
     * Create or fallback manifest creation if audit table doesn't exist
     */
    private IngestionManifest createOrFallbackManifest(MultipartFile file) throws Exception {
        try {
            // Calculate checksum first to check for duplicates
            String checksum = fileChecksumService.calculateFileChecksum(file);
            
            // Check if file already processed (idempotency) - BEFORE creating new manifest
            try {
                IngestionManifest existing = manifestService.findByChecksum(checksum);
                if (existing != null && existing.isCompleted()) {
                    logger.info("File {} already processed in batch {} - returning existing manifest (no new record created)", 
                               file.getOriginalFilename(), existing.getBatchId());
                    existing.setAlreadyProcessed(true); // Mark for API response differentiation
                    return existing;
                }
            } catch (Exception e) {
                logger.warn("Could not check for existing manifest, proceeding with new processing: {}", e.getMessage());
            }
            
            // No duplicate found, create new manifest
            IngestionManifest manifest = createManifest(file);
            return manifest;
            
        } catch (Exception e) {
            logger.warn("Manifest service not available, creating fallback manifest: {}", e.getMessage());
            // Create a simple manifest without database dependency
            return createFallbackManifest(file);
        }
    }
    
    /**
     * Create a fallback manifest when database tables don't exist
     */
    private IngestionManifest createFallbackManifest(MultipartFile file) throws Exception {
        String checksum = fileChecksumService.calculateFileChecksum(file);
        IngestionManifest manifest = new IngestionManifest(
            file.getOriginalFilename(),
            file.getSize(),
            checksum
        );
        manifest.setContentType(file.getContentType());
        manifest.setBatchId(UUID.randomUUID());
        return manifest;
    }
    
    /**
     * Process CSV file for batch processing
     * All tables created in default schema (no schema prefix)
     * @param file the CSV file to process
     * @return ingestion manifest with processing results
     */
    public IngestionManifest processCsvToStaging(MultipartFile file) {
        return processCsvToStaging(file, null); // Delegate to overloaded method with no parent
    }
    
    /**
     * Process CSV file with parent batch tracking
     * All tables created in default schema (no schema prefix)
     * @param file the CSV file to process
     * @param parentBatchId parent batch ID for ZIP processing (null for standalone uploads)
     * @return ingestion manifest with processing results
     */
    public IngestionManifest processCsvToStaging(MultipartFile file, UUID parentBatchId) {
        logger.info("Processing CSV file: {}", file.getOriginalFilename());
        
        long startTime = System.currentTimeMillis();
        
        // Declare manifest outside try block so catch block can access it
        IngestionManifest manifest = null;
        
        try {
            // No schema creation needed - using default schema
            
            // Create or get manifest (with idempotency check)
            // If duplicate found, existing manifest is returned with alreadyProcessed=true
            manifest = createOrFallbackManifest(file);
            
            // If file was already completed, return existing manifest immediately (true idempotency)
            if (manifest.isAlreadyProcessed()) {
                logger.info("DUPLICATE UPLOAD: File {} with checksum {} was already processed in batch {}. Returning existing manifest without creating new record.",
                           file.getOriginalFilename(), 
                           manifest.getFileChecksum(), 
                           manifest.getBatchId());
                return manifest;
            }
            
            // Set parent batch ID for ZIP processing (only for new processing)
            if (parentBatchId != null) {
                manifest.setParentBatchId(parentBatchId);
                logger.debug("Linking CSV manifest to parent ZIP batch: {}", parentBatchId);
            }
            
            // Set file path as upload source and start processing timestamp
            // Format: "upload://filename" to indicate it came from HTTP upload
            String uploadPath = "upload://" + file.getOriginalFilename();
            manifest.setFilePath(uploadPath);
            manifest.setStartedAt(LocalDateTime.now());
            manifest.setStatus(IngestionManifest.Status.PROCESSING);
            
            // Save manifest (with fallback handling)
            try {
                manifestService.save(manifest);
                logger.info("Created manifest for processing: {}", manifest.getBatchId());
            } catch (Exception e) {
                logger.warn("Could not save manifest, continuing with fallback: {}", e.getMessage());
            }
            
            // Extract headers and generate table name with batch_id suffix
            List<String> headers = csvParsingService.extractCsvHeaders(file);
            String tableName = tableNamingService.generateTableNameFromFile(file.getOriginalFilename(), manifest.getBatchId());
            
            logger.info("Processing to table: {} with columns: {}", tableName, headers);
            
            // Store table name in manifest for tracking
            manifest.setTableName(tableName);
            
            // Ensure table exists in default schema
            ensureStagingTableExists(tableName, headers);
            
            // Perform the data loading using PostgreSQL COPY
            long recordCount = performStagingCopyCommand(file, manifest, tableName, headers);
            
            // Calculate processing duration
            long endTime = System.currentTimeMillis();
            long durationMs = endTime - startTime;
            
            // Update manifest with complete results
            // Note: PostgreSQL COPY is atomic - all records succeed or all fail
            // totalRecords = processedRecords because COPY doesn't support partial success
            manifest.setTotalRecords(recordCount);
            manifest.setProcessedRecords(recordCount);
            manifest.setFailedRecords(0L);
            manifest.setCompletedAt(LocalDateTime.now());
            manifest.setProcessingDurationMs(durationMs);
            manifest.setStatus(IngestionManifest.Status.COMPLETED);
            
            try {
                manifestService.update(manifest);
                logger.info("Updated manifest with completion status");
            } catch (Exception e) {
                logger.warn("Could not update manifest: {}", e.getMessage());
            }
            
            logger.info("Successfully processed {} records to table: {} in {}ms", 
                       recordCount, tableName, durationMs);
            return manifest;
            
        } catch (Exception e) {
            logger.error("Failed to process CSV file: {}", file.getOriginalFilename(), e);
            
            // Try to update manifest with error details
            try {
                // Reuse existing manifest if available, otherwise create fallback
                // This prevents duplicate manifest records for error scenarios
                if (manifest == null) {
                    logger.warn("Manifest was null in catch block, creating fallback manifest for error tracking");
                    manifest = createOrFallbackManifest(file);
                }
                
                // Set error details on the SAME manifest that was created in try block
                manifest.setStatus(IngestionManifest.Status.FAILED);
                manifest.setCompletedAt(LocalDateTime.now());
                manifest.setErrorMessage(e.getMessage());
                manifest.setErrorDetails(getStackTraceAsString(e));
                
                // Calculate duration if we have start time
                if (manifest.getStartedAt() != null) {
                    long durationMs = java.time.Duration.between(
                        manifest.getStartedAt(), 
                        manifest.getCompletedAt()
                    ).toMillis();
                    manifest.setProcessingDurationMs(durationMs);
                }
                
                manifestService.update(manifest);
                logger.info("Updated manifest with error details for batch: {}", manifest.getBatchId());
            } catch (Exception updateError) {
                logger.error("Failed to update manifest with error details: {}", updateError.getMessage());
            }
            
            throw new RuntimeException("Failed to process CSV file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Convert exception stack trace to string for error details
     */
    private String getStackTraceAsString(Exception e) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Ensure table exists with the required columns based on CSV headers
     * All tables are created in the default schema (no schema prefix)
     */
    private void ensureStagingTableExists(String tableName, List<String> headers) {
        try (Connection connection = dataSource.getConnection()) {
            // No schema prefix - using default schema
            
            // Check if table exists in default schema
            if (!tableExists(connection, null, tableName)) {
                createStagingTable(connection, tableName, headers);
            } else {
                // Table exists, check if we need to add new columns
                updateStagingTableSchema(connection, tableName, headers);
            }
            
        } catch (SQLException e) {
            logger.error("Error ensuring staging table {} exists: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("Failed to create or update staging table: " + tableName, e);
        }
    }

    /**
     * Create a new table in the default schema based on CSV headers
     * PostgreSQL-specific: Uses TEXT for all CSV columns
     * 
     * NOTE: No metadata columns (id, batch_id, timestamps) are added.
     * The table name itself contains the batch_id (e.g., staging_orders_dc843bd1).
     * All metadata is tracked in the ingestion_manifest table.
     * Uses UNLOGGED table for maximum performance - no WAL overhead
     */
    private void createStagingTable(Connection connection, String tableName, List<String> headers) throws SQLException {
        // No schema prefix - creating in default schema
        // Using IF NOT EXISTS to avoid errors if table already exists
        // UNLOGGED provides 2-3x faster writes for staging data
        
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE UNLOGGED TABLE IF NOT EXISTS ").append(tableName).append(" (\n");
        
        // Add columns for each CSV header (only CSV columns, no metadata)
        for (int i = 0; i < headers.size(); i++) {
            createTableSql.append("    ").append(headers.get(i)).append(" TEXT");
            if (i < headers.size() - 1) {
                createTableSql.append(",");
            }
            createTableSql.append("\n");
        }
        
        createTableSql.append(")");
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql.toString());
            logger.info("Created UNLOGGED staging table {} with {} CSV columns (optimized for high-speed ingestion)", 
                tableName, headers.size());
        }
    }

    /**
     * Update existing table schema by adding missing columns
     * Works on tables in the default schema
     */
    private void updateStagingTableSchema(Connection connection, String tableName, List<String> headers) throws SQLException {
        // No schema prefix - using default schema
        
        // Get existing columns from default schema
        Set<String> existingColumns = getExistingColumns(connection, null, tableName);
        
        // Add missing columns
        for (String header : headers) {
            if (!existingColumns.contains(header.toLowerCase())) {
                String alterTableSql = String.format("ALTER TABLE %s ADD COLUMN %s TEXT", 
                                                    tableName, header);
                
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(alterTableSql);
                    logger.info("Added column {} to table {}", header, tableName);
                } catch (SQLException e) {
                    logger.warn("Could not add column {} to table {}: {}", header, tableName, e.getMessage());
                }
            }
        }
    }

    /**
     * Perform PostgreSQL COPY command for bulk insert into default schema
     * Delegates to PostgresCopyService for high-performance bulk loading
     */
    private long performStagingCopyCommand(MultipartFile file, IngestionManifest manifest, 
                                         String tableName, List<String> headers) throws SQLException, IOException {
        // Delegate to PostgresCopyService for COPY command execution
        return postgresCopyService.executeCopy(file, tableName, headers);
    }
    
    /**
     * Ensure all required schemas exist in the database
     */
    private void ensureSchemasExist() {
        try (Connection connection = dataSource.getConnection()) {
            String[] schemas = {"audit", "staging", "core"};
            
            for (String schema : schemas) {
                try (Statement stmt = connection.createStatement()) {
                    String createSchema = String.format("CREATE SCHEMA IF NOT EXISTS %s", schema);
                    stmt.execute(createSchema);
                    logger.debug("Ensured schema exists: {}", schema);
                }
            }
            
            // Create enum types if they don't exist
            createEnumTypes(connection);
            
            // Create audit table if it doesn't exist
            createAuditTableIfNotExists(connection);
            
        } catch (SQLException e) {
            logger.error("Error ensuring schemas exist: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create required schemas", e);
        }
    }
    
    /**
     * Create enum types if they don't exist
     */
    private void createEnumTypes(Connection connection) throws SQLException {
        String[] enumQueries = {
            "DO $$ BEGIN CREATE TYPE audit.ingestion_status AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED'); EXCEPTION WHEN duplicate_object THEN null; END $$",
            "DO $$ BEGIN CREATE TYPE core.order_status AS ENUM ('NEW', 'PROCESSING', 'COMPLETED', 'CANCELLED'); EXCEPTION WHEN duplicate_object THEN null; END $$"
        };
        
        for (String query : enumQueries) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(query);
            } catch (SQLException e) {
                logger.debug("Enum type might already exist: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Create staging.ingestion_manifest table if it doesn't exist
     */
    private void createAuditTableIfNotExists(Connection connection) throws SQLException {
        String createAuditTable = """
            CREATE TABLE IF NOT EXISTS staging.ingestion_manifest (
                id BIGSERIAL PRIMARY KEY,
                batch_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
                file_name VARCHAR(255) NOT NULL,
                file_path VARCHAR(500),
                file_size_bytes BIGINT NOT NULL,
                file_checksum VARCHAR(64) NOT NULL,
                content_type VARCHAR(100),
                status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                total_records BIGINT DEFAULT 0,
                processed_records BIGINT DEFAULT 0,
                failed_records BIGINT DEFAULT 0,
                started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP WITH TIME ZONE,
                processing_duration_ms BIGINT,
                error_message TEXT,
                error_details JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(100) DEFAULT 'system'
            )
            """;
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createAuditTable);
            logger.info("Created staging.ingestion_manifest table");
        }
    }
    
    /**
     * Check if a table exists in the specified schema
     */
    private boolean tableExists(Connection connection, String schema, String tableName) throws SQLException {
        String query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = ? AND table_name = ?
            )
            """;
        
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }
    
    /**
     * Get existing column names for a table
     */
    private Set<String> getExistingColumns(Connection connection, String schema, String tableName) throws SQLException {
        Set<String> columns = new HashSet<>();
        
        String query = """
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = ? AND table_name = ?
            """;
        
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("column_name").toLowerCase());
                }
            }
        }
        
        return columns;
    }
    
    /**
     * Create ingestion manifest with file metadata
     */
    private IngestionManifest createManifest(MultipartFile file) throws IOException, NoSuchAlgorithmException {
        String checksum = fileChecksumService.calculateFileChecksum(file);
        
        IngestionManifest manifest = new IngestionManifest(
            file.getOriginalFilename(),
            file.getSize(),
            checksum
        );
        
        manifest.setContentType(file.getContentType());
        
        return manifest;
    }
}