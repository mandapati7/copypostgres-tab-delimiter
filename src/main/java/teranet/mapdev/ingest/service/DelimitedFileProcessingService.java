package teranet.mapdev.ingest.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import teranet.mapdev.ingest.config.CsvProcessingConfig;
import teranet.mapdev.ingest.config.IngestConfig;
import teranet.mapdev.ingest.model.IngestionManifest;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.UUID;

/**
 * NEW Service for processing delimited files (CSV/TSV) with advanced routing
 * 
 * This service handles:
 * - TSV files (tab-delimited, no headers)
 * - CSV files (comma-delimited, with headers)
 * - Filename-based table routing (PM162 -> PM1)
 * - Loading to existing tables (not creating staging tables)
 * 
 * Key Differences from CsvProcessingService:
 * - Supports TSV format with custom delimiters
 * - Can load files without headers (using schema metadata)
 * - Routes to existing tables based on filename patterns
 * - Does NOT create dynamic staging tables (uses existing tables)
 * 
 * This is a NEW service - does NOT modify existing CsvProcessingService
 */
@Service
@Slf4j
public class DelimitedFileProcessingService {
    
    private final DataSource dataSource;
    private final IngestConfig ingestConfig;
    private final CsvProcessingConfig csvProcessingConfig;
    private final FileChecksumService fileChecksumService;
    private final IngestionManifestService manifestService;
    private final ColumnOrderResolverService columnOrderResolverService;
    private final FilenameRouterService filenameRouterService;
    
    public DelimitedFileProcessingService(
            DataSource dataSource,
            IngestConfig ingestConfig,
            FileChecksumService fileChecksumService,
            IngestionManifestService manifestService,
            ColumnOrderResolverService columnOrderResolverService,
            FilenameRouterService filenameRouterService,
            CsvProcessingConfig csvProcessingConfig) {
        this.dataSource = dataSource;
        this.ingestConfig = ingestConfig;
        this.fileChecksumService = fileChecksumService;
        this.manifestService = manifestService;
        this.columnOrderResolverService = columnOrderResolverService;
        this.filenameRouterService = filenameRouterService;
        this.csvProcessingConfig = csvProcessingConfig;
    }
    
    /**
     * Process a delimited file with routing support
     * 
     * @param file The file to process
     * @param format File format (csv or tsv)
     * @param hasHeaders Whether file has a header row
     * @param routeByFilename Whether to route to table by filename
     * @return Ingestion manifest with processing results
     */
    public IngestionManifest processDelimitedFile(
            MultipartFile file,
            String format,
            boolean hasHeaders,
            boolean routeByFilename) throws Exception {
        
        log.info("Processing delimited file: {} (format={}, hasHeaders={}, routing={})",
                file.getOriginalFilename(), format, hasHeaders, routeByFilename);
        
        long startTime = System.currentTimeMillis();
        
        // Step 1: Check for duplicate (idempotency)
        String checksum = fileChecksumService.calculateFileChecksum(file);
        IngestionManifest existingManifest = checkForDuplicate(checksum);
        if (existingManifest != null) {
            log.info("File already processed: {}", file.getOriginalFilename());
            return existingManifest;
        }
        
        // Step 2: Determine target table
        String targetTable;
        
        if (routeByFilename) {
            // Route to existing table based on filename (e.g., PM162 -> PM1)
            targetTable = csvProcessingConfig.getStagingTablePrefix() + "_" + filenameRouterService.getFullyQualifiedTableName(file.getOriginalFilename());           

            log.info("Routing {} to {}", file.getOriginalFilename(), targetTable);
        } else {
            // Use default schema and staging table pattern            
            targetTable = csvProcessingConfig.getStagingTablePrefix() + "_" + sanitizeTableName(file.getOriginalFilename());

            log.info("Using staging table: {}", targetTable);
        }
        
        // Step 3: Get column order from database schema
        List<String> columnOrder;
        if (hasHeaders) {
            // Extract columns from file header
            columnOrder = extractHeadersFromFile(file);
            log.debug("Extracted {} columns from file headers", columnOrder.size());
        } else {
            // Get columns from database table schema
            columnOrder = columnOrderResolverService.getColumnOrder(targetTable);
            log.debug("Retrieved {} columns from table {}", columnOrder.size(), targetTable);
        }
        
        // Step 4: Create manifest
        IngestionManifest manifest = createManifest(file, checksum, targetTable );
        
        // Step 5: Load data using PostgreSQL COPY
        long rowCount = loadDataToCopy(file, targetTable, columnOrder, format, hasHeaders);

        // Step 6: Update manifest with success
        completeManifest(manifest, rowCount, System.currentTimeMillis() - startTime);

        log.info("Successfully processed {} rows from {} to {} in {} ms",
                rowCount, file.getOriginalFilename(), targetTable,
                System.currentTimeMillis() - startTime);
        
        return manifest;
    }
    
    /**
     * Check if file was already processed (idempotency)
     */
    private IngestionManifest checkForDuplicate(String checksum) {
        try {
            IngestionManifest existing = manifestService.findByChecksum(checksum);
            if (existing != null && existing.isCompleted()) {
                existing.setAlreadyProcessed(true);
                return existing;
            }
        } catch (Exception e) {
            log.warn("Could not check for duplicate: {}", e.getMessage());
        }
        return null;
    }
    
    /**
     * Create ingestion manifest
     */
    private IngestionManifest createManifest(
            MultipartFile file, 
            String checksum, 
            String tableName) throws Exception {

        IngestionManifest manifest = new IngestionManifest(
            file.getOriginalFilename(),
            file.getSize(),
            checksum
        );
        
        manifest.setContentType(file.getContentType());
        manifest.setBatchId(UUID.randomUUID());
        manifest.setTableName(tableName);  // Store fully qualified table name
        manifest.markAsProcessing();  // Sets status to PROCESSING and startedAt timestamp
        
        try {
            manifestService.save(manifest);
        } catch (Exception e) {
            log.warn("Could not save manifest to database: {}", e.getMessage());
            // Continue anyway - manifest is optional
        }
        
        return manifest;
    }
    
    /**
     * Complete manifest with success status
     */
    private void completeManifest(IngestionManifest manifest, long rowCount, long durationMs) {
        manifest.setTotalRecords(rowCount);
        manifest.setProcessedRecords(rowCount);
        manifest.markAsCompleted();  // Sets status to COMPLETED and completedAt timestamp
        
        try {
            manifestService.save(manifest);
        } catch (Exception e) {
            log.warn("Could not update manifest: {}", e.getMessage());
        }
    }
    
    /**
     * Load data to PostgreSQL using COPY command
     * 
     * This method builds a PostgreSQL COPY command with the correct:
     * - Delimiter (comma for CSV, tab for TSV)
     * - Header setting (true/false)
     * - Quote character (disabled for TSV)
     */
    private long loadDataToCopy(
            MultipartFile file,
            String tableName,
            List<String> columns,
            String format,
            boolean hasHeaders) throws Exception {
        
        // Build column list for COPY command
        String columnList = String.join(", ", columns);
        
        // Build COPY command
        String copyCommand = buildCopyCommand( tableName, columnList, format, hasHeaders);
        
        log.info("Executing COPY: {}", copyCommand);
        
        // Execute COPY using PostgreSQL CopyManager
        try (Connection conn = dataSource.getConnection()) {
            org.postgresql.core.BaseConnection pgConn = conn.unwrap(org.postgresql.core.BaseConnection.class);
            org.postgresql.copy.CopyManager copyManager = new org.postgresql.copy.CopyManager(pgConn);
            
            // Execute COPY
            try (java.io.InputStream inputStream = fileChecksumService.getDecompressedInputStream(file);
                 java.io.Reader reader = new java.io.InputStreamReader(inputStream, java.nio.charset.StandardCharsets.UTF_8)) {
                
                long rowCount = copyManager.copyIn(copyCommand, reader);
                log.info("COPY loaded {} rows", rowCount);
                return rowCount;
            }
        }
    }
    
    /**
     * Build PostgreSQL COPY command
     * 
     * Examples:
     * - CSV with headers: COPY table (col1, col2) FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER true)
     * - TSV no headers: COPY table (col1, col2) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', HEADER false, QUOTE E'\\b')
     */
    private String buildCopyCommand(
            String tableName,
            String columnList,
            String format,
            boolean hasHeaders) {
        
        StringBuilder sql = new StringBuilder();
        sql.append("COPY ").append(tableName);
        sql.append(" (").append(columnList).append(")");
        sql.append(" FROM STDIN WITH (");
        sql.append("FORMAT csv");
        
        if ("tsv".equals(format)) {
            // TSV: use tab delimiter and disable quoting
            // Example problem: If you use FORMAT csv with DELIMITER E'\t' but do not disable quoting,
            // a tab character inside quoted fields will be treated as a delimiter, causing data corruption.
            // Example input:
            //   field1\t"field2\twithtab"\tfield3
            // Without disabling quoting, "field2\twithtab" would be split at the tab inside quotes.
            // Solution: Set QUOTE to a non-occurring character (E'\b') to effectively disable quoting.
            // This ensures tabs inside fields are not treated as delimiters.
            sql.append(", DELIMITER E'\\t'");
            sql.append(", QUOTE E'\\b'");  // Effectively disables quoting
        } else {
            // CSV: use comma delimiter
            sql.append(", DELIMITER ','");
        }
        
        sql.append(", HEADER ").append(hasHeaders);
        sql.append(", NULL ''");  // Empty string = NULL
        sql.append(")");
        
        return sql.toString();
    }
    
    /**
     * Extract column names from file header (first line)
     */
    private List<String> extractHeadersFromFile(MultipartFile file) throws Exception {
        try (java.io.InputStream is = file.getInputStream();
             java.io.BufferedReader reader = new java.io.BufferedReader(
                 new java.io.InputStreamReader(is, java.nio.charset.StandardCharsets.UTF_8))) {
            
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                throw new IllegalArgumentException("File is empty or has no header");
            }
            
            // Simple CSV parsing (for more complex cases, use a CSV library)
            String[] headers = headerLine.split(",");
            List<String> columnNames = new java.util.ArrayList<>();
            for (String header : headers) {
                columnNames.add(header.trim().toLowerCase());
            }
            
            return columnNames;
        }
    }
    
    /**
     * Sanitize filename to valid table name
     */
    private String sanitizeTableName(String filename) {
        // Remove extension
        String name = filename;
        int lastDot = name.lastIndexOf('.');
        if (lastDot > 0) {
            name = name.substring(0, lastDot);
        }
        
        // Replace invalid characters with underscore
        name = name.replaceAll("[^a-zA-Z0-9_]", "_");
        
        // Ensure lowercase
        return name.toLowerCase();
    }
}
