package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import teranet.mapdev.ingest.config.CsvProcessingConfig;
import teranet.mapdev.ingest.config.IngestConfig;
import teranet.mapdev.ingest.model.IngestionManifest;
import teranet.mapdev.ingest.model.FileValidationIssue;
import teranet.mapdev.ingest.model.FileValidationRule;
import teranet.mapdev.ingest.transformer.DataTransformer;
import teranet.mapdev.ingest.stream.TransformingInputStream;
import teranet.mapdev.ingest.repository.FileValidationRuleRepository;
import teranet.mapdev.ingest.repository.FileValidationIssueRepository;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

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
public class DelimitedFileProcessingService {

    private static final Logger log = LoggerFactory.getLogger(DelimitedFileProcessingService.class);

    private final DataSource dataSource;
    private final IngestConfig ingestConfig;
    private final CsvProcessingConfig csvProcessingConfig;
    private final FileChecksumService fileChecksumService;
    private final IngestionManifestService manifestService;
    private final ColumnOrderResolverService columnOrderResolverService;
    private final FilenameRouterService filenameRouterService;
    private final FileValidationService fileValidationService;
    private final DataTransformerFactory dataTransformerFactory;
    private final FileValidationRuleRepository validationRuleRepository;
    private final FileValidationIssueRepository validationIssueRepository;

    public DelimitedFileProcessingService(
            DataSource dataSource,
            IngestConfig ingestConfig,
            FileChecksumService fileChecksumService,
            IngestionManifestService manifestService,
            ColumnOrderResolverService columnOrderResolverService,
            FilenameRouterService filenameRouterService,
            CsvProcessingConfig csvProcessingConfig,
            FileValidationService fileValidationService,
            DataTransformerFactory dataTransformerFactory,
            FileValidationRuleRepository validationRuleRepository,
            FileValidationIssueRepository validationIssueRepository) {
        this.dataSource = dataSource;
        this.ingestConfig = ingestConfig;
        this.fileChecksumService = fileChecksumService;
        this.manifestService = manifestService;
        this.columnOrderResolverService = columnOrderResolverService;
        this.filenameRouterService = filenameRouterService;
        this.csvProcessingConfig = csvProcessingConfig;
        this.fileValidationService = fileValidationService;
        this.dataTransformerFactory = dataTransformerFactory;
        this.validationRuleRepository = validationRuleRepository;
        this.validationIssueRepository = validationIssueRepository;
    }

    /**
     * Process a delimited file with routing support
     * 
     * @param file            The file to process
     * @param format          File format (csv or tsv)
     * @param hasHeaders      Whether file has a header row
     * @param routeByFilename Whether to route to table by filename
     * @param parentBatchId   Optional parent batch ID (for ZIP file processing) - can be null
     * @return Ingestion manifest with processing results
     */
    public IngestionManifest processDelimitedFile(
            MultipartFile file,
            String format,
            boolean hasHeaders,
            boolean routeByFilename,
            UUID parentBatchId) throws Exception {

        log.info("Processing delimited file: {} (format={}, hasHeaders={}, routing={})",
                file.getOriginalFilename(), format, hasHeaders, routeByFilename);

        long startTime = System.currentTimeMillis();
        IngestionManifest manifest = null;

        try {
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
                // Route to table based on filename (e.g., PM162 -> pm1)
                // FilenameRouterService resolves the table name directly from filename
                targetTable = filenameRouterService.resolveTableName(file.getOriginalFilename());

                log.info("Routing {} to {}", file.getOriginalFilename(), targetTable);
            } else {
                // Use default schema and table name from filename
                targetTable = sanitizeTableName(file.getOriginalFilename());

                log.info("Using table: {}", targetTable);
            }

            // Step 3: Get column order from database schema
            List<String> columnOrder;
            if (hasHeaders) {
                // Extract columns from file header
                columnOrder = extractHeadersFromFile(file);
                log.debug("Extracted {} columns from file headers", columnOrder.size());
            } else {
                // Get columns from database table schema (excluding metadata columns)
                List<String> allColumns = columnOrderResolverService.getColumnOrder(targetTable);

                // Filter out metadata columns that are auto-generated (not in source file)
                List<String> dataColumns = allColumns.stream()
                        .filter(col -> !col.equals("batch_id") &&
                                !col.equals("row_number") &&
                                !col.equals("loaded_at"))
                        .toList();

                // Count actual fields in the file to match column count
                int fieldCount = countFieldsInFile(file, format);
                log.debug("File contains {} fields, table has {} data columns", fieldCount, dataColumns.size());

                // Use only the columns that exist in the file (first N columns)
                if (fieldCount < dataColumns.size()) {
                    columnOrder = dataColumns.subList(0, fieldCount);
                    log.info("Using first {} columns from table {} (file has fewer fields than table columns)",
                            fieldCount, targetTable);
                } else if (fieldCount > dataColumns.size()) {
                    throw new IllegalArgumentException(
                            String.format("File has %d fields but table %s only has %d data columns",
                                    fieldCount, targetTable, dataColumns.size()));
                } else {
                    columnOrder = dataColumns;
                }

                log.debug("Retrieved {} data columns from table {} (excluded metadata columns)",
                        columnOrder.size(), targetTable);
            }

            // Step 4: Create manifest (with optional parent batch ID)
            manifest = createManifest(file, checksum, targetTable, parentBatchId);

            // Step 5: Validate and fix file BEFORE loading (if validation is enabled)
            InputStream fileStreamToLoad;
            String filePattern = extractFilePattern(file.getOriginalFilename());
            FileValidationService.ValidationResult validationResult = null;

            try {
                validationResult = fileValidationService.validateAndFix(
                        file.getInputStream(),
                        file.getOriginalFilename(),
                        filePattern,
                        manifest.getBatchId());

                // Check if file was rejected due to critical validation issues
                if (validationResult.isRejected()) {
                    String errorMsg = String.format(
                            "File rejected: %d critical validation issues found",
                            validationResult.getIssues().size());
                    log.error(errorMsg);

                    // Update data quality metrics before marking as failed
                    updateDataQualityMetrics(manifest, validationResult);

                    manifest.markAsFailed(errorMsg,
                            "See file_validation_issues table for details (batch_id: " + manifest.getBatchId() + ")");
                    manifestService.update(manifest);

                    throw new IllegalArgumentException(errorMsg);
                }

                // Update data quality metrics if issues were found
                if (validationResult.hasIssues()) {
                    updateDataQualityMetrics(manifest, validationResult);

                    long autoFixedCount = validationResult.getIssues().stream()
                            .filter(FileValidationIssue::getAutoFixed)
                            .count();

                    log.warn("File {} processed with {} validation issues ({} auto-fixed). " +
                            "See file_validation_issues table for details (batch_id: {})",
                            file.getOriginalFilename(),
                            validationResult.getIssues().size(),
                            autoFixedCount,
                            manifest.getBatchId());
                } else {
                    // No validation issues - mark as CLEAN
                    manifest.updateDataQualityMetrics(0, 0, 0);
                }

                // Use the validated/fixed file stream for loading
                fileStreamToLoad = validationResult.getFixedInputStream();

            } catch (IOException ioEx) {
                log.error("Validation failed for file: {}", file.getOriginalFilename(), ioEx);
                throw new RuntimeException("File validation error: " + ioEx.getMessage(), ioEx);
            }

            // Step 5.5: Apply data transformation if configured (AFTER validation, BEFORE COPY)
            try {
                java.util.Optional<FileValidationRule> ruleOpt = validationRuleRepository.findByFilePattern(filePattern);
                if (ruleOpt.isPresent()) {
                    FileValidationRule rule = ruleOpt.get();
                    DataTransformer transformer = dataTransformerFactory.getTransformer(rule);
                    
                    // Apply transformation if needed
                    if (transformer.requiresTransformation()) {
                        log.info("Applying data transformation for file pattern: {} using transformer: {}", 
                                filePattern, transformer.getClass().getSimpleName());
                        fileStreamToLoad = new TransformingInputStream(fileStreamToLoad, transformer, filePattern,
                                manifest.getBatchId(), validationIssueRepository);
                    } else {
                        log.debug("No transformation required for file pattern: {}", filePattern);
                    }
                } else {
                    log.debug("No validation rule found for pattern: {}, skipping transformation", filePattern);
                }
            } catch (Exception transEx) {
                log.error("Error applying transformation for file: {}. Proceeding without transformation.", 
                        file.getOriginalFilename(), transEx);
                // Continue with unmodified stream if transformation fails
            }

            // Step 6: Load data using PostgreSQL COPY (with validated+transformed file stream)
            long rowCount = loadDataToCopy(fileStreamToLoad, targetTable, columnOrder, format, hasHeaders,
                    manifest.getBatchId());

            // Step 7: Update manifest with success (data quality already set in Step 5)
            completeManifest(manifest, rowCount, System.currentTimeMillis() - startTime);

            log.info("Successfully processed {} rows from {} to {} in {} ms",
                    rowCount, file.getOriginalFilename(), targetTable,
                    System.currentTimeMillis() - startTime);

            return manifest;

        } catch (Exception e) {
            log.error("Failed to process delimited file: {}", file.getOriginalFilename(), e);

            // CRITICAL: Update manifest status to FAILED to prevent stuck PROCESSING
            // records
            if (manifest != null) {
                try {
                    // Use helper method to set status, error details, and duration
                    manifest.markAsFailed(e.getMessage(), getStackTraceAsString(e));

                    // Force update to ensure status change is persisted
                    manifestService.update(manifest);
                    log.info("Updated manifest with FAILED status for batch: {}", manifest.getBatchId());
                } catch (Exception updateError) {
                    log.error("CRITICAL ERROR: Failed to update manifest with FAILED status. " +
                            "Record will remain stuck in PROCESSING state: {}", updateError.getMessage(), updateError);
                }
            }

            // Re-throw to propagate error to caller
            throw e;
        }
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
     * 
     * @param file The file to create manifest for
     * @param checksum File checksum
     * @param tableName Target table name
     * @param parentBatchId Optional parent batch ID (for ZIP processing) - can be null
     * @return Created manifest
     */
    private IngestionManifest createManifest(
            MultipartFile file,
            String checksum,
            String tableName,
            UUID parentBatchId) throws Exception {

        IngestionManifest manifest = new IngestionManifest(
                file.getOriginalFilename(),
                file.getSize(),
                checksum);

        manifest.setContentType(file.getContentType());
        manifest.setBatchId(UUID.randomUUID());
        manifest.setParentBatchId(parentBatchId); // Set parent batch ID if provided (for ZIP processing)
        manifest.setTableName(tableName); // Store fully qualified table name
        manifest.markAsProcessing(); // Sets status to PROCESSING and startedAt timestamp

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
        manifest.markAsCompleted(); // Sets status to COMPLETED and completedAt timestamp

        try {
            manifestService.save(manifest);
        } catch (Exception e) {
            log.warn("Could not update manifest: {}", e.getMessage());
        }
    }

    /**
     * Load data to PostgreSQL using COPY command with batch tracking
     * 
     * This method:
     * 1. Begins a transaction
     * 2. Executes COPY to load data (batch_id will be NULL initially)
     * 3. Updates all rows with NULL batch_id to the current batch UUID
     * 4. Commits the transaction
     * 
     * This approach ensures:
     * - Atomicity: Either all rows are loaded with batch_id or none
     * - Efficiency: UPDATE on NULL is fast with indexed batch_id column
     * - Simplicity: No need to manipulate input streams
     * 
     * @param file       The file to load
     * @param tableName  Target table name
     * @param columns    Column list for COPY
     * @param format     File format (csv or tsv)
     * @param hasHeaders Whether file has header row
     * @param batchId    The batch UUID to track this load
     * @return Number of rows loaded
     */
    private long loadDataToCopy(
            InputStream inputStream,
            String tableName,
            List<String> columns,
            String format,
            boolean hasHeaders,
            UUID batchId) throws Exception {

        // Build column list for COPY command (exclude tracking columns)
        String columnList = String.join(", ", columns);

        // Build COPY command
        String copyCommand = buildCopyCommand(tableName, columnList, format, hasHeaders);

        log.info("Executing COPY with batch tracking: {}", copyCommand);
        log.info("Batch ID: {}", batchId);

        long rowCount = 0;

        // Execute COPY within a transaction for atomicity
        try (Connection conn = dataSource.getConnection()) {
            // Disable auto-commit for transaction control
            conn.setAutoCommit(false);

            try {
                // Step 1: Execute COPY to load data (batch_id will be NULL)
                org.postgresql.core.BaseConnection pgConn = conn.unwrap(org.postgresql.core.BaseConnection.class);
                org.postgresql.copy.CopyManager copyManager = new org.postgresql.copy.CopyManager(pgConn);

                // Use the provided input stream (which may be validated/fixed stream)
                try (java.io.Reader reader = new java.io.InputStreamReader(inputStream,
                        java.nio.charset.StandardCharsets.UTF_8)) {

                    rowCount = copyManager.copyIn(copyCommand, reader);
                    log.info("COPY loaded {} rows", rowCount);
                }

                // Step 2: Update all rows with NULL batch_id to current batch UUID
                String updateSQL = String.format(
                        "UPDATE %s SET batch_id = ? WHERE batch_id IS NULL",
                        tableName);

                try (java.sql.PreparedStatement pstmt = conn.prepareStatement(updateSQL)) {
                    pstmt.setObject(1, batchId);
                    int updatedRows = pstmt.executeUpdate();
                    log.info("Updated {} rows with batch_id: {}", updatedRows, batchId);

                    // Verify row count matches
                    if (updatedRows != rowCount) {
                        log.warn("Row count mismatch: COPY={}, UPDATE={}. This may indicate concurrent modifications.",
                                rowCount, updatedRows);
                    }
                }

                // Step 3: Commit transaction
                conn.commit();
                log.info("Transaction committed successfully");

            } catch (Exception e) {
                // Rollback on any error
                conn.rollback();
                log.error("Transaction rolled back due to error", e);
                throw e;
            } finally {
                // Restore auto-commit
                conn.setAutoCommit(true);
            }
        }

        return rowCount;
    }

    /**
     * Build PostgreSQL COPY command
     * 
     * Examples:
     * - CSV with headers: COPY table (col1, col2) FROM STDIN WITH (FORMAT csv,
     * DELIMITER ',', HEADER true)
     * - TSV no headers: COPY table (col1, col2) FROM STDIN WITH (FORMAT csv,
     * DELIMITER E'\t', HEADER false, QUOTE E'\\b')
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
            // Example problem: If you use FORMAT csv with DELIMITER E'\t' but do not
            // disable quoting,
            // a tab character inside quoted fields will be treated as a delimiter, causing
            // data corruption.
            // Example input:
            // field1\t"field2\twithtab"\tfield3
            // Without disabling quoting, "field2\twithtab" would be split at the tab inside
            // quotes.
            // Solution: Set QUOTE to a non-occurring character (E'\b') to effectively
            // disable quoting.
            // This ensures tabs inside fields are not treated as delimiters.
            sql.append(", DELIMITER E'\\t'");
            sql.append(", QUOTE E'\\b'"); // Effectively disables quoting
        } else {
            // CSV: use comma delimiter
            sql.append(", DELIMITER ','");
        }

        sql.append(", HEADER ").append(hasHeaders);
        sql.append(", NULL ''"); // Empty string = NULL
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
     * Count the number of fields in the first data line of the file
     * This helps match file structure to table columns when no headers present
     * 
     * @param file   The file to analyze
     * @param format The delimiter format (csv, tsv, etc.)
     * @return Number of tab-delimited fields in first line
     */
    private int countFieldsInFile(MultipartFile file, String format) throws IOException {
        char delimiter = getDelimiter(format);

        try (InputStream is = file.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

            String firstLine = reader.readLine();
            if (firstLine == null || firstLine.isEmpty()) {
                return 0;
            }

            // Split by delimiter and count fields
            String[] fields = firstLine.split(Pattern.quote(String.valueOf(delimiter)), -1);
            return fields.length;
        }
    }

    /**
     * Get delimiter character based on format string
     */
    private char getDelimiter(String format) {
        return switch (format.toLowerCase()) {
            case "tsv" -> '\t';
            case "csv" -> ',';
            case "pipe" -> '|';
            default -> ',';
        };
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
     * Extract file pattern from filename for validation rule lookup
     * 
     * Examples:
     * - "IM262" -> "IM2"
     * - "PM362" -> "PM3"
     * - "IM162.txt" -> "IM1"
     * - "PM1_data.tsv" -> "PM1"
     * 
     * @param fileName The original filename
     * @return The file pattern (e.g., "PM3", "IM2") or the filename if pattern not
     *         found
     */
    private String extractFilePattern(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return fileName;
        }

        // Remove extension first
        String nameWithoutExt = fileName;
        int lastDot = fileName.lastIndexOf('.');
        if (lastDot > 0) {
            nameWithoutExt = fileName.substring(0, lastDot);
        }

        // Check if filename matches pattern like IM262, PM362, etc.
        // Pattern: (IM|PM) followed by digits
        if (nameWithoutExt.matches("(IM|PM)\\d+.*")) {
            // Extract first 3 characters (e.g., "IM2", "PM3")
            return nameWithoutExt.substring(0, 3);
        }

        // If no pattern found, return the name without extension
        return nameWithoutExt;
    }

    /**
     * Update manifest with data quality metrics from validation result
     * 
     * @param manifest         The manifest to update
     * @param validationResult The validation result containing issues
     */
    private void updateDataQualityMetrics(
            IngestionManifest manifest,
            FileValidationService.ValidationResult validationResult) {

        if (validationResult == null || !validationResult.hasIssues()) {
            manifest.updateDataQualityMetrics(0, 0, 0);
            return;
        }

        // Count issues by severity
        long autoFixedCount = 0;
        int warningCount = 0;
        int errorCount = 0;

        for (FileValidationIssue issue : validationResult.getIssues()) {
            if (issue.getAutoFixed() != null && issue.getAutoFixed()) {
                autoFixedCount++;
            }

            if (issue.getSeverity() != null) {
                switch (issue.getSeverity()) {
                    case WARNING:
                        warningCount++;
                        break;
                    case FileValidationIssue.Severity.ERROR:
                    case CRITICAL:
                        errorCount++;
                        break;
                    default:
                        // INFO or others - no action
                        break;
                }
            }
        }

        // Update manifest with metrics
        manifest.updateDataQualityMetrics(autoFixedCount, warningCount, errorCount);
    }
}
