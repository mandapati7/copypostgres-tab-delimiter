package teranet.mapdev.ingest.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import teranet.mapdev.ingest.config.IngestConfig;
import teranet.mapdev.ingest.dto.CsvUploadResponseDto;
import teranet.mapdev.ingest.dto.ErrorResponseDto;
import teranet.mapdev.ingest.dto.IngestionStatusDto;
import teranet.mapdev.ingest.model.IngestionManifest;
import teranet.mapdev.ingest.service.DelimitedFileProcessingService;
import teranet.mapdev.ingest.service.FilenameRouterService;
import teranet.mapdev.ingest.util.FileValidationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import teranet.mapdev.ingest.service.IngestionManifestService;

/**
 * Controller for delimited file operations (CSV/TSV)
 * 
 * Supports:
 * - TSV files (tab-delimited, no headers)
 * - CSV files (comma-delimited, with headers)
 * - Filename-based table routing (PM162 -> staging_pm1)
 * 
 * This is a NEW endpoint that doesn't affect existing CSV upload functionality.
 */
@RestController
@RequestMapping("/api/v1/ingest/delimited")
@CrossOrigin(origins = "*", maxAge = 3600)
@Tag(name = "Delimited File Operations", description = "Upload TSV/CSV files with advanced routing options")
public class DelimitedFileController {

    private static final Logger log = LoggerFactory.getLogger(DelimitedFileController.class);

    private final IngestConfig ingestConfig;
    private final FilenameRouterService filenameRouterService;
    private final DelimitedFileProcessingService delimitedFileProcessingService;

    @Autowired
    private IngestionManifestService manifestService;

    public DelimitedFileController(
            IngestConfig ingestConfig,
            FilenameRouterService filenameRouterService,
            DelimitedFileProcessingService delimitedFileProcessingService) {
        this.ingestConfig = ingestConfig;
        this.filenameRouterService = filenameRouterService;
        this.delimitedFileProcessingService = delimitedFileProcessingService;
    }

    /**
     * Upload a delimited file (CSV or TSV) with optional routing
     * 
     * Examples:
     * - Upload TSV with routing: POST
     * ?format=tsv&hasHeaders=false&routeByFilename=true
     * - Upload CSV to staging: POST
     * ?format=csv&hasHeaders=true&routeByFilename=false
     */
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(summary = "Upload delimited file (CSV/TSV)", description = """
            Upload a delimited file with configurable options:
            - format: csv or tsv (default: csv)
            - hasHeaders: true or false (default: from config based on format)
            - routeByFilename: true or false (default: from config)

            Example for TSV with routing:
            POST /api/v1/ingest/delimited/upload?format=tsv&hasHeaders=false&routeByFilename=true
            """)
    @ApiResponse(responseCode = "200", description = "File successfully uploaded and processed")
    @ApiResponse(responseCode = "400", description = "Invalid file, format, or routing error")
    @ApiResponse(responseCode = "500", description = "Processing error")
    public ResponseEntity<?> uploadDelimitedFile(
            @Parameter(description = "File to upload", required = true) @RequestParam("file") MultipartFile file,

            @Parameter(description = "File format: csv or tsv (default: csv)") @RequestParam(value = "format", required = false) String format,

            @Parameter(description = "Does file have headers? (default: true for CSV, false for TSV)") @RequestParam(value = "hasHeaders", required = false) Boolean hasHeaders,

            @Parameter(description = "Route by filename? (default: from config)") @RequestParam(value = "routeByFilename", required = false) Boolean routeByFilename) {

        log.info("Delimited file upload requested: {} (format={}, hasHeaders={}, routeByFilename={})",
                file.getOriginalFilename(), format, hasHeaders, routeByFilename);

        try {
            // === STEP 1: Determine format ===
            String effectiveFormat = determineFormat(file, format);
            log.debug("Effective format: {}", effectiveFormat);

            // === STEP 2: Determine if file has headers ===
            boolean effectiveHasHeaders = (hasHeaders != null)
                    ? hasHeaders
                    : ingestConfig.hasHeadersForFormat(effectiveFormat);
            log.debug("File has headers: {}", effectiveHasHeaders);

            // === STEP 3: Determine if routing is enabled ===
            boolean effectiveRouting = (routeByFilename != null)
                    ? routeByFilename
                    : filenameRouterService.isEnabled();
            log.debug("Filename routing enabled: {}", effectiveRouting);

            // === STEP 4: Validate file ===
            validateFile(file, effectiveFormat);

            // === STEP 5: Validate routing (if enabled) ===
            String targetTable = null;
            if (effectiveRouting) {
                targetTable = validateAndResolveRouting(file.getOriginalFilename());
                log.info("File {} will be routed to table: {}", file.getOriginalFilename(), targetTable);
            }

            // === STEP 6: Process file ===
            IngestionManifest manifest = delimitedFileProcessingService.processDelimitedFile(
                    file,
                    effectiveFormat,
                    effectiveHasHeaders,
                    effectiveRouting);

            // Build response
            String message;
            String status;

            if (manifest.isAlreadyProcessed()) {
                // File was already processed (idempotency)
                message = String.format(
                        "File already processed previously. Original processing completed on %s. " +
                                "Batch ID: %s. No duplicate data was inserted.",
                        manifest.getCompletedAt().toString(),
                        manifest.getBatchId());
                status = "ALREADY_PROCESSED";
            } else {
                // Newly processed file
                message = String.format(
                        "File processed successfully. Format: %s, Headers: %s, Routing: %s. " +
                                "Loaded %d rows to table: %s",
                        effectiveFormat.toUpperCase(),
                        effectiveHasHeaders ? "Yes" : "No",
                        effectiveRouting ? "Enabled" : "Disabled",
                        manifest.getTotalRecords(),
                        manifest.getTableName());
                status = "COMPLETED";
            }

            CsvUploadResponseDto response = new CsvUploadResponseDto(
                    manifest.getBatchId(),
                    file.getOriginalFilename(),
                    manifest.getTableName(),
                    file.getSize(),
                    status,
                    message);

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            log.warn("Validation error: {}", e.getMessage());
            return ResponseEntity.badRequest().body(
                    new ErrorResponseDto("Validation Error", e.getMessage()));

        } catch (Exception e) {
            log.error("Failed to process delimited file: {}", file.getOriginalFilename(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                    new ErrorResponseDto("Processing Error", "Failed to process file: " + e.getMessage()));
        }
    }

    /**
     * Get processing status by batch ID
     */
    @GetMapping("/status/{batchId}")
    @Operation(summary = "Get CSV processing status", description = "Retrieve the processing status for a specific batch ID")
    @ApiResponse(responseCode = "200", description = "Status retrieved successfully")
    @ApiResponse(responseCode = "404", description = "Batch ID not found")
    public ResponseEntity<IngestionStatusDto> getProcessingStatus(@PathVariable String batchId) {

        log.info("Retrieving processing status for batch: {}", batchId);

        try {
            IngestionManifest manifest = manifestService.findByBatchId(java.util.UUID.fromString(batchId));

            if (manifest == null) {
                return ResponseEntity.notFound().build();
            }

            IngestionStatusDto status = new IngestionStatusDto(manifest);

            return ResponseEntity.ok(status);

        } catch (IllegalArgumentException e) {
            log.warn("Invalid batch ID format: {}", batchId);
            return ResponseEntity.badRequest().build();

        } catch (Exception e) {
            log.error("Failed to retrieve status for batch: {}", batchId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // ========================================
    // HELPER METHODS
    // ========================================

    /**
     * Determine the file format (csv or tsv)
     * Handles files with extensions (.csv, .tsv) and files without extensions
     */
    private String determineFormat(MultipartFile file, String requestedFormat) {
        // If format explicitly provided, use it
        if (requestedFormat != null && !requestedFormat.isBlank()) {
            String normalized = requestedFormat.toLowerCase().trim();
            if (!normalized.equals("csv") && !normalized.equals("tsv")) {
                throw new IllegalArgumentException(
                        "Invalid format: " + requestedFormat + ". Must be 'csv' or 'tsv'");
            }
            return normalized;
        }

        // If format inference is enabled, infer from extension
        if (ingestConfig.getApi().isInferFormatFromExtension()) {
            String filename = file.getOriginalFilename();
            if (filename != null) {
                if (filename.toLowerCase().endsWith(".tsv")) {
                    return "tsv";
                } else if (filename.toLowerCase().endsWith(".csv")) {
                    return "csv";
                }
                // For files without extensions, use default format
                int lastDotIndex = filename.lastIndexOf('.');
                if (lastDotIndex == -1) {
                    log.debug("File {} has no extension, using default format: {}",
                            filename, ingestConfig.getApi().getDefaultFormat());
                }
            }
        }

        // Default to configured default format
        return ingestConfig.getApi().getDefaultFormat();
    }

    /**
     * Validate the uploaded file
     * Supports files without extensions based on ingest.api.supported-extensions
     * configuration
     */
    private void validateFile(MultipartFile file, String format) {
        // Get supported extensions from API config
        String supportedExtensions = ingestConfig.getApi().getSupportedExtensions();

        // Build array of allowed extensions
        String[] allowedExtensions;
        if (supportedExtensions == null || supportedExtensions.trim().isEmpty()) {
            // Empty config means allow files without extensions (and also the specified
            // format)
            allowedExtensions = new String[] { format, "" };
            log.debug("Allowing files with format '{}' or no extension", format);
        } else {
            // Parse comma-separated extensions and add empty string for no-extension files
            String[] configured = supportedExtensions.split(",");
            allowedExtensions = new String[configured.length + 2];
            allowedExtensions[0] = format; // Current format
            allowedExtensions[1] = ""; // No extension
            System.arraycopy(configured, 0, allowedExtensions, 2, configured.length);
            log.debug("Allowed extensions: {}", String.join(", ", allowedExtensions));
        }

        // Validate file
        FileValidationUtil.validateFile(file, allowedExtensions);

        // Additional validation
        if (file.isEmpty()) {
            throw new IllegalArgumentException("Uploaded file is empty");
        }
    }

    /**
     * Validate routing is possible and resolve table name
     */
    private String validateAndResolveRouting(String filename) {
        if (!filenameRouterService.canRoute(filename)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Filename '%s' does not match routing pattern. " +
                                    "Expected pattern: %s",
                            filename,
                            ingestConfig.getFilenameRouting().getRegex()));
        }

        return filenameRouterService.getFullyQualifiedTableName(filename);
    }
}