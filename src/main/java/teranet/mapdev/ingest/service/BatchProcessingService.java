package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import teranet.mapdev.ingest.dto.BatchProcessingResultDto;
import teranet.mapdev.ingest.dto.BatchProcessingResultDto.FileProcessingResult;
import teranet.mapdev.ingest.dto.BatchProcessingResultDto.ValidationSummary;
import teranet.mapdev.ingest.dto.ZipAnalysisDto;
import teranet.mapdev.ingest.dto.ZipAnalysisDto.ExtractedFileInfo;
import teranet.mapdev.ingest.model.IngestionManifest;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Batch processing service for handling multiple CSV files
 * Supports ZIP file extraction and parallel processing to staging area
 */
@Service
public class BatchProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessingService.class);

    @Autowired
    private ZipProcessingService zipProcessingService;

    @Autowired
    private DelimitedFileProcessingService delimitedFileProcessingService;

    @Autowired
    private IngestionManifestService manifestService;

    @Autowired
    private FilenameRouterService filenameRouterService;

    private static final String TEMP_BATCH_DIR = "temp_batch_processing";

    /**
     * Process ZIP file containing multiple CSV files to staging area
     * 
     * @param zipFile the ZIP file containing CSV files
     * @return BatchProcessingResultDto with comprehensive results
     */
    public BatchProcessingResultDto processBatchFromZip(MultipartFile zipFile) {
        LocalDateTime startTime = LocalDateTime.now();
        UUID parentBatchId = null; // Initialize for scope
        String batchId = null; // Initialize for scope

        logger.info("Starting batch processing for ZIP file: {}", zipFile.getOriginalFilename());

        try {
            // Calculate checksum first to check for duplicates
            String checksum = calculateChecksum(zipFile);

            // Check if this ZIP file was already processed (idempotency check)
            IngestionManifest existingManifest = manifestService.findByChecksum(checksum);
            if (existingManifest != null && existingManifest.isCompleted()) {
                logger.info(
                        "DUPLICATE UPLOAD: ZIP file {} with checksum {} was already processed in batch {}. Returning existing result.",
                        zipFile.getOriginalFilename(),
                        checksum,
                        existingManifest.getBatchId());

                // Return existing processing result - don't reprocess
                return createDuplicateBatchResult(existingManifest, startTime);
            }

            // No duplicate found, proceed with new processing
            parentBatchId = UUID.randomUUID();
            batchId = parentBatchId.toString();

            logger.info("Processing new ZIP file with batch ID: {}", batchId);

            // No schema creation needed - using default schema

            // Create parent manifest for the ZIP file
            IngestionManifest zipManifest = createParentZipManifest(zipFile, parentBatchId, startTime, checksum);
            manifestService.save(zipManifest);
            logger.info("Created parent ZIP manifest with batch ID: {}", parentBatchId);

            // Extract and analyze ZIP file
            ZipAnalysisDto analysis = zipProcessingService.analyzeZipFile(zipFile);

            if (!"SUCCESS".equals(analysis.getExtractionStatus())) {
                // Update parent manifest to FAILED
                zipManifest.setStatus(IngestionManifest.Status.FAILED);
                zipManifest.setErrorMessage("ZIP extraction failed: " + analysis.getExtractionStatus());
                zipManifest.setCompletedAt(LocalDateTime.now());
                manifestService.update(zipManifest);
                return createFailedBatchResult(batchId, startTime,
                        "ZIP extraction failed: " + analysis.getExtractionStatus());
            }

            // Extract CSV files to temporary directory for processing
            Path batchDir = extractZipForBatch(zipFile, batchId);

            // Process each CSV file to staging (with parent batch ID linking)
            List<FileProcessingResult> fileResults = processCsvFilesToStaging(batchDir, analysis.getExtractedFiles(),
                    batchId, parentBatchId);

            // Calculate totals from all child CSV manifests
            long totalRecords = fileResults.stream().mapToLong(FileProcessingResult::getRowsLoaded).sum();
            long totalProcessed = fileResults.stream().mapToLong(FileProcessingResult::getRowsLoaded).sum();

            // Generate validation summary
            ValidationSummary validationSummary = generateValidationSummary(fileResults);

            // Update parent manifest to COMPLETED
            LocalDateTime endTime = LocalDateTime.now();
            long durationMs = java.time.Duration.between(startTime, endTime).toMillis();

            zipManifest.setStatus(IngestionManifest.Status.COMPLETED);
            zipManifest.setCompletedAt(endTime);
            zipManifest.setProcessingDurationMs(durationMs);
            zipManifest.setTotalRecords(totalRecords);
            zipManifest.setProcessedRecords(totalProcessed);
            manifestService.update(zipManifest);
            logger.info("Updated parent ZIP manifest to COMPLETED with {} total records", totalRecords);

            // Create final result
            BatchProcessingResultDto result = createSuccessfulBatchResult(
                    batchId, startTime, endTime, durationMs, fileResults, validationSummary);

            // Cleanup temporary files
            cleanupBatchDirectory(batchDir);

            logger.info("Batch processing completed successfully - Batch ID: {}, Files: {}, Duration: {}ms",
                    batchId, fileResults.size(), durationMs);

            return result;

        } catch (Exception e) {
            logger.error("Batch processing failed for batch ID: {}", batchId, e);

            // Try to update parent manifest to FAILED
            try {
                IngestionManifest failedManifest = manifestService.findByBatchId(parentBatchId);
                if (failedManifest != null) {
                    failedManifest.setStatus(IngestionManifest.Status.FAILED);
                    failedManifest.setErrorMessage("Batch processing failed: " + e.getMessage());
                    failedManifest.setCompletedAt(LocalDateTime.now());
                    manifestService.update(failedManifest);
                }
            } catch (Exception updateEx) {
                logger.warn("Could not update parent manifest to FAILED status", updateEx);
            }

            return createFailedBatchResult(batchId, startTime, "Batch processing failed: " + e.getMessage());
        }
    }

    /**
     * Extract ZIP file to temporary directory for batch processing
     * Preserves directory structure and extracts files matching CSV or PM/IM
     * pattern
     * 
     * @param zipFile the ZIP file to extract
     * @param batchId the batch ID for organization
     * @return path to extracted directory
     */
    private Path extractZipForBatch(MultipartFile zipFile, String batchId) throws IOException {
        Path batchDir = createBatchDirectory(batchId);

        try (ZipInputStream zipInputStream = new ZipInputStream(zipFile.getInputStream())) {
            ZipEntry entry;

            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    String entryName = entry.getName();
                    String fileName = new File(entryName).getName();

                    // Check if file is CSV or matches PM/IM pattern
                    boolean isCsv = fileName.toLowerCase().endsWith(".csv") ||
                            fileName.toLowerCase().endsWith(".tsv");
                    boolean isDataFile = fileName.matches("^([A-Z]{2})(\\d+)$");

                    if (isCsv || isDataFile) {
                        // Preserve directory structure
                        Path targetPath = batchDir.resolve(entryName);

                        // Create parent directories if needed
                        Files.createDirectories(targetPath.getParent());

                        try (FileOutputStream fileOutputStream = new FileOutputStream(targetPath.toFile())) {
                            byte[] buffer = new byte[8192];
                            int length;
                            while ((length = zipInputStream.read(buffer)) != -1) {
                                fileOutputStream.write(buffer, 0, length);
                            }
                        }

                        logger.debug("Extracted file for batch processing: {}", entryName);
                    }
                }

                zipInputStream.closeEntry();
            }
        }

        return batchDir;
    }

    /**
     * Process CSV files to staging area
     * 
     * @param batchDir       directory containing extracted CSV files
     * @param extractedFiles file analysis information
     * @param batchId        batch identifier
     * @param parentBatchId  parent batch identifier for ZIP processing
     * @return list of file processing results
     */
    private List<FileProcessingResult> processCsvFilesToStaging(Path batchDir,
            List<ExtractedFileInfo> extractedFiles,
            String batchId,
            UUID parentBatchId) {
        List<FileProcessingResult> results = new ArrayList<>();

        for (ExtractedFileInfo fileInfo : extractedFiles) {
            if (!"CSV".equals(fileInfo.getFileType())) {
                continue; // Skip non-CSV files
            }

            try {
                // Use relativePath to locate the file (includes subdirectory structure like
                // 62.2023_05_24.08_46_06/IM162)
                Path csvPath = batchDir.resolve(fileInfo.getRelativePath());
                if (!Files.exists(csvPath)) {
                    logger.warn("CSV file not found in batch directory: {} (relative path: {})",
                            fileInfo.getFilename(), fileInfo.getRelativePath());
                    results.add(
                            createFailedFileResult(fileInfo.getFilename(), "File not found in extracted directory"));
                    continue;
                }

                // Convert Path to MultipartFile for processing
                MultipartFile csvFile = createMultipartFileFromPath(csvPath);

                FileProcessingResult result = processSingleCsvToStaging(csvFile, batchId, parentBatchId);
                results.add(result);

            } catch (Exception e) {
                logger.error("Failed to process CSV file from batch: {}", fileInfo.getFilename(), e);
                results.add(createFailedFileResult(fileInfo.getFilename(), e.getMessage()));
            }
        }

        return results;
    }

    /**
     * Process a single CSV file to staging area using
     * DelimitedFileProcessingService
     * This routes files to existing staging tables (e.g., PM162 -> staging_pm1)
     * 
     * @param csvFile       the CSV file to process
     * @param batchId       the batch identifier
     * @param parentBatchId the parent batch identifier (for ZIP processing)
     * @return FileProcessingResult with processing details
     */
    private FileProcessingResult processSingleCsvToStaging(MultipartFile csvFile, String batchId, UUID parentBatchId)
            throws Exception {
        long startTime = System.currentTimeMillis();
        String filename = csvFile.getOriginalFilename();

        logger.info("Processing CSV file to staging: {}", filename);

        try {
            // Process delimited file with routing to existing staging tables
            // Format: tsv (tab-delimited)
            // Has headers: false
            // Route by filename: true (PM162 -> staging_pm1, IM262 -> staging_im2)
            IngestionManifest manifest = delimitedFileProcessingService.processDelimitedFile(
                    csvFile,
                    "tsv", // Tab-delimited format
                    false, // No headers
                    true // Route by filename (PM162 -> staging_pm1)
            );

            long processingTime = System.currentTimeMillis() - startTime;
            // Get table name from manifest (which now includes batch_id suffix)
            String tableName = manifest.getTableName();

            // Check if file was already processed (idempotency)
            if (manifest.isAlreadyProcessed()) {
                logger.info("DUPLICATE: File {} was already processed as batch {} in table {}",
                        filename, manifest.getBatchId(), tableName);

                return new FileProcessingResult(
                        filename,
                        tableName,
                        "DUPLICATE", // Mark as duplicate
                        manifest.getTotalRecords(),
                        0,
                        processingTime,
                        String.format("File already processed in batch %s. Original upload completed on %s.",
                                manifest.getBatchId().toString(),
                                manifest.getCompletedAt() != null ? manifest.getCompletedAt().toString() : "unknown"));
            }

            FileProcessingResult result = new FileProcessingResult(
                    filename,
                    tableName,
                    "SUCCESS",
                    manifest.getTotalRecords(),
                    0, // columns created (would need to track this)
                    processingTime,
                    null);

            logger.info("Successfully processed {} to staging table {} with {} records",
                    filename, tableName, manifest.getTotalRecords());

            return result;

        } catch (Exception e) {
            long processingTime = System.currentTimeMillis() - startTime;
            logger.error("Failed to process CSV file: {}", filename, e);

            // For failed processing, resolve table name estimate (won't actually be
            // created)
            String estimatedTableName = filenameRouterService.resolveTableName(filename);

            return new FileProcessingResult(
                    filename,
                    estimatedTableName,
                    "FAILED",
                    0L,
                    0,
                    processingTime,
                    e.getMessage());
        }
    }

    /**
     * Generate validation summary from file processing results
     * 
     * @param fileResults list of file processing results
     * @return ValidationSummary with analysis
     */
    private ValidationSummary generateValidationSummary(List<FileProcessingResult> fileResults) {
        int readyForProduction = 0;
        int requireReview = 0;
        List<String> dataQualityIssues = new ArrayList<>();
        List<String> schemaConflicts = new ArrayList<>();

        for (FileProcessingResult result : fileResults) {
            if ("SUCCESS".equals(result.getStatus())) {
                // Newly processed files
                if (result.getRowsLoaded() > 0) {
                    readyForProduction++;
                } else {
                    requireReview++;
                    dataQualityIssues.add("File " + result.getFilename() + " contains no data rows");
                }
            } else if ("DUPLICATE".equals(result.getStatus())) {
                // Duplicates are not errors - they're already processed
                // Don't count them in readyForProduction or requireReview
                // They're handled by idempotency and shouldn't be flagged as issues
                logger.debug("Skipping duplicate file in validation summary: {}", result.getFilename());
            } else {
                // Only actual failures are data quality issues
                requireReview++;
                dataQualityIssues.add("Failed to process: " + result.getFilename() + " - " + result.getErrorMessage());
            }
        }

        return new ValidationSummary(readyForProduction, requireReview, dataQualityIssues, schemaConflicts);
    }

    // Helper methods

    private Path createBatchDirectory(String batchId) throws IOException {
        Path batchDir = Paths.get(System.getProperty("java.io.tmpdir"), TEMP_BATCH_DIR, "batch_" + batchId);
        Files.createDirectories(batchDir);
        logger.debug("Created batch processing directory: {}", batchDir);
        return batchDir;
    }

    private void cleanupBatchDirectory(Path batchDir) {
        try {
            if (Files.exists(batchDir)) {
                Files.walk(batchDir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
                logger.debug("Cleaned up batch directory: {}", batchDir);
            }
        } catch (Exception e) {
            logger.warn("Could not fully clean up batch directory: {}", batchDir, e);
        }
    }

    private MultipartFile createMultipartFileFromPath(Path csvPath) throws IOException {
        byte[] content = Files.readAllBytes(csvPath);
        String filename = csvPath.getFileName().toString();

        return new MultipartFile() {
            @Override
            public String getName() {
                return "file";
            }

            @Override
            public String getOriginalFilename() {
                return filename;
            }

            @Override
            public String getContentType() {
                return "text/csv";
            }

            @Override
            public boolean isEmpty() {
                return content.length == 0;
            }

            @Override
            public long getSize() {
                return content.length;
            }

            @Override
            public byte[] getBytes() {
                return content;
            }

            @Override
            public InputStream getInputStream() {
                return new ByteArrayInputStream(content);
            }

            @Override
            public void transferTo(File dest) throws IOException {
                Files.write(dest.toPath(), content);
            }
        };
    }

    private FileProcessingResult createFailedFileResult(String filename, String errorMessage) {
        return new FileProcessingResult(filename, "", "FAILED", 0L, 0, 0L, errorMessage);
    }

    private BatchProcessingResultDto createFailedBatchResult(String batchId, LocalDateTime startTime,
            String errorMessage) {
        BatchProcessingResultDto result = new BatchProcessingResultDto();
        result.setBatchId(batchId);
        result.setProcessingStatus("FAILED");
        result.setTotalFilesProcessed(0);
        result.setSuccessfulFiles(0);
        result.setFailedFiles(1);
        result.setTotalRowsLoaded(0L);
        result.setProcessingStartTime(startTime);
        result.setProcessingEndTime(LocalDateTime.now());
        result.setProcessingDurationMs(java.time.Duration.between(startTime, LocalDateTime.now()).toMillis());
        result.setStagingSchema(null); // No longer using staging schema
        result.setFileResults(new ArrayList<>());

        ValidationSummary validationSummary = new ValidationSummary(0, 1,
                Arrays.asList(errorMessage), new ArrayList<>());
        result.setValidationSummary(validationSummary);

        return result;
    }

    private BatchProcessingResultDto createSuccessfulBatchResult(String batchId, LocalDateTime startTime,
            LocalDateTime endTime, long durationMs,
            List<FileProcessingResult> fileResults,
            ValidationSummary validationSummary) {
        BatchProcessingResultDto result = new BatchProcessingResultDto();
        result.setBatchId(batchId);

        // Count successful, duplicate, and failed files
        int successful = (int) fileResults.stream()
                .filter(r -> "SUCCESS".equals(r.getStatus()))
                .count();
        int duplicates = (int) fileResults.stream()
                .filter(r -> "DUPLICATE".equals(r.getStatus()))
                .count();
        int failed = (int) fileResults.stream()
                .filter(r -> "FAILED".equals(r.getStatus()))
                .count();

        // Only count rows from newly processed files (not duplicates)
        long totalRows = fileResults.stream()
                .filter(r -> "SUCCESS".equals(r.getStatus()))
                .mapToLong(FileProcessingResult::getRowsLoaded)
                .sum();

        // Determine overall status
        String status;
        if (failed == 0 && duplicates == 0) {
            status = "SUCCESS"; // All files processed successfully
        } else if (failed == 0 && duplicates > 0 && successful > 0) {
            status = "PARTIAL_SUCCESS"; // Some new, some duplicates, no failures
        } else if (failed == 0 && duplicates > 0 && successful == 0) {
            status = "ALL_DUPLICATES"; // All files were already processed
        } else if (failed > 0 && successful > 0) {
            status = "PARTIAL_SUCCESS"; // Mix of success and failures
        } else {
            status = "FAILED"; // All files failed
        }

        result.setProcessingStatus(status);
        result.setTotalFilesProcessed(fileResults.size());
        result.setSuccessfulFiles(successful);
        result.setFailedFiles(failed);
        result.setTotalRowsLoaded(totalRows);
        result.setProcessingStartTime(startTime);
        result.setProcessingEndTime(endTime);
        result.setProcessingDurationMs(durationMs);
        result.setStagingSchema(null); // No longer using staging schema
        result.setFileResults(fileResults);
        result.setValidationSummary(validationSummary);

        return result;
    }

    /**
     * Create parent ZIP manifest entry
     */
    private IngestionManifest createParentZipManifest(MultipartFile zipFile, UUID batchId, LocalDateTime startTime,
            String checksum) {
        try {
            IngestionManifest manifest = new IngestionManifest();
            manifest.setBatchId(batchId);
            manifest.setParentBatchId(null);
            manifest.setFileName(zipFile.getOriginalFilename());
            manifest.setFilePath("upload://" + zipFile.getOriginalFilename());
            manifest.setFileSizeBytes(zipFile.getSize());
            manifest.setContentType(zipFile.getContentType());
            manifest.setStatus(IngestionManifest.Status.PROCESSING);
            manifest.setStartedAt(startTime);
            manifest.setTotalRecords(0L);
            manifest.setProcessedRecords(0L);
            manifest.setFailedRecords(0L);
            manifest.setFileChecksum(checksum);
            return manifest;
        } catch (Exception e) {
            logger.error("Error creating parent ZIP manifest", e);
            throw new RuntimeException("Failed to create parent ZIP manifest", e);
        }
    }

    private String calculateChecksum(MultipartFile file) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(file.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            logger.warn("Could not calculate checksum", e);
            return "unknown_" + System.currentTimeMillis();
        }
    }

    /**
     * Create a result for a duplicate upload (file already processed)
     */
    private BatchProcessingResultDto createDuplicateBatchResult(IngestionManifest existingManifest,
            LocalDateTime requestStartTime) {
        BatchProcessingResultDto result = new BatchProcessingResultDto();
        result.setBatchId(existingManifest.getBatchId().toString());
        result.setProcessingStatus("ALREADY_PROCESSED");
        result.setTotalFilesProcessed(0);
        result.setSuccessfulFiles(0);
        result.setFailedFiles(0);
        result.setTotalRowsLoaded(existingManifest.getTotalRecords());
        result.setProcessingStartTime(requestStartTime);
        result.setProcessingEndTime(LocalDateTime.now());
        result.setProcessingDurationMs(existingManifest.getProcessingDurationMs());
        result.setStagingSchema(null);
        result.setFileResults(new ArrayList<>());

        ValidationSummary validationSummary = new ValidationSummary();
        validationSummary.setTablesReadyForProduction(0);
        validationSummary.setTablesRequiringReview(0);
        validationSummary.setDataQualityIssues(Arrays.asList(
                "DUPLICATE FILE: This file was already processed on " + existingManifest.getCompletedAt() +
                        ". Original batch ID: " + existingManifest.getBatchId() +
                        ". No new data was inserted."));
        validationSummary.setSchemaConflicts(new ArrayList<>());
        result.setValidationSummary(validationSummary);

        return result;
    }
}
