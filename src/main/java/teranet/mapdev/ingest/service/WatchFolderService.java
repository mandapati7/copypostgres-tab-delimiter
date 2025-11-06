package teranet.mapdev.ingest.service;

import teranet.mapdev.ingest.config.WatchFolderConfig;
import teranet.mapdev.ingest.model.IngestionManifest;
import teranet.mapdev.ingest.dto.BatchProcessingResultDto;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired; 
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Main watch folder service that monitors upload folder for new files
 * Uses Java NIO WatchService for efficient file monitoring
 * Processes files using existing CsvProcessingService and ZipProcessingService
 */
@Service
public class WatchFolderService {

    private static final Logger logger = LoggerFactory.getLogger(WatchFolderService.class);

    @Autowired
    private WatchFolderConfig config;

    @Autowired
    private WatchFolderManager folderManager;

    @Autowired
    private CsvProcessingService csvProcessingService;

    @Autowired
    private BatchProcessingService batchProcessingService;

    @Autowired
    private IngestionManifestService manifestService;

    private WatchService watchService;
    private ExecutorService executorService;
    private volatile boolean running = false;

    // Track files currently being processed to prevent duplicate processing
    private final Set<String> filesInProgress = ConcurrentHashMap.newKeySet();

    /**
     * Start watching the upload folder on application startup
     */
    @PostConstruct
    public void startWatching() {
        if (!config.isEnabled()) {
            logger.info("Watch folder is DISABLED - service will not start");
            return;
        }

        try {
            // Initialize watch service
            this.watchService = FileSystems.getDefault().newWatchService();

            // Register upload folder for monitoring
            Path uploadPath = folderManager.getUploadPath();
            uploadPath.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY);

            // Create thread pool for concurrent file processing
            this.executorService = Executors.newFixedThreadPool(config.getMaxConcurrentFiles());

            this.running = true;

            logger.info("========================================");
            logger.info("[SUCCESS] WATCH FOLDER SERVICE STARTED");
            logger.info("========================================");
            logger.info("Monitoring folder: {}", uploadPath);
            logger.info("Marker file pattern: *{}", config.getMarkerExtension());
            logger.info("Polling interval: {} ms", config.getPollingInterval());
            logger.info("Max concurrent files: {}", config.getMaxConcurrentFiles());
            logger.info("Supported extensions: {}", config.getSupportedExtensions());
            logger.info("========================================");

            // Start watch loop in separate thread
            Thread watchThread = new Thread(this::watchForFiles, "WatchFolderThread");
            watchThread.setDaemon(true);
            watchThread.start();

        } catch (IOException e) {
            logger.error("Failed to start watch folder service", e);
            throw new RuntimeException("Watch folder service initialization failed", e);
        }
    }

    /**
     * Stop watching and cleanup resources
     */
    @PreDestroy
    public void stopWatching() {
        logger.info("Stopping watch folder service...");

        running = false;

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException e) {
                logger.warn("Error closing watch service", e);
            }
        }

        logger.info("[SUCCESS] Watch folder service stopped");
    }

    /**
     * Main watch loop - monitors folder for new marker files
     */
    private void watchForFiles() {
        logger.info("Watch loop started - waiting for files...");

        while (running) {
            try {
                // Wait for events
                WatchKey key = watchService.poll(config.getPollingInterval(), TimeUnit.MILLISECONDS);

                if (key == null) {
                    continue; // No events, continue polling
                }

                // Process events
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        logger.warn("Watch service overflow - some events may have been lost");
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                    Path fileName = pathEvent.context();

                    // Only process marker files
                    if (fileName.toString().endsWith(config.getMarkerExtension())) {
                        logger.info("[MARKER] Marker file detected: {}", fileName);

                        // Submit file processing to thread pool
                        executorService.submit(() -> handleMarkerFile(fileName));
                    }
                }

                // Reset the key
                boolean valid = key.reset();
                if (!valid) {
                    logger.error("Watch key no longer valid - stopping watch loop");
                    break;
                }

            } catch (InterruptedException e) {
                logger.info("Watch loop interrupted - shutting down");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in watch loop", e);
                // Continue running even if there's an error
            }
        }

        logger.info("Watch loop ended");
    }

    /**
     * Handle detection of a marker file
     * Extract CSV/ZIP filename and process it
     */
    private void handleMarkerFile(Path markerFileName) {
        String markerName = markerFileName.toString();
        String dataFileName = config.getCsvFileNameFromMarker(markerName);

        // Check if this file is already being processed (prevent duplicate processing)
        if (!filesInProgress.add(dataFileName)) {
            logger.debug("File {} is already being processed by another thread - skipping", dataFileName);
            return;
        }

        try {
            logger.info("Processing marker: {} -> data file: {}", markerName, dataFileName);

            // Validate marker file naming convention
            if (dataFileName.equals(markerName) || dataFileName.isEmpty()) {
                logger.error("[ERROR] Invalid marker file name: {}. Expected format: filename.csv{}",
                        markerName, config.getMarkerExtension());
                logger.error("   Example: orders.csv{} (not orders{})",
                        config.getMarkerExtension(), config.getMarkerExtension());
                // Don't delete marker - let user see the mistake
                return;
            }

            // Check if data file has an extension
            if (!dataFileName.contains(".")) {
                logger.error("[ERROR] Data file name has no extension: {}. Expected: {}.csv or {}.zip",
                        dataFileName, dataFileName, dataFileName);
                logger.error("   Marker file should be: {}.csv{} or {}.zip{}",
                        dataFileName, config.getMarkerExtension(), dataFileName, config.getMarkerExtension());
                // Don't delete marker - let user see the mistake
                return;
            }

            // Check if data file exists
            Path dataFilePath = folderManager.getUploadPath().resolve(dataFileName);

            if (!Files.exists(dataFilePath)) {
                logger.warn("[WARNING] Marker found but data file missing: {}", dataFileName);
                logger.warn("   Expected file: {}", dataFilePath);
                deleteMarkerWithRetry(markerFileName);
                return;
            }

            // Validate file type
            String extension = getFileExtension(dataFileName);
            if (!config.isSupportedExtension(extension)) {
                logger.warn("[WARNING] Unsupported file type: {} (expected: {})",
                        extension, config.getSupportedExtensions());
                deleteMarkerWithRetry(markerFileName);
                return;
            }

            // Check file stability if configured
            if (config.isUseMarkerFiles()) {
                // With marker files, we trust that file is complete
                // But still do a quick stability check
                if (!isFileStable(dataFilePath)) {
                    logger.warn("[WARNING] File is not stable yet: {}", dataFileName);
                    return; // Will be picked up on next poll
                }
            }

            // Process the file
            processFile(dataFilePath, dataFileName);

        } catch (Exception e) {
            logger.error("[ERROR] Error handling marker file: {}", markerName, e);
        } finally {
            // Always remove from in-progress set when done (success or failure)
            filesInProgress.remove(dataFileName);
        }
    }

    /**
     * Delete marker file with retry logic (for Windows file locking issues)
     */
    private void deleteMarkerWithRetry(Path markerFileName) {
        String markerName = markerFileName.toString();
        String dataFileName = config.getCsvFileNameFromMarker(markerName);

        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                Thread.sleep(100 * attempt); // Increasing delay: 100ms, 200ms, 300ms
                folderManager.deleteMarkerFileFromUpload(dataFileName);
                logger.debug("Deleted marker file on attempt {}: {}", attempt, markerName);
                return;
            } catch (Exception e) {
                if (attempt == 3) {
                    logger.warn("Failed to delete marker file after {} attempts: {}", attempt, markerName);
                }
            }
        }
    }

    /**
     * Main file processing orchestration
     * Moves file through lifecycle: upload -> wip -> archive/error
     */
    private void processFile(Path uploadedFilePath, String fileName) {
        logger.info("========================================");
        logger.info("[START] START PROCESSING: {}", fileName);
        logger.info("========================================");

        Path wipFilePath = null;
        long startTime = System.currentTimeMillis();
        try {
            // Step 1: Move to WIP folder
            wipFilePath = folderManager.moveToWip(uploadedFilePath);

            // Step 2: Determine file type and process
            String extension = getFileExtension(fileName);
            IngestionManifest manifest;

            if (".csv".equalsIgnoreCase(extension)) {
                manifest = processCsvFile(wipFilePath);
            } else if (".zip".equalsIgnoreCase(extension)) {
                manifest = processZipFile(wipFilePath);
            } else {
                throw new IllegalArgumentException("Unsupported file type: " + extension);
            }

            // Step 3: Check processing result
            long endTime = System.currentTimeMillis();
            long processingTimeMs = endTime - startTime;

            if (manifest != null && manifest.getStatus() == IngestionManifest.Status.COMPLETED) {
                // Success - move to archive
                Path archivedPath = folderManager.moveToArchive(wipFilePath);

                // Create .done file with sucess details
                createDoneFile(archivedPath, manifest, processingTimeMs, true);

                logger.info("========================================");
                logger.info("[SUCCESS] SUCCESS: {} -> {}", fileName, archivedPath.getFileName());
                logger.info("   Batch ID: {}", manifest.getBatchId());
                logger.info("   Records: {}", manifest.getTotalRecords());
                logger.info("   Processing Time: {} and ms ({} seconds)", processingTimeMs, processingTimeMs / 1000.0);
                logger.info("========================================");
            } else {
                // Failed - move to error
                String errorMsg = manifest != null ? manifest.getErrorMessage() : "Unknown error";
                String errorDetails = manifest != null ? manifest.getErrorDetails() : "";

                Path errorPath = folderManager.moveToError(wipFilePath, errorMsg, errorDetails, null);

                // Create .done file with error details
                createDoneFile(errorPath, manifest, processingTimeMs, false);
                logger.error("========================================");
                logger.error("[FAILED] FAILED: {} -> {}", fileName, errorPath.getFileName());
                logger.error("   Error: {}", errorMsg);
                logger.error("   Processing Time: {} ms ({} seconds)", processingTimeMs, processingTimeMs / 1000.0);
                logger.error("========================================");
            }

            // Step 4: Delete marker file
            folderManager.deleteMarkerFileFromUpload(fileName);

        } catch (Exception e) {
            logger.error("[CRITICAL] CRITICAL ERROR processing file: {}", fileName, e);

            // Try to move to error folder
            if (wipFilePath != null && Files.exists(wipFilePath)) {
                try {
                    folderManager.moveToError(wipFilePath, "Processing exception", e.getMessage(), e);
                    folderManager.deleteMarkerFileFromUpload(fileName);
                } catch (IOException ex) {
                    logger.error("[ERROR] Failed to move file to error folder", ex);
                }
            }
        }
    }

    /**
     * Process a CSV file
     */
    private IngestionManifest processCsvFile(Path csvFilePath) {
        logger.info("Processing CSV file: {}", csvFilePath.getFileName());
        logger.debug("[DEBUG] About to convert Path to MultipartFile");

        try {
            // Convert Path to MultipartFile
            MultipartFile multipartFile = convertToMultipartFile(csvFilePath);

            logger.debug("[DEBUG] MultipartFile created, about to call CsvProcessingService");

            // Use existing CSV processing service
            IngestionManifest manifest = csvProcessingService.processCsvToStaging(multipartFile);

            logger.info("CSV processing completed - Status: {}", manifest.getStatus());

            return manifest;

        } catch (Exception e) {
            logger.error("CSV processing failed", e);

            // Create error manifest
            IngestionManifest errorManifest = new IngestionManifest();
            errorManifest.setStatus(IngestionManifest.Status.FAILED);
            errorManifest.setErrorMessage("CSV processing failed: " + e.getMessage());
            errorManifest.setErrorDetails(getStackTraceAsString(e));

            return errorManifest;
        }
    }

    /**
     * Process a ZIP file using BatchProcessingService
     * This will extract the ZIP, create a parent manifest, and process each CSV
     * file with child manifests
     */
    private IngestionManifest processZipFile(Path zipFilePath) {
        logger.info("Processing ZIP file: {}", zipFilePath.getFileName());

        try {
            // Convert Path to MultipartFile
            MultipartFile multipartFile = convertToMultipartFile(zipFilePath);

            // Use BatchProcessingService to process ZIP file
            // This will:
            // 1. Create parent manifest for ZIP file
            // 2. Extract CSV files from ZIP
            // 3. Process each CSV file with child manifests (parent_batch_id set)
            logger.info("Using BatchProcessingService to process ZIP file with child CSV manifests");
            BatchProcessingResultDto batchResult = batchProcessingService.processBatchFromZip(multipartFile);

            // Retrieve the parent manifest created by BatchProcessingService
            UUID parentBatchId = UUID.fromString(batchResult.getBatchId());
            IngestionManifest parentManifest = manifestService.findByBatchId(parentBatchId);

            if (parentManifest == null) {
                logger.error("Parent manifest not found for batch ID: {}", parentBatchId);
                throw new RuntimeException("Parent manifest not found after batch processing");
            }

            logger.info("ZIP processing completed - Parent batch: {}, Files processed: {}, Status: {}",
                    batchResult.getBatchId(),
                    batchResult.getTotalFilesProcessed(),
                    batchResult.getProcessingStatus());

            return parentManifest;

        } catch (Exception e) {
            logger.error("ZIP processing failed: {}", zipFilePath.getFileName(), e);

            // Create error manifest for failed ZIP processing
            IngestionManifest errorManifest = new IngestionManifest();
            errorManifest.setFileName(zipFilePath.getFileName().toString());
            errorManifest.setStatus(IngestionManifest.Status.FAILED);
            errorManifest.setErrorMessage("ZIP processing failed: " + e.getMessage());
            errorManifest.setErrorDetails(getStackTraceAsString(e));

            return errorManifest;
        }
    }

    /**
     * Convert Path to MultipartFile for service compatibility
     */
    private MultipartFile convertToMultipartFile(Path filePath) throws IOException {
        final String fileName = filePath.getFileName().toString();
        String detectedContentType = Files.probeContentType(filePath);
        final String contentType = (detectedContentType != null) ? detectedContentType : "application/octet-stream";

        final File file = filePath.toFile();

        return new MultipartFile() {
            @Override
            public String getName() {
                return "file";
            }

            @Override
            public String getOriginalFilename() {
                return fileName;
            }

            @Override
            public String getContentType() {
                return contentType;
            }

            @Override
            public boolean isEmpty() {
                return file.length() == 0;
            }

            @Override
            public long getSize() {
                return file.length();
            }

            @Override
            public byte[] getBytes() throws IOException {
                return Files.readAllBytes(filePath);
            }

            @Override
            public InputStream getInputStream() throws IOException {
                return new FileInputStream(file);
            }

            @Override
            public void transferTo(File dest) throws IOException {
                Files.copy(filePath, dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        };
    }

    /**
     * Check if file is stable (not being written to)
     */
    private boolean isFileStable(Path file) {
        try {
            long sizeBefore = Files.size(file);
            long modifiedBefore = Files.getLastModifiedTime(file).toMillis();

            Thread.sleep(config.getStabilityCheckDelay());

            long sizeAfter = Files.size(file);
            long modifiedAfter = Files.getLastModifiedTime(file).toMillis();

            return sizeBefore == sizeAfter && modifiedBefore == modifiedAfter;

        } catch (IOException | InterruptedException e) {
            logger.warn("File stability check failed for: {}", file.getFileName(), e);
            return false;
        }
    }

    /**
     * Get file extension including dot (e.g., ".csv")
     */
    private String getFileExtension(String fileName) {
        int lastDot = fileName.lastIndexOf('.');
        if (lastDot > 0) {
            return fileName.substring(lastDot);
        }
        return "";
    }

    /**
     * Convert exception to stack trace string
     */
    private String getStackTraceAsString(Exception exception) {
        StringBuilder sb = new StringBuilder();
        sb.append(exception.toString()).append("\n");
        for (StackTraceElement element : exception.getStackTrace()) {
            sb.append("\tat ").append(element.toString()).append("\n");
            if (sb.length() > 2000) {
                sb.append("\t... (truncated)");
                break;
            }
        }
        return sb.toString();
    }

    /**
     * Check if watch service is running
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Create a done marker file 'filename.csv.done' or 'filename.zip.done'
     * @param filePath Path to the processed file (in archive or error folder)
     * @param manifest The Ingestion manifest with processing details
     * @param processingTimeMs Time taken to process the file in milliseconds
     * @param isSuccess Whether the processing was successful
     * ' */
    private void createDoneFile(Path filePath, IngestionManifest manifest, long processingTimeMs, boolean isSuccess) {
        try {
            String doneFileName = filePath.getFileName().toString() + config.getMarkerExtension();
            Path doneFilePath = filePath.getParent().resolve(doneFileName);

            //Build done file content
            StringBuilder content = new StringBuilder();
            content.append("================================================\n");
            content.append("FILE PROCESING SUMMARY");
            content.append("\n================================================\n");

            // Basic details
            content.append("File Name: ").append(filePath.getFileName()).append("\n");
            content.append("Batch ID: ").append(manifest.getBatchId()).append("\n");
            content.append("Status: ").append(manifest.getStatus()).append("\n");
            content.append("Processing Time (ms): ").append(processingTimeMs).append("\n");
            content.append("Success: ").append(isSuccess ? "Success" : "Failure").append("\n");

            // Processing Details
            content.append("------------------------------------------------\n");
            content.append("Processing Details:\n");    
            content.append("------------------------------------------------\n");
            content.append("Total Records: ").append(manifest.getTotalRecords() != null ? manifest.getTotalRecords() : 0).append("\n");
            content.append("File Size (bytes): ").append(Files.size(filePath)).append("\n");

            content.append("------------------------------------------------\n");
            if (manifest != null) {
                if (manifest.getStartedAt() != null) {
                    content.append("Started At: ").append(manifest.getStartedAt()).append("\n");
                }
                if (manifest.getCompletedAt() != null) {
                    content.append("Completed At: ").append(manifest.getCompletedAt()).append("\n");
                }
                if (manifest.getProcessingDurationMs() != null) {
                    content.append("Processing Processing Duration (ms): ").append(manifest.getProcessingDurationMs())
                    .append(" ms (").append(String.format("%.2f", manifest.getProcessingDurationMs() / 1000.0))
                    .append(" seconds)\n");
                }
                if (manifest.getTableName() != null) {
                    content.append("Staging Table: ").append(manifest.getTableName()).append("\n");
                }

                content.append("\n------------------------------------------------\n");

                // Error information if failed
                if (!isSuccess && manifest != null) {
                    content.append("ERROR INFORMATION:\n");
                    content.append("------------------------------------------------\n");
                    if (manifest.getErrorMessage() != null) {
                        content.append("Error Message: ").append(manifest.getErrorMessage()).append("\n");
                    }
                    if (manifest.getErrorDetails() != null && !manifest.getErrorDetails().isEmpty()) {
                        content.append("Error Details: ").append(manifest.getErrorDetails()).append("\n");
                    }
                    content.append("\n------------------------------------------------\n");
                }

                content.append("================================================\n");
                content.append("Generated at:").append(java.time.LocalDateTime.now()).append("\n");
                content.append("================================================\n");


                // Write content to done file
                Files.writeString(doneFilePath, content.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

                logger.info("[Done] Created .done file: {}", doneFileName);
            }

            Files.createFile(doneFilePath);
            logger.info("Created done marker file: {}", doneFilePath.getFileName());
        } catch (IOException e) {
            logger.error("Failed to create done marker file: {}", filePath.getFileName(), e);
        }
    }
}
