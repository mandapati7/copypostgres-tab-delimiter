package teranet.mapdev.ingest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import teranet.mapdev.ingest.config.WatchFolderConfig;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Service for managing watch folder file operations
 * Handles file movement between folders and error report generation
 */
@Service
public class WatchFolderManager {
    
    private static final Logger logger = LoggerFactory.getLogger(WatchFolderManager.class);
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    
    @Autowired
    private WatchFolderConfig config;
    
    private final ObjectMapper objectMapper;
    
    public WatchFolderManager() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
    
    /**
     * Initialize folder structure on application startup
     * Creates all required folders if they don't exist
     */
    @PostConstruct
    public void initializeFolders() {
        if (!config.isEnabled()) {
            logger.info("Watch folder is DISABLED - skipping folder initialization");
            return;
        }
        
        try {
            createFolderIfNotExists(config.getUpload(), "UPLOAD");
            createFolderIfNotExists(config.getWip(), "WIP");
            createFolderIfNotExists(config.getError(), "ERROR");
            createFolderIfNotExists(config.getArchive(), "ARCHIVE");
            
            logger.info("[SUCCESS] Watch folder structure initialized successfully");
            logger.info("   [UPLOAD]  Upload:  {}", config.getUpload());
            logger.info("   [WIP]     WIP:     {}", config.getWip());
            logger.info("   [ERROR]   Error:   {}", config.getError());
            logger.info("   [ARCHIVE] Archive: {}", config.getArchive());
            
        } catch (IOException e) {
            logger.error("Failed to initialize watch folder structure", e);
            throw new RuntimeException("Watch folder initialization failed", e);
        }
    }
    
    /**
     * Create a folder if it doesn't exist
     */
    private void createFolderIfNotExists(String folderPath, String folderName) throws IOException {
        Path path = Paths.get(folderPath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
            logger.info("Created {} folder: {}", folderName, folderPath);
        } else {
            logger.debug("{} folder already exists: {}", folderName, folderPath);
        }
    }
    
    /**
     * Move file from upload folder to WIP folder
     * @param uploadedFile Path to file in upload folder
     * @return Path to file in WIP folder
     */
    public Path moveToWip(Path uploadedFile) throws IOException {
        String fileName = uploadedFile.getFileName().toString();
        Path wipPath = Paths.get(config.getWip(), fileName);
        
        // Move file (replace if exists)
        Files.move(uploadedFile, wipPath, StandardCopyOption.REPLACE_EXISTING);
        logger.info("[WIP] Moved to WIP: {}", fileName);
        
        return wipPath;
    }
    
    /**
     * Move file from WIP folder to archive folder with timestamp
     * @param wipFile Path to file in WIP folder
     * @return Path to file in archive folder
     */
    public Path moveToArchive(Path wipFile) throws IOException {
        String timestampedName = createTimestampedFileName(wipFile.getFileName().toString());
        Path archivePath = Paths.get(config.getArchive(), timestampedName);
        
        // Move file
        Files.move(wipFile, archivePath, StandardCopyOption.REPLACE_EXISTING);
        logger.info("[ARCHIVE] Moved to ARCHIVE: {} -> {}", wipFile.getFileName(), timestampedName);
        
        return archivePath;
    }
    
    /**
     * Move file from WIP folder to error folder with timestamp and create error report
     * @param wipFile Path to file in WIP folder
     * @param errorMessage Error message to include in report
     * @param errorDetails Detailed error information
     * @param exception Exception that caused the error
     * @return Path to file in error folder
     */
    public Path moveToError(Path wipFile, String errorMessage, String errorDetails, Exception exception) throws IOException {
        String timestampedName = createTimestampedFileName(wipFile.getFileName().toString());
        Path errorPath = Paths.get(config.getError(), timestampedName);
        
        // Move file
        Files.move(wipFile, errorPath, StandardCopyOption.REPLACE_EXISTING);
        logger.error("[ERROR] Moved to ERROR: {} -> {}", wipFile.getFileName(), timestampedName);
        
        // Create error report JSON
        createErrorReport(errorPath, wipFile.getFileName().toString(), errorMessage, errorDetails, exception);
        
        return errorPath;
    }
    
    /**
     * Delete marker file
     * @param csvFile Path to the CSV file (marker name will be derived)
     */
    public void deleteMarkerFile(Path csvFile) {
        try {
            // Marker file is in the same directory as CSV file
            String markerFileName = config.getMarkerFileName(csvFile.getFileName().toString());
            Path markerPath = csvFile.getParent().resolve(markerFileName);
            
            if (Files.exists(markerPath)) {
                Files.delete(markerPath);
                logger.debug("Deleted marker file: {}", markerFileName);
            }
        } catch (IOException e) {
            logger.warn("Failed to delete marker file for: {}", csvFile.getFileName(), e);
        }
    }
    
    /**
     * Delete marker file from upload folder
     * @param originalFileName The original file name (without marker extension)
     */
    public void deleteMarkerFileFromUpload(String originalFileName) {
        try {
            String markerFileName = config.getMarkerFileName(originalFileName);
            Path markerPath = Paths.get(config.getUpload(), markerFileName);
            
            if (Files.exists(markerPath)) {
                // On Windows, sometimes the file is locked by the watch service
                // Try to delete with a small delay
                try {
                    Files.delete(markerPath);
                    logger.debug("Deleted marker file: {}", markerFileName);
                } catch (Exception e) {
                    // If deletion fails, it might be due to file lock
                    // The file will be cleaned up on next restart or manually
                    logger.debug("Could not delete marker file (may be locked): {}", markerFileName);
                }
            }
        } catch (Exception e) {
            // Don't throw exception - marker file cleanup is not critical
            logger.debug("Note: Marker file cleanup skipped for: {}", originalFileName);
        }
    }
    
    /**
     * Create timestamped filename
     * Format: filename_2025-10-30_14-30-00.csv
     */
    public String createTimestampedFileName(String originalFileName) {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        
        // Extract name and extension
        int lastDot = originalFileName.lastIndexOf('.');
        if (lastDot > 0) {
            String nameWithoutExt = originalFileName.substring(0, lastDot);
            String extension = originalFileName.substring(lastDot);
            return String.format("%s_%s%s", nameWithoutExt, timestamp, extension);
        }
        
        return String.format("%s_%s", originalFileName, timestamp);
    }
    
    /**
     * Create error report JSON file
     */
    private void createErrorReport(Path errorFilePath, String originalFileName, 
                                   String errorMessage, String errorDetails, Exception exception) {
        try {
            String errorReportName = errorFilePath.getFileName().toString() + ".error.json";
            Path errorReportPath = errorFilePath.getParent().resolve(errorReportName);
            
            Map<String, Object> errorReport = new HashMap<>();
            errorReport.put("file", errorFilePath.getFileName().toString());
            errorReport.put("original_filename", originalFileName);
            errorReport.put("error_type", "PROCESSING_ERROR");
            errorReport.put("error_message", errorMessage);
            errorReport.put("error_details", errorDetails);
            
            if (exception != null) {
                errorReport.put("exception_class", exception.getClass().getName());
                errorReport.put("stack_trace", getStackTraceAsString(exception));
            }
            
            try {
                errorReport.put("file_size_bytes", Files.size(errorFilePath));
            } catch (IOException e) {
                errorReport.put("file_size_bytes", "unknown");
            }
            
            errorReport.put("failed_at", LocalDateTime.now().toString());
            errorReport.put("retry_recommended", true);
            errorReport.put("retry_instructions", "Fix the issue and re-upload with .done marker");
            
            // Write JSON report
            objectMapper.writeValue(errorReportPath.toFile(), errorReport);
            logger.info("Created error report: {}", errorReportName);
            
        } catch (IOException e) {
            logger.error("Failed to create error report for: {}", errorFilePath.getFileName(), e);
        }
    }
    
    /**
     * Convert exception stack trace to string
     */
    private String getStackTraceAsString(Exception exception) {
        StringBuilder sb = new StringBuilder();
        sb.append(exception.toString()).append("\n");
        for (StackTraceElement element : exception.getStackTrace()) {
            sb.append("\tat ").append(element.toString()).append("\n");
            if (sb.length() > 2000) { // Limit stack trace size
                sb.append("\t... (truncated)");
                break;
            }
        }
        return sb.toString();
    }
    
    /**
     * Get path to upload folder
     */
    public Path getUploadPath() {
        return Paths.get(config.getUpload());
    }
    
    /**
     * Get path to WIP folder
     */
    public Path getWipPath() {
        return Paths.get(config.getWip());
    }
    
    /**
     * Get path to error folder
     */
    public Path getErrorPath() {
        return Paths.get(config.getError());
    }
    
    /**
     * Get path to archive folder
     */
    public Path getArchivePath() {
        return Paths.get(config.getArchive());
    }
}
