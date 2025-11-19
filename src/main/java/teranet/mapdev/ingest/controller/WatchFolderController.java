package teranet.mapdev.ingest.controller;

import teranet.mapdev.ingest.config.WatchFolderConfig;
import teranet.mapdev.ingest.dto.WatchFolderStatusDto;
import teranet.mapdev.ingest.service.WatchFolderManager;
import teranet.mapdev.ingest.service.WatchFolderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Controller for watch folder management and monitoring
 * Provides REST API for monitoring watch folder status and managing files
 */
@RestController
@RequestMapping("/api/v1/ingest/watch-folder")
@CrossOrigin(origins = "*", maxAge = 3600)
@Tag(name = "Watch Folder Operations", description = "Automated file ingestion monitoring")
public class WatchFolderController {
    
    private static final Logger logger = LoggerFactory.getLogger(WatchFolderController.class);
    
    @Autowired
    private WatchFolderConfig config;
    
    @Autowired
    private WatchFolderService watchFolderService;
    
    @Autowired
    private WatchFolderManager folderManager;
    
    /**
     * Get watch folder status with file counts
     */
    @GetMapping("/status")
    @Operation(
        summary = "Get watch folder status",
        description = "Returns current status and file counts for all watch folders"
    )
    @ApiResponse(responseCode = "200", description = "Status retrieved successfully")
    public ResponseEntity<WatchFolderStatusDto> getStatus() {
        logger.debug("Retrieving watch folder status");
        
        try {
            WatchFolderStatusDto status = new WatchFolderStatusDto();
            
            status.setUploadCount(countFilesInFolder(config.getUpload()));
            status.setWipCount(countFilesInFolder(config.getWip()));
            status.setErrorCount(countFilesInFolder(config.getError()));
            status.setArchiveCount(countFilesInFolder(config.getArchive()));
            
            status.setWatchEnabled(config.isEnabled());
            status.setWatchRunning(watchFolderService.isRunning());
            status.setLastCheck(LocalDateTime.now());
            status.setTotalProcessedToday(0); // TODO: Implement daily counter
            
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            logger.error("Failed to get watch folder status", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
    
    /**
     * List files in all folders
     */
    @GetMapping("/files")
    @Operation(
        summary = "List all files",
        description = "List files in all watch folders (upload, wip, error, archive)"
    )
    @ApiResponse(responseCode = "200", description = "Files listed successfully")
    public ResponseEntity<Map<String, Object>> listAllFiles() {
        logger.debug("Listing all files in watch folders");
        
        try {
            Map<String, Object> response = new HashMap<>();
            
            response.put("upload", listFilesInFolder(config.getUpload()));
            response.put("wip", listFilesInFolder(config.getWip()));
            response.put("error", listFilesInFolder(config.getError()));
            response.put("archive", listFilesInFolder(config.getArchive()));
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to list files", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
    
    /**
     * List files in a specific folder
     */
    @GetMapping("/files/{folder}")
    @Operation(
        summary = "List files in specific folder",
        description = "List files in a specific folder (upload, wip, error, or archive)"
    )
    @ApiResponse(responseCode = "200", description = "Files listed successfully")
    @ApiResponse(responseCode = "400", description = "Invalid folder name")
    public ResponseEntity<Map<String, Object>> listFilesInSpecificFolder(@PathVariable String folder) {
        logger.debug("Listing files in folder: {}", folder);
        
        try {
            String folderPath;
            switch (folder.toLowerCase()) {
                case "upload": folderPath = config.getUpload(); break;
                case "wip": folderPath = config.getWip(); break;
                case "error": folderPath = config.getError(); break;
                case "archive": folderPath = config.getArchive(); break;
                default:
                    return ResponseEntity.badRequest().body(
                        Map.of("error", "Invalid folder name. Use: upload, wip, error, or archive")
                    );
            }
            
            List<String> files = listFilesInFolder(folderPath);
            
            Map<String, Object> response = new HashMap<>();
            response.put("folder", folder);
            response.put("count", files.size());
            response.put("files", files);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to list files in folder: {}", folder, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
    
    /**
     * List all error reports
     */
    @GetMapping("/errors")
    @Operation(
        summary = "List error reports",
        description = "List all error report JSON files with details"
    )
    @ApiResponse(responseCode = "200", description = "Error reports retrieved successfully")
    public ResponseEntity<Map<String, Object>> listErrors() {
        logger.debug("Listing error reports");
        
        try {
            Path errorPath = Paths.get(config.getError());
            
            List<Map<String, Object>> errorReports = new ArrayList<>();
            
            try (Stream<Path> files = Files.list(errorPath)) {
                files.filter(p -> p.toString().endsWith(".error.json"))
                     .forEach(errorFile -> {
                         try {
                             Map<String, Object> errorInfo = new HashMap<>();
                             errorInfo.put("file", errorFile.getFileName().toString());
                             errorInfo.put("size", Files.size(errorFile));
                             errorInfo.put("created", Files.getLastModifiedTime(errorFile).toString());
                             errorReports.add(errorInfo);
                         } catch (IOException e) {
                             logger.warn("Error reading error report: {}", errorFile, e);
                         }
                     });
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("total_errors", errorReports.size());
            response.put("errors", errorReports);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to list error reports", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
    
    /**
     * Retry a failed file from error folder
     */
    @PostMapping("/retry/{filename}")
    @Operation(
        summary = "Retry failed file",
        description = "Move a file from error folder back to upload folder for reprocessing"
    )
    @ApiResponse(responseCode = "200", description = "File queued for retry")
    @ApiResponse(responseCode = "404", description = "File not found in error folder")
    public ResponseEntity<Map<String, Object>> retryFailedFile(@PathVariable String filename) {
        logger.info("Retry requested for file: {}", filename);
        
        try {
            Path errorPath = Paths.get(config.getError(), filename);
            
            if (!Files.exists(errorPath)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(
                    Map.of("error", "File not found in error folder: " + filename)
                );
            }
            
            // Move file back to upload folder
            Path uploadPath = Paths.get(config.getUpload(), filename);
            Files.move(errorPath, uploadPath, StandardCopyOption.REPLACE_EXISTING);
            
            // Create marker file (delete if exists first to avoid FileAlreadyExistsException)
            if (config.isUseMarkerFiles()) {
                String markerName = config.getMarkerFileName(filename);
                Path markerPath = Paths.get(config.getUpload(), markerName);
                
                // Delete existing marker file if present
                Files.deleteIfExists(markerPath);
                
                // Create fresh marker file
                Files.createFile(markerPath);
            }
            
            // Delete error report if exists
            String errorReportName = filename + ".error.json";
            Path errorReportPath = Paths.get(config.getError(), errorReportName);
            if (Files.exists(errorReportPath)) {
                Files.delete(errorReportPath);
            }
            
            logger.info("✅ File queued for retry: {}", filename);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("message", "File moved to upload folder and will be processed automatically");
            response.put("filename", filename);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to retry file: {}", filename, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                Map.of("error", "Failed to retry file: " + e.getMessage())
            );
        }
    }
    
    // Helper methods
    
    private int countFilesInFolder(String folderPath) {
        try {
            Path path = Paths.get(folderPath);
            if (!Files.exists(path)) {
                return 0;
            }
            try (Stream<Path> files = Files.list(path)) {
                return (int) files.filter(Files::isRegularFile).count();
            }
        } catch (IOException e) {
            logger.warn("Error counting files in: {}", folderPath, e);
            return 0;
        }
    }
    
    private List<String> listFilesInFolder(String folderPath) {
        try {
            Path path = Paths.get(folderPath);
            if (!Files.exists(path)) {
                return Collections.emptyList();
            }
            try (Stream<Path> files = Files.list(path)) {
                return files.filter(Files::isRegularFile)
                           .map(p -> p.getFileName().toString())
                           .sorted()
                           .collect(Collectors.toList());
            }
        } catch (IOException e) {
            logger.warn("Error listing files in: {}", folderPath, e);
            return Collections.emptyList();
        }
    }
}
