package teranet.mapdev.ingest.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Configuration properties for watch folder functionality
 * Maps properties from application.properties with prefix "watch.folder"
 */
@Configuration
@ConfigurationProperties(prefix = "watch.folder")
@Data
public class WatchFolderConfig {
    
    // Enable/disable watch folder
    private boolean enabled = true;
    
    // Folder paths
    private String root = "C:/data/csv-loader";
    private String upload;
    private String wip;
    private String error;
    private String archive;
    
    // Marker file settings
    private boolean useMarkerFiles = true;
    private String markerExtension = ".done";
    
    // File stability checks
    private long stabilityCheckDelay = 2000;  // milliseconds
    private int stabilityCheckRetries = 3;
    
    // Processing settings
    private long pollingInterval = 5000;  // milliseconds
    private int maxConcurrentFiles = 5;
    private List<String> supportedExtensions = List.of(".csv", ".zip");
    
    // Retention policies
    private ArchiveConfig archiveConfig = new ArchiveConfig();
    private ErrorConfig errorConfig = new ErrorConfig();
    
    // Cleanup settings
    private CleanupConfig cleanup = new CleanupConfig();
    
    /**
     * Archive folder configuration
     */
    @Data
    public static class ArchiveConfig {
        private int retentionDays = 90;
    }
    
    /**
     * Error folder configuration
     */
    @Data
    public static class ErrorConfig {
        private int retentionDays = 30;
    }
    
    /**
     * Cleanup configuration
     */
    @Data
    public static class CleanupConfig {
        private boolean enabled = true;
        private String cron = "0 0 2 * * *";  // 2 AM daily
    }
    
    /**
     * Check if a file extension is supported
     */
    public boolean isSupportedExtension(String extension) {
        return supportedExtensions.contains(extension.toLowerCase());
    }
    
    /**
     * Get the CSV file name from marker file name
     * Example: "orders.csv.done" -> "orders.csv"
     */
    public String getCsvFileNameFromMarker(String markerFileName) {
        if (markerFileName.endsWith(markerExtension)) {
            return markerFileName.substring(0, markerFileName.length() - markerExtension.length());
        }
        return markerFileName;
    }
    
    /**
     * Get marker file name for a CSV file
     * Example: "orders.csv" -> "orders.csv.done"
     */
    public String getMarkerFileName(String csvFileName) {
        return csvFileName + markerExtension;
    }
}
