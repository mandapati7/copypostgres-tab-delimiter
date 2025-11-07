package teranet.mapdev.ingest.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Simple configuration for TSV and filename routing support
 * 
 * Maps directly to properties in application.properties:
 * - ingest.default-format
 * - ingest.tsv.delimiter
 * - ingest.tsv.has-headers
 * - ingest.csv.has-headers
 * - ingest.filename-routing.enabled
 * - ingest.filename-routing.regex
 * - ingest.filename-routing.template
 * - ingest.target.schema
 * - ingest.infer-format-from-extension
 */
@Configuration
@ConfigurationProperties(prefix = "ingest")
@Data
public class IngestConfig {
    
    // ========================================
    // FORMAT SETTINGS
    // ========================================
    
    /** Default file format: "csv" or "tsv" */
    private String defaultFormat = "csv";
    
    /** Infer format from file extension (.csv -> csv, .tsv -> tsv) */
    private boolean inferFormatFromExtension = true;
    /**
     * List of main tables for staging table creation, mapped from ingest.main-tables in application.properties
     */
    /**
     * List of main tables for staging table creation, mapped from ingest.main-tables in application.properties
     */
    private java.util.List<String> mainTables;
    
    // ========================================
    // TSV SETTINGS (ingest.tsv.*)
    // ========================================
    
    /** Nested config for ingest.tsv.* properties */
    private Tsv tsv = new Tsv();
    
    @Data
    public static class Tsv {
        /** Tab delimiter for TSV files */
        private String delimiter = "\t";
        
        /** TSV files typically have no headers */
        private boolean hasHeaders = false;
    }
    
    // ========================================
    // CSV SETTINGS (ingest.csv.*)
    // ========================================
    
    /** Nested config for ingest.csv.* properties */
    private Csv csv = new Csv();
    
    @Data
    public static class Csv {
        /** CSV files typically have headers */
        private boolean hasHeaders = true;
    }
    
    // ========================================
    // FILENAME ROUTING (ingest.filename-routing.*)
    // ========================================
    
    /** Nested config for ingest.filename-routing.* properties */
    private FilenameRouting filenameRouting = new FilenameRouting();
    
    @Data
    public static class FilenameRouting {
        /** Enable filename-based table routing (PM162 -> PM1) */
        private boolean enabled = false;
        
        /** Regex to extract table parts from filename. Example: ^([A-Z]{2})(\d)(?:\d{2})$ */
        private String regex = "^([A-Z]{2})(\\d)(?:\\d{2})$";
        
        /** Template to build table name. Example: ${g1}${g2} becomes PM1 */
        private String template = "${g1}${g2}";
    }
    
    
    // ========================================
    // HELPER METHODS
    // ========================================
    
    /**
     * Get the delimiter character for a given format
     * @param format "csv" or "tsv"
     * @return "," for CSV, "\t" for TSV
     */
    public String getDelimiterForFormat(String format) {
        if ("tsv".equalsIgnoreCase(format)) {
            return tsv.getDelimiter();
        }
        return ","; // CSV delimiter
    }
    
    /**
     * Check if a format has headers
     * @param format "csv" or "tsv"
     * @return true if format has headers, false otherwise
     */
    public boolean hasHeadersForFormat(String format) {
        if ("tsv".equalsIgnoreCase(format)) {
            return tsv.isHasHeaders();
        }
        return csv.isHasHeaders();
    }
}
