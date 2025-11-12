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
    // API UPLOAD SETTINGS
    // ========================================

    /**
     * API upload configuration (ingest.api.*)
     */
    private Api api = new Api();

    @Data
    public static class Api {
        /** Default file format: "csv" or "tsv" (ingest.api.default-format) */
        private String defaultFormat = "csv";

        /**
         * Supported file extensions for API upload (comma-separated, e.g., "csv,tsv")
         * Leave empty or null to allow files without extensions
         * (ingest.api.supported-extensions)
         */
        private String supportedExtensions = "";

        /**
         * Infer format from file extension (.csv -> csv, .tsv -> tsv)
         * (ingest.api.infer-format-from-extension)
         */
        private boolean inferFormatFromExtension = true;
    }

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

        /**
         * Regex to extract table parts from filename. Example:
         * ^([A-Z]{2})(\d)(?:\d{2})$
         */
        private String regex = "^([A-Z]{2})(\\d)(?:\\d{2})$";

        /** Template to build table name. Example: ${g1}${g2} becomes PM1 */
        private String template = "${g1}${g2}";
    }

    // ========================================
    // STAGING TABLES CONFIGURATION
    // ========================================

    /**
     * List of main tables for staging table creation and validation.
     * These tables are required for file processing.
     * Example: im1,im2,im3,pm1,pm2,pm3,pm4,pm5,pm6,pm7
     * (ingest.main-tables)
     */
    private String mainTables = "im1,im2,im3,pm1,pm2,pm3,pm4,pm5,pm6,pm7";

    /**
     * Get the list of main tables as an array.
     * Splits the comma-separated string and trims whitespace.
     * 
     * @return Array of table names
     */
    public String[] getMainTablesArray() {
        if (mainTables == null || mainTables.trim().isEmpty()) {
            return new String[0];
        }
        return mainTables.split(",\\s*");
    }

    // ========================================
    // HELPER METHODS
    // ========================================

    /**
     * Get the delimiter character for a given format
     * 
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
     * 
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
