package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Service responsible for generating and sanitizing database table names.
 * 
 * Features:
 * - Generate table names from filename
 * - Sanitize table names for SQL safety
 * - Handle PostgreSQL 63-character identifier limit
 * 
 * This service is stateless and can be safely used concurrently.
 */
@Service
public class TableNamingService {

    private static final Logger logger = LoggerFactory.getLogger(TableNamingService.class);

    // PostgreSQL identifier limit
    private static final int POSTGRESQL_MAX_IDENTIFIER_LENGTH = 63;

    /**
     * Generate a safe table name from the CSV file name.
     * 
     * PostgreSQL identifier limit: 63 characters
     * Format: {filename}
     * 
     * Examples:
     * - IM162 → IM1
     * - PM162 → PM2
     * - PM762 → PM7
     * 
     * @param fileName the CSV file name (can include .csv, .gz, .zip extensions)
     * @param batchId  the batch UUID (not used, kept for backward compatibility)
     * @return safe table name
     */
    public String generateTableNameFromFile(String fileName, UUID batchId) {

        logger.debug("Generating table name for file: {}", fileName);

        // Filename must not be null - this indicates a serious error in the calling code
        if (fileName == null) {
            logger.error("Filename is null - cannot generate table name");
            throw new IllegalArgumentException("Filename cannot be null. Please provide a valid filename.");
        }

        // Remove file extension and sanitize
        String baseName = removeFileExtension(fileName);
        String sanitizedName = sanitizeTableName(baseName);

        // Ensure sanitized name is not empty
        if (sanitizedName.isEmpty()) {
            sanitizedName = "csv_data";
            logger.warn("Sanitized name was empty, using default: {}", sanitizedName);
        }

        // Truncate if necessary to stay within PostgreSQL limit
        if (sanitizedName.length() > POSTGRESQL_MAX_IDENTIFIER_LENGTH) {
            String originalName = sanitizedName;
            sanitizedName = sanitizedName.substring(0, POSTGRESQL_MAX_IDENTIFIER_LENGTH);
            logger.debug("Truncated table name from {} to {} characters: {} → {}",
                    originalName.length(), POSTGRESQL_MAX_IDENTIFIER_LENGTH, originalName, sanitizedName);
        }

        logger.debug("Generated table name: {}", sanitizedName);

        return sanitizedName;
    }

    /**
     * Sanitize table name to be safe for SQL.
     * 
     * Transformation steps:
     * 1. Convert to lowercase
     * 2. Replace all non-alphanumeric characters (except underscore) with
     * underscore
     * 3. Replace multiple consecutive underscores with single underscore
     * 4. Remove leading/trailing underscores
     * 
     * Examples:
     * "Customer-Orders" → "customer_orders"
     * "PRODUCT__DATA" → "product_data"
     * "_test_table_" → "test_table"
     * "123-ABC-XYZ" → "123_abc_xyz"
     * "sales@2024.csv" → "sales_2024_csv"
     * 
     * @param name the raw table name from file name
     * @return sanitized table name safe for PostgreSQL
     */
    public String sanitizeTableName(String name) {
        if (name == null || name.isEmpty()) {
            return "";
        }

        return name.toLowerCase()
                .replaceAll("[^a-zA-Z0-9_]", "_")
                .replaceAll("_{2,}", "_")
                .replaceAll("^_|_$", "");
    }

    /**
     * Remove common file extensions from filename.
     * Supports: .csv, .gz, .zip (case-insensitive)
     * Handles multiple extensions (e.g., .csv.gz)
     * 
     * Examples:
     * "orders.csv" → "orders"
     * "data.csv.gz" → "data"
     * "archive.zip" → "archive"
     * "DATA.CSV" → "DATA"
     * 
     * @param fileName the file name with extension
     * @return file name without extension
     */
    private String removeFileExtension(String fileName) {
        // Remove all common extensions iteratively (case-insensitive)
        String result = fileName;
        String previous;
        do {
            previous = result;
            result = result.replaceAll("(?i)\\.(csv|gz|zip)$", "");
        } while (!result.equals(previous));
        return result;
    }
}
