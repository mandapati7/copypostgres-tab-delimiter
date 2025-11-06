package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import teranet.mapdev.ingest.config.CsvProcessingConfig;

import java.util.UUID;

/**
 * Service responsible for generating and sanitizing database table names.
 * 
 * Features:
 * - Generate unique staging table names with batch ID suffix
 * - Sanitize table names for SQL safety
 * - Handle PostgreSQL 63-character identifier limit
 * - Configurable table prefix via CsvProcessingConfig
 * 
 * This service is stateless and can be safely used concurrently.
 */
@Service
public class TableNamingService {
    
    private static final Logger logger = LoggerFactory.getLogger(TableNamingService.class);
    
    // PostgreSQL identifier limit
    private static final int POSTGRESQL_MAX_IDENTIFIER_LENGTH = 63;
    
    // Batch ID suffix length (first 8 characters of UUID)
    private static final int BATCH_ID_SUFFIX_LENGTH = 8;
    
    @Autowired
    private CsvProcessingConfig csvConfig;
    
    /**
     * Generate a safe table name from the CSV file name with prefix and batch_id suffix.
     * Always appends 8 characters from batch_id to ensure unique tables for each upload.
     * 
     * PostgreSQL identifier limit: 63 characters
     * Format: {prefix}_{filename}_{batch_id_8_chars}
     * 
     * Examples:
     *   - orders.csv + batch a1b2c3d4 → staging_orders_a1b2c3d4
     *   - products.csv + batch e5f6789a → staging_products_e5f6789a
     *   - customer-data.csv + batch 1234abcd → staging_customer_data_1234abcd
     * 
     * @param fileName the CSV file name (can include .csv, .gz, .zip extensions)
     * @param batchId the batch UUID (first 8 chars will be used)
     * @return safe table name with batch_id suffix
     */
    public String generateTableNameFromFile(String fileName, UUID batchId) {
        // Get the configurable staging table prefix (default: "staging")
        String prefix = csvConfig.getStagingTablePrefix();
        
        logger.debug("Generating table name for file: {} with batch: {} using prefix: {}", 
                    fileName, batchId, prefix);
        
        // Handle null filename with timestamp fallback
        if (fileName == null) {
            String fallbackName = prefix + "_csv_data_" + System.currentTimeMillis();
            logger.warn("Filename is null, using fallback: {}", fallbackName);
            return fallbackName;
        }
        
        // Extract first 8 characters from batch_id for suffix
        String batchIdSuffix = batchId.toString().substring(0, BATCH_ID_SUFFIX_LENGTH);
        
        // Remove file extension and sanitize
        String baseName = removeFileExtension(fileName);
        String sanitizedName = sanitizeTableName(baseName);
        
        // Ensure sanitized name is not empty
        if (sanitizedName.isEmpty()) {
            sanitizedName = "csv_data";
            logger.warn("Sanitized name was empty, using default: {}", sanitizedName);
        }
        
        // Calculate maximum length for base name to stay within PostgreSQL limit
        // Format: {prefix}_{basename}_{batch_id}
        // Reserve space for: prefix + "_" + "_" + batch_id (8) = prefix.length() + 10
        final int reservedLength = prefix.length() + 10;
        final int maxBaseNameLength = POSTGRESQL_MAX_IDENTIFIER_LENGTH - reservedLength;
        
        // Truncate if necessary
        if (sanitizedName.length() > maxBaseNameLength) {
            String originalName = sanitizedName;
            sanitizedName = sanitizedName.substring(0, maxBaseNameLength);
            logger.debug("Truncated table base name from {} to {} characters: {} → {}", 
                        originalName.length(), maxBaseNameLength, originalName, sanitizedName);
        }
        
        // Build final table name: {prefix}_{filename}_{batch_id_8_chars}
        String tableName = prefix + "_" + sanitizedName + "_" + batchIdSuffix;
        
        logger.debug("Generated table name: {}", tableName);
        
        return tableName;
    }
    
    /**
     * Sanitize table name to be safe for SQL.
     * 
     * Transformation steps:
     * 1. Convert to lowercase
     * 2. Replace all non-alphanumeric characters (except underscore) with underscore
     * 3. Replace multiple consecutive underscores with single underscore
     * 4. Remove leading/trailing underscores
     * 
     * Examples:
     *   "Customer-Orders" → "customer_orders"
     *   "PRODUCT__DATA" → "product_data"
     *   "_test_table_" → "test_table"
     *   "123-ABC-XYZ" → "123_abc_xyz"
     *   "sales@2024.csv" → "sales_2024_csv"
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
     *   "orders.csv" → "orders"
     *   "data.csv.gz" → "data"
     *   "archive.zip" → "archive"
     *   "DATA.CSV" → "DATA"
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
