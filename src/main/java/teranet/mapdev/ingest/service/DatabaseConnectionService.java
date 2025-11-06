package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import teranet.mapdev.ingest.config.CsvProcessingConfig;
import teranet.mapdev.ingest.dto.DatabaseConnectionInfoDto;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for managing database connection information and displaying details
 * Provides connection status, schema information, and table management
 * Note: All tables are created in the default schema (no separate staging schema)
 */
@Service
public class DatabaseConnectionService {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnectionService.class);
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private CsvProcessingConfig csvConfig;
    
    @Value("${spring.datasource.url:Unknown}")
    private String databaseUrl;
    
    @Value("${spring.datasource.username:Unknown}")
    private String username;
    
    // Removed staging schema - now using default schema (public) for all tables

    /**
     * Retrieves comprehensive database connection information for dashboard
     * @return DatabaseConnectionInfoDto with all connection details
     */
    public DatabaseConnectionInfoDto getConnectionInfo() {
        logger.info("Retrieving database connection information for dashboard");
        
        try (Connection connection = dataSource.getConnection()) {
            
            // Extract database name from URL
            String databaseName = extractDatabaseName(databaseUrl);
            
            // Get connection pool size if using HikariCP
            Integer poolSize = getConnectionPoolSize();
            
            // Get available schemas
            List<String> schemas = getAvailableSchemas(connection);
            
            // No longer using separate staging schema - all tables in default schema
            Boolean stagingExists = true; // Always true since we use default schema
            
            // Test connection status
            String connectionStatus = testConnectionHealth(connection);
            
            DatabaseConnectionInfoDto connectionInfo = new DatabaseConnectionInfoDto(
                databaseUrl,
                databaseName,
                username,
                poolSize,
                connectionStatus,
                schemas,
                stagingExists
            );
            
            logger.info("Successfully retrieved connection info - Database: {}, Schemas: {}, Staging Exists: {}", 
                       databaseName, schemas.size(), stagingExists);
            
            return connectionInfo;
            
        } catch (Exception e) {
            logger.error("Failed to retrieve database connection information", e);
            
            // Return basic info even if detailed retrieval fails
            return new DatabaseConnectionInfoDto(
                databaseUrl,
                "Connection Failed",
                username,
                0,
                "ERROR: " + e.getMessage(),
                new ArrayList<>(),
                false
            );
        }
    }

    /**
     * No longer used - removed staging schema concept.
     * All tables are created in the default schema (typically 'public' in PostgreSQL)
     * @return true (always succeeds as no schema creation needed)
     * @deprecated Schema creation removed - using default schema only
     */
    @Deprecated
    public boolean ensureStagingSchemaExists() {
        logger.info("Schema creation no longer needed - using default schema for all tables");
        return true; // No-op - using default schema
    }

    /**
     * Returns null as we no longer use a separate staging schema.
     * All tables are in the default schema.
     * @return null (no schema prefix needed)
     * @deprecated Using default schema - no schema prefix needed
     */
    @Deprecated
    public String getStagingSchema() {
        return null; // No schema prefix needed
    }

    /**
     * Checks if a table exists in the default schema
     * @param tableName the table name to check
     * @return true if table exists, false otherwise
     */
    public boolean doesStagingTableExist(String tableName) {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            
            // Check in default schema (null = current schema in PostgreSQL)
            try (ResultSet resultSet = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
                boolean exists = resultSet.next();
                logger.debug("Table '{}' exists in default schema: {}", tableName, exists);
                return exists;
            }
            
        } catch (Exception e) {
            logger.error("Failed to check if table '{}' exists", tableName, e);
            return false;
        }
    }

    /**
     * Lists all tables in the default schema that match the staging table prefix
     * Uses configurable prefix (default: 'staging') to filter tables
     * @return list of staging table names matching the prefix pattern
     */
    public List<String> getStagingTables() {
        List<String> tables = new ArrayList<>();
        
        // Get the staging table prefix from configuration (default: "staging")
        String prefix = csvConfig.getStagingTablePrefix();
        String likePattern = prefix + "_%"; // e.g., "staging_%"
        
        logger.info("Retrieving staging tables with prefix pattern: {}", likePattern);
        
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            
            // Get tables from default schema matching the staging prefix pattern
            // Using LIKE pattern: "staging_%" to filter only staging tables
            try (ResultSet resultSet = metaData.getTables(null, null, likePattern, new String[]{"TABLE"})) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    tables.add(tableName);
                }
            }
            
            logger.info("Found {} staging tables with prefix '{}'", tables.size(), prefix);
            
        } catch (Exception e) {
            logger.error("Failed to retrieve staging tables with prefix: {}", prefix, e);
        }
        
        return tables;
    }
    
    /**
     * Drops all staging tables that match the staging table prefix
     * Uses configurable prefix (default: 'staging') to filter tables
     * WARNING: This operation is irreversible and will permanently delete all staging tables
     * 
     * @return map containing status, count of dropped tables, and list of dropped table names
     */
    public Map<String, Object> dropAllStagingTables() {
        Map<String, Object> result = new HashMap<>();
        List<String> droppedTables = new ArrayList<>();
        int dropCount = 0;
        
        // Get the staging table prefix from configuration (default: "staging")
        String prefix = csvConfig.getStagingTablePrefix();
        
        logger.warn("Starting to drop all staging tables with prefix: {}", prefix);
        
        try (Connection connection = dataSource.getConnection()) {
            // First, get list of all staging tables
            List<String> tablesToDrop = getStagingTables();
            
            if (tablesToDrop.isEmpty()) {
                logger.info("No staging tables found to drop");
                result.put("status", "SUCCESS");
                result.put("message", "No staging tables found with prefix: " + prefix);
                result.put("tables_dropped", 0);
                result.put("dropped_tables", droppedTables);
                return result;
            }
            
            logger.info("Found {} staging tables to drop", tablesToDrop.size());
            
            // Drop each table
            try (Statement statement = connection.createStatement()) {
                for (String tableName : tablesToDrop) {
                    try {
                        String dropSql = "DROP TABLE IF EXISTS " + tableName + " CASCADE";
                        statement.execute(dropSql);
                        droppedTables.add(tableName);
                        dropCount++;
                        logger.info("Dropped table: {}", tableName);
                    } catch (SQLException e) {
                        logger.error("Failed to drop table: {}", tableName, e);
                        // Continue with other tables even if one fails
                    }
                }
            }
            
            logger.info("Successfully dropped {} out of {} staging tables", dropCount, tablesToDrop.size());
            
            result.put("status", "SUCCESS");
            result.put("message", "Successfully dropped " + dropCount + " staging table(s)");
            result.put("tables_dropped", dropCount);
            result.put("dropped_tables", droppedTables);
            
        } catch (Exception e) {
            logger.error("Failed to drop staging tables", e);
            result.put("status", "ERROR");
            result.put("message", "Error: " + e.getMessage());
            result.put("tables_dropped", dropCount);
            result.put("dropped_tables", droppedTables);
        }
        
        return result;
    }

    // Private helper methods
    
    private String extractDatabaseName(String url) {
        try {
            if (url == null || url.isEmpty()) {
                return "Unknown";
            }
            
            // Extract database name from PostgreSQL URL
            // Format: jdbc:postgresql://localhost:5432/database_name
            String[] parts = url.split("/");
            if (parts.length > 0) {
                String lastPart = parts[parts.length - 1];
                // Remove query parameters if present
                return lastPart.split("\\?")[0];
            }
            
            return "Unknown";
            
        } catch (Exception e) {
            logger.warn("Could not extract database name from URL: {}", url, e);
            return "Unknown";
        }
    }
    
    private Integer getConnectionPoolSize() {
        try {
            if (dataSource instanceof HikariDataSource) {
                HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
                return hikariDataSource.getMaximumPoolSize();
            }
            return null;
            
        } catch (Exception e) {
            logger.warn("Could not determine connection pool size", e);
            return null;
        }
    }
    
    private List<String> getAvailableSchemas(Connection connection) {
        List<String> schemas = new ArrayList<>();
        
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            
            try (ResultSet resultSet = metaData.getSchemas()) {
                while (resultSet.next()) {
                    String schemaName = resultSet.getString("TABLE_SCHEM");
                    schemas.add(schemaName);
                }
            }
            
        } catch (Exception e) {
            logger.warn("Could not retrieve available schemas", e);
        }
        
        return schemas;
    }
    
    private String testConnectionHealth(Connection connection) {
        try {
            if (connection.isValid(5)) { // 5 second timeout
                return "HEALTHY";
            } else {
                return "UNHEALTHY";
            }
            
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }
}