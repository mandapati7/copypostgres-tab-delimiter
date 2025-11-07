package teranet.mapdev.ingest.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * Database initialization component that runs at application startup.
 * Validates database connection, checks for required tables, and verifies permissions.
 */
@Component
public class DatabaseInitializer {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseInitializer.class);
    
    // ANSI Color codes for console output
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_BOLD = "\u001B[1m";
    
    @Autowired
    private DataSource dataSource;

    @Autowired
    private IngestConfig ingestConfig;

    @Autowired
    private CsvProcessingConfig csvProcessingConfig;
    
    /**
     * Runs after the application is fully started.
     * Validates database setup and creates required tables if missing.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeDatabase() {
        logger.info(ANSI_CYAN + "========================================" + ANSI_RESET);
        logger.info(ANSI_CYAN + ANSI_BOLD + "DATABASE INITIALIZATION - Starting validation" + ANSI_RESET);
        logger.info(ANSI_CYAN + "========================================" + ANSI_RESET);
        
        try {
            // Step 1: Test database connection
            if (!testDatabaseConnection()) {
                logger.error(ANSI_RED + ANSI_BOLD + "[CRITICAL] Cannot establish database connection!" + ANSI_RESET);
                logger.error(ANSI_RED + "   Please check your database configuration in application.properties" + ANSI_RESET);
                logger.error(ANSI_RED + "   - spring.datasource.url" + ANSI_RESET);
                logger.error(ANSI_RED + "   - spring.datasource.username" + ANSI_RESET);
                throw new RuntimeException("Database connection failed - Application cannot start");
            }
            logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] Database connection: OK" + ANSI_RESET);
            
            // Step 2: Check if ingestion_manifest table exists
            if (!checkTableExists("ingestion_manifest")) {
                logger.warn(ANSI_YELLOW + ANSI_BOLD + "[WARNING] Table 'ingestion_manifest' does not exist" + ANSI_RESET);
                logger.info(ANSI_YELLOW + "   Attempting to create 'ingestion_manifest' table..." + ANSI_RESET);
                
                // Step 3: Check CREATE TABLE permission
                if (!checkCreateTablePermission()) {
                    logger.error(ANSI_RED + ANSI_BOLD + "[CRITICAL] No permission to create tables!" + ANSI_RESET);
                    logger.error(ANSI_RED + "   Current database user does not have CREATE TABLE privilege" + ANSI_RESET);
                    logger.error(ANSI_RED + "   Please grant CREATE TABLE permission or create the table manually" + ANSI_RESET);
                    throw new RuntimeException("Cannot create required tables - Insufficient permissions");
                }
                logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] CREATE TABLE permission: OK" + ANSI_RESET);
                
                // Step 4: Create ingestion_manifest table
                createIngestionManifestTable();
                logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] Table 'ingestion_manifest' created successfully" + ANSI_RESET);
            } else {
                logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] Table 'ingestion_manifest' exists: OK" + ANSI_RESET);
            }
            
            // Step 5: Verify table structure
            if (!verifyTableStructure("ingestion_manifest")) {
                logger.warn(ANSI_YELLOW + "[WARNING] Table 'ingestion_manifest' may have incorrect structure" + ANSI_RESET);
                logger.warn(ANSI_YELLOW + "   Please verify the table schema matches the entity definition" + ANSI_RESET);
            } else {
                logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] Table structure validation: OK" + ANSI_RESET);
            }

             // Step 6: Create staging tables for main tables
            createStagingTablesFromMainTables();
            
            logger.info(ANSI_CYAN + "========================================" + ANSI_RESET);
            logger.info(ANSI_GREEN + ANSI_BOLD + "DATABASE INITIALIZATION - COMPLETED SUCCESSFULLY" + ANSI_RESET);
            logger.info(ANSI_CYAN + "Application is ready to process CSV files" + ANSI_RESET);
            logger.info(ANSI_CYAN + "========================================" + ANSI_RESET);
            
        } catch (Exception e) {
            logger.error(ANSI_RED + "========================================" + ANSI_RESET);
            logger.error(ANSI_RED + ANSI_BOLD + "DATABASE INITIALIZATION - FAILED" + ANSI_RESET);
            logger.error(ANSI_RED + "========================================" + ANSI_RESET);
            logger.error(ANSI_RED + "Error during database initialization" + ANSI_RESET, e);
            
            // Don't let the application start if database is not ready
            throw new RuntimeException("Database initialization failed - Application cannot start safely", e);
        }
    }
    
    /**
     * Test if database connection can be established
     */
    private boolean testDatabaseConnection() {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            logger.info(ANSI_BLUE + "   Database: {} {}" + ANSI_RESET, metaData.getDatabaseProductName(), metaData.getDatabaseProductVersion());
            logger.info(ANSI_BLUE + "   JDBC URL: {}" + ANSI_RESET, metaData.getURL());
            logger.info(ANSI_BLUE + "   User: {}" + ANSI_RESET, metaData.getUserName());
            return true;
        } catch (Exception e) {
            logger.error(ANSI_RED + "   Connection error: {}" + ANSI_RESET, e.getMessage());
            return false;
        }
    }
    
    /**
     * Check if a table exists in the database
     */
    private boolean checkTableExists(String tableName) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            
            // Use a direct SQL query to check if table exists in public schema
            String checkSQL = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = ?
                )
                """;
            
            try (var preparedStatement = connection.prepareStatement(checkSQL)) {
                preparedStatement.setString(1, tableName.toLowerCase());
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getBoolean(1);
                    }
                }
            }
            return false;
            
        } catch (Exception e) {
            logger.error("   Error checking table existence: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Check if current user has CREATE TABLE permission
     */
    private boolean checkCreateTablePermission() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            
            // Try to create and drop a temporary test table
            String testTableName = "test_permissions_" + System.currentTimeMillis();
            
            try {
                statement.execute("CREATE TABLE " + testTableName + " (id INTEGER)");
                statement.execute("DROP TABLE " + testTableName);
                return true;
            } catch (Exception e) {
                logger.error("   Permission check failed: {}", e.getMessage());
                return false;
            }
            
        } catch (Exception e) {
            logger.error("   Error checking permissions: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Create the ingestion_manifest table with full schema
     */
    private void createIngestionManifestTable() {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS ingestion_manifest (
                id BIGSERIAL PRIMARY KEY,
                batch_id UUID NOT NULL UNIQUE,
                parent_batch_id UUID,
                file_name VARCHAR(255) NOT NULL,
                file_path VARCHAR(500),
                file_size_bytes BIGINT NOT NULL,
                file_checksum VARCHAR(64) NOT NULL,
                content_type VARCHAR(100),
                table_name VARCHAR(128),
                status VARCHAR(20) NOT NULL,
                total_records BIGINT,
                processed_records BIGINT,
                failed_records BIGINT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                processing_duration_ms BIGINT,
                error_message TEXT,
                error_details TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(100)
            )
            """;

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(createTableSQL);
            logger.info(ANSI_GREEN + "   Created table with columns: batch_id, file_name, status, etc." + ANSI_RESET);

            // Create indexes for better performance (use IF NOT EXISTS)
            statement.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_manifest_batch_id ON ingestion_manifest(batch_id)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_manifest_file_checksum ON ingestion_manifest(file_checksum)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_manifest_status ON ingestion_manifest(status)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_manifest_parent_batch ON ingestion_manifest(parent_batch_id)");

            logger.info(ANSI_GREEN + "   Created indexes for optimal query performance" + ANSI_RESET);

        } catch (Exception e) {
            logger.error("   Failed to create ingestion_manifest table", e);
            throw new RuntimeException("Cannot create ingestion_manifest table", e);
        }
    }
    
    /**
     * Verify the table has minimum required columns
     */
    private boolean verifyTableStructure(String tableName) {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            
            // Check for required columns
            String[] requiredColumns = {"id", "batch_id", "file_name", "status", "file_checksum"};
            int foundColumns = 0;
            
            try (ResultSet resultSet = metaData.getColumns(null, null, tableName.toLowerCase(), null)) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    for (String required : requiredColumns) {
                        if (required.equalsIgnoreCase(columnName)) {
                            foundColumns++;
                            break;
                        }
                    }
                }
            }
            
            if (foundColumns < requiredColumns.length) {
                logger.warn("   Found {}/{} required columns", foundColumns, requiredColumns.length);
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            logger.error("   Error verifying table structure: {}", e.getMessage());
            return false;
        }
    }

    
    /**
     * Create staging tables for each main table listed in ingest.main-tables property.
     * Staging tables are named with the prefix 'staging_'.
     */
    private void createStagingTablesFromMainTables() {

        List<String> mainTables = ingestConfig.getMainTables();

        try (Connection connection = dataSource.getConnection()) {            
            for (String mainTable : mainTables) {
                String trimmed = mainTable.trim();
                if (trimmed.isEmpty()) continue;
                StringBuilder stagingTable = new StringBuilder(csvProcessingConfig.getStagingTablePrefix()).append("_").append(trimmed);
                if (!checkTableExists(stagingTable.toString())) {
                    logger.info(ANSI_YELLOW + "[INFO] Creating staging table: {} from {}" + ANSI_RESET, stagingTable, trimmed);
                    String ddl = String.format("CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL)", stagingTable, trimmed);
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute(ddl);
                        logger.info(ANSI_GREEN + "   Created staging table: {}" + ANSI_RESET, stagingTable);
                    } catch (Exception e) {
                        logger.error(ANSI_RED + "   Failed to create staging table {}: {}" + ANSI_RESET, stagingTable, e.getMessage());
                    }
                } else {
                    logger.info(ANSI_GREEN + "[SUCCESS] Staging table exists: {}" + ANSI_RESET, stagingTable);
                }
            }
        } catch (Exception e) {
            logger.error(ANSI_RED + "Error creating staging tables: {}" + ANSI_RESET, e.getMessage());
        }
    }
}
