package teranet.mapdev.ingest.config;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Database initialization component that runs at application startup.
 * Validates database connection, checks for required tables, and verifies
 * permissions.
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

    @Value("${spring.datasource.schema}")
    private String schema;

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
                logger.error(ANSI_RED + "   Please check your database configuration in application.properties"
                        + ANSI_RESET);
                logger.error(ANSI_RED + "   - spring.datasource.url" + ANSI_RESET);
                logger.error(ANSI_RED + "   - spring.datasource.username" + ANSI_RESET);
                throw new RuntimeException("Database connection failed - Application cannot start");
            }
            logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] Database connection: OK" + ANSI_RESET);

            // Step 2: Check if ingestion_manifest table exists
            if (!checkTableExists("ingestion_manifest")) {
                logger.warn(
                        ANSI_YELLOW + ANSI_BOLD + "[WARNING] Table 'ingestion_manifest' does not exist" + ANSI_RESET);
                logger.info(ANSI_YELLOW + "   Attempting to create 'ingestion_manifest' table..." + ANSI_RESET);

                // Step 3: Check CREATE TABLE permission
                if (!checkCreateTablePermission()) {
                    logger.error(ANSI_RED + ANSI_BOLD + "[CRITICAL] No permission to create tables!" + ANSI_RESET);
                    logger.error(
                            ANSI_RED + "   Current database user does not have CREATE TABLE privilege" + ANSI_RESET);
                    logger.error(ANSI_RED + "   Please grant CREATE TABLE permission or create the table manually"
                            + ANSI_RESET);
                    throw new RuntimeException("Cannot create required tables - Insufficient permissions");
                }
                logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] CREATE TABLE permission: OK" + ANSI_RESET);

                // Step 4: Create ingestion_manifest table
                createIngestionManifestTable();
                logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] Table 'ingestion_manifest' created successfully"
                        + ANSI_RESET);
            } else {
                logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] Table 'ingestion_manifest' exists: OK" + ANSI_RESET);
            }

            // Step 5: Verify table structure
            if (!verifyTableStructure("ingestion_manifest")) {
                logger.warn(
                        ANSI_YELLOW + "[WARNING] Table 'ingestion_manifest' may have incorrect structure" + ANSI_RESET);
                logger.warn(
                        ANSI_YELLOW + "   Please verify the table schema matches the entity definition" + ANSI_RESET);
            } else {
                logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] Table structure validation: OK" + ANSI_RESET);
            }

            // Step 6: Validate and ensure staging tables have required tracking columns
            logger.info(ANSI_CYAN + "========================================" + ANSI_RESET);
            logger.info(ANSI_CYAN + ANSI_BOLD + "STAGING TABLES VALIDATION" + ANSI_RESET);
            logger.info(ANSI_CYAN + "========================================" + ANSI_RESET);

            validateAndEnsureStagingTables();

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
            logger.info(ANSI_BLUE + "   Database: {} {}" + ANSI_RESET, metaData.getDatabaseProductName(),
                    metaData.getDatabaseProductVersion());
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
            statement.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ingestion_manifest_batch_id ON ingestion_manifest(batch_id)");
            statement.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ingestion_manifest_file_checksum ON ingestion_manifest(file_checksum)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_manifest_status ON ingestion_manifest(status)");
            statement.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ingestion_manifest_parent_batch ON ingestion_manifest(parent_batch_id)");

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
            String[] requiredColumns = { "id", "batch_id", "file_name", "status", "file_checksum" };
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

    /*
     * REMOVED: createStagingTablesFromMainTables()
     * 
     * This method is no longer needed because:
     * 1. Tables are now created directly from filename routing (e.g., PM162 -> pm1)
     * 2. No separate staging tables are required
     * 3. ingest.main-tables property has been removed
     * 4. csv.processing.staging-table-prefix property has been removed
     * 
     * The application now treats the target tables as the direct destination for
     * data loading.
     */

    /**
     * Validate and ensure all required staging tables exist with tracking columns.
     * These tables are essential for file processing - without them, the
     * application cannot function.
     */
    private void validateAndEnsureStagingTables() {
        // Get list of required staging tables from configuration
        String[] requiredTables = ingestConfig.getMainTablesArray();

        boolean allTablesValid = true;
        int tablesChecked = 0;
        int tablesUpdated = 0;
        int tablesMissing = 0;

        try (Connection connection = dataSource.getConnection()) {

            for (String tableName : requiredTables) {
                String fullTableName = schema + "." + tableName;
                tablesChecked++;

                logger.info(ANSI_BLUE + "   Checking table: {}" + ANSI_RESET, fullTableName);

                // Check if table exists
                if (!checkTableExistsInSchema(connection, schema, tableName)) {
                    logger.error(ANSI_RED + ANSI_BOLD + "   [CRITICAL] Table '{}' does not exist!" + ANSI_RESET,
                            fullTableName);
                    logger.error(ANSI_RED + "   This table is REQUIRED for processing {} files" + ANSI_RESET,
                            tableName.toUpperCase());
                    tablesMissing++;
                    allTablesValid = false;
                    continue;
                }

                // Check if table has required tracking columns
                boolean hasTrackingColumns = checkTrackingColumns(connection, schema, tableName);

                if (!hasTrackingColumns) {
                    logger.warn(
                            ANSI_YELLOW + "   Table '{}' is missing tracking columns (batch_id, row_number, loaded_at)"
                                    + ANSI_RESET,
                            fullTableName);
                    logger.info(ANSI_YELLOW + "   Attempting to add tracking columns..." + ANSI_RESET);

                    try {
                        addTrackingColumns(connection, fullTableName);
                        logger.info(ANSI_GREEN + ANSI_BOLD + "   [SUCCESS] Added tracking columns to {}"
                                + ANSI_RESET, fullTableName);
                        tablesUpdated++;
                    } catch (Exception e) {
                        logger.error(ANSI_RED + ANSI_BOLD + "   [CRITICAL] Failed to add tracking columns to {}"
                                + ANSI_RESET, fullTableName);
                        logger.error(ANSI_RED + "   Error: {}" + ANSI_RESET, e.getMessage());
                        allTablesValid = false;
                    }
                } else {
                    logger.info(ANSI_GREEN + "   Table '{}' has all required tracking columns" + ANSI_RESET,
                            fullTableName);
                }
            }

            // Summary
            logger.info(ANSI_CYAN + "========================================" + ANSI_RESET);
            logger.info(ANSI_CYAN + "Staging Tables Summary:" + ANSI_RESET);
            logger.info(ANSI_CYAN + "  - Tables checked: {}" + ANSI_RESET, tablesChecked);
            logger.info(ANSI_CYAN + "  - Tables updated: {}" + ANSI_RESET, tablesUpdated);
            if (tablesMissing > 0) {
                logger.error(ANSI_RED + "  - Tables MISSING: {}" + ANSI_RESET, tablesMissing);
            }
            logger.info(ANSI_CYAN + "========================================" + ANSI_RESET);

            if (!allTablesValid) {
                logger.error(
                        ANSI_RED + ANSI_BOLD + "[CRITICAL] Some staging tables are missing or invalid!" + ANSI_RESET);
                logger.error(ANSI_RED + "Without these tables, the application CANNOT process files." + ANSI_RESET);
                logger.error(ANSI_RED + "Please create the missing tables using the migration scripts:" + ANSI_RESET);
                logger.error(ANSI_RED + "  - V2__Create_Title_D_App_Tables.sql" + ANSI_RESET);
                logger.error(ANSI_RED + "  - V6__Add_Batch_Id_To_Staging_Tables.sql" + ANSI_RESET);
                throw new RuntimeException("Required staging tables are missing - Application cannot start");
            }

            logger.info(ANSI_GREEN + ANSI_BOLD + "[SUCCESS] All staging tables validated: OK" + ANSI_RESET);

        } catch (RuntimeException e) {
            throw e; // Re-throw RuntimeException to stop application startup
        } catch (Exception e) {
            logger.error(ANSI_RED + "Error validating staging tables: {}" + ANSI_RESET, e.getMessage(), e);
            throw new RuntimeException("Staging table validation failed - Application cannot start", e);
        }
    }

    /**
     * Check if a table exists in a specific schema.
     */
    private boolean checkTableExistsInSchema(Connection connection, String schema, String tableName) throws Exception {
        String checkSQL = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = ? AND table_name = ?
                )
                """;

        try (var preparedStatement = connection.prepareStatement(checkSQL)) {
            preparedStatement.setString(1, schema.toLowerCase());
            preparedStatement.setString(2, tableName.toLowerCase());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getBoolean(1);
                }
            }
        }
        return false;
    }

    /**
     * Check if a table has all required tracking columns (batch_id, row_number,
     * loaded_at).
     */
    private boolean checkTrackingColumns(Connection connection, String schema, String tableName) throws Exception {
        String checkSQL = """
                SELECT
                    COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = ?
                AND table_name = ?
                AND column_name IN ('batch_id', 'row_number', 'loaded_at')
                """;

        try (var preparedStatement = connection.prepareStatement(checkSQL)) {
            preparedStatement.setString(1, schema.toLowerCase());
            preparedStatement.setString(2, tableName.toLowerCase());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    int columnCount = resultSet.getInt(1);
                    return columnCount == 3; // All 3 tracking columns must exist
                }
            }
        }
        return false;
    }

    /**
     * Add tracking columns to table if they don't exist.
     * Uses IF NOT EXISTS to safely add columns to existing tables.
     */
    private void addTrackingColumns(Connection connection, String tableName) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            // Add batch_id column (UUID, nullable, will be populated during COPY)
            stmt.execute(String.format(
                    "ALTER TABLE %s ADD COLUMN IF NOT EXISTS batch_id UUID", tableName));
            logger.info(ANSI_GREEN + "   Added batch_id column to {}" + ANSI_RESET, tableName);

            // Add row_number column (BIGSERIAL, auto-incrementing)
            stmt.execute(String.format(
                    "ALTER TABLE %s ADD COLUMN IF NOT EXISTS row_number BIGSERIAL", tableName));
            logger.info(ANSI_GREEN + "   Added row_number column to {}" + ANSI_RESET, tableName);

            // Add loaded_at column (TIMESTAMP, defaults to current timestamp)
            stmt.execute(String.format(
                    "ALTER TABLE %s ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                    tableName));
            logger.info(ANSI_GREEN + "   Added loaded_at column to {}" + ANSI_RESET, tableName);

            // Create index on batch_id for efficient querying
            String indexName = tableName.replaceAll("[^a-zA-Z0-9_]", "_") + "_batch_id_idx";
            stmt.execute(String.format(
                    "CREATE INDEX IF NOT EXISTS %s ON %s(batch_id)", indexName, tableName));
            logger.info(ANSI_GREEN + "   Created index on batch_id for {}" + ANSI_RESET, tableName);
        }
    }
}
