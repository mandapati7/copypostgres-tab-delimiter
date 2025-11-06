package teranet.mapdev.ingest.service;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Service responsible for PostgreSQL COPY command operations.
 * 
 * Handles:
 * - High-performance bulk data loading using PostgreSQL COPY
 * - CSV to tab-delimited format conversion
 * - CopyManager API integration
 * - Stream processing for memory-efficient loading
 * 
 * Design Notes:
 * - Uses PostgreSQL-specific CopyManager API for optimal performance
 * - Converts CSV rows to tab-delimited format on-the-fly
 * - Atomic operations - all records succeed or all fail
 * - No WAL overhead when used with UNLOGGED tables
 * 
 * Phase 5 of CsvProcessingService refactoring
 */
@Service
public class PostgresCopyService {

    private static final Logger logger = LoggerFactory.getLogger(PostgresCopyService.class);

    private final DataSource dataSource;
    private final CsvParsingService csvParsingService;
    private final FileChecksumService fileChecksumService;

    @Autowired
    public PostgresCopyService(DataSource dataSource, 
                              CsvParsingService csvParsingService,
                              FileChecksumService fileChecksumService) {
        this.dataSource = dataSource;
        this.csvParsingService = csvParsingService;
        this.fileChecksumService = fileChecksumService;
    }

    /**
     * Perform PostgreSQL COPY command for bulk insert into default schema.
     * 
     * PostgreSQL-specific features:
     * - Uses CopyManager API for high-performance bulk loading
     * - Atomic operation - all records succeed or all fail
     * - No WAL overhead when used with UNLOGGED tables
     * - Tab-delimited format for COPY FROM STDIN
     * 
     * @param file The CSV file to load
     * @param tableName Target table name (no schema prefix)
     * @param headers List of column names from CSV headers
     * @return Number of records successfully loaded
     * @throws SQLException if COPY command fails
     * @throws IOException if file reading fails
     */
    public long executeCopy(MultipartFile file, String tableName, List<String> headers) 
            throws SQLException, IOException {
        long recordCount = 0;
        // No schema prefix - using default schema
        
        try (Connection connection = dataSource.getConnection();
             InputStream inputStream = fileChecksumService.getDecompressedInputStream(file)) {
            
            // Unwrap HikariCP proxy to get the underlying PostgreSQL connection
            BaseConnection baseConnection = unwrapConnection(connection);
            
            // Get PostgreSQL CopyManager for bulk operations
            CopyManager copyManager = new CopyManager(baseConnection);
            
            // Create a BufferedReader for CSV processing
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                
                // Skip header line (we already extracted it)
                String headerLine = reader.readLine();
                logger.debug("Skipping CSV header: {}", headerLine);
                
                // Build COPY command for table in default schema
                String copyCommand = buildCopyCommand(tableName, headers);
                logger.info("Using COPY command: {}", copyCommand);
                
                // Create a custom InputStream that converts CSV rows to the format expected by COPY
                try (InputStream copyInputStream = createCopyInputStream(reader, headers)) {
                    recordCount = copyManager.copyIn(copyCommand, copyInputStream);
                }
            }
            
            logger.info("COPY command inserted {} records into table {}", recordCount, tableName);
        }
        
        return recordCount;
    }

    /**
     * Unwrap connection to get PostgreSQL BaseConnection.
     * Required for CopyManager API which needs the underlying PostgreSQL connection.
     * 
     * @param connection Wrapped connection (e.g., HikariCP proxy)
     * @return Unwrapped PostgreSQL BaseConnection
     * @throws SQLException if connection cannot be unwrapped
     */
    private BaseConnection unwrapConnection(Connection connection) throws SQLException {
        if (connection.isWrapperFor(BaseConnection.class)) {
            return connection.unwrap(BaseConnection.class);
        } else {
            throw new SQLException("Unable to unwrap connection to PostgreSQL BaseConnection");
        }
    }

    /**
     * Build PostgreSQL COPY command for table in default schema.
     * 
     * PostgreSQL-specific syntax:
     * - COPY FROM STDIN for streaming input
     * - FORMAT CSV with tab delimiter
     * - NULL '' for empty strings as NULL
     * 
     * NOTE: Only includes CSV columns (no metadata columns like batch_id).
     * 
     * @param tableName Target table name (no schema prefix)
     * @param headers List of column names from CSV
     * @return PostgreSQL COPY command string
     */
    public String buildCopyCommand(String tableName, List<String> headers) {
        // No schema prefix - using default schema
        
        StringBuilder copyCommand = new StringBuilder();
        copyCommand.append("COPY ").append(tableName);
        copyCommand.append(" (");
        
        // Add all CSV column names only
        for (int i = 0; i < headers.size(); i++) {
            if (i > 0) copyCommand.append(", ");
            copyCommand.append(headers.get(i));
        }
        
        copyCommand.append(") FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')");
        
        return copyCommand.toString();
    }

    /**
     * Create InputStream for COPY command that converts CSV data to tab-delimited format.
     * 
     * This creates a streaming adapter that:
     * 1. Reads CSV rows one at a time
     * 2. Parses CSV format (handles quotes, escapes, etc.)
     * 3. Converts to tab-delimited format for COPY
     * 4. Handles special characters (tabs, newlines)
     * 5. Provides memory-efficient streaming
     * 
     * NOTE: Only includes CSV values (no metadata like batch_id or row numbers).
     * 
     * @param csvReader BufferedReader positioned after header line
     * @param headers List of column names (determines column count)
     * @return InputStream that provides tab-delimited data for COPY
     */
    public InputStream createCopyInputStream(BufferedReader csvReader, List<String> headers) {
        return new InputStream() {
            private ByteArrayInputStream currentChunk = null;
            private boolean hasMore = true;
            
            @Override
            public int read() throws IOException {
                if (currentChunk == null || currentChunk.available() == 0) {
                    if (!prepareNextChunk()) {
                        return -1; // End of stream
                    }
                }
                return currentChunk.read();
            }
            
            /**
             * Prepare next chunk of tab-delimited data from CSV input.
             * Reads one CSV line, parses it, and converts to tab-delimited format.
             * 
             * @return true if chunk prepared successfully, false if end of stream
             * @throws IOException if CSV reading or parsing fails
             */
            private boolean prepareNextChunk() throws IOException {
                if (!hasMore) {
                    return false;
                }
                
                String csvLine = csvReader.readLine();
                if (csvLine == null) {
                    hasMore = false;
                    return false;
                }
                
                // Parse CSV row and convert to tab-delimited format for COPY
                List<String> values = csvParsingService.parseCsvRow(csvLine);
                
                StringBuilder copyLine = new StringBuilder();
                
                // Add values for each header (only CSV columns, no metadata)
                for (int i = 0; i < headers.size(); i++) {
                    if (i > 0) copyLine.append("\t");
                    
                    String value = "";
                    if (i < values.size() && values.get(i) != null) {
                        // Sanitize value for COPY format
                        // Replace tabs, newlines, carriage returns with spaces
                        value = values.get(i)
                            .replace("\t", " ")
                            .replace("\n", " ")
                            .replace("\r", "");
                    }
                    copyLine.append(value);
                }
                
                copyLine.append("\n");
                
                currentChunk = new ByteArrayInputStream(
                    copyLine.toString().getBytes(StandardCharsets.UTF_8)
                );
                
                return true;
            }
        };
    }
}
