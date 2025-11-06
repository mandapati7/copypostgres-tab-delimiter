package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import teranet.mapdev.ingest.config.CsvProcessingConfig;
import teranet.mapdev.ingest.model.IngestionManifest;

import javax.sql.DataSource;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit tests for CsvProcessingService.
 * Tests CSV file processing, table creation, schema evolution, and error
 * handling.
 */
class CsvProcessingServiceTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private IngestionManifestService manifestService;

    @Mock
    private CsvProcessingConfig csvConfig;

    @Mock
    private FileChecksumService fileChecksumService;

    @Mock
    private CsvParsingService csvParsingService;

    @Mock
    private TableNamingService tableNamingService;

    @Mock
    private PostgresCopyService postgresCopyService;

    @InjectMocks
    private CsvProcessingService csvProcessingService;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    private MockMultipartFile testCsvFile;
    private UUID testBatchId;
    private String testChecksum;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Set up test data
        testBatchId = UUID.randomUUID();
        testChecksum = "abc123def456";

        String csvContent = "order_id,customer_name,amount\n1,John Doe,100.50\n2,Jane Smith,200.75\n";
        testCsvFile = new MockMultipartFile(
                "file",
                "test_orders.csv",
                "text/csv",
                csvContent.getBytes());

        // Set default configuration values
        ReflectionTestUtils.setField(csvProcessingService, "batchSize", 1000);
        ReflectionTestUtils.setField(csvProcessingService, "maxFileSize", "100MB");
        ReflectionTestUtils.setField(csvProcessingService, "tempDirectory", "/tmp/csv-loader");
    }

    // ========== Tests for processCsvToStaging() - Success Scenarios ==========

    @Test
    void testProcessCsvToStaging_Success() throws Exception {
        // Given: Successful processing flow
        List<String> headers = Arrays.asList("order_id", "customer_name", "amount");
        String tableName = "staging_orders_" + testBatchId.toString().substring(0, 8);

        // Mock file checksum calculation
        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);

        // Mock no existing manifest (not a duplicate)
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);

        // Mock manifest creation
        IngestionManifest newManifest = new IngestionManifest(
                testCsvFile.getOriginalFilename(),
                testCsvFile.getSize(),
                testChecksum);
        newManifest.setBatchId(testBatchId);
        newManifest.setContentType(testCsvFile.getContentType());

        // Mock CSV parsing
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);

        // Mock table naming - use eq() for literal string and any() for UUID matcher
        when(tableNamingService.generateTableNameFromFile(
                eq(testCsvFile.getOriginalFilename()), any(UUID.class))).thenReturn(tableName);

        // Mock database connection for table creation
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        // Mock table doesn't exist
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);

        // Mock table creation
        when(connection.createStatement()).thenReturn(statement);

        // Mock PostgreSQL COPY execution
        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers))
                .thenReturn(2L); // 2 records processed

        // When: Process the CSV file
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Verify successful processing
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(2L);
        assertThat(result.getProcessedRecords()).isEqualTo(2L);
        assertThat(result.getFailedRecords()).isEqualTo(0L);
        assertThat(result.getStatus()).isEqualTo(IngestionManifest.Status.COMPLETED);
        assertThat(result.getTableName()).isEqualTo(tableName);

        // Verify manifest was saved once initially, then updated once upon completion
        verify(manifestService, times(1)).save(any(IngestionManifest.class));
        verify(manifestService, times(1)).update(any(IngestionManifest.class));

        // Verify COPY command was executed
        verify(postgresCopyService).executeCopy(testCsvFile, tableName, headers);
    }

    @Test
    void testProcessCsvToStaging_WithParentBatchId() throws Exception {
        // Given: Processing with parent batch ID (ZIP file scenario)
        UUID parentBatchId = UUID.randomUUID();
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test_" + testBatchId.toString().substring(0, 8);

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(anyString(), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database operations
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers))
                .thenReturn(10L);

        // When: Process with parent batch ID
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile, parentBatchId);

        // Then: Verify parent batch ID is set
        assertThat(result).isNotNull();
        assertThat(result.getParentBatchId()).isEqualTo(parentBatchId);
        assertThat(result.getTotalRecords()).isEqualTo(10L);
    }

    @Test
    void testProcessCsvToStaging_DuplicateFile() throws Exception {
        // Given: File already processed (idempotency check)
        IngestionManifest existingManifest = new IngestionManifest(
                testCsvFile.getOriginalFilename(),
                testCsvFile.getSize(),
                testChecksum);
        existingManifest.setBatchId(UUID.randomUUID());
        existingManifest.setStatus(IngestionManifest.Status.COMPLETED);
        existingManifest.setTotalRecords(5L);
        existingManifest.setProcessedRecords(5L);

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(existingManifest);

        // When: Process duplicate file
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should return existing manifest without reprocessing
        assertThat(result).isEqualTo(existingManifest);
        assertThat(result.isAlreadyProcessed()).isTrue();

        // Verify no new processing occurred
        verify(csvParsingService, never()).extractCsvHeaders(any());
        verify(postgresCopyService, never()).executeCopy(any(), any(), any());
        verify(manifestService, never()).save(any());
    }
    // ========== Tests for Error Scenarios ==========

    @Test
    void testProcessCsvToStaging_ChecksumCalculationFailure() throws Exception {
        // Given: Checksum calculation fails
        when(fileChecksumService.calculateFileChecksum(testCsvFile))
                .thenThrow(new NoSuchAlgorithmException("SHA-256 not available"));

        // When & Then: Should throw RuntimeException
        assertThatThrownBy(() -> csvProcessingService.processCsvToStaging(testCsvFile))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to process CSV file");

        // Note: When checksum calculation fails, the fallback manifest creation also
        // fails
        // So manifest.update() is never called - this is expected behavior
        // Verify no manifest save happened (because fallback also failed)
        verify(manifestService, never()).save(any());
    }

    @Test
    void testProcessCsvToStaging_HeaderExtractionFailure() throws Exception {
        // Given: CSV header extraction fails
        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile))
                .thenThrow(new IOException("Invalid CSV format"));

        // When & Then: Should throw RuntimeException
        assertThatThrownBy(() -> csvProcessingService.processCsvToStaging(testCsvFile))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to process CSV file");
    }

    @Test
    void testProcessCsvToStaging_TableCreationFailure() throws Exception {
        // Given: Table creation fails
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(anyString(), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database connection failure
        when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

        // When & Then: Should throw RuntimeException
        assertThatThrownBy(() -> csvProcessingService.processCsvToStaging(testCsvFile))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to process CSV file");

        // Verify error was logged in manifest
        verify(manifestService).update(argThat(manifest -> manifest.getStatus() == IngestionManifest.Status.FAILED));
    }

    @Test
    void testProcessCsvToStaging_CopyCommandFailure() throws Exception {
        // Given: PostgreSQL COPY fails
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(anyString(), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database setup
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        // Mock COPY failure
        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers))
                .thenThrow(new SQLException("COPY command failed"));

        // When & Then: Should throw RuntimeException
        assertThatThrownBy(() -> csvProcessingService.processCsvToStaging(testCsvFile))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to process CSV file");
    }

    @Test
    void testProcessCsvToStaging_EmptyFile() throws Exception {
        // Given: Empty CSV file (0 records)
        MockMultipartFile emptyFile = new MockMultipartFile(
                "file", "empty.csv", "text/csv", "header1,header2\n".getBytes());

        List<String> headers = Arrays.asList("header1", "header2");
        String tableName = "staging_empty";

        when(fileChecksumService.calculateFileChecksum(emptyFile)).thenReturn("empty123");
        when(manifestService.findByChecksum("empty123")).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(emptyFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(anyString(), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        // Mock COPY returns 0 records
        when(postgresCopyService.executeCopy(emptyFile, tableName, headers))
                .thenReturn(0L);

        // When: Process empty file
        IngestionManifest result = csvProcessingService.processCsvToStaging(emptyFile);

        // Then: Should complete successfully with 0 records
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(0L);
        assertThat(result.getProcessedRecords()).isEqualTo(0L);
        assertThat(result.getStatus()).isEqualTo(IngestionManifest.Status.COMPLETED);
    }

    // ========== Tests for Manifest Service Fallback ==========

    @Test
    void testProcessCsvToStaging_ManifestServiceUnavailable() throws Exception {
        // Given: Manifest service is unavailable (creates fallback manifest)
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);

        // Mock manifest service throws exception
        when(manifestService.findByChecksum(testChecksum))
                .thenThrow(new RuntimeException("Database unavailable"));

        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(anyString(), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers))
                .thenReturn(5L);

        // When: Process with fallback manifest
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should still succeed with fallback manifest
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(5L);
        assertThat(result.getBatchId()).isNotNull();
    }

    @Test
    void testProcessCsvToStaging_ManifestSaveFailure() throws Exception {
        // Given: Manifest save fails but processing continues
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);

        // Manifest save throws exception
        doThrow(new RuntimeException("Save failed"))
                .when(manifestService).save(any(IngestionManifest.class));

        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(anyString(), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers))
                .thenReturn(3L);

        // When: Process despite save failure
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should complete successfully (fallback behavior)
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(3L);
    }

    // ========== Tests for Large Files ==========

    @Test
    void testProcessCsvToStaging_LargeFile() throws Exception {
        // Given: Large CSV file with many records
        StringBuilder largeCsv = new StringBuilder("id,name,value\n");
        for (int i = 0; i < 10000; i++) {
            largeCsv.append(i).append(",name").append(i).append(",value").append(i).append("\n");
        }

        MockMultipartFile largeFile = new MockMultipartFile(
                "file", "large.csv", "text/csv", largeCsv.toString().getBytes());

        List<String> headers = Arrays.asList("id", "name", "value");
        String tableName = "staging_large";

        when(fileChecksumService.calculateFileChecksum(largeFile)).thenReturn("large123");
        when(manifestService.findByChecksum("large123")).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(largeFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(anyString(), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        // Mock COPY processes all 10000 records
        when(postgresCopyService.executeCopy(largeFile, tableName, headers))
                .thenReturn(10000L);

        // When: Process large file
        IngestionManifest result = csvProcessingService.processCsvToStaging(largeFile);

        // Then: Should handle large file successfully
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(10000L);
        assertThat(result.getProcessedRecords()).isEqualTo(10000L);
        assertThat(result.getProcessingDurationMs()).isNotNull();
    }

    // ========== Tests for Special Characters in Table Names ==========

    @Test
    void testProcessCsvToStaging_SpecialCharactersInFileName() throws Exception {
        // Given: File with special characters in name
        MockMultipartFile specialFile = new MockMultipartFile(
                "file", "orders-2024_v2.csv", "text/csv", "col1,col2\nval1,val2\n".getBytes());

        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_orders_2024_v2_abc123";

        when(fileChecksumService.calculateFileChecksum(specialFile)).thenReturn("special123");
        when(manifestService.findByChecksum("special123")).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(specialFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(
                eq("orders-2024_v2.csv"), any(UUID.class))).thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(specialFile, tableName, headers))
                .thenReturn(1L);

        // When: Process file with special characters
        IngestionManifest result = csvProcessingService.processCsvToStaging(specialFile);

        // Then: Should handle special characters correctly
        assertThat(result).isNotNull();
        assertThat(result.getTableName()).isEqualTo(tableName);
        assertThat(result.getTotalRecords()).isEqualTo(1L);
    }

    // ========== Tests for processCsvToStaging() without parentBatchId ==========

    @Test
    void testProcessCsvToStaging_WithoutParentBatchId() throws Exception {
        // Given: Standard processing without parent batch
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(anyString(), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers))
                .thenReturn(2L);

        // When: Process without parent batch ID
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Parent batch ID should be null
        assertThat(result).isNotNull();
        assertThat(result.getParentBatchId()).isNull();
        assertThat(result.getTotalRecords()).isEqualTo(2L);
    }

    // ========== Tests for Timing and Performance Metrics ==========

    @Test
    void testProcessCsvToStaging_RecordsProcessingDuration() throws Exception {
        // Given: Normal processing
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(anyString(), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers))
                .thenReturn(100L);

        // When: Process file
        long startTime = System.currentTimeMillis();
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);
        long endTime = System.currentTimeMillis();

        // Then: Should record processing duration
        assertThat(result.getProcessingDurationMs()).isNotNull();
        assertThat(result.getProcessingDurationMs()).isGreaterThanOrEqualTo(0L);
        assertThat(result.getProcessingDurationMs()).isLessThanOrEqualTo(endTime - startTime + 100);
        assertThat(result.getStartedAt()).isNotNull();
        assertThat(result.getCompletedAt()).isNotNull();
    }

    // ============================================================================
    // Additional Coverage Tests for Private Methods and Edge Cases
    // ============================================================================

    @Test
    void testProcessCsvToStaging_ManifestUpdateFailure() throws Exception {
        // Given: Manifest update fails but processing succeeds
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(eq(testCsvFile.getOriginalFilename()), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database for table creation
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers)).thenReturn(5L);

        // Manifest update throws exception
        doThrow(new RuntimeException("Update failed")).when(manifestService).update(any(IngestionManifest.class));

        // When: Process CSV
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should still return successful result despite update failure
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(5L);

        // Verify update was attempted
        verify(manifestService).update(any(IngestionManifest.class));
    }

    @Test
    void testProcessCsvToStaging_FindByChecksumFailure() throws Exception {
        // Given: Finding existing manifest by checksum fails
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        // findByChecksum throws exception instead of returning null
        when(manifestService.findByChecksum(testChecksum))
                .thenThrow(new RuntimeException("Database unavailable"));
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(eq(testCsvFile.getOriginalFilename()), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers)).thenReturn(3L);

        // When: Process CSV - should continue despite findByChecksum failure
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should succeed
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(3L);
    }

    @Test
    void testProcessCsvToStaging_CompletedDuplicateFile() throws Exception {
        // Given: File already processed AND completed (full idempotency)
        IngestionManifest completedManifest = new IngestionManifest(
                testCsvFile.getOriginalFilename(),
                testCsvFile.getSize(),
                testChecksum);
        completedManifest.setBatchId(UUID.randomUUID());
        completedManifest.setStatus(IngestionManifest.Status.COMPLETED);
        completedManifest.setTotalRecords(100L);
        completedManifest.setCompletedAt(LocalDateTime.now());

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(completedManifest);

        // When: Attempt to process already completed file
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should return existing manifest marked as already processed
        assertThat(result).isNotNull();
        assertThat(result.isAlreadyProcessed()).isTrue();
        assertThat(result.getBatchId()).isEqualTo(completedManifest.getBatchId());
        assertThat(result.getTotalRecords()).isEqualTo(100L);

        // Verify NO new processing occurred
        verify(csvParsingService, never()).extractCsvHeaders(any());
        verify(postgresCopyService, never()).executeCopy(any(), any(), any());
        verify(manifestService, never()).save(any());
        verify(manifestService, never()).update(any());
    }

    @Test
    void testProcessCsvToStaging_InProgressDuplicateFile() throws Exception {
        // Given: File exists but still IN_PROGRESS (not completed)
        IngestionManifest inProgressManifest = new IngestionManifest(
                testCsvFile.getOriginalFilename(),
                testCsvFile.getSize(),
                testChecksum);
        inProgressManifest.setBatchId(UUID.randomUUID());
        inProgressManifest.setStatus(IngestionManifest.Status.PROCESSING);

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        // Return in-progress manifest (not completed)
        when(manifestService.findByChecksum(testChecksum)).thenReturn(inProgressManifest);

        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(eq(testCsvFile.getOriginalFilename()), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers)).thenReturn(5L);

        // When: Process file that's in progress
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should proceed with processing (not treated as duplicate)
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(5L);
        assertThat(result.getStatus()).isEqualTo(IngestionManifest.Status.COMPLETED);

        // Verify processing occurred
        verify(csvParsingService).extractCsvHeaders(testCsvFile);
        verify(postgresCopyService).executeCopy(testCsvFile, tableName, headers);
    }

    @Test
    void testProcessCsvToStaging_ErrorWithStartedAt() throws Exception {
        // Given: Processing starts successfully but fails later
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(eq(testCsvFile.getOriginalFilename()), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database for table creation
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        // COPY command fails
        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers))
                .thenThrow(new SQLException("COPY failed"));

        // When & Then: Should calculate processing duration in error handler
        assertThatThrownBy(() -> csvProcessingService.processCsvToStaging(testCsvFile))
                .isInstanceOf(RuntimeException.class);

        // Verify manifest was updated with error and duration was calculated
        verify(manifestService).update(argThat(manifest -> manifest.getStatus() == IngestionManifest.Status.FAILED &&
                manifest.getErrorMessage() != null &&
                manifest.getProcessingDurationMs() != null &&
                manifest.getProcessingDurationMs() >= 0L));
    }

    @Test
    void testProcessCsvToStaging_FilePath() throws Exception {
        // Given: Processing succeeds
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(eq(testCsvFile.getOriginalFilename()), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers)).thenReturn(5L);

        // When: Process CSV
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: File path should be set correctly
        verify(manifestService).save(argThat(manifest -> manifest.getFilePath() != null &&
                manifest.getFilePath().startsWith("upload://") &&
                manifest.getFilePath().contains(testCsvFile.getOriginalFilename())));
    }

    @Test
    void testProcessCsvToStaging_TableExistsWithSchemaEvolution() throws Exception {
        // Given: Table already exists but missing one column (schema evolution)
        List<String> headers = Arrays.asList("col1", "col2", "new_col3");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(eq(testCsvFile.getOriginalFilename()), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database - table EXISTS
        when(dataSource.getConnection()).thenReturn(connection);

        // First PreparedStatement - check if table exists
        PreparedStatement tableExistsStmt = mock(PreparedStatement.class);
        ResultSet tableExistsRs = mock(ResultSet.class);
        when(connection.prepareStatement(contains("SELECT EXISTS"))).thenReturn(tableExistsStmt);
        when(tableExistsStmt.executeQuery()).thenReturn(tableExistsRs);
        when(tableExistsRs.next()).thenReturn(true);
        when(tableExistsRs.getBoolean(1)).thenReturn(true); // TABLE EXISTS

        // Second PreparedStatement - get existing columns
        PreparedStatement getColumnsStmt = mock(PreparedStatement.class);
        ResultSet columnsRs = mock(ResultSet.class);
        when(connection.prepareStatement(contains("SELECT column_name"))).thenReturn(getColumnsStmt);
        when(getColumnsStmt.executeQuery()).thenReturn(columnsRs);
        // Return only 2 existing columns, missing col3
        when(columnsRs.next()).thenReturn(true, true, false);
        when(columnsRs.getString("column_name")).thenReturn("col1", "col2");

        // Mock ALTER TABLE statement for adding new column
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(contains("ALTER TABLE"))).thenReturn(true);

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers)).thenReturn(3L);

        // When: Process CSV
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should succeed with schema evolution
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(3L);

        // Verify ALTER TABLE was executed for the missing column
        verify(statement, atLeastOnce())
                .execute(argThat(sql -> sql != null && sql.contains("ALTER TABLE") && sql.contains("new_col3")));
    }

    @Test
    void testProcessCsvToStaging_AlterTableFailsSilently() throws Exception {
        // Given: Table exists, ALTER TABLE fails but processing continues
        List<String> headers = Arrays.asList("col1", "col2", "new_col3");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(eq(testCsvFile.getOriginalFilename()), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database
        when(dataSource.getConnection()).thenReturn(connection);

        PreparedStatement tableExistsStmt = mock(PreparedStatement.class);
        ResultSet tableExistsRs = mock(ResultSet.class);
        when(connection.prepareStatement(contains("SELECT EXISTS"))).thenReturn(tableExistsStmt);
        when(tableExistsStmt.executeQuery()).thenReturn(tableExistsRs);
        when(tableExistsRs.next()).thenReturn(true);
        when(tableExistsRs.getBoolean(1)).thenReturn(true);

        PreparedStatement getColumnsStmt = mock(PreparedStatement.class);
        ResultSet columnsRs = mock(ResultSet.class);
        when(connection.prepareStatement(contains("SELECT column_name"))).thenReturn(getColumnsStmt);
        when(getColumnsStmt.executeQuery()).thenReturn(columnsRs);
        when(columnsRs.next()).thenReturn(true, true, false);
        when(columnsRs.getString("column_name")).thenReturn("col1", "col2");

        // ALTER TABLE fails
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(contains("ALTER TABLE")))
                .thenThrow(new SQLException("Column already exists"));

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers)).thenReturn(3L);

        // When: Process CSV - should continue despite ALTER TABLE failure
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should succeed (ALTER failure is logged but not thrown)
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(3L);
        assertThat(result.getStatus()).isEqualTo(IngestionManifest.Status.COMPLETED);
    }

    @Test
    void testProcessCsvToStaging_ErrorManifestUpdateFails() throws Exception {
        // Given: Processing fails AND manifest update in error handler also fails
        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile))
                .thenThrow(new IOException("Parse failed"));

        // Manifest update in catch block also fails
        doThrow(new RuntimeException("Update failed"))
                .when(manifestService).update(any(IngestionManifest.class));

        // When & Then: Should throw original exception
        assertThatThrownBy(() -> csvProcessingService.processCsvToStaging(testCsvFile))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to process CSV file");

        // Verify update was attempted despite failure
        verify(manifestService).update(any(IngestionManifest.class));
    }

    @Test
    void testProcessCsvToStaging_NullManifestInErrorHandler() throws Exception {
        // Given: Error occurs so early that manifest creation throws exception
        when(fileChecksumService.calculateFileChecksum(testCsvFile))
                .thenThrow(new NoSuchAlgorithmException("SHA-256 not available"));

        // When trying to create fallback manifest, it also fails
        // This tests the "manifest == null" branch in the catch block

        // When & Then: Should throw exception
        assertThatThrownBy(() -> csvProcessingService.processCsvToStaging(testCsvFile))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testProcessCsvToStaging_ErrorWithNullStartedAt() throws Exception {
        // Given: Processing fails before startedAt is set
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        // Manually create a manifest without startedAt
        IngestionManifest preCreatedManifest = new IngestionManifest(
                testCsvFile.getOriginalFilename(),
                testCsvFile.getSize(),
                testChecksum);
        preCreatedManifest.setBatchId(UUID.randomUUID());
        // Note: startedAt is null

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(manifestService.save(any(IngestionManifest.class))).thenReturn(preCreatedManifest);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(eq(testCsvFile.getOriginalFilename()), any(UUID.class)))
                .thenReturn(tableName);

        // Table creation fails
        when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

        // When & Then: Should throw exception
        assertThatThrownBy(() -> csvProcessingService.processCsvToStaging(testCsvFile))
                .isInstanceOf(RuntimeException.class);

        // Verify manifest was updated without duration (since startedAt was null)
        verify(manifestService).update(argThat(manifest -> manifest.getStatus() == IngestionManifest.Status.FAILED &&
                manifest.getErrorMessage() != null));
    }

    @Test
    void testProcessCsvToStaging_AllColumnsAlreadyExist() throws Exception {
        // Given: Table exists and already has all columns (no ALTER needed)
        List<String> headers = Arrays.asList("col1", "col2");
        String tableName = "staging_test";

        when(fileChecksumService.calculateFileChecksum(testCsvFile)).thenReturn(testChecksum);
        when(manifestService.findByChecksum(testChecksum)).thenReturn(null);
        when(csvParsingService.extractCsvHeaders(testCsvFile)).thenReturn(headers);
        when(tableNamingService.generateTableNameFromFile(eq(testCsvFile.getOriginalFilename()), any(UUID.class)))
                .thenReturn(tableName);

        // Mock database - table EXISTS
        when(dataSource.getConnection()).thenReturn(connection);

        PreparedStatement tableExistsStmt = mock(PreparedStatement.class);
        ResultSet tableExistsRs = mock(ResultSet.class);
        when(connection.prepareStatement(contains("SELECT EXISTS"))).thenReturn(tableExistsStmt);
        when(tableExistsStmt.executeQuery()).thenReturn(tableExistsRs);
        when(tableExistsRs.next()).thenReturn(true);
        when(tableExistsRs.getBoolean(1)).thenReturn(true);

        PreparedStatement getColumnsStmt = mock(PreparedStatement.class);
        ResultSet columnsRs = mock(ResultSet.class);
        when(connection.prepareStatement(contains("SELECT column_name"))).thenReturn(getColumnsStmt);
        when(getColumnsStmt.executeQuery()).thenReturn(columnsRs);
        // Return ALL columns that match headers
        when(columnsRs.next()).thenReturn(true, true, false);
        when(columnsRs.getString("column_name")).thenReturn("col1", "col2");

        when(postgresCopyService.executeCopy(testCsvFile, tableName, headers)).thenReturn(5L);

        // When: Process CSV
        IngestionManifest result = csvProcessingService.processCsvToStaging(testCsvFile);

        // Then: Should succeed without any ALTER TABLE
        assertThat(result).isNotNull();
        assertThat(result.getTotalRecords()).isEqualTo(5L);

        // Verify NO ALTER TABLE was executed (all columns already exist)
        verify(statement, never()).execute(contains("ALTER TABLE"));
    }
}
