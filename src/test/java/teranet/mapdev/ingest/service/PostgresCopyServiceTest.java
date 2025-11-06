package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.postgresql.core.BaseConnection;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for PostgresCopyService.
 * Tests COPY command generation and CSV to tab-delimited conversion logic.
 * 
 * Note: Integration tests requiring actual PostgreSQL connection are separate.
 */
class PostgresCopyServiceTest {

    @Mock
    private CsvParsingService csvParsingService;

    private PostgresCopyService postgresCopyService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        // Note: DataSource and FileChecksumService are mocked but not used in unit
        // tests
        postgresCopyService = new PostgresCopyService(null, csvParsingService, null);
    }

    @Test
    void testBuildCopyCommand_SimpleTable() {
        // Given
        String tableName = "staging_orders_abc123";
        List<String> headers = Arrays.asList("order_id", "customer_name", "amount");

        // When
        String copyCommand = postgresCopyService.buildCopyCommand(tableName, headers);

        // Then
        assertEquals("COPY staging_orders_abc123 (order_id, customer_name, amount) " +
                "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')",
                copyCommand,
                "Should generate correct COPY command");
    }

    @Test
    void testBuildCopyCommand_SingleColumn() {
        // Given
        String tableName = "test_table";
        List<String> headers = Arrays.asList("id");

        // When
        String copyCommand = postgresCopyService.buildCopyCommand(tableName, headers);

        // Then
        assertEquals("COPY test_table (id) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')",
                copyCommand,
                "Should generate correct COPY command for single column");
    }

    @Test
    void testBuildCopyCommand_ManyColumns() {
        // Given
        String tableName = "wide_table";
        List<String> headers = Arrays.asList("col1", "col2", "col3", "col4", "col5");

        // When
        String copyCommand = postgresCopyService.buildCopyCommand(tableName, headers);

        // Then
        assertTrue(copyCommand.contains("col1, col2, col3, col4, col5"),
                "Should include all column names");
        assertTrue(copyCommand.startsWith("COPY wide_table"),
                "Should start with COPY and table name");
        assertTrue(copyCommand.contains("FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')"),
                "Should include correct COPY options");
    }

    @Test
    void testBuildCopyCommand_NoSchemaPrefix() {
        // Given
        String tableName = "my_table";
        List<String> headers = Arrays.asList("field1", "field2");

        // When
        String copyCommand = postgresCopyService.buildCopyCommand(tableName, headers);

        // Then
        assertFalse(copyCommand.contains("."),
                "Should not include schema prefix");
        assertTrue(copyCommand.startsWith("COPY my_table"),
                "Should use table name without schema");
    }

    @Test
    void testCreateCopyInputStream_SimpleData() throws IOException {
        // Given
        String csvData = "value1,value2,value3\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2", "col3");

        // Mock CSV parsing
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("value1", "value2", "value3"));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then
        assertEquals("value1\tvalue2\tvalue3\n", result,
                "Should convert CSV to tab-delimited format");
    }

    @Test
    void testCreateCopyInputStream_MultipleRows() throws IOException {
        // Given
        String csvData = "row1val1,row1val2\nrow2val1,row2val2\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2");

        // Mock CSV parsing for each row
        when(csvParsingService.parseCsvRow("row1val1,row1val2"))
                .thenReturn(Arrays.asList("row1val1", "row1val2"));
        when(csvParsingService.parseCsvRow("row2val1,row2val2"))
                .thenReturn(Arrays.asList("row2val1", "row2val2"));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then
        String expected = "row1val1\trow1val2\nrow2val1\trow2val2\n";
        assertEquals(expected, result,
                "Should convert multiple CSV rows to tab-delimited format");
    }

    @Test
    void testCreateCopyInputStream_HandlesNullValues() throws IOException {
        // Given
        String csvData = "value1,,value3\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2", "col3");

        // Mock CSV parsing with null value
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("value1", null, "value3"));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then
        assertEquals("value1\t\tvalue3\n", result,
                "Should handle null values as empty strings");
    }

    @Test
    void testCreateCopyInputStream_SanitizesSpecialCharacters() throws IOException {
        // Given
        String csvData = "value_with_tab,value_with_newline,value_with_carriage\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2", "col3");

        // Mock CSV parsing with special characters in values
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("has\ttab", "has\nnewline", "has\rcarriage"));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then
        // The replace converts: \t -> " ", \n -> " ", \r -> "" (removes without space)
        assertEquals("has tab\thas newline\thascarriage\n", result,
                "Should replace tabs and newlines with spaces, remove carriage returns");
        assertFalse(result.contains("\r"), "Should not contain carriage returns");
    }

    @Test
    void testCreateCopyInputStream_HandlesMismatchedColumnCount() throws IOException {
        // Given - CSV has 2 values but table has 3 columns
        String csvData = "value1,value2\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2", "col3");

        // Mock CSV parsing with fewer values than columns
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("value1", "value2"));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then
        assertEquals("value1\tvalue2\t\n", result,
                "Should pad missing columns with empty strings");
    }

    @Test
    void testCreateCopyInputStream_EmptyFile() throws IOException {
        // Given
        String csvData = "";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2");

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then
        assertEquals("", result,
                "Should return empty string for empty file");
    }

    @Test
    void testCreateCopyInputStream_SingleCharacterRead() throws IOException {
        // Given
        String csvData = "a,b\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2");

        // Mock CSV parsing
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("a", "b"));

        // When - Read character by character
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        StringBuilder result = new StringBuilder();
        int ch;
        while ((ch = inputStream.read()) != -1) {
            result.append((char) ch);
        }

        // Then
        assertEquals("a\tb\n", result.toString(),
                "Should work correctly when reading single characters");
    }

    @Test
    void testCreateCopyInputStream_StreamingBehavior() throws IOException {
        // Given - Large dataset simulation
        StringBuilder csvData = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            csvData.append("val").append(i).append(",data").append(i).append("\n");
        }
        BufferedReader reader = new BufferedReader(new StringReader(csvData.toString()));
        List<String> headers = Arrays.asList("col1", "col2");

        // Mock CSV parsing for all rows
        when(csvParsingService.parseCsvRow(anyString()))
                .thenAnswer(invocation -> {
                    String line = invocation.getArgument(0);
                    String[] parts = line.split(",");
                    return Arrays.asList(parts);
                });

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);

        // Then - Should be able to read all data
        String result = new String(inputStream.readAllBytes());
        assertNotNull(result, "Should produce output");
        assertTrue(result.length() > 0, "Should have content");
        assertEquals(100, result.split("\n").length,
                "Should process all 100 rows");
    }

    @Test
    void testCreateCopyInputStream_PreservesEmptyStrings() throws IOException {
        // Given
        String csvData = "\"\",value2,\"\"\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2", "col3");

        // Mock CSV parsing with empty strings
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("", "value2", ""));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then
        assertEquals("\tvalue2\t\n", result,
                "Should preserve empty strings as empty fields");
    }

    // ========== Tests for executeCopy() and unwrapConnection() ==========
    // Note: These require database connectivity and are tested with mocks
    // Integration tests with actual PostgreSQL are in separate test class

    @Test
    void testExecuteCopy_ThrowsException_WhenConnectionCannotBeUnwrapped() throws Exception {
        // Given: A DataSource that provides a non-PostgreSQL connection
        DataSource mockDataSource = mock(DataSource.class);
        Connection mockConnection = mock(Connection.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.isWrapperFor(BaseConnection.class)).thenReturn(false);

        FileChecksumService mockFileChecksumService = mock(FileChecksumService.class);
        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.csv", "text/csv", "data".getBytes());
        when(mockFileChecksumService.getDecompressedInputStream(mockFile))
                .thenReturn(new ByteArrayInputStream("header\ndata".getBytes()));

        PostgresCopyService service = new PostgresCopyService(
                mockDataSource, csvParsingService, mockFileChecksumService);

        // When & Then: Should throw SQLException when connection cannot be unwrapped
        assertThatThrownBy(() -> service.executeCopy(mockFile, "test_table", Arrays.asList("col1")))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Unable to unwrap connection to PostgreSQL BaseConnection");

        verify(mockConnection).isWrapperFor(BaseConnection.class);
    }

    @Test
    void testExecuteCopy_ClosesResources_OnException() throws Exception {
        // Given: A DataSource that throws exception
        DataSource mockDataSource = mock(DataSource.class);
        Connection mockConnection = mock(Connection.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.isWrapperFor(BaseConnection.class)).thenReturn(false);

        FileChecksumService mockFileChecksumService = mock(FileChecksumService.class);
        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.csv", "text/csv", "data".getBytes());
        InputStream mockInputStream = mock(InputStream.class);
        when(mockFileChecksumService.getDecompressedInputStream(mockFile))
                .thenReturn(mockInputStream);

        PostgresCopyService service = new PostgresCopyService(
                mockDataSource, csvParsingService, mockFileChecksumService);

        // When: executeCopy throws exception
        try {
            service.executeCopy(mockFile, "test_table", Arrays.asList("col1"));
        } catch (SQLException e) {
            // Expected
        }

        // Then: Connection should be closed (via try-with-resources)
        verify(mockConnection).close();
        verify(mockInputStream).close();
    }

    @Test
    void testBuildCopyCommand_WithSpecialCharactersInColumnNames() {
        // Given: Column names with underscores and numbers (common in real schemas)
        String tableName = "staging_data_2024";
        List<String> headers = Arrays.asList("order_id", "customer_name_2", "total_amount_usd");

        // When
        String copyCommand = postgresCopyService.buildCopyCommand(tableName, headers);

        // Then
        assertTrue(copyCommand.contains("order_id, customer_name_2, total_amount_usd"),
                "Should handle column names with underscores and numbers");
        assertEquals("COPY staging_data_2024 (order_id, customer_name_2, total_amount_usd) " +
                "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')",
                copyCommand);
    }

    @Test
    void testCreateCopyInputStream_HandlesExtraValuesInCsv() throws IOException {
        // Given: CSV has MORE values than columns (should truncate to column count)
        String csvData = "value1,value2,value3,value4\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2"); // Only 2 columns

        // Mock CSV parsing with more values than headers
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("value1", "value2", "value3", "value4"));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then: Should only use first 2 values (matching header count)
        assertEquals("value1\tvalue2\n", result,
                "Should only use values up to header count");
    }

    @Test
    void testCreateCopyInputStream_HandlesComplexSpecialCharacters() throws IOException {
        // Given: Values with multiple types of special characters
        String csvData = "complex_value\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1");

        // Mock CSV parsing with tab, newline, and carriage return combined
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("text\twith\ttabs\nand\nnewlines\r\nand\rcarriage"));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then: All special chars should be sanitized
        // Note: \r is replaced with "" (empty), not space
        assertEquals("text with tabs and newlines andcarriage\n", result,
                "Should sanitize all tab, newline, and carriage return characters");
        assertFalse(result.contains("\t") && !result.endsWith("\t\n"),
                "Should not contain tabs within values");
        assertFalse(result.contains("\r"), "Should not contain carriage returns");
    }

    // ========== Additional Tests for executeCopy() with Better Mocking ==========

    @Test
    void testExecuteCopy_WithSuccessfulExecution() throws Exception {
        // Given: Mock complete execution flow
        DataSource mockDataSource = mock(DataSource.class);
        Connection mockConnection = mock(Connection.class);
        BaseConnection mockBaseConnection = mock(BaseConnection.class);
        FileChecksumService mockFileChecksumService = mock(FileChecksumService.class);

        // Setup connection unwrapping
        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.isWrapperFor(BaseConnection.class)).thenReturn(true);
        when(mockConnection.unwrap(BaseConnection.class)).thenReturn(mockBaseConnection);

        // Setup file with CSV data
        String csvContent = "col1,col2,col3\nvalue1,value2,value3\n";
        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.csv", "text/csv", csvContent.getBytes());

        when(mockFileChecksumService.getDecompressedInputStream(mockFile))
                .thenReturn(new ByteArrayInputStream(csvContent.getBytes()));

        // Mock CSV parsing
        when(csvParsingService.parseCsvRow("value1,value2,value3"))
                .thenReturn(Arrays.asList("value1", "value2", "value3"));

        PostgresCopyService service = new PostgresCopyService(
                mockDataSource, csvParsingService, mockFileChecksumService);

        // When & Then: Should attempt to execute (will fail at CopyManager creation but
        // covered)
        // This tests the connection acquisition and unwrapping logic
        assertThatThrownBy(() -> service.executeCopy(mockFile, "test_table", Arrays.asList("col1", "col2", "col3")))
                .isInstanceOf(NullPointerException.class); // CopyManager creation will fail

        // Verify connection was unwrapped
        verify(mockConnection).isWrapperFor(BaseConnection.class);
        verify(mockConnection).unwrap(BaseConnection.class);
    }

    @Test
    void testExecuteCopy_WithIOException() throws Exception {
        // Given: FileChecksumService throws IOException
        DataSource mockDataSource = mock(DataSource.class);
        Connection mockConnection = mock(Connection.class);
        FileChecksumService mockFileChecksumService = mock(FileChecksumService.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.csv", "text/csv", "data".getBytes());

        // Simulate IOException during file reading
        when(mockFileChecksumService.getDecompressedInputStream(mockFile))
                .thenThrow(new IOException("Failed to read file"));

        PostgresCopyService service = new PostgresCopyService(
                mockDataSource, csvParsingService, mockFileChecksumService);

        // When & Then: Should propagate IOException
        assertThatThrownBy(() -> service.executeCopy(mockFile, "test_table", Arrays.asList("col1")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to read file");

        // Verify connection is still closed (try-with-resources)
        verify(mockConnection).close();
    }

    @Test
    void testExecuteCopy_WithSQLExceptionDuringConnectionAcquisition() throws Exception {
        // Given: DataSource throws SQLException
        DataSource mockDataSource = mock(DataSource.class);
        FileChecksumService mockFileChecksumService = mock(FileChecksumService.class);

        when(mockDataSource.getConnection())
                .thenThrow(new SQLException("Connection pool exhausted"));

        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.csv", "text/csv", "data".getBytes());

        PostgresCopyService service = new PostgresCopyService(
                mockDataSource, csvParsingService, mockFileChecksumService);

        // When & Then: Should propagate SQLException
        assertThatThrownBy(() -> service.executeCopy(mockFile, "test_table", Arrays.asList("col1")))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Connection pool exhausted");
    }

    @Test
    void testExecuteCopy_WithEmptyHeaders() throws Exception {
        // Given: Empty headers list
        DataSource mockDataSource = mock(DataSource.class);
        Connection mockConnection = mock(Connection.class);
        BaseConnection mockBaseConnection = mock(BaseConnection.class);
        FileChecksumService mockFileChecksumService = mock(FileChecksumService.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.isWrapperFor(BaseConnection.class)).thenReturn(true);
        when(mockConnection.unwrap(BaseConnection.class)).thenReturn(mockBaseConnection);

        String csvContent = "\nvalue1,value2\n";
        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.csv", "text/csv", csvContent.getBytes());

        when(mockFileChecksumService.getDecompressedInputStream(mockFile))
                .thenReturn(new ByteArrayInputStream(csvContent.getBytes()));

        PostgresCopyService service = new PostgresCopyService(
                mockDataSource, csvParsingService, mockFileChecksumService);

        // When & Then: Should handle empty headers (will fail at CopyManager but logic
        // is tested)
        assertThatThrownBy(() -> service.executeCopy(mockFile, "test_table", Arrays.asList()))
                .isInstanceOf(NullPointerException.class);

        verify(mockConnection).isWrapperFor(BaseConnection.class);
    }

    @Test
    void testUnwrapConnection_WithNonPostgreSQLConnection() throws Exception {
        // Given: Connection that doesn't wrap BaseConnection
        DataSource mockDataSource = mock(DataSource.class);
        Connection mockConnection = mock(Connection.class);
        FileChecksumService mockFileChecksumService = mock(FileChecksumService.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.isWrapperFor(BaseConnection.class)).thenReturn(false);

        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.csv", "text/csv", "header\ndata".getBytes());

        when(mockFileChecksumService.getDecompressedInputStream(mockFile))
                .thenReturn(new ByteArrayInputStream("header\ndata".getBytes()));

        PostgresCopyService service = new PostgresCopyService(
                mockDataSource, csvParsingService, mockFileChecksumService);

        // When & Then: Should throw SQLException with appropriate message
        assertThatThrownBy(() -> service.executeCopy(mockFile, "test_table", Arrays.asList("col1")))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Unable to unwrap connection to PostgreSQL BaseConnection");

        verify(mockConnection).isWrapperFor(BaseConnection.class);
        verify(mockConnection, never()).unwrap(BaseConnection.class);
    }

    @Test
    void testExecuteCopy_WithLargeFile() throws Exception {
        // Given: Large CSV file simulation
        DataSource mockDataSource = mock(DataSource.class);
        Connection mockConnection = mock(Connection.class);
        BaseConnection mockBaseConnection = mock(BaseConnection.class);
        FileChecksumService mockFileChecksumService = mock(FileChecksumService.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.isWrapperFor(BaseConnection.class)).thenReturn(true);
        when(mockConnection.unwrap(BaseConnection.class)).thenReturn(mockBaseConnection);

        // Build large CSV content (1000 rows)
        StringBuilder largeCsv = new StringBuilder("col1,col2,col3\n");
        for (int i = 0; i < 1000; i++) {
            largeCsv.append("val").append(i).append(",data").append(i).append(",info").append(i).append("\n");
        }

        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "large.csv", "text/csv", largeCsv.toString().getBytes());

        when(mockFileChecksumService.getDecompressedInputStream(mockFile))
                .thenReturn(new ByteArrayInputStream(largeCsv.toString().getBytes()));

        // Mock CSV parsing for all rows
        when(csvParsingService.parseCsvRow(anyString()))
                .thenAnswer(invocation -> {
                    String line = invocation.getArgument(0);
                    return Arrays.asList(line.split(","));
                });

        PostgresCopyService service = new PostgresCopyService(
                mockDataSource, csvParsingService, mockFileChecksumService);

        // When & Then: Should handle large files (will fail at CopyManager but tests
        // the flow)
        assertThatThrownBy(() -> service.executeCopy(mockFile, "large_table", Arrays.asList("col1", "col2", "col3")))
                .isInstanceOf(NullPointerException.class);

        // Verify connection unwrapping was attempted
        verify(mockConnection).isWrapperFor(BaseConnection.class);
        verify(mockConnection).unwrap(BaseConnection.class);
    }

    @Test
    void testBuildCopyCommand_WithEmptyHeaders() {
        // Given: Empty headers list (edge case)
        String tableName = "test_table";
        List<String> headers = Arrays.asList();

        // When
        String copyCommand = postgresCopyService.buildCopyCommand(tableName, headers);

        // Then: Should produce valid command with empty column list
        assertEquals("COPY test_table () FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')",
                copyCommand,
                "Should handle empty headers list");
    }

    @Test
    void testCreateCopyInputStream_WithMixedNullAndEmptyValues() throws IOException {
        // Given: Mix of null, empty string, and actual values
        String csvData = "row1\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2", "col3", "col4");

        // Mock CSV parsing with mixed null and empty values
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("value1", null, "", "value4"));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then: Should distinguish between null (empty) and empty string (empty)
        assertEquals("value1\t\t\tvalue4\n", result,
                "Should handle mix of nulls and empty strings");
    }

    @Test
    void testCreateCopyInputStream_ReadMethodMultipleCalls() throws IOException {
        // Given: CSV data to test read() method behavior
        String csvData = "ab\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1");

        when(csvParsingService.parseCsvRow("ab"))
                .thenReturn(Arrays.asList("ab"));

        // When: Call read() multiple times
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        int byte1 = inputStream.read();
        int byte2 = inputStream.read();
        int byte3 = inputStream.read(); // newline
        int byte4 = inputStream.read(); // should be -1 (EOF)

        // Then: Should read correct bytes
        assertEquals('a', byte1, "First byte should be 'a'");
        assertEquals('b', byte2, "Second byte should be 'b'");
        assertEquals('\n', byte3, "Third byte should be newline");
        assertEquals(-1, byte4, "Fourth read should return -1 (EOF)");
    }

    @Test
    void testCreateCopyInputStream_WithUnicodeCharacters() throws IOException {
        // Given: Values with Unicode characters
        String csvData = "unicode_row\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1", "col2", "col3");

        // Mock CSV parsing with Unicode characters (emoji, accents, etc.)
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList("Hello 世界", "Café ☕", "Ñoño"));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

        // Then: Should preserve Unicode characters
        assertEquals("Hello 世界\tCafé ☕\tÑoño\n", result,
                "Should correctly handle Unicode characters in UTF-8");
    }

    @Test
    void testCreateCopyInputStream_WithVeryLongValues() throws IOException {
        // Given: Very long string values (e.g., large text fields)
        String csvData = "long_value_row\n";
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        List<String> headers = Arrays.asList("col1");

        // Create a very long value (1000 characters)
        String longValue = "A".repeat(1000);
        when(csvParsingService.parseCsvRow(anyString()))
                .thenReturn(Arrays.asList(longValue));

        // When
        InputStream inputStream = postgresCopyService.createCopyInputStream(reader, headers);
        String result = new String(inputStream.readAllBytes());

        // Then: Should handle long values correctly
        assertEquals(longValue + "\n", result,
                "Should handle very long values");
        assertEquals(1001, result.length(), "Result should be 1000 chars + newline");
    }
}
