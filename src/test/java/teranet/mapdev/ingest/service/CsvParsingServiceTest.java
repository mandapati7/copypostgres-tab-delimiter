package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockMultipartFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CsvParsingService
 * Tests CSV parsing, header extraction, and column name sanitization
 */
class CsvParsingServiceTest {

    private CsvParsingService csvParsingService;

    @Mock
    private FileChecksumService fileChecksumService;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        csvParsingService = new CsvParsingService();

        // Use reflection to inject the mock FileChecksumService
        Field field = CsvParsingService.class.getDeclaredField("fileChecksumService");
        field.setAccessible(true);
        field.set(csvParsingService, fileChecksumService);
    }

    // ========== extractCsvHeaders Tests ==========

    @Test
    void testExtractCsvHeaders_Success() throws IOException {
        // Given: CSV file with valid headers
        String csvContent = "name,age,city\nJohn,30,NYC\n";
        MockMultipartFile file = createMockFile("test.csv", csvContent);

        when(fileChecksumService.getDecompressedInputStream(any()))
                .thenReturn(new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8)));

        // When: Extract headers
        List<String> headers = csvParsingService.extractCsvHeaders(file);

        // Then: Should return sanitized headers
        assertNotNull(headers);
        assertEquals(3, headers.size());
        assertEquals("name", headers.get(0));
        assertEquals("age", headers.get(1));
        assertEquals("city", headers.get(2));
    }

    @Test
    void testExtractCsvHeaders_WithSpecialCharacters() throws IOException {
        // Given: CSV headers with special characters
        String csvContent = "Customer Name,Order#,Email@Domain,Price ($)\nJohn,123,test@test.com,100\n";
        MockMultipartFile file = createMockFile("test.csv", csvContent);

        when(fileChecksumService.getDecompressedInputStream(any()))
                .thenReturn(new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8)));

        // When: Extract headers
        List<String> headers = csvParsingService.extractCsvHeaders(file);

        // Then: Should sanitize special characters
        assertNotNull(headers);
        assertEquals(4, headers.size());
        assertEquals("customer_name", headers.get(0));
        assertEquals("order", headers.get(1));
        assertEquals("email_domain", headers.get(2));
        assertEquals("price", headers.get(3));
    }

    @Test
    void testExtractCsvHeaders_EmptyFile() throws IOException {
        // Given: Empty CSV file
        String csvContent = "";
        MockMultipartFile file = createMockFile("empty.csv", csvContent);

        when(fileChecksumService.getDecompressedInputStream(any()))
                .thenReturn(new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8)));

        // When/Then: Should throw exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            csvParsingService.extractCsvHeaders(file);
        });

        assertEquals("CSV file is empty or has no header", exception.getMessage());
    }

    @Test
    void testExtractCsvHeaders_OnlyWhitespace() throws IOException {
        // Given: CSV file with only whitespace
        String csvContent = "   \n  \n";
        MockMultipartFile file = createMockFile("whitespace.csv", csvContent);

        when(fileChecksumService.getDecompressedInputStream(any()))
                .thenReturn(new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8)));

        // When/Then: Should throw exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            csvParsingService.extractCsvHeaders(file);
        });

        assertEquals("CSV file is empty or has no header", exception.getMessage());
    }

    @Test
    void testExtractCsvHeaders_WithQuotedFields() throws IOException {
        // Given: CSV headers with quoted fields
        String csvContent = "\"First Name\",\"Last Name\",\"Age\"\nJohn,Smith,30\n";
        MockMultipartFile file = createMockFile("test.csv", csvContent);

        when(fileChecksumService.getDecompressedInputStream(any()))
                .thenReturn(new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8)));

        // When: Extract headers
        List<String> headers = csvParsingService.extractCsvHeaders(file);

        // Then: Should handle quoted fields
        assertNotNull(headers);
        assertEquals(3, headers.size());
        assertEquals("first_name", headers.get(0));
        assertEquals("last_name", headers.get(1));
        assertEquals("age", headers.get(2));
    }

    // ========== parseCsvRow Tests ==========

    @Test
    void testParseCsvRow_SimpleRow() {
        // Given: Simple CSV row
        String csvLine = "John,30,NYC";

        // When: Parse row
        List<String> values = csvParsingService.parseCsvRow(csvLine);

        // Then: Should return 3 values
        assertNotNull(values);
        assertEquals(3, values.size());
        assertEquals("John", values.get(0));
        assertEquals("30", values.get(1));
        assertEquals("NYC", values.get(2));
    }

    @Test
    void testParseCsvRow_WithQuotedFields() {
        // Given: CSV row with quoted fields
        String csvLine = "John,\"Smith, Jr.\",30";

        // When: Parse row
        List<String> values = csvParsingService.parseCsvRow(csvLine);

        // Then: Should handle comma inside quotes
        assertNotNull(values);
        assertEquals(3, values.size());
        assertEquals("John", values.get(0));
        assertEquals("Smith, Jr.", values.get(1));
        assertEquals("30", values.get(2));
    }

    @Test
    void testParseCsvRow_WithEscapedQuotes() {
        // Given: CSV row with escaped quotes (double quotes)
        String csvLine = "\"O'Reilly\",\"Author \"\"John\"\" Doe\",50";

        // When: Parse row
        List<String> values = csvParsingService.parseCsvRow(csvLine);

        // Then: Should handle escaped quotes
        assertNotNull(values);
        assertEquals(3, values.size());
        assertEquals("O'Reilly", values.get(0));
        assertEquals("Author \"John\" Doe", values.get(1));
        assertEquals("50", values.get(2));
    }

    @Test
    void testParseCsvRow_EmptyFields() {
        // Given: CSV row with empty fields
        String csvLine = "John,,NYC";

        // When: Parse row
        List<String> values = csvParsingService.parseCsvRow(csvLine);

        // Then: Should preserve empty fields
        assertNotNull(values);
        assertEquals(3, values.size());
        assertEquals("John", values.get(0));
        assertEquals("", values.get(1));
        assertEquals("NYC", values.get(2));
    }

    @Test
    void testParseCsvRow_AllEmptyFields() {
        // Given: CSV row with all empty fields
        String csvLine = ",,";

        // When: Parse row
        List<String> values = csvParsingService.parseCsvRow(csvLine);

        // Then: Should return 3 empty values
        assertNotNull(values);
        assertEquals(3, values.size());
        assertEquals("", values.get(0));
        assertEquals("", values.get(1));
        assertEquals("", values.get(2));
    }

    @Test
    void testParseCsvRow_SingleValue() {
        // Given: CSV row with single value
        String csvLine = "John";

        // When: Parse row
        List<String> values = csvParsingService.parseCsvRow(csvLine);

        // Then: Should return single value
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals("John", values.get(0));
    }

    @Test
    void testParseCsvRow_WithWhitespace() {
        // Given: CSV row with extra whitespace
        String csvLine = " John , 30 , NYC ";

        // When: Parse row
        List<String> values = csvParsingService.parseCsvRow(csvLine);

        // Then: Should trim whitespace
        assertNotNull(values);
        assertEquals(3, values.size());
        assertEquals("John", values.get(0));
        assertEquals("30", values.get(1));
        assertEquals("NYC", values.get(2));
    }

    // ========== sanitizeColumnName Tests ==========

    @Test
    void testSanitizeColumnName_Simple() {
        // Given/When/Then
        assertEquals("customer_name", csvParsingService.sanitizeColumnName("Customer Name"));
        assertEquals("order_id", csvParsingService.sanitizeColumnName("Order ID"));
        assertEquals("price", csvParsingService.sanitizeColumnName("Price"));
    }

    @Test
    void testSanitizeColumnName_SpecialCharacters() {
        // Given/When/Then: Replace special characters with underscore
        assertEquals("order", csvParsingService.sanitizeColumnName("Order#"));
        assertEquals("email_domain", csvParsingService.sanitizeColumnName("Email@Domain"));
        assertEquals("price", csvParsingService.sanitizeColumnName("Price ($)"));
        assertEquals("customer_id", csvParsingService.sanitizeColumnName("Customer-ID"));
    }

    @Test
    void testSanitizeColumnName_MultipleUnderscores() {
        // Given/When/Then: Collapse multiple underscores
        assertEquals("test_name", csvParsingService.sanitizeColumnName("test___name"));
        assertEquals("test_name", csvParsingService.sanitizeColumnName("test__name"));
    }

    @Test
    void testSanitizeColumnName_LeadingTrailingUnderscores() {
        // Given/When/Then: Remove leading/trailing underscores
        assertEquals("test", csvParsingService.sanitizeColumnName("__test__"));
        assertEquals("test", csvParsingService.sanitizeColumnName("_test_"));
    }

    @Test
    void testSanitizeColumnName_StartsWithNumber() {
        // Given/When/Then: Prepend "col_" if starts with number
        assertEquals("col_123abc", csvParsingService.sanitizeColumnName("123abc"));
        assertEquals("col_1st_place", csvParsingService.sanitizeColumnName("1st Place"));
    }

    @Test
    void testSanitizeColumnName_Uppercase() {
        // Given/When/Then: Convert to lowercase
        assertEquals("customer_name", csvParsingService.sanitizeColumnName("CUSTOMER_NAME"));
        assertEquals("first_name", csvParsingService.sanitizeColumnName("FIRST-NAME"));
    }

    @Test
    void testSanitizeColumnName_AlreadySanitized() {
        // Given/When/Then: Already clean names should remain unchanged
        assertEquals("customer_name", csvParsingService.sanitizeColumnName("customer_name"));
        assertEquals("order_id", csvParsingService.sanitizeColumnName("order_id"));
    }

    // Helper methods

    private MockMultipartFile createMockFile(String filename, String content) {
        return new MockMultipartFile(
                "file",
                filename,
                "text/csv",
                content.getBytes(StandardCharsets.UTF_8));
    }
}
