package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TableNamingService
 * Tests table name generation and sanitization
 */
class TableNamingServiceTest {

    private TableNamingService tableNamingService = new TableNamingService();

    // ========== generateTableNameFromFile Tests ==========

    @Test
    void testGenerateTableNameFromFile_SimpleCase() {
        // Given: Simple CSV file
        String fileName = "orders.csv";
        UUID batchId = UUID.randomUUID(); // Not used anymore

        // When: Generate table name
        String tableName = tableNamingService.generateTableNameFromFile(fileName, batchId);

        // Then: Should return sanitized filename without prefix
        assertEquals("orders", tableName);
    }

    @Test
    void testGenerateTableNameFromFile_WithGzExtension() {
        // Given: Gzipped CSV file
        String fileName = "products.csv.gz";
        UUID batchId = UUID.randomUUID(); // Not used anymore

        // When: Generate table name
        String tableName = tableNamingService.generateTableNameFromFile(fileName, batchId);

        // Then: Should remove .gz extension
        assertEquals("products", tableName);
    }

    @Test
    void testGenerateTableNameFromFile_WithZipExtension() {
        // Given: Zipped file
        String fileName = "data.zip";
        UUID batchId = UUID.randomUUID(); // Not used anymore

        // When: Generate table name
        String tableName = tableNamingService.generateTableNameFromFile(fileName, batchId);

        // Then: Should remove .zip extension
        assertEquals("data", tableName);
    }

    @Test
    void testGenerateTableNameFromFile_WithSpecialCharacters() {
        // Given: Filename with special characters
        String fileName = "customer-data@2024.csv";
        UUID batchId = UUID.randomUUID(); // Not used anymore

        // When: Generate table name
        String tableName = tableNamingService.generateTableNameFromFile(fileName, batchId);

        // Then: Should sanitize special characters
        assertEquals("customer_data_2024", tableName);
    }

    @Test
    void testGenerateTableNameFromFile_LongFileName() {
        // Given: Very long filename (should be truncated to fit PostgreSQL 63 char limit)
        String fileName = "this_is_a_very_long_file_name_that_exceeds_the_postgresql_limit.csv";
        UUID batchId = UUID.randomUUID(); // Not used anymore

        // When: Generate table name
        String tableName = tableNamingService.generateTableNameFromFile(fileName, batchId);

        // Then: Should be truncated but valid
        assertTrue(tableName.length() <= 63);
        assertTrue(tableName.startsWith("this_is_a_very_long_file_name"));
    }

    @Test
    void testGenerateTableNameFromFile_ExactlyAtLimit() {
        // Given: Filename that results in exactly 63 characters
        String fileName = "a".repeat(63) + ".csv";
        UUID batchId = UUID.randomUUID(); // Not used anymore

        // When: Generate table name
        String tableName = tableNamingService.generateTableNameFromFile(fileName, batchId);

        // Then: Should be exactly 63 characters
        assertEquals(63, tableName.length());
        assertEquals("a".repeat(63), tableName);
    }

    @Test
    void testGenerateTableNameFromFile_NullFileName() {
        // Given: Null filename
        String fileName = null;
        UUID batchId = UUID.randomUUID();

        // When/Then: Should throw IllegalArgumentException
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableNamingService.generateTableNameFromFile(fileName, batchId);
        });
        
        // Verify error message
        assertEquals("Filename cannot be null. Please provide a valid filename.", exception.getMessage());
    }

    @Test
    void testGenerateTableNameFromFile_EmptyAfterSanitization() {
        // Given: Filename that becomes empty after sanitization
        String fileName = "@@@###.csv";
        UUID batchId = UUID.randomUUID(); // Not used anymore

        // When: Generate table name
        String tableName = tableNamingService.generateTableNameFromFile(fileName, batchId);

        // Then: Should use default "csv_data"
        assertEquals("csv_data", tableName);
    }

    @Test
    void testGenerateTableNameFromFile_UppercaseExtension() {
        // Given: Uppercase extension
        String fileName = "DATA.CSV";
        UUID batchId = UUID.randomUUID(); // Not used anymore

        // When: Generate table name
        String tableName = tableNamingService.generateTableNameFromFile(fileName, batchId);

        // Then: Should convert to lowercase and remove extension
        assertEquals("data", tableName);
    }

    @Test
    void testGenerateTableNameFromFile_MultipleUnderscores() {
        // Given: Filename with multiple underscores
        String fileName = "test___data___file.csv";
        UUID batchId = UUID.randomUUID(); // Not used anymore

        // When: Generate table name
        String tableName = tableNamingService.generateTableNameFromFile(fileName, batchId);

        // Then: Should collapse multiple underscores
        assertEquals("test_data_file", tableName);
    }

    // ========== sanitizeTableName Tests ==========

    @Test
    void testSanitizeTableName_Simple() {
        // Given/When/Then
        assertEquals("customer_orders", tableNamingService.sanitizeTableName("Customer-Orders"));
        assertEquals("product_data", tableNamingService.sanitizeTableName("PRODUCT__DATA"));
        assertEquals("test_table", tableNamingService.sanitizeTableName("_test_table_"));
    }

    @Test
    void testSanitizeTableName_SpecialCharacters() {
        // Given/When/Then
        assertEquals("123_abc_xyz", tableNamingService.sanitizeTableName("123-ABC-XYZ"));
        assertEquals("sales_2024_csv", tableNamingService.sanitizeTableName("sales@2024.csv"));
        assertEquals("order", tableNamingService.sanitizeTableName("Order#"));
        assertEquals("email_domain", tableNamingService.sanitizeTableName("Email@Domain"));
    }

    @Test
    void testSanitizeTableName_MultipleUnderscores() {
        // Given/When/Then: Should collapse multiple underscores
        assertEquals("test_name", tableNamingService.sanitizeTableName("test___name"));
        assertEquals("test_name", tableNamingService.sanitizeTableName("test__name"));
        assertEquals("a_b_c", tableNamingService.sanitizeTableName("a___b___c"));
    }

    @Test
    void testSanitizeTableName_LeadingTrailingUnderscores() {
        // Given/When/Then: Should remove leading/trailing underscores
        assertEquals("test", tableNamingService.sanitizeTableName("__test__"));
        assertEquals("test", tableNamingService.sanitizeTableName("_test_"));
        assertEquals("test", tableNamingService.sanitizeTableName("___test___"));
    }

    @Test
    void testSanitizeTableName_Uppercase() {
        // Given/When/Then: Should convert to lowercase
        assertEquals("customer_name", tableNamingService.sanitizeTableName("CUSTOMER_NAME"));
        assertEquals("first_name", tableNamingService.sanitizeTableName("FIRST-NAME"));
        assertEquals("abc", tableNamingService.sanitizeTableName("ABC"));
    }

    @Test
    void testSanitizeTableName_AlreadySanitized() {
        // Given/When/Then: Already clean names should remain unchanged
        assertEquals("customer_name", tableNamingService.sanitizeTableName("customer_name"));
        assertEquals("order_id", tableNamingService.sanitizeTableName("order_id"));
        assertEquals("test123", tableNamingService.sanitizeTableName("test123"));
    }

    @Test
    void testSanitizeTableName_OnlySpecialCharacters() {
        // Given/When/Then: Only special characters should result in empty string
        assertEquals("", tableNamingService.sanitizeTableName("@#$%^&*()"));
        assertEquals("", tableNamingService.sanitizeTableName("---"));
        assertEquals("", tableNamingService.sanitizeTableName("..."));
    }

    @Test
    void testSanitizeTableName_NullOrEmpty() {
        // Given/When/Then: Null or empty should return empty string
        assertEquals("", tableNamingService.sanitizeTableName(null));
        assertEquals("", tableNamingService.sanitizeTableName(""));
        assertEquals("", tableNamingService.sanitizeTableName("   "));
    }

    @Test
    void testSanitizeTableName_NumbersAllowed() {
        // Given/When/Then: Numbers should be preserved
        assertEquals("table123", tableNamingService.sanitizeTableName("table123"));
        assertEquals("123table", tableNamingService.sanitizeTableName("123table"));
        assertEquals("12345", tableNamingService.sanitizeTableName("12345"));
    }

    @Test
    void testSanitizeTableName_MixedCase() {
        // Given/When/Then
        assertEquals("camelcase", tableNamingService.sanitizeTableName("CamelCase"));
        assertEquals("mixedcase123", tableNamingService.sanitizeTableName("MixedCase123"));
        assertEquals("test_name", tableNamingService.sanitizeTableName("Test-Name"));
    }
}
