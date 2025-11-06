package teranet.mapdev.ingest.util;

import org.springframework.mock.web.MockMultipartFile;
import teranet.mapdev.ingest.model.IngestionManifest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Utility class for creating test data and mock objects.
 * Provides reusable test fixtures for CSV files, ZIP files, and manifests.
 */
public class TestDataFactory {
    
    // CSV Test Data
    public static final String VALID_CSV_HEADER = "order_id,customer_name,product_name,quantity,price,order_date";
    public static final String VALID_CSV_ROW_1 = "1001,John Doe,Laptop,1,1200.00,2024-01-15";
    public static final String VALID_CSV_ROW_2 = "1002,Jane Smith,Mouse,2,25.50,2024-01-16";
    public static final String VALID_CSV_ROW_3 = "1003,Bob Johnson,Keyboard,1,75.00,2024-01-17";
    
    public static final String INVALID_CSV_MISSING_COLUMNS = "order_id,customer_name\n1001,John Doe";
    public static final String INVALID_CSV_WRONG_TYPES = "order_id,customer_name,product_name,quantity,price,order_date\n"
            + "ABC,John Doe,Laptop,NOT_A_NUMBER,1200.00,2024-01-15";
    
    /**
     * Creates a valid CSV file with sample order data.
     */
    public static MockMultipartFile createValidCsvFile(String filename) {
        String content = String.join("\n",
                VALID_CSV_HEADER,
                VALID_CSV_ROW_1,
                VALID_CSV_ROW_2,
                VALID_CSV_ROW_3
        );
        return new MockMultipartFile(
                "file",
                filename,
                "text/csv",
                content.getBytes(StandardCharsets.UTF_8)
        );
    }
    
    /**
     * Creates a valid CSV file with custom number of rows.
     */
    public static MockMultipartFile createValidCsvFile(String filename, int numRows) {
        StringBuilder content = new StringBuilder(VALID_CSV_HEADER).append("\n");
        for (int i = 1; i <= numRows; i++) {
            content.append(String.format("%d,Customer %d,Product %d,1,100.00,2024-01-%02d\n", 
                    1000 + i, i, i, (i % 28) + 1));
        }
        return new MockMultipartFile(
                "file",
                filename,
                "text/csv",
                content.toString().getBytes(StandardCharsets.UTF_8)
        );
    }
    
    /**
     * Creates an empty CSV file (only header).
     */
    public static MockMultipartFile createEmptyCsvFile(String filename) {
        return new MockMultipartFile(
                "file",
                filename,
                "text/csv",
                VALID_CSV_HEADER.getBytes(StandardCharsets.UTF_8)
        );
    }
    
    /**
     * Creates a completely empty CSV file (no content).
     */
    public static MockMultipartFile createCompletelyEmptyFile(String filename) {
        return new MockMultipartFile(
                "file",
                filename,
                "text/csv",
                new byte[0]
        );
    }
    
    /**
     * Creates a CSV file with invalid format (missing required columns).
     */
    public static MockMultipartFile createInvalidCsvFileMissingColumns(String filename) {
        return new MockMultipartFile(
                "file",
                filename,
                "text/csv",
                INVALID_CSV_MISSING_COLUMNS.getBytes(StandardCharsets.UTF_8)
        );
    }
    
    /**
     * Creates a CSV file with invalid data types.
     */
    public static MockMultipartFile createInvalidCsvFileWrongTypes(String filename) {
        return new MockMultipartFile(
                "file",
                filename,
                "text/csv",
                INVALID_CSV_WRONG_TYPES.getBytes(StandardCharsets.UTF_8)
        );
    }
    
    /**
     * Creates a ZIP file containing multiple CSV files.
     */
    public static MockMultipartFile createZipFileWithCsvs(String zipFilename, String... csvFilenames) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            for (String csvFilename : csvFilenames) {
                ZipEntry entry = new ZipEntry(csvFilename);
                zos.putNextEntry(entry);
                
                String csvContent = String.join("\n",
                        VALID_CSV_HEADER,
                        VALID_CSV_ROW_1,
                        VALID_CSV_ROW_2
                );
                zos.write(csvContent.getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        
        return new MockMultipartFile(
                "file",
                zipFilename,
                "application/zip",
                baos.toByteArray()
        );
    }
    
    /**
     * Creates a ZIP file with mixed valid and invalid CSV files.
     */
    public static MockMultipartFile createZipFileWithMixedCsvs(String zipFilename) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            // Valid CSV
            ZipEntry entry1 = new ZipEntry("valid_orders.csv");
            zos.putNextEntry(entry1);
            String validCsv = String.join("\n", VALID_CSV_HEADER, VALID_CSV_ROW_1);
            zos.write(validCsv.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            
            // Empty CSV
            ZipEntry entry2 = new ZipEntry("empty_file.csv");
            zos.putNextEntry(entry2);
            zos.write(new byte[0]);
            zos.closeEntry();
            
            // Invalid CSV
            ZipEntry entry3 = new ZipEntry("invalid_file.csv");
            zos.putNextEntry(entry3);
            zos.write(INVALID_CSV_MISSING_COLUMNS.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }
        
        return new MockMultipartFile(
                "file",
                zipFilename,
                "application/zip",
                baos.toByteArray()
        );
    }
    
    /**
     * Creates an empty ZIP file.
     */
    public static MockMultipartFile createEmptyZipFile(String zipFilename) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            // Create empty ZIP
        }
        return new MockMultipartFile(
                "file",
                zipFilename,
                "application/zip",
                baos.toByteArray()
        );
    }
    
    /**
     * Creates a corrupted ZIP file.
     */
    public static MockMultipartFile createCorruptedZipFile(String zipFilename) {
        return new MockMultipartFile(
                "file",
                zipFilename,
                "application/zip",
                "NOT_A_VALID_ZIP_FILE".getBytes(StandardCharsets.UTF_8)
        );
    }
    
    /**
     * Creates a test IngestionManifest with default values.
     */
    public static IngestionManifest createTestManifest(String filename) {
        IngestionManifest manifest = new IngestionManifest();
        manifest.setBatchId(UUID.randomUUID());
        manifest.setFileName(filename);
        manifest.setFilePath("/test/path/" + filename);
        manifest.setFileSizeBytes(1024L);
        manifest.setFileChecksum("abc123def456");
        manifest.setContentType("text/csv");
        manifest.setStatus(IngestionManifest.Status.PENDING);
        manifest.setTotalRecords(0L);
        manifest.setProcessedRecords(0L);
        manifest.setFailedRecords(0L);
        manifest.setCreatedBy("test-user");
        return manifest;
    }
    
    /**
     * Creates a completed test IngestionManifest.
     */
    public static IngestionManifest createCompletedManifest(String filename, long totalRecords) {
        IngestionManifest manifest = createTestManifest(filename);
        manifest.setStatus(IngestionManifest.Status.COMPLETED);
        manifest.setTotalRecords(totalRecords);
        manifest.setProcessedRecords(totalRecords);
        manifest.setFailedRecords(0L);
        manifest.setStartedAt(LocalDateTime.now().minusMinutes(5));
        manifest.setCompletedAt(LocalDateTime.now());
        manifest.setProcessingDurationMs(300000L);
        manifest.setTableName("staging_orders_" + UUID.randomUUID().toString().substring(0, 8));
        return manifest;
    }
    
    /**
     * Creates a failed test IngestionManifest.
     */
    public static IngestionManifest createFailedManifest(String filename, String errorMessage) {
        IngestionManifest manifest = createTestManifest(filename);
        manifest.setStatus(IngestionManifest.Status.FAILED);
        manifest.setErrorMessage(errorMessage);
        manifest.setErrorDetails("{\"error\":\"" + errorMessage + "\"}");
        manifest.setStartedAt(LocalDateTime.now().minusMinutes(1));
        manifest.setCompletedAt(LocalDateTime.now());
        return manifest;
    }
    
    /**
     * Creates a processing test IngestionManifest.
     */
    public static IngestionManifest createProcessingManifest(String filename) {
        IngestionManifest manifest = createTestManifest(filename);
        manifest.setStatus(IngestionManifest.Status.PROCESSING);
        manifest.setStartedAt(LocalDateTime.now());
        return manifest;
    }
}
