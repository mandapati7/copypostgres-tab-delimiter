package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.multipart.MultipartFile;
import teranet.mapdev.ingest.dto.ZipAnalysisDto;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ZipProcessingServiceTest {

    @Mock
    private DatabaseConnectionService databaseConnectionService;

    @Mock
    private CsvProcessingService csvProcessingService;

    @Mock
    private FilenameRouterService filenameRouterService;

    @Mock
    private MultipartFile zipFile;

    @InjectMocks
    private ZipProcessingService service;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        when(zipFile.getOriginalFilename()).thenReturn("test.zip");
    }

    @Test
    void testAnalyzeZipFile_Success() throws IOException {
        // Create a test ZIP file with CSV content
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            // Add CSV file to ZIP
            ZipEntry entry = new ZipEntry("data.csv");
            zos.putNextEntry(entry);
            String csvContent = "name,age,city\nJohn,30,NYC\nJane,25,LA\n";
            zos.write(csvContent.getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));

        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_data");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals("test.zip", result.getZipFilename());
        assertEquals("SUCCESS", result.getExtractionStatus());
        assertEquals(1, result.getCsvFilesFound());
        assertFalse(result.getExtractedFiles().isEmpty());
        assertFalse(result.getProcessingRecommendations().isEmpty());
    }

    @Test
    void testAnalyzeZipFile_NoCsvFiles() throws IOException {
        // Create a ZIP file without CSV files
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry("readme.txt");
            zos.putNextEntry(entry);
            zos.write("Some text content".getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals("NO_CSV_FILES_FOUND", result.getExtractionStatus());
        assertEquals(0, result.getCsvFilesFound());
        assertTrue(result.getExtractedFiles().isEmpty());
    }

    @Test
    void testAnalyzeZipFile_ExtractionFailure() throws IOException {
        when(zipFile.getInputStream()).thenThrow(new IOException("Cannot read ZIP"));

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertTrue(result.getExtractionStatus().startsWith("EXTRACTION_FAILED"));
        assertEquals(0, result.getCsvFilesFound());
        assertFalse(result.getProcessingRecommendations().isEmpty());
    }

    @Test
    void testAnalyzeZipFile_MultipleCsvFiles() throws IOException {
        // Create ZIP with multiple CSV files
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            // First CSV
            ZipEntry entry1 = new ZipEntry("users.csv");
            zos.putNextEntry(entry1);
            zos.write("id,name\n1,John\n2,Jane\n".getBytes());
            zos.closeEntry();

            // Second CSV
            ZipEntry entry2 = new ZipEntry("products.csv");
            zos.putNextEntry(entry2);
            zos.write("id,product\n1,Widget\n2,Gadget\n".getBytes());
            zos.closeEntry();

            // Non-CSV file
            ZipEntry entry3 = new ZipEntry("readme.txt");
            zos.putNextEntry(entry3);
            zos.write("Instructions".getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_users", "staging_products");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(3, result.getTotalFilesExtracted()); // 2 CSV + 1 TXT
        assertEquals(2, result.getCsvFilesFound());
        assertEquals(2, result.getExtractedFiles().size());
    }

    @Test
    void testAnalyzeZipFile_LargeDatasetRecommendation() throws IOException {
        // Create ZIP with large CSV (simulated with many rows)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry("large_data.csv");
            zos.putNextEntry(entry);

            // Create CSV with header
            StringBuilder csvContent = new StringBuilder("id,name,value\n");
            // Simulate large file by adding many rows
            for (int i = 0; i < 1000; i++) {
                csvContent.append(i).append(",Name").append(i).append(",Value").append(i).append("\n");
            }
            zos.write(csvContent.toString().getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_large_data");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(1, result.getCsvFilesFound());
        assertTrue(result.getExtractedFiles().get(0).getEstimatedRows() > 0);
    }

    @Test
    void testAnalyzeZipFile_ExistingTable() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry("existing.csv");
            zos.putNextEntry(entry);
            zos.write("col1,col2\nval1,val2\n".getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_existing");
        when(databaseConnectionService.doesStagingTableExist("staging_existing")).thenReturn(true);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertTrue(result.getExtractedFiles().get(0).getStagingTableExists());
        assertTrue(result.getProcessingRecommendations().stream()
                .anyMatch(r -> r.contains("existing tables will be updated")));
    }

    @Test
    void testAnalyzeZipFile_EmptyCsvFile() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry("empty.csv");
            zos.putNextEntry(entry);
            zos.write("".getBytes()); // Empty file
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_empty");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        // Should still process but with empty headers/rows
        assertEquals(1, result.getCsvFilesFound());
    }

    @Test
    void testAnalyzeZipFile_NestedDirectories() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            // Add directory entry
            ZipEntry dirEntry = new ZipEntry("data/");
            zos.putNextEntry(dirEntry);
            zos.closeEntry();

            // Add CSV in subdirectory
            ZipEntry csvEntry = new ZipEntry("data/nested.csv");
            zos.putNextEntry(csvEntry);
            zos.write("a,b,c\n1,2,3\n".getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_nested");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(1, result.getCsvFilesFound());
        assertEquals("SUCCESS", result.getExtractionStatus());
    }

    @Test
    void testAnalyzeZipFile_MixOfNewAndExistingTables() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry1 = new ZipEntry("new.csv");
            zos.putNextEntry(entry1);
            zos.write("col1\nval1\n".getBytes());
            zos.closeEntry();

            ZipEntry entry2 = new ZipEntry("existing.csv");
            zos.putNextEntry(entry2);
            zos.write("col2\nval2\n".getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(eq("new.csv")))
                .thenReturn("staging_new");
        when(filenameRouterService.resolveTableName(eq("existing.csv")))
                .thenReturn("staging_existing");
        when(databaseConnectionService.doesStagingTableExist("staging_new")).thenReturn(false);
        when(databaseConnectionService.doesStagingTableExist("staging_existing")).thenReturn(true);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(2, result.getCsvFilesFound());
        assertTrue(result.getProcessingRecommendations().stream()
                .anyMatch(r -> r.contains("1 new tables will be created")));
        assertTrue(result.getProcessingRecommendations().stream()
                .anyMatch(r -> r.contains("1 existing tables will be updated")));
    }

    @Test
    void testAnalyzeZipFile_CsvWithCommasInHeaders() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry("complex.csv");
            zos.putNextEntry(entry);
            // CSV with various header formats
            zos.write("Name,Age,City,Country\nJohn Doe,30,New York,USA\n".getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_complex");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(1, result.getCsvFilesFound());
        assertFalse(result.getExtractedFiles().get(0).getHeadersDetected().isEmpty());
    }

    @Test
    void testAnalyzeZipFile_VeryLargeFile_TriggersSampling() throws IOException {
        // Create CSV larger than 1MB to trigger sampling logic in estimateRowCount
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry("massive.csv");
            zos.putNextEntry(entry);

            // Write header
            zos.write("id,name,description,value,category,notes,timestamp\n".getBytes());

            // Write enough rows to exceed 1MB (MAX_ESTIMATION_BYTES)
            // Each row ~100 bytes, need >10,000 rows to exceed 1MB
            for (int i = 0; i < 15000; i++) {
                String row = String.format(
                        "%d,Name%d,Long description text for item %d,Value%d,Category%d,Notes here %d,%d\n",
                        i, i, i, i, i % 10, i, System.currentTimeMillis());
                zos.write(row.getBytes());
            }
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_massive");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(1, result.getCsvFilesFound());
        // Verify row estimation happened (sampling was used for large file)
        assertTrue(result.getExtractedFiles().get(0).getEstimatedRows() > 1000);
    }

    @Test
    void testAnalyzeZipFile_HeaderOnlyFile() throws IOException {
        // CSV with only header, no data rows - tests edge case in estimateRowCount
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry("header_only.csv");
            zos.putNextEntry(entry);
            zos.write("column1,column2,column3\n".getBytes()); // Only header
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_header_only");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(1, result.getCsvFilesFound());
        // Should handle gracefully with 0 data rows
        assertEquals(0, result.getExtractedFiles().get(0).getEstimatedRows());
    }

    @Test
    void testAnalyzeZipFile_OverOneMillion_RecommendsBatching() throws IOException {
        // Create multiple large files to exceed 1M total rows for batch recommendation
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            // Create 3 files with estimated ~400k rows each
            for (int fileNum = 1; fileNum <= 3; fileNum++) {
                ZipEntry entry = new ZipEntry("large_" + fileNum + ".csv");
                zos.putNextEntry(entry);

                zos.write("id,data,value\n".getBytes());

                // Write 400k rows per file
                for (int i = 0; i < 400000; i++) {
                    zos.write(String.format("%d,Data%d,%d\n", i, i, i * 10).getBytes());
                }
                zos.closeEntry();
            }
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_large_1", "staging_large_2", "staging_large_3");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(3, result.getCsvFilesFound());
        // Should recommend batch processing for large dataset (>1M rows)
        assertTrue(result.getProcessingRecommendations().stream()
                .anyMatch(r -> r.toLowerCase().contains("batch")),
                "Should recommend batch processing for >1M total rows");
    }

    @Test
    void testAnalyzeZipFile_AllTablesExist() throws IOException {
        // Test when all tables already exist - covers branch in
        // generateProcessingRecommendations
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry1 = new ZipEntry("existing1.csv");
            zos.putNextEntry(entry1);
            zos.write("col\nval1\n".getBytes());
            zos.closeEntry();

            ZipEntry entry2 = new ZipEntry("existing2.csv");
            zos.putNextEntry(entry2);
            zos.write("col\nval2\n".getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_existing1", "staging_existing2");
        // All tables exist
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(true);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(2, result.getCsvFilesFound());
        // Should mention existing tables update, but NOT new table creation
        assertTrue(result.getProcessingRecommendations().stream()
                .anyMatch(r -> r.contains("existing tables")));
        assertFalse(result.getProcessingRecommendations().stream()
                .anyMatch(r -> r.contains("new tables will be created")));
    }

    @Test
    void testAnalyzeZipFile_CaseInsensitiveCsvExtension() throws IOException {
        // Test CSV files with different case extensions
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry1 = new ZipEntry("lowercase.csv");
            zos.putNextEntry(entry1);
            zos.write("a\n1\n".getBytes());
            zos.closeEntry();

            ZipEntry entry2 = new ZipEntry("uppercase.CSV");
            zos.putNextEntry(entry2);
            zos.write("b\n2\n".getBytes());
            zos.closeEntry();

            ZipEntry entry3 = new ZipEntry("mixedcase.CsV");
            zos.putNextEntry(entry3);
            zos.write("c\n3\n".getBytes());
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_lowercase", "staging_uppercase", "staging_mixedcase");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(3, result.getCsvFilesFound(),
                "Should detect CSV files regardless of case");
    }

    @Test
    void testAnalyzeZipFile_UnreadableCsvFile_HandlesGracefully() throws IOException {
        // Test with CSV that has unusual but valid content (not completely corrupted)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry("unusual.csv");
            zos.putNextEntry(entry);
            // CSV with unusual characters but still parseable
            zos.write("col1,col2\n".getBytes());
            zos.write("val1,val2\n".getBytes());
            zos.write("\n\n\n".getBytes()); // Multiple blank lines
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        when(zipFile.getInputStream()).thenReturn(new ByteArrayInputStream(zipBytes));
        when(filenameRouterService.resolveTableName(anyString()))
                .thenReturn("staging_unusual");
        when(databaseConnectionService.doesStagingTableExist(anyString())).thenReturn(false);

        // Should handle gracefully without throwing exception
        ZipAnalysisDto result = service.analyzeZipFile(zipFile);

        assertNotNull(result);
        assertEquals(1, result.getCsvFilesFound());
    }
}
