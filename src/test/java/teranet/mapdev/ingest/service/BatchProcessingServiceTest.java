package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;
import teranet.mapdev.ingest.dto.BatchProcessingResultDto;
import teranet.mapdev.ingest.dto.ZipAnalysisDto;
import teranet.mapdev.ingest.dto.ZipAnalysisDto.ExtractedFileInfo;
import teranet.mapdev.ingest.model.IngestionManifest;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class BatchProcessingServiceTest {

    @Mock
    private ZipProcessingService zipProcessingService;
    @Mock
    private CsvProcessingService csvProcessingService;
    @Mock
    private IngestionManifestService manifestService;
    @Mock
    private TableNamingService tableNamingService;

    @InjectMocks
    private BatchProcessingService batchProcessingService;

    @TempDir
    Path tempDir;
    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    void testProcessBatchFromZip_Success() throws Exception {
        UUID parentBatchId = UUID.randomUUID();
        IngestionManifest zipManifest = createManifest(parentBatchId, "batch.zip");
        IngestionManifest csv1 = createManifest(UUID.randomUUID(), "f1.csv");
        csv1.setTotalRecords(1L);

        when(manifestService.findByChecksum(anyString())).thenReturn(null);
        when(manifestService.save(any())).thenReturn(zipManifest);
        when(zipProcessingService.analyzeZipFile(any())).thenReturn(createAnalysis(List.of("f1.csv")));
        when(csvProcessingService.processCsvToStaging(any(), any())).thenReturn(csv1);
        when(tableNamingService.generateTableNameFromFile(anyString(), any())).thenReturn("staging_f1_abc");

        MockMultipartFile zip = createZip("test.zip", Map.of("f1.csv", "id,name\n1,A"));
        BatchProcessingResultDto result = batchProcessingService.processBatchFromZip(zip);

        assertThat(result.getProcessingStatus()).isEqualTo("SUCCESS");
        assertThat(result.getTotalFilesProcessed()).isEqualTo(1);
        verify(csvProcessingService).processCsvToStaging(any(), any());
    }

    @Test
    void testProcessBatchFromZip_Duplicate() throws Exception {
        IngestionManifest existing = createCompletedManifest(UUID.randomUUID(), "dup.zip");
        when(manifestService.findByChecksum(anyString())).thenReturn(existing);

        MockMultipartFile zip = createZip("dup.zip", Map.of("f.csv", "data"));
        BatchProcessingResultDto result = batchProcessingService.processBatchFromZip(zip);

        assertThat(result.getProcessingStatus()).isEqualTo("ALREADY_PROCESSED");
        verify(csvProcessingService, never()).processCsvToStaging(any(), any());
    }

    @Test
    void testProcessBatchFromFiles_Success() throws Exception {
        IngestionManifest m1 = createManifest(UUID.randomUUID(), "f1.csv");
        m1.setTotalRecords(1L);

        when(csvProcessingService.processCsvToStaging(any(), isNull())).thenReturn(m1);
        when(tableNamingService.generateTableNameFromFile(anyString(), any())).thenReturn("staging_f1");

        List<MultipartFile> files = List.of(
                new MockMultipartFile("f", "f1.csv", "text/csv", "id\n1".getBytes()));

        BatchProcessingResultDto result = batchProcessingService.processBatchFromFiles(files);

        assertThat(result.getProcessingStatus()).isEqualTo("SUCCESS");
        assertThat(result.getTotalFilesProcessed()).isEqualTo(1);
    }

    @Test
    void testProcessBatchFromZip_ExtractionFailure() throws Exception {
        ZipAnalysisDto failedAnalysis = new ZipAnalysisDto();
        failedAnalysis.setExtractionStatus("FAILED");

        UUID parentBatchId = UUID.randomUUID();
        IngestionManifest zipManifest = createManifest(parentBatchId, "bad.zip");

        when(manifestService.findByChecksum(anyString())).thenReturn(null);
        when(manifestService.save(any())).thenReturn(zipManifest);
        when(zipProcessingService.analyzeZipFile(any())).thenReturn(failedAnalysis);

        MockMultipartFile zip = createZip("bad.zip", Map.of());
        BatchProcessingResultDto result = batchProcessingService.processBatchFromZip(zip);

        assertThat(result.getProcessingStatus()).isEqualTo("FAILED");
        verify(manifestService).update(argThat(m -> m.getStatus() == IngestionManifest.Status.FAILED));
    }

    @Test
    void testProcessBatchFromZip_WithMultipleFiles() throws Exception {
        UUID parentBatchId = UUID.randomUUID();
        IngestionManifest zipManifest = createManifest(parentBatchId, "multi.zip");

        IngestionManifest csv1 = createManifest(UUID.randomUUID(), "f1.csv");
        csv1.setTotalRecords(10L);
        IngestionManifest csv2 = createManifest(UUID.randomUUID(), "f2.csv");
        csv2.setTotalRecords(20L);

        when(manifestService.findByChecksum(anyString())).thenReturn(null);
        when(manifestService.save(any())).thenReturn(zipManifest);
        when(zipProcessingService.analyzeZipFile(any()))
                .thenReturn(createAnalysis(List.of("f1.csv", "f2.csv")));
        when(csvProcessingService.processCsvToStaging(any(), any()))
                .thenReturn(csv1, csv2);
        when(tableNamingService.generateTableNameFromFile(anyString(), any()))
                .thenReturn("staging_f1_abc", "staging_f2_abc");

        MockMultipartFile zip = createZip("multi.zip", Map.of(
                "f1.csv", "id,name\n1,A",
                "f2.csv", "id,val\n2,B"));

        BatchProcessingResultDto result = batchProcessingService.processBatchFromZip(zip);

        assertThat(result.getProcessingStatus()).isEqualTo("SUCCESS");
        assertThat(result.getTotalFilesProcessed()).isEqualTo(2);
        assertThat(result.getTotalRowsLoaded()).isEqualTo(30L);
        assertThat(result.getSuccessfulFiles()).isEqualTo(2);
    }

    @Test
    void testProcessBatchFromZip_PartialFailure() throws Exception {
        UUID parentBatchId = UUID.randomUUID();
        IngestionManifest zipManifest = createManifest(parentBatchId, "partial.zip");

        IngestionManifest csv1 = createManifest(UUID.randomUUID(), "good.csv");
        csv1.setTotalRecords(5L);

        when(manifestService.findByChecksum(anyString())).thenReturn(null);
        when(manifestService.save(any())).thenReturn(zipManifest);
        when(zipProcessingService.analyzeZipFile(any()))
                .thenReturn(createAnalysis(List.of("good.csv", "bad.csv")));
        when(csvProcessingService.processCsvToStaging(any(), any()))
                .thenReturn(csv1)
                .thenThrow(new RuntimeException("Parse error"));
        when(tableNamingService.generateTableNameFromFile(anyString(), any()))
                .thenReturn("staging_good", "staging_bad");

        MockMultipartFile zip = createZip("partial.zip", Map.of(
                "good.csv", "id\n1",
                "bad.csv", "malformed"));

        BatchProcessingResultDto result = batchProcessingService.processBatchFromZip(zip);

        assertThat(result.getProcessingStatus()).isEqualTo("PARTIAL_SUCCESS");
        assertThat(result.getSuccessfulFiles()).isEqualTo(1);
        assertThat(result.getFailedFiles()).isEqualTo(1);
    }

    @Test
    void testProcessBatchFromFiles_WithFailure() throws Exception {
        IngestionManifest m1 = createManifest(UUID.randomUUID(), "good.csv");
        m1.setTotalRecords(5L);

        when(csvProcessingService.processCsvToStaging(any(), isNull()))
                .thenReturn(m1)
                .thenThrow(new RuntimeException("Processing failed"));
        when(tableNamingService.generateTableNameFromFile(anyString(), any()))
                .thenReturn("staging_good", "staging_bad");

        List<MultipartFile> files = List.of(
                new MockMultipartFile("f1", "good.csv", "text/csv", "id\n1".getBytes()),
                new MockMultipartFile("f2", "bad.csv", "text/csv", "bad".getBytes()));

        BatchProcessingResultDto result = batchProcessingService.processBatchFromFiles(files);

        assertThat(result.getProcessingStatus()).isEqualTo("PARTIAL_SUCCESS");
        assertThat(result.getSuccessfulFiles()).isEqualTo(1);
        assertThat(result.getFailedFiles()).isEqualTo(1);
        assertThat(result.getTotalFilesProcessed()).isEqualTo(2);
    }

    @Test
    void testProcessBatchFromFiles_AllFailed() throws Exception {
        when(csvProcessingService.processCsvToStaging(any(), isNull()))
                .thenThrow(new RuntimeException("Processing failed"));
        when(tableNamingService.generateTableNameFromFile(anyString(), any()))
                .thenReturn("staging_fail");

        List<MultipartFile> files = List.of(
                new MockMultipartFile("f", "fail.csv", "text/csv", "bad".getBytes()));

        BatchProcessingResultDto result = batchProcessingService.processBatchFromFiles(files);

        assertThat(result.getProcessingStatus()).isEqualTo("FAILED");
        assertThat(result.getFailedFiles()).isEqualTo(1);
        assertThat(result.getSuccessfulFiles()).isEqualTo(0);
    }

    @Test
    void testProcessBatchFromZip_GeneralException() throws Exception {
        when(manifestService.findByChecksum(anyString()))
                .thenThrow(new RuntimeException("Database error"));

        MockMultipartFile zip = createZip("error.zip", Map.of("f.csv", "data"));
        BatchProcessingResultDto result = batchProcessingService.processBatchFromZip(zip);

        assertThat(result.getProcessingStatus()).isEqualTo("FAILED");
        assertThat(result.getTotalFilesProcessed()).isEqualTo(0);
    }

    @Test
    void testProcessBatchFromZip_EmptyFile() throws Exception {
        UUID parentBatchId = UUID.randomUUID();
        IngestionManifest zipManifest = createManifest(parentBatchId, "empty.zip");

        IngestionManifest emptyCsv = createManifest(UUID.randomUUID(), "empty.csv");
        emptyCsv.setTotalRecords(0L);

        when(manifestService.findByChecksum(anyString())).thenReturn(null);
        when(manifestService.save(any())).thenReturn(zipManifest);
        when(zipProcessingService.analyzeZipFile(any()))
                .thenReturn(createAnalysis(List.of("empty.csv")));
        when(csvProcessingService.processCsvToStaging(any(), any()))
                .thenReturn(emptyCsv);
        when(tableNamingService.generateTableNameFromFile(anyString(), any()))
                .thenReturn("staging_empty");

        MockMultipartFile zip = createZip("empty.zip", Map.of("empty.csv", "id\n"));
        BatchProcessingResultDto result = batchProcessingService.processBatchFromZip(zip);

        assertThat(result.getProcessingStatus()).isEqualTo("SUCCESS");
        assertThat(result.getTotalRowsLoaded()).isEqualTo(0L);
        assertThat(result.getValidationSummary().getTablesRequiringReview()).isGreaterThan(0);
    }

    @Test
    void testProcessBatchFromFiles_DuplicateFile() throws Exception {
        IngestionManifest duplicate = createManifest(UUID.randomUUID(), "dup.csv");
        duplicate.setTotalRecords(10L);
        duplicate.setAlreadyProcessed(true);
        duplicate.setCompletedAt(LocalDateTime.now());

        when(csvProcessingService.processCsvToStaging(any(), isNull()))
                .thenReturn(duplicate);
        when(tableNamingService.generateTableNameFromFile(anyString(), any()))
                .thenReturn("staging_dup");

        List<MultipartFile> files = List.of(
                new MockMultipartFile("f", "dup.csv", "text/csv", "id\n1".getBytes()));

        BatchProcessingResultDto result = batchProcessingService.processBatchFromFiles(files);

        assertThat(result.getTotalFilesProcessed()).isEqualTo(1);
        assertThat(result.getFileResults().get(0).getStatus()).isEqualTo("DUPLICATE");
    }

    @Test
    void testProcessBatchFromZip_AllDuplicates() throws Exception {
        UUID parentBatchId = UUID.randomUUID();
        IngestionManifest zipManifest = createManifest(parentBatchId, "alldup.zip");

        IngestionManifest dup1 = createManifest(UUID.randomUUID(), "dup1.csv");
        dup1.setTotalRecords(5L);
        dup1.setAlreadyProcessed(true);

        IngestionManifest dup2 = createManifest(UUID.randomUUID(), "dup2.csv");
        dup2.setTotalRecords(3L);
        dup2.setAlreadyProcessed(true);

        when(manifestService.findByChecksum(anyString())).thenReturn(null);
        when(manifestService.save(any())).thenReturn(zipManifest);
        when(zipProcessingService.analyzeZipFile(any()))
                .thenReturn(createAnalysis(List.of("dup1.csv", "dup2.csv")));
        when(csvProcessingService.processCsvToStaging(any(), any()))
                .thenReturn(dup1, dup2);
        when(tableNamingService.generateTableNameFromFile(anyString(), any()))
                .thenReturn("staging_dup1", "staging_dup2");

        MockMultipartFile zip = createZip("alldup.zip", Map.of(
                "dup1.csv", "id\n1",
                "dup2.csv", "id\n2"));

        BatchProcessingResultDto result = batchProcessingService.processBatchFromZip(zip);

        assertThat(result.getProcessingStatus()).isEqualTo("ALL_DUPLICATES");
        assertThat(result.getSuccessfulFiles()).isEqualTo(0);
        assertThat(result.getTotalFilesProcessed()).isEqualTo(2);
    }

    private MockMultipartFile createZip(String name, Map<String, String> files) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            for (Map.Entry<String, String> e : files.entrySet()) {
                zos.putNextEntry(new ZipEntry(e.getKey()));
                zos.write(e.getValue().getBytes());
                zos.closeEntry();
            }
        }
        return new MockMultipartFile("file", name, "application/zip", baos.toByteArray());
    }

    private ZipAnalysisDto createAnalysis(List<String> names) {
        ZipAnalysisDto a = new ZipAnalysisDto();
        a.setExtractionStatus("SUCCESS");
        List<ExtractedFileInfo> files = new ArrayList<>();
        for (String n : names) {
            ExtractedFileInfo f = new ExtractedFileInfo();
            f.setFilename(n);
            f.setFileType("CSV");
            f.setFileSize(100L);
            files.add(f);
        }
        a.setExtractedFiles(files);
        return a;
    }

    private IngestionManifest createManifest(UUID id, String name) {
        IngestionManifest m = new IngestionManifest();
        m.setBatchId(id);
        m.setFileName(name);
        m.setStatus(IngestionManifest.Status.PROCESSING);
        m.setFileSizeBytes(100L);
        m.setFileChecksum("abc");
        m.setAlreadyProcessed(false);
        return m;
    }

    private IngestionManifest createCompletedManifest(UUID id, String name) {
        IngestionManifest m = createManifest(id, name);
        m.setStatus(IngestionManifest.Status.COMPLETED);
        m.setCompletedAt(LocalDateTime.now());
        m.setTotalRecords(10L);
        m.setProcessingDurationMs(1000L);
        return m;
    }
}
