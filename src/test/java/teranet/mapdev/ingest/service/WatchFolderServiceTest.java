package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;
import teranet.mapdev.ingest.config.WatchFolderConfig;
import teranet.mapdev.ingest.dto.BatchProcessingResultDto;
import teranet.mapdev.ingest.model.IngestionManifest;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit tests for WatchFolderService
 * Tests watch folder functionality, file processing lifecycle, and error handling
 */
class WatchFolderServiceTest {

    @Mock
    private WatchFolderConfig config;

    @Mock
    private WatchFolderManager folderManager;

    @Mock
    private CsvProcessingService csvProcessingService;

    @Mock
    private BatchProcessingService batchProcessingService;

    @Mock
    private IngestionManifestService manifestService;

    @InjectMocks
    private WatchFolderService watchFolderService;

    @TempDir
    Path tempDir;

    private Path uploadDir;
    private Path wipDir;
    private Path errorDir;
    private Path archiveDir;

    private AutoCloseable mocks;

    @BeforeEach
    void setUp() throws IOException {
        // Initialize mocks
        mocks = MockitoAnnotations.openMocks(this);

        // Create test directories
        uploadDir = tempDir.resolve("upload");
        wipDir = tempDir.resolve("wip");
        errorDir = tempDir.resolve("error");
        archiveDir = tempDir.resolve("archive");

        Files.createDirectories(uploadDir);
        Files.createDirectories(wipDir);
        Files.createDirectories(errorDir);
        Files.createDirectories(archiveDir);

        // Configure mock behavior for WatchFolderConfig
        lenient().when(config.isEnabled()).thenReturn(true);
        lenient().when(config.getMarkerExtension()).thenReturn(".done");
        lenient().when(config.getPollingInterval()).thenReturn(100L);
        lenient().when(config.getMaxConcurrentFiles()).thenReturn(2);
        lenient().when(config.getSupportedExtensions()).thenReturn(List.of(".csv", ".zip"));
        lenient().when(config.isUseMarkerFiles()).thenReturn(true);
        lenient().when(config.getStabilityCheckDelay()).thenReturn(50L);
        lenient().when(config.isSupportedExtension(".csv")).thenReturn(true);
        lenient().when(config.isSupportedExtension(".zip")).thenReturn(true);
        lenient().when(config.isSupportedExtension(".txt")).thenReturn(false);
        lenient().when(config.getCsvFileNameFromMarker("orders.csv.done")).thenReturn("orders.csv");
        lenient().when(config.getCsvFileNameFromMarker("batch.zip.done")).thenReturn("batch.zip");
        lenient().when(config.getCsvFileNameFromMarker("invalid.done")).thenReturn("invalid.done");

        // Configure mock behavior for WatchFolderManager
        lenient().when(folderManager.getUploadPath()).thenReturn(uploadDir);
        lenient().when(folderManager.getWipPath()).thenReturn(wipDir);
        lenient().when(folderManager.getErrorPath()).thenReturn(errorDir);
        lenient().when(folderManager.getArchivePath()).thenReturn(archiveDir);
    }

    @AfterEach
    void tearDown() throws Exception {
        // Stop the service if running
        if (watchFolderService.isRunning()) {
            watchFolderService.stopWatching();
        }
        // Close mocks
        if (mocks != null) {
            mocks.close();
        }
    }

    // ===== SERVICE LIFECYCLE TESTS =====

    @Test
    void testStartWatching_WhenEnabled_StartsSuccessfully() throws Exception {
        // Act
        watchFolderService.startWatching();

        // Give service time to initialize
        Thread.sleep(200);

        // Assert
        assertThat(watchFolderService.isRunning()).isTrue();

        // Verify config was checked
        verify(config, atLeastOnce()).isEnabled();
    }

    @Test
    void testStartWatching_WhenDisabled_DoesNotStart() {
        // Arrange
        when(config.isEnabled()).thenReturn(false);

        // Act
        watchFolderService.startWatching();

        // Assert
        assertThat(watchFolderService.isRunning()).isFalse();
    }

    @Test
    void testStopWatching_WhenRunning_StopsSuccessfully() throws Exception {
        // Arrange
        watchFolderService.startWatching();
        Thread.sleep(200);
        assertThat(watchFolderService.isRunning()).isTrue();

        // Act
        watchFolderService.stopWatching();

        // Give service time to shutdown
        Thread.sleep(200);

        // Assert
        assertThat(watchFolderService.isRunning()).isFalse();
    }

    @Test
    void testIsRunning_InitialState_ReturnsFalse() {
        // Assert
        assertThat(watchFolderService.isRunning()).isFalse();
    }

    // ===== FILE EXTENSION TESTS =====

    @Test
    void testGetFileExtension_WithCsvFile_ReturnsExtension() throws Exception {
        // Use reflection to access private method
        String extension = (String) ReflectionTestUtils.invokeMethod(
                watchFolderService, "getFileExtension", "orders.csv");

        // Assert
        assertThat(extension).isEqualTo(".csv");
    }

    @Test
    void testGetFileExtension_WithZipFile_ReturnsExtension() throws Exception {
        // Use reflection to access private method
        String extension = (String) ReflectionTestUtils.invokeMethod(
                watchFolderService, "getFileExtension", "batch.zip");

        // Assert
        assertThat(extension).isEqualTo(".zip");
    }

    @Test
    void testGetFileExtension_WithMultipleDots_ReturnsLastExtension() throws Exception {
        // Use reflection to access private method
        String extension = (String) ReflectionTestUtils.invokeMethod(
                watchFolderService, "getFileExtension", "data.backup.csv");

        // Assert
        assertThat(extension).isEqualTo(".csv");
    }

    @Test
    void testGetFileExtension_WithNoExtension_ReturnsEmpty() throws Exception {
        // Use reflection to access private method
        String extension = (String) ReflectionTestUtils.invokeMethod(
                watchFolderService, "getFileExtension", "noextension");

        // Assert
        assertThat(extension).isEmpty();
    }

    // ===== FILE STABILITY TESTS =====

    @Test
    void testIsFileStable_WithStableFile_ReturnsTrue() throws Exception {
        // Arrange
        Path testFile = uploadDir.resolve("stable.csv");
        Files.writeString(testFile, "test content");

        // Wait a bit to ensure file is stable
        Thread.sleep(100);

        // Use reflection to access private method
        Boolean isStable = (Boolean) ReflectionTestUtils.invokeMethod(
                watchFolderService, "isFileStable", testFile);

        // Assert
        assertThat(isStable).isTrue();
    }

    @Test
    void testIsFileStable_WithNonExistentFile_ReturnsFalse() throws Exception {
        // Arrange
        Path nonExistentFile = uploadDir.resolve("does-not-exist.csv");

        // Use reflection to access private method
        Boolean isStable = (Boolean) ReflectionTestUtils.invokeMethod(
                watchFolderService, "isFileStable", nonExistentFile);

        // Assert
        assertThat(isStable).isFalse();
    }

    // ===== MULTIPART FILE CONVERSION TESTS =====

    @Test
    void testConvertToMultipartFile_WithCsvFile_CreatesValidMultipartFile() throws Exception {
        // Arrange
        Path csvFile = uploadDir.resolve("test.csv");
        String content = "id,name\n1,Test";
        Files.writeString(csvFile, content);

        // Use reflection to access private method
        MultipartFile multipartFile = (MultipartFile) ReflectionTestUtils.invokeMethod(
                watchFolderService, "convertToMultipartFile", csvFile);

        // Assert
        assertThat(multipartFile).isNotNull();
        assertThat(multipartFile.getOriginalFilename()).isEqualTo("test.csv");
        assertThat(multipartFile.getSize()).isEqualTo(content.length());
        assertThat(multipartFile.isEmpty()).isFalse();
        assertThat(new String(multipartFile.getBytes())).isEqualTo(content);
    }

    @Test
    void testConvertToMultipartFile_WithZipFile_CreatesValidMultipartFile() throws Exception {
        // Arrange
        Path zipFile = uploadDir.resolve("test.zip");
        byte[] zipContent = {0x50, 0x4B, 0x03, 0x04}; // ZIP magic number
        Files.write(zipFile, zipContent);

        // Use reflection to access private method
        MultipartFile multipartFile = (MultipartFile) ReflectionTestUtils.invokeMethod(
                watchFolderService, "convertToMultipartFile", zipFile);

        // Assert
        assertThat(multipartFile).isNotNull();
        assertThat(multipartFile.getOriginalFilename()).isEqualTo("test.zip");
        assertThat(multipartFile.getSize()).isEqualTo(zipContent.length);
    }

    @Test
    void testConvertToMultipartFile_WithEmptyFile_HandlesCorrectly() throws Exception {
        // Arrange
        Path emptyFile = uploadDir.resolve("empty.csv");
        Files.createFile(emptyFile);

        // Use reflection to access private method
        MultipartFile multipartFile = (MultipartFile) ReflectionTestUtils.invokeMethod(
                watchFolderService, "convertToMultipartFile", emptyFile);

        // Assert
        assertThat(multipartFile).isNotNull();
        assertThat(multipartFile.isEmpty()).isTrue();
        assertThat(multipartFile.getSize()).isEqualTo(0);
    }

    // ===== STACK TRACE TESTS =====

    @Test
    void testGetStackTraceAsString_WithException_ReturnsFormattedTrace() throws Exception {
        // Arrange
        Exception testException = new RuntimeException("Test error");

        // Use reflection to access private method
        String stackTrace = (String) ReflectionTestUtils.invokeMethod(
                watchFolderService, "getStackTraceAsString", testException);

        // Assert
        assertThat(stackTrace).isNotNull();
        assertThat(stackTrace).contains("RuntimeException: Test error");
        assertThat(stackTrace).contains("at ");
    }

    @Test
    void testGetStackTraceAsString_WithDeepStackTrace_TruncatesCorrectly() throws Exception {
        // Arrange - create exception with deep stack trace
        Exception deepException = createDeepStackTraceException(100);

        // Use reflection to access private method
        String stackTrace = (String) ReflectionTestUtils.invokeMethod(
                watchFolderService, "getStackTraceAsString", deepException);

        // Assert - should be truncated
        assertThat(stackTrace).isNotNull();
        assertThat(stackTrace.length()).isLessThanOrEqualTo(2200); // 2000 + buffer for truncation message + newlines
        if (stackTrace.contains("truncated")) {
            assertThat(stackTrace).contains("... (truncated)");
        }
    }

    // ===== CSV PROCESSING TESTS =====

    @Test
    void testProcessCsvFile_WithValidCsv_ProcessesSuccessfully() throws Exception {
        // Arrange
        Path csvFile = wipDir.resolve("orders.csv");
        Files.writeString(csvFile, "id,name\n1,Test");

        IngestionManifest successManifest = new IngestionManifest();
        successManifest.setStatus(IngestionManifest.Status.COMPLETED);
        successManifest.setTotalRecords(1L);

        when(csvProcessingService.processCsvToStaging(any(MultipartFile.class)))
                .thenReturn(successManifest);

        // Use reflection to access private method
        IngestionManifest result = (IngestionManifest) ReflectionTestUtils.invokeMethod(
                watchFolderService, "processCsvFile", csvFile);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo(IngestionManifest.Status.COMPLETED);
        verify(csvProcessingService).processCsvToStaging(any(MultipartFile.class));
    }

    @Test
    void testProcessCsvFile_WhenServiceThrowsException_ReturnsFailedManifest() throws Exception {
        // Arrange
        Path csvFile = wipDir.resolve("bad.csv");
        Files.writeString(csvFile, "invalid content");

        when(csvProcessingService.processCsvToStaging(any(MultipartFile.class)))
                .thenThrow(new RuntimeException("CSV parsing error"));

        // Use reflection to access private method
        IngestionManifest result = (IngestionManifest) ReflectionTestUtils.invokeMethod(
                watchFolderService, "processCsvFile", csvFile);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo(IngestionManifest.Status.FAILED);
        assertThat(result.getErrorMessage()).contains("CSV processing failed");
        assertThat(result.getErrorDetails()).contains("CSV parsing error");
    }

    // ===== ZIP PROCESSING TESTS =====

    @Test
    void testProcessZipFile_WithValidZip_ProcessesSuccessfully() throws Exception {
        // Arrange
        Path zipFile = wipDir.resolve("batch.zip");
        Files.write(zipFile, new byte[]{0x50, 0x4B, 0x03, 0x04});

        UUID batchId = UUID.randomUUID();
        BatchProcessingResultDto batchResult = new BatchProcessingResultDto();
        batchResult.setBatchId(batchId.toString());
        batchResult.setTotalFilesProcessed(2);
        batchResult.setProcessingStatus("SUCCESS");

        IngestionManifest parentManifest = new IngestionManifest();
        parentManifest.setBatchId(batchId);
        parentManifest.setStatus(IngestionManifest.Status.COMPLETED);

        when(batchProcessingService.processBatchFromZip(any(MultipartFile.class)))
                .thenReturn(batchResult);
        when(manifestService.findByBatchId(batchId)).thenReturn(parentManifest);

        // Use reflection to access private method
        IngestionManifest result = (IngestionManifest) ReflectionTestUtils.invokeMethod(
                watchFolderService, "processZipFile", zipFile);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo(IngestionManifest.Status.COMPLETED);
        verify(batchProcessingService).processBatchFromZip(any(MultipartFile.class));
        verify(manifestService).findByBatchId(batchId);
    }

    @Test
    void testProcessZipFile_WhenServiceThrowsException_ReturnsFailedManifest() throws Exception {
        // Arrange
        Path zipFile = wipDir.resolve("corrupt.zip");
        Files.write(zipFile, new byte[]{0x00, 0x00, 0x00, 0x00});

        when(batchProcessingService.processBatchFromZip(any(MultipartFile.class)))
                .thenThrow(new RuntimeException("Invalid ZIP format"));

        // Use reflection to access private method
        IngestionManifest result = (IngestionManifest) ReflectionTestUtils.invokeMethod(
                watchFolderService, "processZipFile", zipFile);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo(IngestionManifest.Status.FAILED);
        assertThat(result.getErrorMessage()).contains("ZIP processing failed");
        assertThat(result.getErrorDetails()).contains("Invalid ZIP format");
    }

    @Test
    void testProcessZipFile_WhenManifestNotFound_ReturnsFailedManifest() throws Exception {
        // Arrange
        Path zipFile = wipDir.resolve("batch.zip");
        Files.write(zipFile, new byte[]{0x50, 0x4B, 0x03, 0x04});

        UUID batchId = UUID.randomUUID();
        BatchProcessingResultDto batchResult = new BatchProcessingResultDto();
        batchResult.setBatchId(batchId.toString());

        when(batchProcessingService.processBatchFromZip(any(MultipartFile.class)))
                .thenReturn(batchResult);
        when(manifestService.findByBatchId(batchId)).thenReturn(null);

        // Use reflection to access private method
        IngestionManifest result = (IngestionManifest) ReflectionTestUtils.invokeMethod(
                watchFolderService, "processZipFile", zipFile);

        // Assert - processZipFile catches the exception and returns a FAILED manifest
        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo(IngestionManifest.Status.FAILED);
        assertThat(result.getErrorMessage()).contains("ZIP processing failed");
        assertThat(result.getErrorMessage()).contains("Parent manifest not found");
    }

    // ===== FILE PROCESSING INTEGRATION TESTS =====

    @Test
    void testProcessFile_WithCsv_Success_MovesToArchive() throws Exception {
        // Arrange
        Path uploadedFile = uploadDir.resolve("orders.csv");
        Files.writeString(uploadedFile, "id,name\n1,Test");

        Path wipFile = wipDir.resolve("orders.csv");
        Path archivedFile = archiveDir.resolve("orders_2024-01-01_12-00-00.csv");

        IngestionManifest successManifest = new IngestionManifest();
        successManifest.setStatus(IngestionManifest.Status.COMPLETED);
        successManifest.setBatchId(UUID.randomUUID());
        successManifest.setTotalRecords(1L);

        when(folderManager.moveToWip(uploadedFile)).thenReturn(wipFile);
        when(folderManager.moveToArchive(wipFile)).thenReturn(archivedFile);
        when(csvProcessingService.processCsvToStaging(any(MultipartFile.class)))
                .thenReturn(successManifest);

        // Use reflection to access private method
        ReflectionTestUtils.invokeMethod(watchFolderService, "processFile", uploadedFile, "orders.csv");

        // Assert
        verify(folderManager).moveToWip(uploadedFile);
        verify(folderManager).moveToArchive(wipFile);
        verify(folderManager).deleteMarkerFileFromUpload("orders.csv");
        verify(csvProcessingService).processCsvToStaging(any(MultipartFile.class));
    }

    @Test
    void testProcessFile_WithCsv_Failure_MovesToError() throws Exception {
        // Arrange
        Path uploadedFile = uploadDir.resolve("bad.csv");
        Files.writeString(uploadedFile, "invalid");

        Path wipFile = wipDir.resolve("bad.csv");
        Path errorFile = errorDir.resolve("bad.csv");

        IngestionManifest failedManifest = new IngestionManifest();
        failedManifest.setStatus(IngestionManifest.Status.FAILED);
        failedManifest.setErrorMessage("Validation failed");
        failedManifest.setErrorDetails("Invalid CSV format");

        when(folderManager.moveToWip(uploadedFile)).thenReturn(wipFile);
        when(folderManager.moveToError(eq(wipFile), anyString(), anyString(), isNull()))
                .thenReturn(errorFile);
        when(csvProcessingService.processCsvToStaging(any(MultipartFile.class)))
                .thenReturn(failedManifest);

        // Use reflection to access private method
        ReflectionTestUtils.invokeMethod(watchFolderService, "processFile", uploadedFile, "bad.csv");

        // Assert
        verify(folderManager).moveToWip(uploadedFile);
        verify(folderManager).moveToError(eq(wipFile), eq("Validation failed"), eq("Invalid CSV format"), isNull());
        verify(folderManager).deleteMarkerFileFromUpload("bad.csv");
    }

    @Test
    void testProcessFile_WithZip_Success_MovesToArchive() throws Exception {
        // Arrange
        Path uploadedFile = uploadDir.resolve("batch.zip");
        Files.write(uploadedFile, new byte[]{0x50, 0x4B, 0x03, 0x04});

        Path wipFile = wipDir.resolve("batch.zip");
        Path archivedFile = archiveDir.resolve("batch_2024-01-01_12-00-00.zip");

        UUID batchId = UUID.randomUUID();
        BatchProcessingResultDto batchResult = new BatchProcessingResultDto();
        batchResult.setBatchId(batchId.toString());
        batchResult.setTotalFilesProcessed(2);

        IngestionManifest parentManifest = new IngestionManifest();
        parentManifest.setStatus(IngestionManifest.Status.COMPLETED);
        parentManifest.setBatchId(batchId);

        when(folderManager.moveToWip(uploadedFile)).thenReturn(wipFile);
        when(folderManager.moveToArchive(wipFile)).thenReturn(archivedFile);
        when(batchProcessingService.processBatchFromZip(any(MultipartFile.class)))
                .thenReturn(batchResult);
        when(manifestService.findByBatchId(batchId)).thenReturn(parentManifest);

        // Use reflection to access private method
        ReflectionTestUtils.invokeMethod(watchFolderService, "processFile", uploadedFile, "batch.zip");

        // Assert
        verify(folderManager).moveToWip(uploadedFile);
        verify(folderManager).moveToArchive(wipFile);
        verify(folderManager).deleteMarkerFileFromUpload("batch.zip");
        verify(batchProcessingService).processBatchFromZip(any(MultipartFile.class));
    }

    @Test
    void testProcessFile_WithException_MovesToError() throws Exception {
        // Arrange
        Path uploadedFile = uploadDir.resolve("error.csv");
        Files.writeString(uploadedFile, "test");

        Path wipFile = wipDir.resolve("error.csv");
        Path errorFile = errorDir.resolve("error.csv");

        when(folderManager.moveToWip(uploadedFile)).thenReturn(wipFile);
        when(csvProcessingService.processCsvToStaging(any(MultipartFile.class)))
                .thenThrow(new RuntimeException("Critical error"));
        when(folderManager.moveToError(eq(wipFile), anyString(), anyString(), isNull()))
                .thenReturn(errorFile);

        // Use reflection to access private method
        ReflectionTestUtils.invokeMethod(watchFolderService, "processFile", uploadedFile, "error.csv");

        // Assert
        verify(folderManager).moveToWip(uploadedFile);
        // processCsvFile catches the exception and creates a FAILED manifest
        // processFile then moves to error with the manifest's error message
        verify(folderManager).moveToError(eq(wipFile), contains("CSV processing failed"), 
                contains("Critical error"), isNull());
        verify(folderManager).deleteMarkerFileFromUpload("error.csv");
    }

    @Test
    void testProcessFile_WithUnsupportedExtension_ThrowsException() throws Exception {
        // Arrange
        Path uploadedFile = uploadDir.resolve("document.txt");
        Files.writeString(uploadedFile, "not supported");

        Path wipFile = wipDir.resolve("document.txt");
        Path errorFile = errorDir.resolve("document.txt");

        // Create WIP file so that Files.exists check passes in catch block
        when(folderManager.moveToWip(uploadedFile)).thenAnswer(invocation -> {
            Files.copy(uploadedFile, wipFile);
            return wipFile;
        });
        when(folderManager.moveToError(eq(wipFile), eq("Processing exception"), anyString(), any(IllegalArgumentException.class)))
                .thenReturn(errorFile);

        // Use reflection to access private method
        ReflectionTestUtils.invokeMethod(watchFolderService, "processFile", uploadedFile, "document.txt");

        // Assert - should move to error due to unsupported file type
        verify(folderManager).moveToWip(uploadedFile);
        verify(folderManager).moveToError(eq(wipFile), eq("Processing exception"), 
                eq("Unsupported file type: .txt"), any(IllegalArgumentException.class));
        verify(folderManager).deleteMarkerFileFromUpload("document.txt");
    }

    // ===== HELPER METHODS =====

    /**
     * Helper method to create an exception with a deep stack trace
     */
    private Exception createDeepStackTraceException(int depth) {
        if (depth == 0) {
            return new RuntimeException("Deep stack trace exception");
        }
        try {
            throw createDeepStackTraceException(depth - 1);
        } catch (Exception e) {
            return e;
        }
    }
}
