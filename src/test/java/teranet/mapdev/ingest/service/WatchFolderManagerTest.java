package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import teranet.mapdev.ingest.config.WatchFolderConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive test suite for WatchFolderManager
 * Target: 85%+ code coverage
 * 
 * Tests cover:
 * - Folder initialization
 * - File movement between folders (upload -> WIP -> archive/error)
 * - Timestamped file naming
 * - Error report generation
 * - Marker file management
 */
class WatchFolderManagerTest {

    @TempDir
    Path tempDir;

    @Mock
    private WatchFolderConfig config;

    private WatchFolderManager watchFolderManager;

    private Path uploadDir;
    private Path wipDir;
    private Path errorDir;
    private Path archiveDir;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        
        // Create temp directories
        uploadDir = tempDir.resolve("upload");
        wipDir = tempDir.resolve("wip");
        errorDir = tempDir.resolve("error");
        archiveDir = tempDir.resolve("archive");
        
        // Mock config
        when(config.isEnabled()).thenReturn(true);
        when(config.getUpload()).thenReturn(uploadDir.toString());
        when(config.getWip()).thenReturn(wipDir.toString());
        when(config.getError()).thenReturn(errorDir.toString());
        when(config.getArchive()).thenReturn(archiveDir.toString());
        when(config.getMarkerFileName(anyString())).thenAnswer(inv -> inv.getArgument(0) + ".done");
        
        // Create service
        watchFolderManager = new WatchFolderManager();
        org.springframework.test.util.ReflectionTestUtils.setField(
            watchFolderManager, "config", config);
    }

    @AfterEach
    void tearDown() {
        // Cleanup is handled by @TempDir
    }

    // ==================== Folder Initialization Tests ====================

    @Test
    void testInitializeFolders_Success_CreatesAllFolders() {
        // Act
        watchFolderManager.initializeFolders();
        
        // Assert
        assertThat(Files.exists(uploadDir)).isTrue();
        assertThat(Files.exists(wipDir)).isTrue();
        assertThat(Files.exists(errorDir)).isTrue();
        assertThat(Files.exists(archiveDir)).isTrue();
        
        assertThat(Files.isDirectory(uploadDir)).isTrue();
        assertThat(Files.isDirectory(wipDir)).isTrue();
        assertThat(Files.isDirectory(errorDir)).isTrue();
        assertThat(Files.isDirectory(archiveDir)).isTrue();
    }

    @Test
    void testInitializeFolders_WhenDisabled_SkipsCreation() {
        // Arrange
        when(config.isEnabled()).thenReturn(false);
        
        // Act
        watchFolderManager.initializeFolders();
        
        // Assert
        assertThat(Files.exists(uploadDir)).isFalse();
        assertThat(Files.exists(wipDir)).isFalse();
        assertThat(Files.exists(errorDir)).isFalse();
        assertThat(Files.exists(archiveDir)).isFalse();
    }

    @Test
    void testInitializeFolders_WhenFoldersExist_DoesNotFail() throws IOException {
        // Arrange - Create folders beforehand
        Files.createDirectories(uploadDir);
        Files.createDirectories(wipDir);
        Files.createDirectories(errorDir);
        Files.createDirectories(archiveDir);
        
        // Act & Assert - Should not throw
        assertThatCode(() -> watchFolderManager.initializeFolders())
            .doesNotThrowAnyException();
        
        assertThat(Files.exists(uploadDir)).isTrue();
    }

    // ==================== File Movement Tests ====================

    @Test
    void testMoveToWip_Success_MovesFileCorrectly() throws IOException {
        // Arrange
        Files.createDirectories(uploadDir);
        Files.createDirectories(wipDir);
        
        Path uploadFile = uploadDir.resolve("test.csv");
        Files.writeString(uploadFile, "order_id,customer\n1,John\n");
        
        // Act
        Path wipFile = watchFolderManager.moveToWip(uploadFile);
        
        // Assert
        assertThat(Files.exists(uploadFile)).isFalse(); // Original moved
        assertThat(Files.exists(wipFile)).isTrue();
        assertThat(wipFile.getParent()).isEqualTo(wipDir);
        assertThat(wipFile.getFileName().toString()).isEqualTo("test.csv");
        assertThat(Files.readString(wipFile)).contains("order_id,customer");
    }

    @Test
    void testMoveToWip_ReplacesExistingFile() throws IOException {
        // Arrange
        Files.createDirectories(uploadDir);
        Files.createDirectories(wipDir);
        
        Path uploadFile = uploadDir.resolve("test.csv");
        Files.writeString(uploadFile, "new content");
        
        // Create existing file in WIP with old content
        Path existingWipFile = wipDir.resolve("test.csv");
        Files.writeString(existingWipFile, "old content");
        
        // Act
        Path wipFile = watchFolderManager.moveToWip(uploadFile);
        
        // Assert
        assertThat(Files.exists(wipFile)).isTrue();
        assertThat(Files.readString(wipFile)).isEqualTo("new content");
    }

    @Test
    void testMoveToArchive_Success_AddsTimestamp() throws IOException {
        // Arrange
        Files.createDirectories(wipDir);
        Files.createDirectories(archiveDir);
        
        Path wipFile = wipDir.resolve("orders.csv");
        Files.writeString(wipFile, "order_id\n1\n");
        
        // Act
        Path archiveFile = watchFolderManager.moveToArchive(wipFile);
        
        // Assert
        assertThat(Files.exists(wipFile)).isFalse(); // Original moved
        assertThat(Files.exists(archiveFile)).isTrue();
        assertThat(archiveFile.getParent()).isEqualTo(archiveDir);
        assertThat(archiveFile.getFileName().toString()).startsWith("orders_");
        assertThat(archiveFile.getFileName().toString()).endsWith(".csv");
        assertThat(archiveFile.getFileName().toString()).matches("orders_\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}\\.csv");
    }

    @Test
    void testMoveToError_Success_CreatesErrorReport() throws IOException {
        // Arrange
        Files.createDirectories(wipDir);
        Files.createDirectories(errorDir);
        
        Path wipFile = wipDir.resolve("bad.csv");
        Files.writeString(wipFile, "invalid data");
        
        Exception testException = new RuntimeException("Test error");
        
        // Act
        Path errorFile = watchFolderManager.moveToError(
            wipFile, 
            "Processing failed", 
            "Invalid CSV format", 
            testException
        );
        
        // Assert
        assertThat(Files.exists(wipFile)).isFalse(); // Original moved
        assertThat(Files.exists(errorFile)).isTrue();
        assertThat(errorFile.getParent()).isEqualTo(errorDir);
        assertThat(errorFile.getFileName().toString()).startsWith("bad_");
        assertThat(errorFile.getFileName().toString()).endsWith(".csv");
        
        // Check error report file exists
        Path errorReportFile = errorDir.resolve(errorFile.getFileName().toString() + ".error.json");
        assertThat(Files.exists(errorReportFile)).isTrue();
        
        String errorReportContent = Files.readString(errorReportFile);
        assertThat(errorReportContent).contains("Processing failed");
        assertThat(errorReportContent).contains("Invalid CSV format");
        assertThat(errorReportContent).contains("RuntimeException");
        assertThat(errorReportContent).contains("bad.csv");
    }

    @Test
    void testMoveToError_WithoutException_CreatesReportWithoutStackTrace() throws IOException {
        // Arrange
        Files.createDirectories(wipDir);
        Files.createDirectories(errorDir);
        
        Path wipFile = wipDir.resolve("error.csv");
        Files.writeString(wipFile, "data");
        
        // Act
        Path errorFile = watchFolderManager.moveToError(
            wipFile, 
            "Validation failed", 
            "Missing required columns", 
            null
        );
        
        // Assert
        assertThat(Files.exists(errorFile)).isTrue();
        
        Path errorReportFile = errorDir.resolve(errorFile.getFileName().toString() + ".error.json");
        assertThat(Files.exists(errorReportFile)).isTrue();
        
        String errorReportContent = Files.readString(errorReportFile);
        assertThat(errorReportContent).contains("Validation failed");
        assertThat(errorReportContent).contains("Missing required columns");
    }

    // ==================== Timestamp Tests ====================

    @Test
    void testCreateTimestampedFileName_WithExtension_AddsTimestamp() {
        // Act
        String timestamped = watchFolderManager.createTimestampedFileName("orders.csv");
        
        // Assert
        assertThat(timestamped).startsWith("orders_");
        assertThat(timestamped).endsWith(".csv");
        assertThat(timestamped).matches("orders_\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}\\.csv");
    }

    @Test
    void testCreateTimestampedFileName_WithoutExtension_AddsTimestamp() {
        // Act
        String timestamped = watchFolderManager.createTimestampedFileName("datafile");
        
        // Assert
        assertThat(timestamped).startsWith("datafile_");
        assertThat(timestamped).matches("datafile_\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}");
    }

    @Test
    void testCreateTimestampedFileName_MultipleExtensions_HandlesCorrectly() {
        // Act
        String timestamped = watchFolderManager.createTimestampedFileName("archive.tar.gz");
        
        // Assert
        assertThat(timestamped).startsWith("archive.tar_");
        assertThat(timestamped).endsWith(".gz");
    }

    // ==================== Marker File Tests ====================

    @Test
    void testDeleteMarkerFile_WhenExists_DeletesSuccessfully() throws IOException {
        // Arrange
        Files.createDirectories(wipDir);
        
        Path csvFile = wipDir.resolve("test.csv");
        Path markerFile = wipDir.resolve("test.csv.done");
        Files.writeString(csvFile, "data");
        Files.writeString(markerFile, "");
        
        // Act
        watchFolderManager.deleteMarkerFile(csvFile);
        
        // Assert
        assertThat(Files.exists(markerFile)).isFalse();
        assertThat(Files.exists(csvFile)).isTrue(); // CSV file still exists
    }

    @Test
    void testDeleteMarkerFile_WhenNotExists_DoesNotFail() throws IOException {
        // Arrange
        Files.createDirectories(wipDir);
        Path csvFile = wipDir.resolve("test.csv");
        Files.writeString(csvFile, "data");
        
        // Act & Assert
        assertThatCode(() -> watchFolderManager.deleteMarkerFile(csvFile))
            .doesNotThrowAnyException();
    }

    @Test
    void testDeleteMarkerFileFromUpload_WhenExists_DeletesSuccessfully() throws IOException {
        // Arrange
        Files.createDirectories(uploadDir);
        
        Path markerFile = uploadDir.resolve("orders.csv.done");
        Files.writeString(markerFile, "");
        
        // Act
        watchFolderManager.deleteMarkerFileFromUpload("orders.csv");
        
        // Assert
        assertThat(Files.exists(markerFile)).isFalse();
    }

    @Test
    void testDeleteMarkerFileFromUpload_WhenNotExists_DoesNotFail() throws IOException {
        // Arrange
        Files.createDirectories(uploadDir);
        
        // Act & Assert
        assertThatCode(() -> watchFolderManager.deleteMarkerFileFromUpload("nonexistent.csv"))
            .doesNotThrowAnyException();
    }

    // ==================== Path Getter Tests ====================

    @Test
    void testGetUploadPath_ReturnsCorrectPath() {
        // Act
        Path path = watchFolderManager.getUploadPath();
        
        // Assert
        assertThat(path).isEqualTo(uploadDir);
    }

    @Test
    void testGetWipPath_ReturnsCorrectPath() {
        // Act
        Path path = watchFolderManager.getWipPath();
        
        // Assert
        assertThat(path).isEqualTo(wipDir);
    }

    @Test
    void testGetErrorPath_ReturnsCorrectPath() {
        // Act
        Path path = watchFolderManager.getErrorPath();
        
        // Assert
        assertThat(path).isEqualTo(errorDir);
    }

    @Test
    void testGetArchivePath_ReturnsCorrectPath() {
        // Act
        Path path = watchFolderManager.getArchivePath();
        
        // Assert
        assertThat(path).isEqualTo(archiveDir);
    }

    // ==================== Edge Case Tests ====================

    @Test
    void testMoveToWip_LargeFile_HandlesCorrectly() throws IOException {
        // Arrange
        Files.createDirectories(uploadDir);
        Files.createDirectories(wipDir);
        
        Path uploadFile = uploadDir.resolve("large.csv");
        // Create a larger file (1MB)
        byte[] largeData = new byte[1024 * 1024];
        java.util.Arrays.fill(largeData, (byte) 'A');
        Files.write(uploadFile, largeData);
        
        // Act
        Path wipFile = watchFolderManager.moveToWip(uploadFile);
        
        // Assert
        assertThat(Files.exists(wipFile)).isTrue();
        assertThat(Files.size(wipFile)).isEqualTo(1024 * 1024);
    }

    @Test
    void testMoveToError_WithLongStackTrace_TruncatesCorrectly() throws IOException {
        // Arrange
        Files.createDirectories(wipDir);
        Files.createDirectories(errorDir);
        
        Path wipFile = wipDir.resolve("test.csv");
        Files.writeString(wipFile, "data");
        
        // Create exception with deep stack trace
        Exception deepException = createDeepStackTraceException(100);
        
        // Act
        Path errorFile = watchFolderManager.moveToError(
            wipFile, 
            "Deep error", 
            "Stack trace test", 
            deepException
        );
        
        // Assert
        assertThat(Files.exists(errorFile)).isTrue();
        
        Path errorReportFile = errorDir.resolve(errorFile.getFileName().toString() + ".error.json");
        String errorReport = Files.readString(errorReportFile);
        assertThat(errorReport).contains("Deep error");
        // Stack trace should be truncated
        assertThat(errorReport.length()).isLessThan(10000); // Reasonable size
    }

    @Test
    void testMoveToArchive_SpecialCharactersInFilename_HandlesCorrectly() throws IOException {
        // Arrange
        Files.createDirectories(wipDir);
        Files.createDirectories(archiveDir);
        
        Path wipFile = wipDir.resolve("order-data_2024.csv");
        Files.writeString(wipFile, "data");
        
        // Act
        Path archiveFile = watchFolderManager.moveToArchive(wipFile);
        
        // Assert
        assertThat(Files.exists(archiveFile)).isTrue();
        assertThat(archiveFile.getFileName().toString()).contains("order-data_2024_");
    }

    // ==================== Helper Methods ====================

    private Exception createDeepStackTraceException(int depth) {
        if (depth == 0) {
            return new RuntimeException("Deep exception");
        }
        try {
            throw createDeepStackTraceException(depth - 1);
        } catch (Exception e) {
            return new RuntimeException("Level " + depth, e);
        }
    }
}
