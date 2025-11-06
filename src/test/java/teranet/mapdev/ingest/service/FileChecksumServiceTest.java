package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FileChecksumService
 * Tests checksum calculation and compression handling
 */
class FileChecksumServiceTest {

    private FileChecksumService fileChecksumService;

    @BeforeEach
    void setUp() {
        fileChecksumService = new FileChecksumService();
    }

    @Test
    void testCalculateFileChecksum_PlainCsv() throws IOException, NoSuchAlgorithmException {
        // Given: Plain CSV file
        String csvContent = "name,age,city\nJohn,30,NYC\nJane,25,LA\n";
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                csvContent.getBytes(StandardCharsets.UTF_8));

        // When: Calculate checksum
        String checksum = fileChecksumService.calculateFileChecksum(file);

        // Then: Should return valid SHA-256 checksum (64 hex characters)
        assertNotNull(checksum);
        assertEquals(64, checksum.length());
        assertTrue(checksum.matches("[a-f0-9]{64}"));
    }

    @Test
    void testCalculateFileChecksum_SameContent_SameChecksum() throws IOException, NoSuchAlgorithmException {
        // Given: Two files with identical content
        String csvContent = "name,age\nAlice,35\n";
        MockMultipartFile file1 = new MockMultipartFile(
                "file1",
                "test1.csv",
                "text/csv",
                csvContent.getBytes(StandardCharsets.UTF_8));
        MockMultipartFile file2 = new MockMultipartFile(
                "file2",
                "test2.csv",
                "text/csv",
                csvContent.getBytes(StandardCharsets.UTF_8));

        // When: Calculate checksums
        String checksum1 = fileChecksumService.calculateFileChecksum(file1);
        String checksum2 = fileChecksumService.calculateFileChecksum(file2);

        // Then: Should be identical (idempotency)
        assertEquals(checksum1, checksum2);
    }

    @Test
    void testCalculateFileChecksum_DifferentContent_DifferentChecksum() throws IOException, NoSuchAlgorithmException {
        // Given: Two files with different content
        MockMultipartFile file1 = new MockMultipartFile(
                "file1",
                "test1.csv",
                "text/csv",
                "name,age\nBob,40\n".getBytes(StandardCharsets.UTF_8));
        MockMultipartFile file2 = new MockMultipartFile(
                "file2",
                "test2.csv",
                "text/csv",
                "name,age\nCarol,45\n".getBytes(StandardCharsets.UTF_8));

        // When: Calculate checksums
        String checksum1 = fileChecksumService.calculateFileChecksum(file1);
        String checksum2 = fileChecksumService.calculateFileChecksum(file2);

        // Then: Should be different
        assertNotEquals(checksum1, checksum2);
    }

    @Test
    void testCalculateFileChecksum_EmptyFile() throws IOException, NoSuchAlgorithmException {
        // Given: Empty file
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "empty.csv",
                "text/csv",
                new byte[0]);

        // When: Calculate checksum
        String checksum = fileChecksumService.calculateFileChecksum(file);

        // Then: Should return valid checksum for empty content
        assertNotNull(checksum);
        assertEquals(64, checksum.length());
        // SHA-256 of empty string is known value
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", checksum);
    }

    @Test
    void testGetDecompressedInputStream_PlainCsv() throws IOException {
        // Given: Plain CSV file
        String csvContent = "header1,header2\nvalue1,value2\n";
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                csvContent.getBytes(StandardCharsets.UTF_8));

        // When: Get decompressed input stream
        try (InputStream inputStream = fileChecksumService.getDecompressedInputStream(file)) {
            String content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

            // Then: Should return original content
            assertEquals(csvContent, content);
        }
    }

    @Test
    void testGetDecompressedInputStream_GzipFile() throws IOException {
        // Given: GZIP compressed CSV file
        String csvContent = "header1,header2\nvalue1,value2\n";
        byte[] gzippedContent = compressWithGzip(csvContent);

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv.gz",
                "application/gzip",
                gzippedContent);

        // When: Get decompressed input stream
        try (InputStream inputStream = fileChecksumService.getDecompressedInputStream(file)) {
            String content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

            // Then: Should return decompressed content
            assertEquals(csvContent, content);
        }
    }

    @Test
    void testGetDecompressedInputStream_ZipFile() throws IOException {
        // Given: ZIP compressed CSV file
        String csvContent = "header1,header2\nvalue1,value2\n";
        byte[] zippedContent = compressWithZip(csvContent, "test.csv");

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.zip",
                "application/zip",
                zippedContent);

        // When: Get decompressed input stream
        try (InputStream inputStream = fileChecksumService.getDecompressedInputStream(file)) {
            String content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

            // Then: Should return decompressed content
            assertEquals(csvContent, content);
        }
    }

    @Test
    void testGetDecompressedInputStream_ZipWithNoCsv() throws IOException {
        // Given: ZIP file without CSV entry
        byte[] zippedContent = compressWithZip("some content", "test.txt");

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.zip",
                "application/zip",
                zippedContent);

        // When: Get decompressed input stream
        try (InputStream inputStream = fileChecksumService.getDecompressedInputStream(file)) {
            // Then: Should still return stream (but won't find CSV entry)
            assertNotNull(inputStream);
        }
    }

    @Test
    void testGetDecompressedInputStream_NullFilename() throws IOException {
        // Given: File with null filename
        String csvContent = "header1,header2\nvalue1,value2\n";
        MockMultipartFile file = new MockMultipartFile(
                "file",
                null, // null filename
                "text/csv",
                csvContent.getBytes(StandardCharsets.UTF_8));

        // When: Get decompressed input stream
        try (InputStream inputStream = fileChecksumService.getDecompressedInputStream(file)) {
            String content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

            // Then: Should return original content
            assertEquals(csvContent, content);
        }
    }

    // Helper methods for compression

    private byte[] compressWithGzip(String content) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            gzipStream.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return byteStream.toByteArray();
    }

    private byte[] compressWithZip(String content, String entryName) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (ZipOutputStream zipStream = new ZipOutputStream(byteStream)) {
            ZipEntry entry = new ZipEntry(entryName);
            zipStream.putNextEntry(entry);
            zipStream.write(content.getBytes(StandardCharsets.UTF_8));
            zipStream.closeEntry();
        }
        return byteStream.toByteArray();
    }
}
