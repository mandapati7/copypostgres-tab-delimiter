package teranet.mapdev.ingest.util;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for FileValidationUtil.
 * Tests all file validation methods with various scenarios.
 */
class FileValidationUtilTest {

    // ========== validateFileNotEmpty() Tests ==========

    @Test
    void testValidateFileNotEmpty_WithValidFile_DoesNotThrowException() {
        // Given: a valid non-empty file
        MultipartFile file = new MockMultipartFile(
            "file",
            "test.csv",
            "text/csv",
            "name,age\nJohn,30".getBytes()
        );

        // When & Then: validation passes without exception
        FileValidationUtil.validateFileNotEmpty(file);
    }

    @Test
    void testValidateFileNotEmpty_WithNullFile_ThrowsException() {
        // Given: a null file
        MultipartFile file = null;

        // When & Then: throws IllegalArgumentException
        assertThatThrownBy(() -> FileValidationUtil.validateFileNotEmpty(file))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("File is empty or not provided");
    }

    @Test
    void testValidateFileNotEmpty_WithEmptyFile_ThrowsException() {
        // Given: an empty file (0 bytes)
        MultipartFile file = new MockMultipartFile(
            "file",
            "test.csv",
            "text/csv",
            new byte[0]
        );

        // When & Then: throws IllegalArgumentException
        assertThatThrownBy(() -> FileValidationUtil.validateFileNotEmpty(file))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("File is empty or not provided");
    }

    // ========== validateAndGetFilename() Tests ==========

    @Test
    void testValidateAndGetFilename_WithValidFilename_ReturnsFilename() {
        // Given: a file with valid filename
        String expectedFilename = "test-data.csv";
        MultipartFile file = new MockMultipartFile(
            "file",
            expectedFilename,
            "text/csv",
            "data".getBytes()
        );

        // When: validating and getting filename
        String filename = FileValidationUtil.validateAndGetFilename(file);

        // Then: returns the correct filename
        assertThat(filename).isEqualTo(expectedFilename);
    }

    @Test
    void testValidateAndGetFilename_WithNullFilename_ThrowsException() {
        // Given: a file with null filename
        MultipartFile file = new MockMultipartFile(
            "file",
            null,  // null filename
            "text/csv",
            "data".getBytes()
        );

        // When & Then: throws IllegalArgumentException
        assertThatThrownBy(() -> FileValidationUtil.validateAndGetFilename(file))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid filename");
    }

    @Test
    void testValidateAndGetFilename_WithEmptyStringFilename_ThrowsException() {
        // Given: a file with empty string filename
        MultipartFile file = new MockMultipartFile(
            "file",
            "",  // empty filename
            "text/csv",
            "data".getBytes()
        );

        // When & Then: throws IllegalArgumentException
        assertThatThrownBy(() -> FileValidationUtil.validateAndGetFilename(file))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid filename");
    }

    @Test
    void testValidateAndGetFilename_WithWhitespaceOnlyFilename_ThrowsException() {
        // Given: a file with whitespace-only filename
        MultipartFile file = new MockMultipartFile(
            "file",
            "   ",  // whitespace only
            "text/csv",
            "data".getBytes()
        );

        // When & Then: throws IllegalArgumentException
        assertThatThrownBy(() -> FileValidationUtil.validateAndGetFilename(file))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid filename");
    }

    // ========== validateFileExtension() Tests ==========

    @Test
    void testValidateFileExtension_WithValidCsvExtension_DoesNotThrowException() {
        // Given: a CSV file
        MultipartFile file = new MockMultipartFile(
            "file",
            "test.csv",
            "text/csv",
            "data".getBytes()
        );

        // When & Then: validation passes for CSV extension
        FileValidationUtil.validateFileExtension(file, "csv");
    }

    @Test
    void testValidateFileExtension_WithValidZipExtension_DoesNotThrowException() {
        // Given: a ZIP file
        MultipartFile file = new MockMultipartFile(
            "file",
            "archive.zip",
            "application/zip",
            "data".getBytes()
        );

        // When & Then: validation passes for ZIP extension
        FileValidationUtil.validateFileExtension(file, "zip");
    }

    @Test
    void testValidateFileExtension_WithMultipleAllowedExtensions_AcceptsCsv() {
        // Given: a CSV file
        MultipartFile file = new MockMultipartFile(
            "file",
            "data.csv",
            "text/csv",
            "data".getBytes()
        );

        // When & Then: validation passes when CSV is in allowed list
        FileValidationUtil.validateFileExtension(file, "csv", "zip", "txt");
    }

    @Test
    void testValidateFileExtension_WithMultipleAllowedExtensions_AcceptsZip() {
        // Given: a ZIP file
        MultipartFile file = new MockMultipartFile(
            "file",
            "data.zip",
            "application/zip",
            "data".getBytes()
        );

        // When & Then: validation passes when ZIP is in allowed list
        FileValidationUtil.validateFileExtension(file, "csv", "zip", "txt");
    }

    @Test
    void testValidateFileExtension_WithInvalidExtension_ThrowsException() {
        // Given: a TXT file when only CSV is allowed
        MultipartFile file = new MockMultipartFile(
            "file",
            "data.txt",
            "text/plain",
            "data".getBytes()
        );

        // When & Then: throws IllegalArgumentException with descriptive message
        assertThatThrownBy(() -> FileValidationUtil.validateFileExtension(file, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid file type")
            .hasMessageContaining("Expected CSV")
            .hasMessageContaining("data.txt");
    }

    @Test
    void testValidateFileExtension_WithInvalidExtensionMultipleAllowed_ThrowsException() {
        // Given: a PDF file when CSV and ZIP are allowed
        MultipartFile file = new MockMultipartFile(
            "file",
            "document.pdf",
            "application/pdf",
            "data".getBytes()
        );

        // When & Then: throws IllegalArgumentException listing all allowed types
        assertThatThrownBy(() -> FileValidationUtil.validateFileExtension(file, "csv", "zip"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid file type")
            .hasMessageContaining("CSV, ZIP")
            .hasMessageContaining("document.pdf");
    }

    @Test
    void testValidateFileExtension_CaseInsensitive_AcceptsUppercaseExtension() {
        // Given: a file with uppercase extension
        MultipartFile file = new MockMultipartFile(
            "file",
            "data.CSV",
            "text/csv",
            "data".getBytes()
        );

        // When & Then: validation is case-insensitive and passes
        FileValidationUtil.validateFileExtension(file, "csv");
    }

    @Test
    void testValidateFileExtension_CaseInsensitive_AcceptsMixedCaseExtension() {
        // Given: a file with mixed case extension
        MultipartFile file = new MockMultipartFile(
            "file",
            "data.CsV",
            "text/csv",
            "data".getBytes()
        );

        // When & Then: validation is case-insensitive and passes
        FileValidationUtil.validateFileExtension(file, "csv");
    }

    @Test
    void testValidateFileExtension_WithCompoundFilename_ValidatesCorrectly() {
        // Given: a filename with multiple dots
        MultipartFile file = new MockMultipartFile(
            "file",
            "my.data.backup.csv",
            "text/csv",
            "data".getBytes()
        );

        // When & Then: validates only the last extension
        FileValidationUtil.validateFileExtension(file, "csv");
    }

    @Test
    void testValidateFileExtension_WithGzExtension_ValidatesCorrectly() {
        // Given: a gzipped file
        MultipartFile file = new MockMultipartFile(
            "file",
            "data.csv.gz",
            "application/gzip",
            "data".getBytes()
        );

        // When & Then: validates the .gz extension
        FileValidationUtil.validateFileExtension(file, "gz");
    }

    // ========== validateFile() Tests ==========

    @Test
    void testValidateFile_WithValidFile_DoesNotThrowException() {
        // Given: a valid CSV file
        MultipartFile file = new MockMultipartFile(
            "file",
            "test.csv",
            "text/csv",
            "name,age\nJohn,30".getBytes()
        );

        // When & Then: validation passes
        FileValidationUtil.validateFile(file, "csv");
    }

    @Test
    void testValidateFile_WithEmptyFile_ThrowsException() {
        // Given: an empty file
        MultipartFile file = new MockMultipartFile(
            "file",
            "test.csv",
            "text/csv",
            new byte[0]
        );

        // When & Then: throws exception due to empty file
        assertThatThrownBy(() -> FileValidationUtil.validateFile(file, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("File is empty or not provided");
    }

    @Test
    void testValidateFile_WithWrongExtension_ThrowsException() {
        // Given: a file with wrong extension
        MultipartFile file = new MockMultipartFile(
            "file",
            "test.txt",
            "text/plain",
            "data".getBytes()
        );

        // When & Then: throws exception due to wrong extension
        assertThatThrownBy(() -> FileValidationUtil.validateFile(file, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid file type");
    }

    // ========== validateFiles(array) Tests ==========

    @Test
    void testValidateFilesArray_WithValidFiles_DoesNotThrowException() {
        // Given: multiple valid CSV files
        MultipartFile[] files = {
            new MockMultipartFile("file1", "test1.csv", "text/csv", "data1".getBytes()),
            new MockMultipartFile("file2", "test2.csv", "text/csv", "data2".getBytes())
        };

        // When & Then: validation passes
        FileValidationUtil.validateFiles(files, "csv");
    }

    @Test
    void testValidateFilesArray_WithNullArray_ThrowsException() {
        // Given: null array
        MultipartFile[] files = null;

        // When & Then: throws IllegalArgumentException
        assertThatThrownBy(() -> FileValidationUtil.validateFiles(files, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No files provided");
    }

    @Test
    void testValidateFilesArray_WithEmptyArray_ThrowsException() {
        // Given: empty array
        MultipartFile[] files = new MultipartFile[0];

        // When & Then: throws IllegalArgumentException
        assertThatThrownBy(() -> FileValidationUtil.validateFiles(files, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No files provided");
    }

    @Test
    void testValidateFilesArray_WithOneInvalidFile_ThrowsExceptionWithIndex() {
        // Given: multiple files where second one is invalid
        MultipartFile[] files = {
            new MockMultipartFile("file1", "test1.csv", "text/csv", "data1".getBytes()),
            new MockMultipartFile("file2", "test2.txt", "text/plain", "data2".getBytes())
        };

        // When & Then: throws exception indicating which file failed
        assertThatThrownBy(() -> FileValidationUtil.validateFiles(files, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("File #2 validation failed")
            .hasMessageContaining("Invalid file type");
    }

    @Test
    void testValidateFilesArray_WithEmptyFile_ThrowsExceptionWithIndex() {
        // Given: multiple files where second one is empty
        MultipartFile[] files = {
            new MockMultipartFile("file1", "test1.csv", "text/csv", "data1".getBytes()),
            new MockMultipartFile("file2", "test2.csv", "text/csv", new byte[0])
        };

        // When & Then: throws exception indicating which file failed
        assertThatThrownBy(() -> FileValidationUtil.validateFiles(files, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("File #2 validation failed")
            .hasMessageContaining("File is empty");
    }

    @Test
    void testValidateFilesArray_WithMultipleExtensions_ValidatesCorrectly() {
        // Given: mixed CSV and ZIP files
        MultipartFile[] files = {
            new MockMultipartFile("file1", "test.csv", "text/csv", "data1".getBytes()),
            new MockMultipartFile("file2", "archive.zip", "application/zip", "data2".getBytes())
        };

        // When & Then: validation passes for multiple allowed extensions
        FileValidationUtil.validateFiles(files, "csv", "zip");
    }

    // ========== validateFiles(List) Tests ==========

    @Test
    void testValidateFilesList_WithValidFiles_DoesNotThrowException() {
        // Given: multiple valid CSV files in a list
        List<MultipartFile> files = Arrays.asList(
            new MockMultipartFile("file1", "test1.csv", "text/csv", "data1".getBytes()),
            new MockMultipartFile("file2", "test2.csv", "text/csv", "data2".getBytes())
        );

        // When & Then: validation passes
        FileValidationUtil.validateFiles(files, "csv");
    }

    @Test
    void testValidateFilesList_WithNullList_ThrowsException() {
        // Given: null list
        List<MultipartFile> files = null;

        // When & Then: throws IllegalArgumentException
        assertThatThrownBy(() -> FileValidationUtil.validateFiles(files, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No files provided");
    }

    @Test
    void testValidateFilesList_WithEmptyList_ThrowsException() {
        // Given: empty list
        List<MultipartFile> files = Collections.emptyList();

        // When & Then: throws IllegalArgumentException
        assertThatThrownBy(() -> FileValidationUtil.validateFiles(files, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No files provided");
    }

    @Test
    void testValidateFilesList_WithOneInvalidFile_ThrowsExceptionWithIndex() {
        // Given: multiple files where third one is invalid
        List<MultipartFile> files = Arrays.asList(
            new MockMultipartFile("file1", "test1.csv", "text/csv", "data1".getBytes()),
            new MockMultipartFile("file2", "test2.csv", "text/csv", "data2".getBytes()),
            new MockMultipartFile("file3", "test3.txt", "text/plain", "data3".getBytes())
        );

        // When & Then: throws exception indicating which file failed
        assertThatThrownBy(() -> FileValidationUtil.validateFiles(files, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("File #3 validation failed")
            .hasMessageContaining("Invalid file type");
    }

    @Test
    void testValidateFilesList_WithSingleFile_ValidatesCorrectly() {
        // Given: single file in list
        List<MultipartFile> files = Collections.singletonList(
            new MockMultipartFile("file", "test.csv", "text/csv", "data".getBytes())
        );

        // When & Then: validation passes
        FileValidationUtil.validateFiles(files, "csv");
    }

    @Test
    void testValidateFilesList_WithMultipleExtensions_ValidatesCorrectly() {
        // Given: mixed CSV and ZIP files in list
        List<MultipartFile> files = Arrays.asList(
            new MockMultipartFile("file1", "test.csv", "text/csv", "data1".getBytes()),
            new MockMultipartFile("file2", "archive.zip", "application/zip", "data2".getBytes()),
            new MockMultipartFile("file3", "data.txt", "text/plain", "data3".getBytes())
        );

        // When & Then: validation passes for multiple allowed extensions
        FileValidationUtil.validateFiles(files, "csv", "zip", "txt");
    }

    // ========== Edge Cases and Special Scenarios ==========

    @Test
    void testValidateFile_WithSpecialCharactersInFilename_ValidatesCorrectly() {
        // Given: filename with special characters
        MultipartFile file = new MockMultipartFile(
            "file",
            "my-data_file (copy) [1].csv",
            "text/csv",
            "data".getBytes()
        );

        // When & Then: validation handles special characters correctly
        FileValidationUtil.validateFile(file, "csv");
    }

    @Test
    void testValidateFile_WithUnicodeCharactersInFilename_ValidatesCorrectly() {
        // Given: filename with unicode characters
        MultipartFile file = new MockMultipartFile(
            "file",
            "données_日本語_데이터.csv",
            "text/csv",
            "data".getBytes()
        );

        // When & Then: validation handles unicode correctly
        FileValidationUtil.validateFile(file, "csv");
    }

    @Test
    void testValidateFile_WithVeryLongFilename_ValidatesCorrectly() {
        // Given: very long filename
        String longFilename = "a".repeat(200) + ".csv";
        MultipartFile file = new MockMultipartFile(
            "file",
            longFilename,
            "text/csv",
            "data".getBytes()
        );

        // When & Then: validation handles long filenames
        FileValidationUtil.validateFile(file, "csv");
    }

    @Test
    void testValidateFileExtension_WithNoExtension_ThrowsException() {
        // Given: filename with no extension
        MultipartFile file = new MockMultipartFile(
            "file",
            "filewithoutext",
            "text/plain",
            "data".getBytes()
        );

        // When & Then: throws exception
        assertThatThrownBy(() -> FileValidationUtil.validateFileExtension(file, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid file type");
    }

    @Test
    void testValidateFileExtension_WithDotOnlyFilename_ThrowsException() {
        // Given: filename that is just a dot
        MultipartFile file = new MockMultipartFile(
            "file",
            ".",
            "text/plain",
            "data".getBytes()
        );

        // When & Then: throws exception
        assertThatThrownBy(() -> FileValidationUtil.validateFileExtension(file, "csv"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid file type");
    }
}
