package teranet.mapdev.ingest.util;

import org.springframework.web.multipart.MultipartFile;

import java.util.Arrays;
import java.util.List;

/**
 * Utility class for file validation operations.
 * Provides reusable methods for validating uploaded files.
 */
public class FileValidationUtil {

    private FileValidationUtil() {
        // Private constructor to prevent instantiation
    }

    /**
     * Validates that the file is not null and not empty.
     * 
     * @param file the file to validate
     * @throws IllegalArgumentException if file is null or empty
     */
    public static void validateFileNotEmpty(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("File is empty or not provided");
        }
    }

    /**
     * Validates that the file has a valid filename.
     * 
     * @param file the file to validate
     * @return the filename
     * @throws IllegalArgumentException if filename is null or empty
     */
    public static String validateAndGetFilename(MultipartFile file) {
        String filename = file.getOriginalFilename();
        if (filename == null || filename.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid filename");
        }
        return filename;
    }

    /**
     * Validates that the file has one of the allowed extensions.
     * Supports files without extensions (pass empty string "" in
     * allowedExtensions).
     * 
     * @param file              the file to validate
     * @param allowedExtensions allowed file extensions (without dot, e.g., "csv",
     *                          "zip", or "" for no extension)
     * @throws IllegalArgumentException if file extension is not allowed
     */
    public static void validateFileExtension(MultipartFile file, String... allowedExtensions) {
        String filename = validateAndGetFilename(file);
        String lowerCaseFilename = filename.toLowerCase();

        // Check if filename has no extension
        int lastDotIndex = filename.lastIndexOf('.');
        boolean hasNoExtension = lastDotIndex == -1 || lastDotIndex == filename.length() - 1;

        boolean isValid = Arrays.stream(allowedExtensions)
                .anyMatch(ext -> {
                    // Support files with no extension (empty string or null)
                    if (ext == null || ext.isEmpty() || ext.trim().isEmpty()) {
                        return hasNoExtension;
                    }
                    // Standard extension check: filename ends with ".ext"
                    return lowerCaseFilename.endsWith("." + ext.toLowerCase());
                });

        if (!isValid) {
            String allowedTypes = Arrays.stream(allowedExtensions)
                    .map(ext -> (ext == null || ext.trim().isEmpty()) ? "(no extension)" : ext.toUpperCase())
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("unknown");
            throw new IllegalArgumentException(
                    String.format("Invalid file type. Expected %s file but received '%s'. Please upload a valid file.",
                            allowedTypes, filename));
        }
    }

    /**
     * Comprehensive file validation: checks if file is not empty and has correct
     * extension.
     * 
     * @param file              the file to validate
     * @param allowedExtensions allowed file extensions (without dot, e.g., "csv",
     *                          "zip")
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateFile(MultipartFile file, String... allowedExtensions) {
        validateFileNotEmpty(file);
        validateFileExtension(file, allowedExtensions);
    }

    /**
     * Validates multiple files: checks if all files are not empty and have correct
     * extension.
     * 
     * @param files             the files to validate
     * @param allowedExtensions allowed file extensions (without dot, e.g., "csv",
     *                          "zip")
     * @throws IllegalArgumentException if any validation fails
     */
    public static void validateFiles(MultipartFile[] files, String... allowedExtensions) {
        if (files == null || files.length == 0) {
            throw new IllegalArgumentException("No files provided");
        }

        for (int i = 0; i < files.length; i++) {
            MultipartFile file = files[i];
            try {
                validateFile(file, allowedExtensions);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format("File #%d validation failed: %s", i + 1, e.getMessage()));
            }
        }
    }

    /**
     * Validates multiple files from a List: checks if all files are not empty and
     * have correct extension.
     * 
     * @param files             the list of files to validate
     * @param allowedExtensions allowed file extensions (without dot, e.g., "csv",
     *                          "zip")
     * @throws IllegalArgumentException if any validation fails
     */
    public static void validateFiles(List<MultipartFile> files, String... allowedExtensions) {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("No files provided");
        }

        for (int i = 0; i < files.size(); i++) {
            MultipartFile file = files.get(i);
            try {
                validateFile(file, allowedExtensions);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format("File #%d validation failed: %s", i + 1, e.getMessage()));
            }
        }
    }
}
