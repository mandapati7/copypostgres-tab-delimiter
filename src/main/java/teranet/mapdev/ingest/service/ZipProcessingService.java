package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import teranet.mapdev.ingest.dto.ZipAnalysisDto;
import teranet.mapdev.ingest.dto.ZipAnalysisDto.ExtractedFileInfo;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Service for handling ZIP file operations in data processing
 * Extracts ZIP files, analyzes contents, and prepares for batch processing
 */
@Service
public class ZipProcessingService {
    
    private static final Logger logger = LoggerFactory.getLogger(ZipProcessingService.class);
    
    @Autowired
    private DatabaseConnectionService databaseConnectionService;

    @Autowired
    private FilenameRouterService filenameRouterService;
    
    private static final String TEMP_EXTRACTION_DIR = "temp_extracted";
    private static final long MAX_ESTIMATION_BYTES = 1024 * 1024; // 1MB for row estimation

    /**
     * Analyzes a ZIP file and extracts information about contained CSV files
     * @param zipFile the uploaded ZIP file
     * @return ZipAnalysisDto with extraction results and file analysis
     */
    public ZipAnalysisDto analyzeZipFile(MultipartFile zipFile) {
        logger.info("Starting ZIP file analysis for processing: {}", zipFile.getOriginalFilename());
        
        String zipFilename = zipFile.getOriginalFilename();
        List<ExtractedFileInfo> extractedFiles = new ArrayList<>();
        List<String> recommendations = new ArrayList<>();
        
        // Create temporary extraction directory
        Path extractionPath = createTemporaryExtractionDir();
        
        try {
            // Extract ZIP file
            int totalFiles = extractZipFile(zipFile, extractionPath);
            
            // Analyze extracted files
            List<Path> csvFiles = findCsvFiles(extractionPath);
            
            logger.info("Extracted {} total files, found {} CSV files", totalFiles, csvFiles.size());
            
            // Analyze each CSV file
            for (Path csvPath : csvFiles) {
                ExtractedFileInfo fileInfo = analyzeCsvFile(csvPath, extractionPath);
                if (fileInfo != null) {
                    extractedFiles.add(fileInfo);
                }
            }
            
            // Generate processing recommendations
            recommendations = generateProcessingRecommendations(extractedFiles);
            
            String status = extractedFiles.isEmpty() ? "NO_CSV_FILES_FOUND" : "SUCCESS";
            
            ZipAnalysisDto analysis = new ZipAnalysisDto(
                zipFilename,
                totalFiles,
                csvFiles.size(),
                status,
                extractedFiles,
                recommendations
            );
            
            logger.info("ZIP analysis completed successfully - {} CSV files ready for processing", csvFiles.size());
            return analysis;
            
        } catch (Exception e) {
            logger.error("Failed to analyze ZIP file: {}", zipFilename, e);
            
            return new ZipAnalysisDto(
                zipFilename,
                0,
                0,
                "EXTRACTION_FAILED: " + e.getMessage(),
                extractedFiles,
                Arrays.asList("Please check ZIP file format and try again")
            );
            
        } finally {
            // Clean up temporary files
            cleanupTemporaryFiles(extractionPath);
        }
    }

    /**
     * Extracts ZIP file to temporary directory
     * @param zipFile the ZIP file to extract
     * @param extractionPath the path to extract to
     * @return number of files extracted
     */
    private int extractZipFile(MultipartFile zipFile, Path extractionPath) throws IOException {
        int fileCount = 0;
        
        try (ZipInputStream zipInputStream = new ZipInputStream(zipFile.getInputStream())) {
            ZipEntry entry;
            
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    Path filePath = extractionPath.resolve(entry.getName());
                    
                    // Create parent directories if needed
                    Files.createDirectories(filePath.getParent());
                    
                    // Extract file
                    try (FileOutputStream fileOutputStream = new FileOutputStream(filePath.toFile())) {
                        byte[] buffer = new byte[8192];
                        int length;
                        while ((length = zipInputStream.read(buffer)) != -1) {
                            fileOutputStream.write(buffer, 0, length);
                        }
                    }
                    
                    fileCount++;
                    logger.debug("Extracted file: {}", entry.getName());
                }
                
                zipInputStream.closeEntry();
            }
        }
        
        logger.info("Successfully extracted {} files from ZIP", fileCount);
        return fileCount;
    }

    /**
     * Finds all CSV/TSV files and files matching routing patterns in the extracted directory
     * Accepts files with .csv, .tsv extensions or files matching PM/IM naming patterns (no extension)
     * 
     * @param extractionPath the extraction directory
     * @return list of data file paths
     */
    private List<Path> findCsvFiles(Path extractionPath) throws IOException {
        List<Path> csvFiles = new ArrayList<>();
        
        // Pattern to match PM/IM files: PM162, IM262, etc. (2 letters + digits)
        java.util.regex.Pattern routingPattern = java.util.regex.Pattern.compile("^([A-Z]{2})(\\d+)$");
        
        Files.walk(extractionPath)
            .filter(Files::isRegularFile)
            .filter(path -> {
                String fileName = path.getFileName().toString();
                String lowerFileName = fileName.toLowerCase();
                
                // Accept .csv or .tsv files
                if (lowerFileName.endsWith(".csv") || lowerFileName.endsWith(".tsv")) {
                    return true;
                }
                
                // Accept files matching PM/IM routing pattern (no extension)
                // Remove any extension first to check base name
                String baseName = fileName;
                int lastDot = fileName.lastIndexOf('.');
                if (lastDot > 0) {
                    baseName = fileName.substring(0, lastDot);
                }
                
                return routingPattern.matcher(baseName).matches();
            })
            .forEach(csvFiles::add);
        
        logger.info("Found {} CSV files in extracted content", csvFiles.size());
        return csvFiles;
    }

    /**
     * Analyzes a single CSV file to extract metadata
     * @param csvPath path to the CSV file
     * @param extractionPath root extraction directory for computing relative path
     * @return ExtractedFileInfo with file analysis
     */
    private ExtractedFileInfo analyzeCsvFile(Path csvPath, Path extractionPath) {
        try {
            // Get relative path from extraction directory (preserves folder structure for file location)
            String relativePath = extractionPath.relativize(csvPath).toString();
            // Normalize path separators to forward slashes for consistency
            relativePath = relativePath.replace('\\', '/');
            // Extract just the filename (not the parent folder) for routing
            String filename = csvPath.getFileName().toString();
            long fileSize = Files.size(csvPath);
            
            // Extract headers from CSV file
            List<String> headers = extractCsvHeaders(csvPath);
            
            // Estimate row count
            long estimatedRows = estimateRowCount(csvPath, fileSize);
            
            // Resolve table name from filename using routing rules
            String suggestedTableName = filenameRouterService.resolveTableName(filename);
            
            // Check if table exists in default schema
            boolean tableExists = databaseConnectionService.doesStagingTableExist(suggestedTableName);
            
            ExtractedFileInfo fileInfo = new ExtractedFileInfo(
                filename,
                relativePath,
                fileSize,
                "CSV",
                estimatedRows,
                headers,
                suggestedTableName,
                tableExists
            );
            
            logger.debug("Analyzed CSV file: {} - {} rows estimated, {} headers, table exists: {}", 
                        filename, estimatedRows, headers.size(), tableExists);
            
            return fileInfo;
            
        } catch (Exception e) {
            logger.error("Failed to analyze CSV file: {}", csvPath, e);
            return null;
        }
    }

    /**
     * Extracts headers from CSV file
     * @param csvPath path to CSV file
     * @return list of column headers
     */
    private List<String> extractCsvHeaders(Path csvPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            String headerLine = reader.readLine();
            
            if (headerLine != null && !headerLine.trim().isEmpty()) {
                // Parse CSV headers (simple comma-split for now)
                return Arrays.asList(headerLine.split(","));
            }
        }
        
        return new ArrayList<>();
    }

    /**
     * Estimates row count in CSV file
     * @param csvPath path to CSV file
     * @param fileSize total file size
     * @return estimated number of rows
     */
    private long estimateRowCount(Path csvPath, long fileSize) {
        try {
            if (fileSize <= MAX_ESTIMATION_BYTES) {
                // For small files, count actual lines
                return Files.lines(csvPath).count() - 1; // Subtract header
            } else {
                // For large files, estimate based on sample
                return estimateRowsFromSample(csvPath, fileSize);
            }
            
        } catch (Exception e) {
            logger.warn("Could not estimate row count for {}, using file size approximation", csvPath, e);
            // Rough estimation: assume average 50 bytes per row
            return Math.max(1, fileSize / 50);
        }
    }

    /**
     * Estimates rows by sampling first part of file
     * @param csvPath path to CSV file
     * @param totalFileSize total file size
     * @return estimated row count
     */
    private long estimateRowsFromSample(Path csvPath, long totalFileSize) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            long bytesRead = 0;
            long linesRead = 0;
            String line;
            
            // Skip header
            reader.readLine();
            
            // Read sample
            while ((line = reader.readLine()) != null && bytesRead < MAX_ESTIMATION_BYTES) {
                bytesRead += line.length() + 1; // +1 for newline
                linesRead++;
            }
            
            if (linesRead > 0 && bytesRead > 0) {
                double avgLineSize = (double) bytesRead / linesRead;
                return Math.round(totalFileSize / avgLineSize) - 1; // Subtract header
            }
        }
        
        return 1; // Fallback
    }

    /**
     * Generates processing recommendations based on file analysis
     * @param extractedFiles list of analyzed files
     * @return list of recommendations
     */
    private List<String> generateProcessingRecommendations(List<ExtractedFileInfo> extractedFiles) {
        List<String> recommendations = new ArrayList<>();
        
        if (extractedFiles.isEmpty()) {
            recommendations.add("No CSV files found in ZIP. Please ensure ZIP contains CSV files.");
            return recommendations;
        }
        
        long totalEstimatedRows = extractedFiles.stream()
            .mapToLong(ExtractedFileInfo::getEstimatedRows)
            .sum();
        
        int newTables = (int) extractedFiles.stream()
            .mapToLong(file -> file.getStagingTableExists() ? 0 : 1)
            .sum();
        
        int existingTables = extractedFiles.size() - newTables;
        
        // Generate specific recommendations
        if (newTables > 0) {
            recommendations.add(String.format("%d new tables will be created", newTables));
        }
        
        if (existingTables > 0) {
            recommendations.add(String.format("%d existing tables will be updated", existingTables));
        }
        
        recommendations.add(String.format("Estimated total rows to process: %,d", totalEstimatedRows));
        
        if (totalEstimatedRows > 1000000) {
            recommendations.add("Large dataset detected - consider processing in batches for optimal performance");
        }
        
        recommendations.add("Review table structures before proceeding to production migration");
        
        return recommendations;
    }

    /**
     * Creates temporary directory for ZIP extraction
     * Uses UUID to ensure unique directory for concurrent ZIP processing
     * @return path to temporary directory
     */
    private Path createTemporaryExtractionDir() {
        try {
            // Use UUID instead of timestamp to guarantee uniqueness for concurrent processing
            String uniqueId = java.util.UUID.randomUUID().toString();
            Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"), TEMP_EXTRACTION_DIR, 
                                   "extract_" + uniqueId);
            Files.createDirectories(tempDir);
            
            logger.debug("Created temporary extraction directory: {}", tempDir);
            return tempDir;
            
        } catch (IOException e) {
            logger.error("Failed to create temporary extraction directory", e);
            throw new RuntimeException("Could not create temporary directory for ZIP extraction", e);
        }
    }

    /**
     * Cleans up temporary files after processing
     * @param extractionPath path to cleanup
     */
    private void cleanupTemporaryFiles(Path extractionPath) {
        try {
            if (Files.exists(extractionPath)) {
                Files.walk(extractionPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
                
                logger.debug("Cleaned up temporary extraction directory: {}", extractionPath);
            }
            
        } catch (Exception e) {
            logger.warn("Could not fully clean up temporary directory: {}", extractionPath, e);
        }
    }
}