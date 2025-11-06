package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Service responsible for CSV parsing operations.
 * 
 * Features:
 * - Extract CSV headers from files
 * - Parse CSV rows handling quoted fields and commas within quotes
 * - Sanitize column names for database safety
 * - Support for compressed files (via FileChecksumService)
 * 
 * This service is stateless and can be safely used concurrently.
 */
@Service
public class CsvParsingService {
    
    private static final Logger logger = LoggerFactory.getLogger(CsvParsingService.class);
    
    @Autowired
    private FileChecksumService fileChecksumService;
    
    /**
     * Extract CSV headers from the file to determine table schema.
     * Automatically handles compressed files (.gz, .zip).
     * 
     * @param file the CSV file (can be compressed)
     * @return list of sanitized column names
     * @throws IOException if file cannot be read
     * @throws IllegalArgumentException if file is empty or has no valid headers
     */
    public List<String> extractCsvHeaders(MultipartFile file) throws IOException {
        logger.debug("Extracting CSV headers from file: {}", file.getOriginalFilename());
        
        try (InputStream inputStream = fileChecksumService.getDecompressedInputStream(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                throw new IllegalArgumentException("CSV file is empty or has no header");
            }
            
            // Parse CSV headers (handle quoted fields)
            List<String> headers = parseCsvRow(headerLine);
            
            // Clean and validate headers
            List<String> cleanHeaders = new ArrayList<>();
            for (String header : headers) {
                String cleanHeader = sanitizeColumnName(header.trim());
                if (!cleanHeader.isEmpty()) {
                    cleanHeaders.add(cleanHeader);
                }
            }
            
            if (cleanHeaders.isEmpty()) {
                throw new IllegalArgumentException("No valid headers found in CSV file");
            }
            
            logger.debug("Extracted {} headers from file: {}", cleanHeaders.size(), file.getOriginalFilename());
            return cleanHeaders;
        }
    }
    
    /**
     * Parse CSV row handling quoted fields and commas within quotes.
     * Follows RFC 4180 CSV specification for quote handling.
     * 
     * Examples:
     *   "John,Smith,30" → ["John", "Smith", "30"]
     *   "John,\"Smith, Jr.\",30" → ["John", "Smith, Jr.", "30"]
     *   "\"O'Reilly\",\"Author \"\"John\"\" Doe\",50" → ["O'Reilly", "Author \"John\" Doe", "50"]
     * 
     * @param csvLine the CSV line to parse
     * @return list of field values
     */
    public List<String> parseCsvRow(String csvLine) {
        List<String> values = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentValue = new StringBuilder();
        
        for (int i = 0; i < csvLine.length(); i++) {
            char c = csvLine.charAt(i);
            
            if (c == '"') {
                // Handle escaped quotes (double quotes "" represent a single quote)
                if (inQuotes && i + 1 < csvLine.length() && csvLine.charAt(i + 1) == '"') {
                    currentValue.append('"');
                    i++; // Skip next quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                // End of field
                values.add(currentValue.toString().trim());
                currentValue = new StringBuilder();
            } else {
                currentValue.append(c);
            }
        }
        
        // Add the last field
        values.add(currentValue.toString().trim());
        
        return values;
    }
    
    /**
     * Sanitize column name to be safe for SQL.
     * 
     * Transformation steps:
     * 1. Convert to lowercase
     * 2. Replace all non-alphanumeric characters (except underscore) with underscore
     * 3. Replace multiple consecutive underscores with single underscore
     * 4. Remove leading/trailing underscores
     * 5. Prepend "col_" if name starts with a number
     * 
     * Examples:
     *   "Customer Name" → "customer_name"
     *   "Order#123" → "order_123"
     *   "123abc" → "col_123abc"
     *   "__test__" → "test"
     *   "FIRST-NAME" → "first_name"
     * 
     * @param name the raw column name from CSV header
     * @return sanitized column name safe for PostgreSQL
     */
    public String sanitizeColumnName(String name) {
        String sanitized = name.toLowerCase()
                              .replaceAll("[^a-zA-Z0-9_]", "_")
                              .replaceAll("_{2,}", "_")
                              .replaceAll("^_|_$", "");
        
        // Ensure it doesn't start with a number (PostgreSQL requirement)
        if (sanitized.matches("^\\d.*")) {
            sanitized = "col_" + sanitized;
        }
        
        return sanitized;
    }
}
