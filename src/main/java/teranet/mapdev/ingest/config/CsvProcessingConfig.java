package teranet.mapdev.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for CSV processing
 */
@Configuration
@ConfigurationProperties(prefix = "csv.processing")
public class CsvProcessingConfig {

    private int batchSize = 1000;
    private String maxFileSize = "100MB";
    private String tempDirectory = System.getProperty("java.io.tmpdir") + "/csv-loader";
    private boolean enableCompression = true;
    private boolean enableIdempotency = true;
    private int maxConcurrentProcessing = 3;
    private long processingTimeoutMs = 1800000; // 30 minutes

    // File type settings
    private String[] allowedFileTypes = { "csv", "gz", "zip" };
    private String[] allowedContentTypes = {
            "text/csv",
            "text/plain",
            "application/csv",
            "application/gzip",
            "application/zip",
            "application/x-zip-compressed"
    };

    // CSV parsing settings
    private char csvDelimiter = ',';
    private char csvQuoteChar = '"';
    private char csvEscapeChar = '\\';
    private boolean csvHasHeader = true;
    private String csvCharset = "UTF-8";

    // Validation settings
    private boolean enableDataValidation = true;
    private boolean skipInvalidRecords = false;
    private int maxValidationErrors = 100;

    // Performance settings
    private boolean enableBulkInsert = true;
    private boolean enableParallelProcessing = true;
    private int bufferSize = 8192;

    // Getters and Setters
    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(String maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public String getTempDirectory() {
        return tempDirectory;
    }

    public void setTempDirectory(String tempDirectory) {
        this.tempDirectory = tempDirectory;
    }

    public boolean isEnableCompression() {
        return enableCompression;
    }

    public void setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
    }

    public boolean isEnableIdempotency() {
        return enableIdempotency;
    }

    public void setEnableIdempotency(boolean enableIdempotency) {
        this.enableIdempotency = enableIdempotency;
    }

    public int getMaxConcurrentProcessing() {
        return maxConcurrentProcessing;
    }

    public void setMaxConcurrentProcessing(int maxConcurrentProcessing) {
        this.maxConcurrentProcessing = maxConcurrentProcessing;
    }

    public long getProcessingTimeoutMs() {
        return processingTimeoutMs;
    }

    public void setProcessingTimeoutMs(long processingTimeoutMs) {
        this.processingTimeoutMs = processingTimeoutMs;
    }

    public String[] getAllowedFileTypes() {
        return allowedFileTypes;
    }

    public void setAllowedFileTypes(String[] allowedFileTypes) {
        this.allowedFileTypes = allowedFileTypes;
    }

    public String[] getAllowedContentTypes() {
        return allowedContentTypes;
    }

    public void setAllowedContentTypes(String[] allowedContentTypes) {
        this.allowedContentTypes = allowedContentTypes;
    }

    public char getCsvDelimiter() {
        return csvDelimiter;
    }

    public void setCsvDelimiter(char csvDelimiter) {
        this.csvDelimiter = csvDelimiter;
    }

    public char getCsvQuoteChar() {
        return csvQuoteChar;
    }

    public void setCsvQuoteChar(char csvQuoteChar) {
        this.csvQuoteChar = csvQuoteChar;
    }

    public char getCsvEscapeChar() {
        return csvEscapeChar;
    }

    public void setCsvEscapeChar(char csvEscapeChar) {
        this.csvEscapeChar = csvEscapeChar;
    }

    public boolean isCsvHasHeader() {
        return csvHasHeader;
    }

    public void setCsvHasHeader(boolean csvHasHeader) {
        this.csvHasHeader = csvHasHeader;
    }

    public String getCsvCharset() {
        return csvCharset;
    }

    public void setCsvCharset(String csvCharset) {
        this.csvCharset = csvCharset;
    }

    public boolean isEnableDataValidation() {
        return enableDataValidation;
    }

    public void setEnableDataValidation(boolean enableDataValidation) {
        this.enableDataValidation = enableDataValidation;
    }

    public boolean isSkipInvalidRecords() {
        return skipInvalidRecords;
    }

    public void setSkipInvalidRecords(boolean skipInvalidRecords) {
        this.skipInvalidRecords = skipInvalidRecords;
    }

    public int getMaxValidationErrors() {
        return maxValidationErrors;
    }

    public void setMaxValidationErrors(int maxValidationErrors) {
        this.maxValidationErrors = maxValidationErrors;
    }

    public boolean isEnableBulkInsert() {
        return enableBulkInsert;
    }

    public void setEnableBulkInsert(boolean enableBulkInsert) {
        this.enableBulkInsert = enableBulkInsert;
    }

    public boolean isEnableParallelProcessing() {
        return enableParallelProcessing;
    }

    public void setEnableParallelProcessing(boolean enableParallelProcessing) {
        this.enableParallelProcessing = enableParallelProcessing;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    /**
     * Get max file size in bytes
     */
    public long getMaxFileSizeBytes() {
        String size = maxFileSize.toLowerCase();
        long multiplier = 1;

        if (size.endsWith("kb")) {
            multiplier = 1024;
            size = size.substring(0, size.length() - 2);
        } else if (size.endsWith("mb")) {
            multiplier = 1024 * 1024;
            size = size.substring(0, size.length() - 2);
        } else if (size.endsWith("gb")) {
            multiplier = 1024 * 1024 * 1024;
            size = size.substring(0, size.length() - 2);
        }

        try {
            return Long.parseLong(size.trim()) * multiplier;
        } catch (NumberFormatException e) {
            return 100 * 1024 * 1024; // Default 100MB
        }
    }
}