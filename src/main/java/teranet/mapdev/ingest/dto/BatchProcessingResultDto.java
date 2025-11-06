package teranet.mapdev.ingest.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

/**
 * DTO for batch processing results
 * Shows results of processing multiple files to staging area
 */
@Data
@NoArgsConstructor
public class BatchProcessingResultDto {
    
    @JsonProperty("batch_id")
    private String batchId;
    
    @JsonProperty("processing_status")
    private String processingStatus; // SUCCESS, PARTIAL_SUCCESS, FAILED
    
    @JsonProperty("total_files_processed")
    private Integer totalFilesProcessed;
    
    @JsonProperty("successful_files")
    private Integer successfulFiles;
    
    @JsonProperty("failed_files")
    private Integer failedFiles;
    
    @JsonProperty("total_rows_loaded")
    private Long totalRowsLoaded;
    
    @JsonProperty("processing_start_time")
    private LocalDateTime processingStartTime;
    
    @JsonProperty("processing_end_time")
    private LocalDateTime processingEndTime;
    
    @JsonProperty("processing_duration_ms")
    private Long processingDurationMs;
    
    @JsonProperty("staging_schema")
    private String stagingSchema;
    
    @JsonProperty("file_results")
    private List<FileProcessingResult> fileResults;
    
    @JsonProperty("validation_summary")
    private ValidationSummary validationSummary;

    // Inner class for individual file processing results
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileProcessingResult {
        @JsonProperty("filename")
        private String filename;
        
        @JsonProperty("table_name")
        private String tableName;
        
        @JsonProperty("status")
        private String status; // SUCCESS, FAILED, SKIPPED
        
        @JsonProperty("rows_loaded")
        private Long rowsLoaded;
        
        @JsonProperty("columns_created")
        private Integer columnsCreated;
        
        @JsonProperty("processing_time_ms")
        private Long processingTimeMs;
        
        @JsonProperty("error_message")
        private String errorMessage;
    }

    // Inner class for validation summary
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValidationSummary {
        @JsonProperty("tables_ready_for_production")
        private Integer tablesReadyForProduction;
        
        @JsonProperty("tables_requiring_review")
        private Integer tablesRequiringReview;
        
        @JsonProperty("data_quality_issues")
        private List<String> dataQualityIssues;
        
        @JsonProperty("schema_conflicts")
        private List<String> schemaConflicts;
    }
}