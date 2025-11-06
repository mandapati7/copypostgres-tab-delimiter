package teranet.mapdev.ingest.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * DTO for ZIP file analysis results
 * Shows extracted files and their analysis
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ZipAnalysisDto {
    
    @JsonProperty("zip_filename")
    private String zipFilename;
    
    @JsonProperty("total_files_extracted")
    private Integer totalFilesExtracted;
    
    @JsonProperty("csv_files_found")
    private Integer csvFilesFound;
    
    @JsonProperty("extraction_status")
    private String extractionStatus;
    
    @JsonProperty("extracted_files")
    private List<ExtractedFileInfo> extractedFiles;
    
    @JsonProperty("processing_recommendations")
    private List<String> processingRecommendations;

    // Inner class for extracted file information
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ExtractedFileInfo {
        @JsonProperty("filename")
        private String filename;
        
        @JsonProperty("file_size")
        private Long fileSize;
        
        @JsonProperty("file_type")
        private String fileType;
        
        @JsonProperty("estimated_rows")
        private Long estimatedRows;
        
        @JsonProperty("headers_detected")
        private List<String> headersDetected;
        
        @JsonProperty("suggested_table_name")
        private String suggestedTableName;
        
        @JsonProperty("staging_table_exists")
        private Boolean stagingTableExists;
    }
}