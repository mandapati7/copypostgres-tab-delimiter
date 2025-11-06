package teranet.mapdev.ingest.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * DTO for CSV upload responses
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CsvUploadResponseDto {
    
    private UUID batchId;
    private String fileName;
    private String tableName;  // Staging table name (e.g., staging_orders_a1b2c3d4)
    private Long fileSizeBytes;
    private String status;
    private String message;
    
    // Static factory methods
    public static CsvUploadResponseDto success(UUID batchId, String fileName, String tableName, Long fileSizeBytes) {
        return new CsvUploadResponseDto(batchId, fileName, tableName, fileSizeBytes, "ACCEPTED", 
            "File uploaded successfully and queued for processing");
    }
    
    public static CsvUploadResponseDto duplicate(UUID existingBatchId, String fileName, String tableName) {
        return new CsvUploadResponseDto(existingBatchId, fileName, tableName, null, "DUPLICATE", 
            "File with same content already processed or in progress");
    }
    
    public static CsvUploadResponseDto error(String fileName, String errorMessage) {
        return new CsvUploadResponseDto(null, fileName, null, null, "ERROR", errorMessage);
    }
}