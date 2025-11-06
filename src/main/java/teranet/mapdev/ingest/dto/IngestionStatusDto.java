package teranet.mapdev.ingest.dto;

import teranet.mapdev.ingest.model.IngestionManifest;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * DTO for ingestion status responses
 */
@Data
@NoArgsConstructor
public class IngestionStatusDto {
    
    private UUID batchId;
    private String fileName;
    private String tableName;  // Staging table name (e.g., staging_orders_a1b2c3d4)
    private String status;
    private Long totalRecords;
    private Long processedRecords;
    private Long failedRecords;
    private Double completionPercentage;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private Long processingDurationMs;
    private String errorMessage;
    
    // Constructor from IngestionManifest
    public IngestionStatusDto(IngestionManifest manifest) {
        this.batchId = manifest.getBatchId();
        this.fileName = manifest.getFileName();
        this.tableName = manifest.getTableName();  // Get the staging table name
        this.status = manifest.getStatus().name();
        this.totalRecords = manifest.getTotalRecords();
        this.processedRecords = manifest.getProcessedRecords();
        this.failedRecords = manifest.getFailedRecords();
        this.completionPercentage = manifest.getCompletionPercentage();
        this.startedAt = manifest.getStartedAt();
        this.completedAt = manifest.getCompletedAt();
        this.processingDurationMs = manifest.getProcessingDurationMs();
        this.errorMessage = manifest.getErrorMessage();
    }
}