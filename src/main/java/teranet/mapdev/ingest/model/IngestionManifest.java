package teranet.mapdev.ingest.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Represents the status and metadata of a CSV file ingestion batch.
 * Maps to the ingestion_manifest table in the default schema.
 */
@Entity
@Table(name = "ingestion_manifest")
@Getter
@Setter
public class IngestionManifest {
    
    public enum Status {
        PENDING, PROCESSING, COMPLETED, FAILED, CANCELLED
    }
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "batch_id", nullable = false, unique = true, columnDefinition = "UUID")
    private UUID batchId;
    
    @Column(name = "parent_batch_id", columnDefinition = "UUID")
    private UUID parentBatchId; // For ZIP batch processing - links CSV manifests to parent ZIP manifest
    
    @Column(name = "file_name", nullable = false, length = 255)
    private String fileName;
    
    @Column(name = "file_path", length = 500)
    private String filePath;
    
    @Column(name = "file_size_bytes", nullable = false)
    private Long fileSizeBytes;
    
    @Column(name = "file_checksum", nullable = false, length = 64)
    private String fileChecksum;
    
    @Column(name = "content_type", length = 100)
    private String contentType;
    
    @Column(name = "table_name", length = 128)
    private String tableName; // The actual table name created (e.g., staging_orders_a1b2c3d4)
    
    // Processing metadata
    // Map Java enum to PostgreSQL custom enum type 'ingestion_status'
    // Using custom Hibernate UserType to handle PostgreSQL enum properly
    @Column(name = "status", nullable = false, columnDefinition = "ingestion_status")
    @Type(PostgreSQLEnumUserType.class)
    private Status status;
    
    @Column(name = "total_records")
    private Long totalRecords;
    
    @Column(name = "processed_records")
    private Long processedRecords;
    
    @Column(name = "failed_records")
    private Long failedRecords;
    
    // Timing information
    @Column(name = "started_at")
    private LocalDateTime startedAt;
    
    @Column(name = "completed_at")
    private LocalDateTime completedAt;
    
    @Column(name = "processing_duration_ms")
    private Long processingDurationMs;
    
    // Error handling
    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;
    
    @Column(name = "error_details", columnDefinition = "TEXT")
    private String errorDetails; // JSON string
    
    // Audit fields
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
    
    @Column(name = "created_by", length = 100)
    private String createdBy;
    
    // Transient flag to track if this manifest was returned from idempotency check
    // (not persisted to database, used for API response differentiation)
    @Transient
    private boolean alreadyProcessed = false;
    
    // Constructors
    public IngestionManifest() {
        this.batchId = UUID.randomUUID();
        this.status = Status.PENDING;
        this.createdBy = "system";
        this.totalRecords = 0L;
        this.processedRecords = 0L;
        this.failedRecords = 0L;
    }
    
    public IngestionManifest(String fileName, Long fileSizeBytes, String fileChecksum) {
        this();
        this.fileName = fileName;
        this.fileSizeBytes = fileSizeBytes;
        this.fileChecksum = fileChecksum;
    }
    
    // Helper methods
    public void markAsProcessing() {
        this.status = Status.PROCESSING;
        this.startedAt = LocalDateTime.now();
    }
    
    public void markAsCompleted() {
        this.status = Status.COMPLETED;
        this.completedAt = LocalDateTime.now();
        if (this.startedAt != null) {
            this.processingDurationMs = java.time.Duration.between(startedAt, completedAt).toMillis();
        }
    }
    
    public void markAsFailed(String errorMessage, String errorDetails) {
        this.status = Status.FAILED;
        this.completedAt = LocalDateTime.now();
        this.errorMessage = errorMessage;
        this.errorDetails = errorDetails;
        if (this.startedAt != null) {
            this.processingDurationMs = java.time.Duration.between(startedAt, completedAt).toMillis();
        }
    }
    
    public double getCompletionPercentage() {
        if (totalRecords == null || totalRecords == 0) {
            return 0.0;
        }
        return (processedRecords.doubleValue() / totalRecords.doubleValue()) * 100.0;
    }
    
    public boolean isProcessing() {
        return Status.PROCESSING.equals(this.status);
    }
    
    public boolean isCompleted() {
        return Status.COMPLETED.equals(this.status);
    }
    
    public boolean isFailed() {
        return Status.FAILED.equals(this.status);
    }
}