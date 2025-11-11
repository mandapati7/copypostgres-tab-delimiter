package teranet.mapdev.ingest.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Tracks validation issues found during file processing
 * Used for reporting back to the sender
 */
@Entity
@Table(name = "file_validation_issues", indexes = {
        @Index(name = "idx_validation_batch", columnList = "batch_id"),
        @Index(name = "idx_validation_severity", columnList = "severity"),
        @Index(name = "idx_validation_created", columnList = "created_at")
})
@Getter
@Setter
public class FileValidationIssue {

    public enum Severity {
        INFO, // Informational - no action needed
        WARNING, // Fixed automatically
        ERROR, // Critical issue - record rejected
        CRITICAL // File-level issue - entire file rejected
    }

    public enum IssueType {
        EXCESS_TABS, // More tabs than expected
        INSUFFICIENT_TABS, // Fewer tabs than expected
        INVALID_CHARACTERS, // Invalid characters in data
        EMPTY_REQUIRED_FIELD, // Required field is empty
        DATA_TYPE_MISMATCH, // Data doesn't match expected type
        LENGTH_VIOLATION, // Field exceeds max length
        CUSTOM // Custom validation rule
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "batch_id", nullable = false, columnDefinition = "UUID")
    private UUID batchId;

    @Column(name = "file_name", nullable = false, length = 255)
    private String fileName;

    @Column(name = "line_number", nullable = false)
    private Long lineNumber;

    @Column(name = "issue_type", nullable = false, length = 50)
    @Enumerated(EnumType.STRING)
    private IssueType issueType;

    @Column(name = "severity", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private Severity severity;

    @Column(name = "expected_value", length = 500)
    private String expectedValue;

    @Column(name = "actual_value", length = 500)
    private String actualValue;

    @Column(name = "description", columnDefinition = "TEXT")
    private String description;

    @Column(name = "auto_fixed", nullable = false)
    private Boolean autoFixed = false;

    @Column(name = "fix_description", columnDefinition = "TEXT")
    private String fixDescription;

    @Column(name = "original_line", columnDefinition = "TEXT")
    private String originalLine;

    @Column(name = "corrected_line", columnDefinition = "TEXT")
    private String correctedLine;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}
