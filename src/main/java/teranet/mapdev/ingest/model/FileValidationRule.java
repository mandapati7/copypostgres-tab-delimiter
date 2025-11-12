package teranet.mapdev.ingest.model;

import java.time.LocalDateTime;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * Stores validation rules for different file types
 * This allows dynamic configuration without code changes
 */
@Entity
@Table(name = "file_validation_rules")
@Getter
@Setter
public class FileValidationRule {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "file_pattern", nullable = false, unique = true, length = 100)
    private String filePattern; // e.g., "PM3", "IM2", "PM5"

    @Column(name = "table_name", length = 100)
    private String tableName; // e.g., "staging_pm3"

    @Column(name = "expected_tab_count", nullable = false)
    private Integer expectedTabCount; // Expected number of tabs per row

    @Column(name = "validation_enabled", nullable = false)
    private Boolean validationEnabled = true;

    @Column(name = "auto_fix_enabled", nullable = false)
    private Boolean autoFixEnabled = false; // Whether to automatically fix issues

    @Column(name = "reject_on_violation", nullable = false)
    private Boolean rejectOnViolation = false; // Whether to reject the entire file

    @Column(name = "replace_control_chars", nullable = false)
    private Boolean replaceControlChars = false; // Replace control characters with asterisk

    @Column(name = "replace_non_latin_chars", nullable = false)
    private Boolean replaceNonLatinChars = false; // Replace non-BASIC_LATIN characters with asterisk

    @Column(name = "collapse_consecutive_replaced", nullable = false)
    private Boolean collapseConsecutiveReplaced = false; // Collapse consecutive replaced chars to single asterisk

    // Data transformation configuration
    @Column(name = "enable_data_transformation", nullable = false)
    private Boolean enableDataTransformation = false; // Whether to apply custom data transformations

    @Column(name = "transformer_class_name", length = 255)
    private String transformerClassName; // Fully qualified class name (e.g.,
                                         // "teranet.mapdev.ingest.transformer.IM2Transformer")

    @Column(name = "description", columnDefinition = "TEXT")
    private String description;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "created_by", length = 100)
    private String createdBy;
}
