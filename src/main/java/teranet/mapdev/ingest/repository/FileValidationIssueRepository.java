package teranet.mapdev.ingest.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import teranet.mapdev.ingest.model.FileValidationIssue;

import java.util.List;
import java.util.UUID;

@Repository
public interface FileValidationIssueRepository extends JpaRepository<FileValidationIssue, Long> {

    /**
     * Find all validation issues for a specific batch
     */
    List<FileValidationIssue> findByBatchIdOrderByLineNumber(UUID batchId);

    /**
     * Find validation issues by batch and severity
     */
    List<FileValidationIssue> findByBatchIdAndSeverity(UUID batchId, FileValidationIssue.Severity severity);

    /**
     * Count validation issues by batch and severity
     */
    @Query("SELECT v.severity, COUNT(v) FROM FileValidationIssue v WHERE v.batchId = :batchId GROUP BY v.severity")
    List<Object[]> countIssuesBySeverity(@Param("batchId") UUID batchId);

    /**
     * Find all critical issues that caused file rejection
     */
    List<FileValidationIssue> findBySeverity(FileValidationIssue.Severity severity);
}
