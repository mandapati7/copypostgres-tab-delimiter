package teranet.mapdev.ingest.repository;

import teranet.mapdev.ingest.model.IngestionManifest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data JPA repository for IngestionManifest entity
 * Provides database vendor-independent data access
 */
@Repository
public interface IngestionManifestRepository extends JpaRepository<IngestionManifest, Long> {
    
    /**
     * Find manifest by batch ID
     */
    Optional<IngestionManifest> findByBatchId(UUID batchId);
    
    /**
     * Find manifest by file checksum (for idempotency)
     * Returns the most recently created manifest with this checksum
     */
    Optional<IngestionManifest> findFirstByFileChecksumOrderByCreatedAtDesc(String fileChecksum);
    
    /**
     * Find manifest by file checksum with COMPLETED status only
     * For duplicate detection - only checks against successfully processed files
     * Returns the most recently completed manifest with this checksum
     */
    Optional<IngestionManifest> findFirstByFileChecksumAndStatusOrderByCreatedAtDesc(String fileChecksum, IngestionManifest.Status status);
    
    /**
     * Find all child manifests by parent batch ID (for ZIP processing)
     */
    List<IngestionManifest> findByParentBatchIdOrderByCreatedAt(UUID parentBatchId);
    
    /**
     * Find all manifests by status
     */
    List<IngestionManifest> findByStatusOrderByCreatedAtDesc(IngestionManifest.Status status);
    
    /**
     * Find all manifests by status (multiple statuses)
     */
    List<IngestionManifest> findByStatusInOrderByCreatedAtDesc(List<IngestionManifest.Status> statuses);
    
    /**
     * Get processing statistics
     * Note: Comparing with enum constants instead of string literals
     */
    @Query("""
        SELECT 
            COUNT(m) as totalBatches,
            SUM(CASE WHEN m.status = teranet.mapdev.ingest.model.IngestionManifest$Status.COMPLETED THEN 1 ELSE 0 END) as completedBatches,
            SUM(CASE WHEN m.status = teranet.mapdev.ingest.model.IngestionManifest$Status.FAILED THEN 1 ELSE 0 END) as failedBatches,
            SUM(CASE WHEN m.status = teranet.mapdev.ingest.model.IngestionManifest$Status.PROCESSING THEN 1 ELSE 0 END) as processingBatches,
            COALESCE(SUM(m.totalRecords), 0) as totalRecordsProcessed
        FROM IngestionManifest m
        WHERE m.status IN (teranet.mapdev.ingest.model.IngestionManifest$Status.COMPLETED, teranet.mapdev.ingest.model.IngestionManifest$Status.FAILED, teranet.mapdev.ingest.model.IngestionManifest$Status.PROCESSING)
    """)
    Object[] getProcessingStatistics();
    
    /**
     * Count manifests by status
     */
    long countByStatus(IngestionManifest.Status status);
    
    /**
     * Check if checksum exists
     */
    boolean existsByFileChecksum(String fileChecksum);
    
    /**
     * Find top-level manifests (no parent) - typically ZIP files or standalone CSVs
     */
    List<IngestionManifest> findByParentBatchIdIsNullOrderByCreatedAtDesc();
}
