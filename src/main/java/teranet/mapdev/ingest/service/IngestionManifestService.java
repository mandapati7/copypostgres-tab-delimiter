package teranet.mapdev.ingest.service;

import teranet.mapdev.ingest.model.IngestionManifest;
import teranet.mapdev.ingest.repository.IngestionManifestRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Service for managing ingestion manifest records using Spring Data JPA
 * Database vendor-independent implementation
 */
@Service
@Transactional
public class IngestionManifestService {
    
    private static final Logger logger = LoggerFactory.getLogger(IngestionManifestService.class);
    
    @Autowired
    private IngestionManifestRepository repository;
    
    /**
     * Save a new ingestion manifest
     */
    public IngestionManifest save(IngestionManifest manifest) {
        try {
            IngestionManifest saved = repository.save(manifest);
            logger.info("Saved ingestion manifest with ID {} for batch {}", saved.getId(), saved.getBatchId());
            return saved;
        } catch (Exception e) {
            logger.error("Error saving ingestion manifest: {}", e.getMessage(), e);
            // Fallback: return manifest with generated ID for compatibility
            if (manifest.getBatchId() == null) {
                manifest.setBatchId(UUID.randomUUID());
            }
            return manifest;
        }
    }
    
    /**
     * Update an existing ingestion manifest
     */
    public IngestionManifest update(IngestionManifest manifest) {
        try {
            IngestionManifest updated = repository.save(manifest);
            logger.debug("Updated ingestion manifest {} with status {}", updated.getId(), updated.getStatus());
            return updated;
        } catch (Exception e) {
            logger.warn("Could not update manifest in database: {}", e.getMessage());
            // Return the manifest as-is for fallback mode
            return manifest;
        }
    }
    
    /**
     * Find manifest by batch ID
     */
    public IngestionManifest findByBatchId(UUID batchId) {
        try {
            return repository.findByBatchId(batchId).orElse(null);
        } catch (Exception e) {
            logger.error("Error finding manifest by batch ID {}: {}", batchId, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Find manifest by file checksum (for idempotency)
     */
    public IngestionManifest findByChecksum(String checksum) {
        try {
            return repository.findFirstByFileChecksumAndStatusOrderByCreatedAtDesc(
                    checksum,
                    IngestionManifest.Status.COMPLETED).orElse(null);
                } catch (Exception e) {
            logger.warn("Could not search for existing manifest by checksum: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Find all child manifests by parent batch ID (for ZIP processing)
     */
    public List<IngestionManifest> findByParentBatchId(UUID parentBatchId) {
        try {
            List<IngestionManifest> manifests = repository.findByParentBatchIdOrderByCreatedAt(parentBatchId);
            logger.debug("Found {} child manifests for parent batch {}", manifests.size(), parentBatchId);
            return manifests;
        } catch (Exception e) {
            logger.error("Error finding manifests by parent batch ID {}: {}", parentBatchId, e.getMessage(), e);
            return List.of();
        }
    }
    
    /**
     * Find all manifests by status
     */
    public List<IngestionManifest> findByStatus(IngestionManifest.Status status) {
        try {
            return repository.findByStatusOrderByCreatedAtDesc(status);
        } catch (Exception e) {
            logger.error("Error finding manifests by status {}: {}", status, e.getMessage(), e);
            return List.of();
        }
    }
    
    /**
     * Find all manifests with any of the specified statuses
     */
    public List<IngestionManifest> findByStatuses(List<IngestionManifest.Status> statuses) {
        try {
            return repository.findByStatusInOrderByCreatedAtDesc(statuses);
        } catch (Exception e) {
            logger.error("Error finding manifests by statuses: {}", e.getMessage(), e);
            return List.of();
        }
    }
    
    /**
     * Find all top-level manifests (no parent) - typically ZIP files or standalone CSVs
     */
    public List<IngestionManifest> findTopLevelManifests() {
        try {
            return repository.findByParentBatchIdIsNullOrderByCreatedAtDesc();
        } catch (Exception e) {
            logger.error("Error finding top-level manifests: {}", e.getMessage(), e);
            return List.of();
        }
    }
    
    /**
     * Get processing statistics
     */
    public ProcessingStats getProcessingStats() {
        try {
            Object[] stats = repository.getProcessingStatistics();
            if (stats != null && stats.length > 0) {
                return new ProcessingStats(
                    ((Number) stats[0]).longValue(),  // totalBatches
                    ((Number) stats[1]).longValue(),  // completedBatches
                    ((Number) stats[2]).longValue(),  // failedBatches
                    ((Number) stats[3]).longValue(),  // processingBatches
                    ((Number) stats[4]).longValue()   // totalRecordsProcessed
                );
            }
            return new ProcessingStats(0, 0, 0, 0, 0);
        } catch (Exception e) {
            logger.error("Error retrieving processing statistics: {}", e.getMessage(), e);
            return new ProcessingStats(0, 0, 0, 0, 0);
        }
    }
    
    /**
     * Count manifests by status
     */
    public long countByStatus(IngestionManifest.Status status) {
        try {
            return repository.countByStatus(status);
        } catch (Exception e) {
            logger.error("Error counting manifests by status {}: {}", status, e.getMessage(), e);
            return 0;
        }
    }
    
    /**
     * Check if a file with this checksum already exists
     */
    public boolean existsByChecksum(String checksum) {
        try {
            return repository.existsByFileChecksum(checksum);
        } catch (Exception e) {
            logger.warn("Could not check for existing checksum: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Find manifest by ID
     */
    public IngestionManifest findById(Long id) {
        try {
            return repository.findById(id).orElse(null);
        } catch (Exception e) {
            logger.error("Error finding manifest by ID {}: {}", id, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Delete a manifest by ID
     */
    public void deleteById(Long id) {
        try {
            repository.deleteById(id);
            logger.info("Deleted manifest with ID {}", id);
        } catch (Exception e) {
            logger.error("Error deleting manifest with ID {}: {}", id, e.getMessage(), e);
        }
    }
    
    /**
     * Processing statistics DTO
     */
    public static class ProcessingStats {
        private final long totalBatches;
        private final long completedBatches;
        private final long failedBatches;
        private final long processingBatches;
        private final long totalRecordsProcessed;
        
        public ProcessingStats(long totalBatches, long completedBatches, long failedBatches, 
                             long processingBatches, long totalRecordsProcessed) {
            this.totalBatches = totalBatches;
            this.completedBatches = completedBatches;
            this.failedBatches = failedBatches;
            this.processingBatches = processingBatches;
            this.totalRecordsProcessed = totalRecordsProcessed;
        }
        
        public long getTotalBatches() { return totalBatches; }
        public long getCompletedBatches() { return completedBatches; }
        public long getFailedBatches() { return failedBatches; }
        public long getProcessingBatches() { return processingBatches; }
        public long getTotalRecordsProcessed() { return totalRecordsProcessed; }
    }
}
