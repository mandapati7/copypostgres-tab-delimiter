package teranet.mapdev.ingest.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple unit tests for IngestionManifest - focusing on core functionality.
 * This test file demonstrates basic test structure and will compile successfully.
 */
@DisplayName("IngestionManifest - Simple Unit Tests")
class IngestionManifestSimpleTest {
    
    @Test
    @DisplayName("Should create manifest with constructor")
    void testConstructorWithParameters() {
        // Given
        String filename = "test.csv";
        Long fileSize = 1024L;
        String checksum = "abc123";
        
        // When
        IngestionManifest manifest = new IngestionManifest(filename, fileSize, checksum);
        
        // Then
        assertNotNull(manifest);
        assertNotNull(manifest.getBatchId());
        assertEquals(IngestionManifest.Status.PENDING, manifest.getStatus());
    }
    
    @Test
    @DisplayName("Should mark as processing")
    void testMarkAsProcessing() {
        // Given
        IngestionManifest manifest = new IngestionManifest();
        
        // When
        manifest.markAsProcessing();
        
        // Then
        assertTrue(manifest.isProcessing());
        assertFalse(manifest.isCompleted());
        assertNotNull(manifest.getStartedAt());
    }
    
    @Test
    @DisplayName("Should mark as completed")
    void testMarkAsCompleted() {
        // Given
        IngestionManifest manifest = new IngestionManifest();
        manifest.markAsProcessing();
        
        // When
        manifest.markAsCompleted();
        
        // Then
        assertTrue(manifest.isCompleted());
        assertFalse(manifest.isProcessing());
        assertNotNull(manifest.getCompletedAt());
        assertNotNull(manifest.getProcessingDurationMs());
    }
    
    @Test
    @DisplayName("Should mark as failed with error message")
    void testMarkAsFailed() {
        // Given
        IngestionManifest manifest = new IngestionManifest();
        manifest.markAsProcessing();
        String errorMsg = "Test error";
        String errorDetails = "Error details";
        
        // When
        manifest.markAsFailed(errorMsg, errorDetails);
        
        // Then
        assertTrue(manifest.isFailed());
        assertEquals(errorMsg, manifest.getErrorMessage());
        assertEquals(errorDetails, manifest.getErrorDetails());
    }
    
    @Test
    @DisplayName("Should calculate completion percentage")
    void testCompletionPercentage() {
        // Given
        IngestionManifest manifest = new IngestionManifest();
        manifest.setTotalRecords(100L);
        manifest.setProcessedRecords(75L);
        
        // When
        double percentage = manifest.getCompletionPercentage();
        
        // Then
        assertEquals(75.0, percentage, 0.01);
    }
    
    @Test
    @DisplayName("Should handle parent-child relationship")
    void testParentChildRelationship() {
        // Given
        UUID parentId = UUID.randomUUID();
        IngestionManifest child = new IngestionManifest();
        
        // When
        child.setParentBatchId(parentId);
        
        // Then
        assertEquals(parentId, child.getParentBatchId());
    }
}
