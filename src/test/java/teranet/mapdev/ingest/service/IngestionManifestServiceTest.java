package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import teranet.mapdev.ingest.model.IngestionManifest;
import teranet.mapdev.ingest.repository.IngestionManifestRepository;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IngestionManifestServiceTest {

    @Mock
    private IngestionManifestRepository repository;

    @InjectMocks
    private IngestionManifestService service;

    private IngestionManifest manifest;
    private UUID batchId;

    @BeforeEach
    void setUp() {
        batchId = UUID.randomUUID();
        manifest = new IngestionManifest();
        manifest.setId(1L);
        manifest.setBatchId(batchId);
        manifest.setFileName("test.csv");
        manifest.setFileSizeBytes(1000L);
        manifest.setFileChecksum("abc123");
        manifest.setStatus(IngestionManifest.Status.COMPLETED);
        manifest.setCreatedAt(LocalDateTime.now());
    }

    @Test
    void testSave_Success() {
        when(repository.save(any(IngestionManifest.class))).thenReturn(manifest);

        IngestionManifest result = service.save(manifest);

        assertNotNull(result);
        assertEquals(manifest.getId(), result.getId());
        assertEquals(manifest.getBatchId(), result.getBatchId());
        verify(repository, times(1)).save(manifest);
    }

    @Test
    void testSave_Exception_FallbackMode() {
        IngestionManifest newManifest = new IngestionManifest();
        when(repository.save(any(IngestionManifest.class))).thenThrow(new RuntimeException("DB error"));

        IngestionManifest result = service.save(newManifest);

        assertNotNull(result);
        assertNotNull(result.getBatchId()); // Generated UUID
        verify(repository, times(1)).save(newManifest);
    }

    @Test
    void testUpdate_Success() {
        when(repository.save(any(IngestionManifest.class))).thenReturn(manifest);

        IngestionManifest result = service.update(manifest);

        assertNotNull(result);
        assertEquals(manifest.getId(), result.getId());
        verify(repository, times(1)).save(manifest);
    }

    @Test
    void testUpdate_Exception_ReturnsSameManifest() {
        when(repository.save(any(IngestionManifest.class))).thenThrow(new RuntimeException("DB error"));

        IngestionManifest result = service.update(manifest);

        assertNotNull(result);
        assertEquals(manifest, result);
        verify(repository, times(1)).save(manifest);
    }

    @Test
    void testFindByBatchId_Success() {
        when(repository.findByBatchId(batchId)).thenReturn(Optional.of(manifest));

        IngestionManifest result = service.findByBatchId(batchId);

        assertNotNull(result);
        assertEquals(manifest.getId(), result.getId());
        verify(repository, times(1)).findByBatchId(batchId);
    }

    @Test
    void testFindByBatchId_NotFound() {
        when(repository.findByBatchId(batchId)).thenReturn(Optional.empty());

        IngestionManifest result = service.findByBatchId(batchId);

        assertNull(result);
        verify(repository, times(1)).findByBatchId(batchId);
    }

    @Test
    void testFindByBatchId_Exception() {
        when(repository.findByBatchId(batchId)).thenThrow(new RuntimeException("DB error"));

        IngestionManifest result = service.findByBatchId(batchId);

        assertNull(result);
        verify(repository, times(1)).findByBatchId(batchId);
    }

    @Test
    void testFindByChecksum_Success() {
        String checksum = "abc123";
        when(repository.findFirstByFileChecksumOrderByCreatedAtDesc(checksum)).thenReturn(Optional.of(manifest));

        IngestionManifest result = service.findByChecksum(checksum);

        assertNotNull(result);
        assertEquals(manifest.getFileChecksum(), result.getFileChecksum());
        verify(repository, times(1)).findFirstByFileChecksumOrderByCreatedAtDesc(checksum);
    }

    @Test
    void testFindByChecksum_NotFound() {
        String checksum = "xyz789";
        when(repository.findFirstByFileChecksumOrderByCreatedAtDesc(checksum)).thenReturn(Optional.empty());

        IngestionManifest result = service.findByChecksum(checksum);

        assertNull(result);
        verify(repository, times(1)).findFirstByFileChecksumOrderByCreatedAtDesc(checksum);
    }

    @Test
    void testFindByChecksum_Exception() {
        String checksum = "abc123";
        when(repository.findFirstByFileChecksumOrderByCreatedAtDesc(checksum))
            .thenThrow(new RuntimeException("DB error"));

        IngestionManifest result = service.findByChecksum(checksum);

        assertNull(result);
        verify(repository, times(1)).findFirstByFileChecksumOrderByCreatedAtDesc(checksum);
    }

    @Test
    void testFindByParentBatchId_Success() {
        UUID parentBatchId = UUID.randomUUID();
        List<IngestionManifest> manifests = Arrays.asList(manifest);
        when(repository.findByParentBatchIdOrderByCreatedAt(parentBatchId)).thenReturn(manifests);

        List<IngestionManifest> result = service.findByParentBatchId(parentBatchId);

        assertNotNull(result);
        assertEquals(1, result.size());
        verify(repository, times(1)).findByParentBatchIdOrderByCreatedAt(parentBatchId);
    }

    @Test
    void testFindByParentBatchId_Exception() {
        UUID parentBatchId = UUID.randomUUID();
        when(repository.findByParentBatchIdOrderByCreatedAt(parentBatchId))
            .thenThrow(new RuntimeException("DB error"));

        List<IngestionManifest> result = service.findByParentBatchId(parentBatchId);

        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(repository, times(1)).findByParentBatchIdOrderByCreatedAt(parentBatchId);
    }

    @Test
    void testFindByStatus_Success() {
        List<IngestionManifest> manifests = Arrays.asList(manifest);
        when(repository.findByStatusOrderByCreatedAtDesc(IngestionManifest.Status.COMPLETED))
            .thenReturn(manifests);

        List<IngestionManifest> result = service.findByStatus(IngestionManifest.Status.COMPLETED);

        assertNotNull(result);
        assertEquals(1, result.size());
        verify(repository, times(1)).findByStatusOrderByCreatedAtDesc(IngestionManifest.Status.COMPLETED);
    }

    @Test
    void testFindByStatus_Exception() {
        when(repository.findByStatusOrderByCreatedAtDesc(any())).thenThrow(new RuntimeException("DB error"));

        List<IngestionManifest> result = service.findByStatus(IngestionManifest.Status.FAILED);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testFindByStatuses_Success() {
        List<IngestionManifest.Status> statuses = Arrays.asList(
            IngestionManifest.Status.COMPLETED, IngestionManifest.Status.FAILED
        );
        List<IngestionManifest> manifests = Arrays.asList(manifest);
        when(repository.findByStatusInOrderByCreatedAtDesc(statuses)).thenReturn(manifests);

        List<IngestionManifest> result = service.findByStatuses(statuses);

        assertNotNull(result);
        assertEquals(1, result.size());
        verify(repository, times(1)).findByStatusInOrderByCreatedAtDesc(statuses);
    }

    @Test
    void testFindByStatuses_Exception() {
        List<IngestionManifest.Status> statuses = Arrays.asList(IngestionManifest.Status.COMPLETED);
        when(repository.findByStatusInOrderByCreatedAtDesc(statuses))
            .thenThrow(new RuntimeException("DB error"));

        List<IngestionManifest> result = service.findByStatuses(statuses);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testFindTopLevelManifests_Success() {
        List<IngestionManifest> manifests = Arrays.asList(manifest);
        when(repository.findByParentBatchIdIsNullOrderByCreatedAtDesc()).thenReturn(manifests);

        List<IngestionManifest> result = service.findTopLevelManifests();

        assertNotNull(result);
        assertEquals(1, result.size());
        verify(repository, times(1)).findByParentBatchIdIsNullOrderByCreatedAtDesc();
    }

    @Test
    void testFindTopLevelManifests_Exception() {
        when(repository.findByParentBatchIdIsNullOrderByCreatedAtDesc())
            .thenThrow(new RuntimeException("DB error"));

        List<IngestionManifest> result = service.findTopLevelManifests();

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetProcessingStats_Success() {
        Object[] stats = new Object[]{10L, 7L, 2L, 1L, 1000L};
        when(repository.getProcessingStatistics()).thenReturn(stats);

        IngestionManifestService.ProcessingStats result = service.getProcessingStats();

        assertNotNull(result);
        assertEquals(10L, result.getTotalBatches());
        assertEquals(7L, result.getCompletedBatches());
        assertEquals(2L, result.getFailedBatches());
        assertEquals(1L, result.getProcessingBatches());
        assertEquals(1000L, result.getTotalRecordsProcessed());
        verify(repository, times(1)).getProcessingStatistics();
    }

    @Test
    void testGetProcessingStats_NullStats() {
        when(repository.getProcessingStatistics()).thenReturn(null);

        IngestionManifestService.ProcessingStats result = service.getProcessingStats();

        assertNotNull(result);
        assertEquals(0L, result.getTotalBatches());
        assertEquals(0L, result.getCompletedBatches());
    }

    @Test
    void testGetProcessingStats_Exception() {
        when(repository.getProcessingStatistics()).thenThrow(new RuntimeException("DB error"));

        IngestionManifestService.ProcessingStats result = service.getProcessingStats();

        assertNotNull(result);
        assertEquals(0L, result.getTotalBatches());
    }

    @Test
    void testCountByStatus_Success() {
        when(repository.countByStatus(IngestionManifest.Status.COMPLETED)).thenReturn(5L);

        long result = service.countByStatus(IngestionManifest.Status.COMPLETED);

        assertEquals(5L, result);
        verify(repository, times(1)).countByStatus(IngestionManifest.Status.COMPLETED);
    }

    @Test
    void testCountByStatus_Exception() {
        when(repository.countByStatus(any())).thenThrow(new RuntimeException("DB error"));

        long result = service.countByStatus(IngestionManifest.Status.FAILED);

        assertEquals(0L, result);
    }

    @Test
    void testExistsByChecksum_True() {
        String checksum = "abc123";
        when(repository.existsByFileChecksum(checksum)).thenReturn(true);

        boolean result = service.existsByChecksum(checksum);

        assertTrue(result);
        verify(repository, times(1)).existsByFileChecksum(checksum);
    }

    @Test
    void testExistsByChecksum_False() {
        String checksum = "xyz789";
        when(repository.existsByFileChecksum(checksum)).thenReturn(false);

        boolean result = service.existsByChecksum(checksum);

        assertFalse(result);
        verify(repository, times(1)).existsByFileChecksum(checksum);
    }

    @Test
    void testExistsByChecksum_Exception() {
        String checksum = "abc123";
        when(repository.existsByFileChecksum(checksum)).thenThrow(new RuntimeException("DB error"));

        boolean result = service.existsByChecksum(checksum);

        assertFalse(result);
    }

    @Test
    void testFindById_Success() {
        when(repository.findById(1L)).thenReturn(Optional.of(manifest));

        IngestionManifest result = service.findById(1L);

        assertNotNull(result);
        assertEquals(manifest.getId(), result.getId());
        verify(repository, times(1)).findById(1L);
    }

    @Test
    void testFindById_NotFound() {
        when(repository.findById(1L)).thenReturn(Optional.empty());

        IngestionManifest result = service.findById(1L);

        assertNull(result);
        verify(repository, times(1)).findById(1L);
    }

    @Test
    void testFindById_Exception() {
        when(repository.findById(1L)).thenThrow(new RuntimeException("DB error"));

        IngestionManifest result = service.findById(1L);

        assertNull(result);
    }

    @Test
    void testDeleteById_Success() {
        doNothing().when(repository).deleteById(1L);

        service.deleteById(1L);

        verify(repository, times(1)).deleteById(1L);
    }

    @Test
    void testDeleteById_Exception() {
        doThrow(new RuntimeException("DB error")).when(repository).deleteById(1L);

        service.deleteById(1L);

        verify(repository, times(1)).deleteById(1L);
    }
}
