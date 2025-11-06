package teranet.mapdev.ingest.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for CorrelationIdUtil.
 * Tests all MDC (Mapped Diagnostic Context) operations for correlation ID management.
 */
class CorrelationIdUtilTest {

    // Clean MDC before and after each test to ensure isolation
    @BeforeEach
    void setUp() {
        MDC.clear();
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void testGetCurrentCorrelationId_WhenNotSet_ReturnsDefaultValue() {
        // When: correlation ID is not set
        String correlationId = CorrelationIdUtil.getCurrentCorrelationId();

        // Then: returns default value
        assertThat(correlationId).isEqualTo("NO-CORRELATION-ID");
    }

    @Test
    void testGetCurrentCorrelationId_WhenSet_ReturnsActualValue() {
        // Given: a correlation ID is set
        String expectedId = "test-correlation-id-12345";
        CorrelationIdUtil.setCorrelationId(expectedId);

        // When: getting current correlation ID
        String correlationId = CorrelationIdUtil.getCurrentCorrelationId();

        // Then: returns the actual value
        assertThat(correlationId).isEqualTo(expectedId);
    }

    @Test
    void testSetCorrelationId_StoresValueInMDC() {
        // Given: a correlation ID
        String expectedId = "my-correlation-id";

        // When: setting correlation ID
        CorrelationIdUtil.setCorrelationId(expectedId);

        // Then: value is stored in MDC
        assertThat(MDC.get("correlationId")).isEqualTo(expectedId);
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo(expectedId);
    }

    @Test
    void testSetCorrelationId_CanOverwriteExistingValue() {
        // Given: an existing correlation ID
        CorrelationIdUtil.setCorrelationId("old-id");

        // When: setting a new correlation ID
        String newId = "new-id";
        CorrelationIdUtil.setCorrelationId(newId);

        // Then: new value overwrites old value
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo(newId);
    }

    @Test
    void testClearCorrelationId_RemovesValueFromMDC() {
        // Given: a correlation ID is set
        CorrelationIdUtil.setCorrelationId("test-id");
        assertThat(CorrelationIdUtil.hasCorrelationId()).isTrue();

        // When: clearing correlation ID
        CorrelationIdUtil.clearCorrelationId();

        // Then: value is removed from MDC
        assertThat(MDC.get("correlationId")).isNull();
        assertThat(CorrelationIdUtil.hasCorrelationId()).isFalse();
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo("NO-CORRELATION-ID");
    }

    @Test
    void testClearCorrelationId_WhenNotSet_DoesNotThrowException() {
        // Given: no correlation ID is set
        assertThat(CorrelationIdUtil.hasCorrelationId()).isFalse();

        // When & Then: clearing does not throw exception
        CorrelationIdUtil.clearCorrelationId();
        assertThat(CorrelationIdUtil.hasCorrelationId()).isFalse();
    }

    @Test
    void testHasCorrelationId_WhenNotSet_ReturnsFalse() {
        // When: correlation ID is not set
        boolean hasId = CorrelationIdUtil.hasCorrelationId();

        // Then: returns false
        assertThat(hasId).isFalse();
    }

    @Test
    void testHasCorrelationId_WhenSet_ReturnsTrue() {
        // Given: a correlation ID is set
        CorrelationIdUtil.setCorrelationId("test-id");

        // When: checking if correlation ID exists
        boolean hasId = CorrelationIdUtil.hasCorrelationId();

        // Then: returns true
        assertThat(hasId).isTrue();
    }

    @Test
    void testHasCorrelationId_AfterClear_ReturnsFalse() {
        // Given: a correlation ID is set then cleared
        CorrelationIdUtil.setCorrelationId("test-id");
        CorrelationIdUtil.clearCorrelationId();

        // When: checking if correlation ID exists
        boolean hasId = CorrelationIdUtil.hasCorrelationId();

        // Then: returns false
        assertThat(hasId).isFalse();
    }

    @Test
    void testSetCorrelationId_WithNullValue_StoresNull() {
        // Given: a null correlation ID
        String nullId = null;

        // When: setting null correlation ID
        CorrelationIdUtil.setCorrelationId(nullId);

        // Then: null is stored (MDC allows null)
        assertThat(CorrelationIdUtil.hasCorrelationId()).isFalse();
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo("NO-CORRELATION-ID");
    }

    @Test
    void testSetCorrelationId_WithEmptyString_StoresEmptyString() {
        // Given: an empty string correlation ID
        String emptyId = "";

        // When: setting empty correlation ID
        CorrelationIdUtil.setCorrelationId(emptyId);

        // Then: empty string is stored
        assertThat(CorrelationIdUtil.hasCorrelationId()).isTrue();
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo("");
    }

    @Test
    void testCorrelationIdLifecycle_CompleteFlow() {
        // Test the complete lifecycle: set -> get -> check -> clear

        // Step 1: Initially not set
        assertThat(CorrelationIdUtil.hasCorrelationId()).isFalse();
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo("NO-CORRELATION-ID");

        // Step 2: Set correlation ID
        String correlationId = "lifecycle-test-id";
        CorrelationIdUtil.setCorrelationId(correlationId);
        assertThat(CorrelationIdUtil.hasCorrelationId()).isTrue();
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo(correlationId);

        // Step 3: Update correlation ID
        String newCorrelationId = "updated-lifecycle-id";
        CorrelationIdUtil.setCorrelationId(newCorrelationId);
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo(newCorrelationId);

        // Step 4: Clear correlation ID
        CorrelationIdUtil.clearCorrelationId();
        assertThat(CorrelationIdUtil.hasCorrelationId()).isFalse();
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo("NO-CORRELATION-ID");
    }

    @Test
    void testSetCorrelationId_WithSpecialCharacters_HandlesCorrectly() {
        // Given: correlation ID with special characters
        String specialId = "id-with-special-chars-@#$%^&*()_+-=[]{}|;:,.<>?/~`";

        // When: setting correlation ID
        CorrelationIdUtil.setCorrelationId(specialId);

        // Then: special characters are preserved
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo(specialId);
    }

    @Test
    void testSetCorrelationId_WithUUID_HandlesCorrectly() {
        // Given: a UUID-style correlation ID (common format)
        String uuidId = "550e8400-e29b-41d4-a716-446655440000";

        // When: setting correlation ID
        CorrelationIdUtil.setCorrelationId(uuidId);

        // Then: UUID is preserved
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo(uuidId);
        assertThat(CorrelationIdUtil.hasCorrelationId()).isTrue();
    }

    @Test
    void testThreadIsolation_MDCIsThreadLocal() throws InterruptedException {
        // Given: correlation ID in main thread
        String mainThreadId = "main-thread-id";
        CorrelationIdUtil.setCorrelationId(mainThreadId);

        // When: accessing from another thread
        Thread otherThread = new Thread(() -> {
            // Then: other thread sees no correlation ID (MDC is thread-local)
            assertThat(CorrelationIdUtil.hasCorrelationId()).isFalse();
            assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo("NO-CORRELATION-ID");

            // Set correlation ID in other thread
            CorrelationIdUtil.setCorrelationId("other-thread-id");
            assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo("other-thread-id");
        });

        otherThread.start();
        otherThread.join();

        // Then: main thread still has its original correlation ID
        assertThat(CorrelationIdUtil.getCurrentCorrelationId()).isEqualTo(mainThreadId);
    }
}
