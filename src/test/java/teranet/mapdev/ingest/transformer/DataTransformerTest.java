package teranet.mapdev.ingest.transformer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Test class for DataTransformer interface default methods.
 * Since DataTransformer is an interface, we test the default implementations.
 */
class DataTransformerTest {

    @Test
    void testDefaultRequiresTransformation() {
        // Test that default implementation returns true
        TestTransformer transformer = new TestTransformer();
        assertTrue(transformer.requiresTransformation(),
                "Default requiresTransformation should return true");
    }

    @Test
    void testDefaultGetDelimiter() {
        // Test that default delimiter is tab
        TestTransformer transformer = new TestTransformer();
        assertEquals("\t", transformer.getDelimiter(),
                "Default delimiter should be tab character");
    }

    @Test
    void testDefaultInitialize() {
        // Test that default initialize does nothing (no exception)
        TestTransformer transformer = new TestTransformer();
        assertDoesNotThrow(() -> transformer.initialize(),
                "Default initialize should not throw exception");
    }

    @Test
    void testDefaultCleanup() {
        // Test that default cleanup does nothing (no exception)
        TestTransformer transformer = new TestTransformer();
        assertDoesNotThrow(() -> transformer.cleanup(),
                "Default cleanup should not throw exception");
    }

    /**
     * Simple test implementation of DataTransformer for testing default methods.
     */
    private static class TestTransformer implements DataTransformer {
        @Override
        public String transformLine(String line, long lineNumber) {
            return line; // Simple pass-through for testing
        }
    }
}