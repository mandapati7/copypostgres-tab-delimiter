package teranet.mapdev.ingest.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for NoOpTransformer.
 * Tests the pass-through behavior and optimization flag.
 */
class NoOpTransformerTest {

    private NoOpTransformer transformer;

    @BeforeEach
    void setUp() {
        transformer = new NoOpTransformer();
    }

    @Test
    void testTransformLine_PassThrough() {
        // Test that input is returned unchanged
        String input = "field1\tfield2\tfield3";
        String result = transformer.transformLine(input, 1);
        assertEquals(input, result, "Input should be returned unchanged");
    }

    @Test
    void testTransformLine_NullInput() {
        // Test null input
        String result = transformer.transformLine(null, 1);
        assertNull(result, "Null input should return null");
    }

    @Test
    void testTransformLine_EmptyInput() {
        // Test empty input
        String result = transformer.transformLine("", 1);
        assertEquals("", result, "Empty input should return empty string");
    }

    @Test
    void testTransformLine_LineNumberIgnored() {
        // Test that line number doesn't affect result
        String input = "test data";
        String result1 = transformer.transformLine(input, 1);
        String result2 = transformer.transformLine(input, 100);
        String result3 = transformer.transformLine(input, -1);

        assertEquals(input, result1, "Line number 1 should not affect result");
        assertEquals(input, result2, "Line number 100 should not affect result");
        assertEquals(input, result3, "Negative line number should not affect result");
    }

    @Test
    void testRequiresTransformation_OptimizationFlag() {
        // NoOpTransformer should not require transformation (optimization)
        assertFalse(transformer.requiresTransformation(),
                "NoOpTransformer should not require transformation for optimization");
    }

    @Test
    void testGetDelimiter_Default() {
        // Should use default tab delimiter
        assertEquals("\t", transformer.getDelimiter(),
                "Should use default tab delimiter");
    }

    @Test
    void testInitialize_Default() {
        // Default initialize should do nothing
        assertDoesNotThrow(() -> transformer.initialize(),
                "Default initialize should not throw exception");
    }

    @Test
    void testCleanup_Default() {
        // Default cleanup should do nothing
        assertDoesNotThrow(() -> transformer.cleanup(),
                "Default cleanup should not throw exception");
    }

    @Test
    void testTransformLine_SpecialCharacters() {
        // Test with special characters and various data
        String[] testInputs = {
                "normal\ttext\tdata",
                "with\nnewlines\tand\ttabs",
                "unicode\t🚀\temojis\t✓",
                "quotes\t\"hello\"\t'single'",
                "numbers\t123\t456.78",
                "empty\t\tfields",
                "single field",
                "\tleading tab",
                "trailing tab\t"
        };

        for (String input : testInputs) {
            String result = transformer.transformLine(input, 1);
            assertEquals(input, result,
                    "Input '" + input.replace("\t", "\\t").replace("\n", "\\n") + "' should be unchanged");
        }
    }

    @Test
    void testTransformLine_LargeData() {
        // Test with larger data to ensure no performance issues
        StringBuilder largeInput = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeInput.append("field").append(i).append("\t");
        }
        largeInput.append("end");

        String input = largeInput.toString();
        String result = transformer.transformLine(input, 1);

        assertEquals(input, result, "Large input should be handled correctly");
    }
}