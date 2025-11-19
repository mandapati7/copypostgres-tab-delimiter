package teranet.mapdev.ingest.transformer;

import lombok.extern.slf4j.Slf4j;

/**
 * Data transformer for PM3 files to fix empty PIN issues.
 * 
 * Uses the same logic as PM1 (PTDFixEmptyPin):
 * - Check if line.length() > 7
 * - Check if character at index 6 is a control character
 * - If true, insert "0000" at position 6
 * 
 * Configuration:
 * UPDATE file_validation_rules
 * SET enable_data_transformation = TRUE,
 *     transformer_class_name = 'teranet.mapdev.ingest.transformer.PM3Transformer'
 * WHERE file_pattern = 'PM3';
 */
@Slf4j
public class PM3Transformer implements DataTransformer {

    private static final int MIN_LINE_LENGTH = 7;

    @Override
    public String transformLine(String line, long lineNumber) {
        // Delegate to shared utility method
        return PMTransformerUtils.fixEmptyPinControlCharCheck(line, lineNumber, MIN_LINE_LENGTH, "PM3");
    }

    @Override
    public boolean requiresTransformation() {
        return true;
    }

    @Override
    public void initialize() {
        log.info("PM3Transformer initialized - PTDFixEmptyPin logic");
    }

    @Override
    public void cleanup() {
        log.info("PM3Transformer cleanup completed");
    }
}
