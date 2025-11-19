package teranet.mapdev.ingest.transformer;

import lombok.extern.slf4j.Slf4j;

/**
 * Data transformer for PM5 files to fix empty PIN issues.
 * 
 * Uses the same logic as PM2 (PTDFixEmptyPinPm2):
 * - Check if line.length() > 6
 * - Check if character at index 6 is NOT a digit
 * - If true, rebuild string: substring(0,6) + "0000" + substring(10)
 * 
 * Configuration:
 * UPDATE file_validation_rules
 * SET enable_data_transformation = TRUE,
 *     transformer_class_name = 'teranet.mapdev.ingest.transformer.PM5Transformer'
 * WHERE file_pattern = 'PM5';
 */
@Slf4j
public class PM5Transformer implements DataTransformer {

    private static final int MIN_LINE_LENGTH = 6;

    @Override
    public String transformLine(String line, long lineNumber) {
        // Delegate to shared utility method
        return PMTransformerUtils.fixEmptyPinNonDigitCheck(line, lineNumber, MIN_LINE_LENGTH, "PM5");
    }

    @Override
    public boolean requiresTransformation() {
        return true;
    }

    @Override
    public void initialize() {
        log.info("PM5Transformer initialized - PTDFixEmptyPinPm2 logic");
    }

    @Override
    public void cleanup() {
        log.info("PM5Transformer cleanup completed");
    }
}
