package teranet.mapdev.ingest.transformer;

import lombok.extern.slf4j.Slf4j;

/**
 * Data transformer for PM1 files to fix empty PIN issues.
 * 
 * Based on legacy PTDFixEmptyPin.java logic:
 * - Check if line.length() > 7
 * - Check if character at index 6 is a control character (tab, etc.)
 * - If true, insert "0000" at position 6
 * 
 * Legacy code:
 * ```
 * if (line.length()>7 && line.substring(beginIndex: 6,endIndex: 7).equals(anObject: "\c\c")) {
 *     StringBuffer sb = new StringBuffer(line);
 *     sb.insert(offset: 6,str: "0000");
 *     line = sb.toString();
 * }
 * ```
 * 
 * Configuration:
 * UPDATE file_validation_rules
 * SET enable_data_transformation = TRUE,
 *     transformer_class_name = 'teranet.mapdev.ingest.transformer.PM1Transformer'
 * WHERE file_pattern = 'PM1';
 */
@Slf4j
public class PM1Transformer implements DataTransformer {

    private static final int MIN_LINE_LENGTH = 7;

    @Override
    public String transformLine(String line, long lineNumber) {
        // Delegate to shared utility method
        return PMTransformerUtils.fixEmptyPinControlCharCheck(line, lineNumber, MIN_LINE_LENGTH, "PM1");
    }

    @Override
    public boolean requiresTransformation() {
        return true;
    }

    @Override
    public void initialize() {
        log.info("PM1Transformer initialized - PTDFixEmptyPin logic");
    }

    @Override
    public void cleanup() {
        log.info("PM1Transformer cleanup completed");
    }
}
