package teranet.mapdev.ingest.transformer;

import lombok.extern.slf4j.Slf4j;

/**
 * Data transformer for PM2 files to fix empty PIN issues.
 * 
 * Based on legacy PTDFixEmptyPinPm2.java logic:
 * - Check if line.length() > 6
 * - Check if character at index 6 is NOT a digit
 * - If true, rebuild string: substring(0,6) + "0000" + substring(10)
 * 
 * Legacy code:
 * ```
 * tabCount = 0;
 * if (line.length()>6 && !Character.isDigit(line.charAt(index: 6))) {
 *     StringBuffer sb = new StringBuffer();
 *     sb.append(line.substring(beginIndex: 0,endIndex: 6));
 *     sb.append(str: "0000");
 *     sb.append(line.substring(beginIndex: 10));
 *     line = sb.toString();
 * }
 * ```
 * 
 * Configuration:
 * UPDATE file_validation_rules
 * SET enable_data_transformation = TRUE,
 *     transformer_class_name = 'teranet.mapdev.ingest.transformer.PM2Transformer'
 * WHERE file_pattern = 'PM2';
 */
@Slf4j
public class PM2Transformer implements DataTransformer {

    private static final int MIN_LINE_LENGTH = 6;

    @Override
    public String transformLine(String line, long lineNumber) {
        // Delegate to shared utility method
        return PMTransformerUtils.fixEmptyPinNonDigitCheck(line, lineNumber, MIN_LINE_LENGTH, "PM2");
    }

    @Override
    public boolean requiresTransformation() {
        return true;
    }

    @Override
    public void initialize() {
        log.info("PM2Transformer initialized - PTDFixEmptyPinPm2 logic");
    }

    @Override
    public void cleanup() {
        log.info("PM2Transformer cleanup completed");
    }
}
