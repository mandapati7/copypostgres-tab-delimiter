package teranet.mapdev.ingest.transformer;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class containing common logic for PM file transformers.
 * 
 * This centralizes the two different empty PIN fixing strategies:
 * 1. Control Character Check (PM1, PM3) - Insert "0000" at position 6
 * 2. Non-Digit Check (PM2, PM5, PM6) - Rebuild string with "0000"
 * 
 * Benefits:
 * - Single place to maintain transformation logic
 * - Consistent behavior across all PM transformers
 * - Easy to modify logic in one place
 * - Shared logging and error handling
 */
@Slf4j
public class PMTransformerUtils {

    private static final int PIN_CHECK_INDEX = 6;
    private static final String EMPTY_PIN_VALUE = "0000";
    private static final int SUBSTRING_START_INDEX = 10;

    /**
     * Strategy 1: Insert "0000" when control character is detected at position 6.
     * Used by: PM1, PM3
     * 
     * Legacy: PTDFixEmptyPin.java
     * Logic: if (line.length()>7 && line.charAt(6) is control char) { insert "0000" at position 6 }
     * 
     * @param line The input line
     * @param lineNumber The line number for logging
     * @param minLength Minimum line length required (typically 7)
     * @param filePattern File pattern name for logging (e.g., "PM1")
     * @return Transformed line or original if no transformation needed
     */
    public static String fixEmptyPinControlCharCheck(String line, long lineNumber, int minLength, String filePattern) {
        if (line == null || line.isEmpty()) {
            return line;
        }

        try {
            // Check if line length is sufficient
            if (line.length() <= minLength) {
                return line;
            }

            // Get character at position 6
            char charAtIndex6 = line.charAt(PIN_CHECK_INDEX);

            // Check if character is a control character (tab, newline, etc.)
            if (Character.isISOControl(charAtIndex6)) {
                // Insert "0000" at position 6
                StringBuilder sb = new StringBuilder(line);
                sb.insert(PIN_CHECK_INDEX, EMPTY_PIN_VALUE);
                String transformedLine = sb.toString();
                
                log.debug("{} Line {}: Inserted '{}' at position {} (control char ASCII: {})", 
                         filePattern, lineNumber, EMPTY_PIN_VALUE, PIN_CHECK_INDEX, (int) charAtIndex6);
                
                return transformedLine;
            }

            return line;

        } catch (Exception e) {
            log.error("Error in fixEmptyPinControlCharCheck for {} line {}: {}", 
                     filePattern, lineNumber, e.getMessage(), e);
            return line;
        }
    }

    /**
     * Strategy 2: Rebuild string when character at position 6 is NOT a digit.
     * Used by: PM2, PM5, PM6
     * 
     * Legacy: PTDFixEmptyPinPm2.java
     * Logic: if (line.length()>6 && !Character.isDigit(line.charAt(6))) {
     *          rebuild as: substring(0,6) + "0000" + substring(10)
     *        }
     * 
     * @param line The input line
     * @param lineNumber The line number for logging
     * @param minLength Minimum line length required (typically 6)
     * @param filePattern File pattern name for logging (e.g., "PM2")
     * @return Transformed line or original if no transformation needed
     */
    public static String fixEmptyPinNonDigitCheck(String line, long lineNumber, int minLength, String filePattern) {
        if (line == null || line.isEmpty()) {
            return line;
        }

        try {
            // Check if line length is sufficient
            if (line.length() <= minLength) {
                return line;
            }

            // Get character at position 6
            char charAtIndex6 = line.charAt(PIN_CHECK_INDEX);

            // Check if character at index 6 is NOT a digit
            if (!Character.isDigit(charAtIndex6)) {
                // Rebuild string: substring(0,6) + "0000" + substring(10)
                StringBuilder sb = new StringBuilder();
                sb.append(line.substring(0, PIN_CHECK_INDEX));
                sb.append(EMPTY_PIN_VALUE);
                
                // Only append substring(10) if line is long enough
                if (line.length() > SUBSTRING_START_INDEX) {
                    sb.append(line.substring(SUBSTRING_START_INDEX));
                }
                
                String transformedLine = sb.toString();
                
                log.debug("{} Line {}: Inserted '{}' - rebuilt string (char at 6: '{}')", 
                         filePattern, lineNumber, EMPTY_PIN_VALUE, charAtIndex6);
                
                return transformedLine;
            }

            return line;

        } catch (Exception e) {
            log.error("Error in fixEmptyPinNonDigitCheck for {} line {}: {}", 
                     filePattern, lineNumber, e.getMessage(), e);
            return line;
        }
    }
}
