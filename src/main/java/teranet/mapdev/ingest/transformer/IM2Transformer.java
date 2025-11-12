package teranet.mapdev.ingest.transformer;

import lombok.extern.slf4j.Slf4j;

/**
 * Data transformer for IM2 files (TRAILING_NULLCOLS format).
 * 
 * Transformations applied:
 * 1. Trim all fields
 * 2. Convert date field PARTY_B_DAY (field index 3):
 * - '0000/00/00' -> NULL (empty string for PostgreSQL)
 * - Empty string -> NULL
 * - Valid dates remain in YYYY/MM/DD format (PostgreSQL compatible)
 * 
 * Based on original SQL*Loader control file:
 * PARTY_B_DAY DATE TERMINATED BY " "
 * "decode(TRIM(:PARTY_B_DAY),'0000/00/00', null,
 * to_date(TRIM(:PARTY_B_DAY),'YYYY/MM/DD'))"
 * 
 * Configuration in database:
 * UPDATE file_validation_rules
 * SET enable_data_transformation = TRUE,
 * transformer_class_name = 'teranet.mapdev.ingest.transformer.IM2Transformer'
 * WHERE file_pattern = 'IM2';
 */
@Slf4j
public class IM2Transformer implements DataTransformer {

    // Field positions in IM2 file (0-based index)
    private static final int PARTY_B_DAY_INDEX = 3;

    // Invalid date markers
    private static final String INVALID_DATE_MARKER = "0000/00/00";

    @Override
    public String transformLine(String line, long lineNumber) {
        if (line == null || line.isEmpty()) {
            return line;
        }

        try {
            // Split by tab, preserving empty fields
            String[] fields = line.split("\t", -1);

            // Trim all fields
            for (int i = 0; i < fields.length; i++) {
                fields[i] = fields[i].trim();
            }

            // Transform PARTY_B_DAY field
            if (fields.length > PARTY_B_DAY_INDEX) {
                String dateValue = fields[PARTY_B_DAY_INDEX];

                // Convert invalid dates to NULL (empty string for PostgreSQL COPY)
                if (INVALID_DATE_MARKER.equals(dateValue) || dateValue.isEmpty()) {
                    fields[PARTY_B_DAY_INDEX] = ""; // PostgreSQL NULL
                    log.debug("Line {}: Converted invalid date '{}' to NULL", lineNumber, dateValue);
                }
                // Valid dates in YYYY/MM/DD format are PostgreSQL-compatible, no conversion
                // needed
            }

            // Rejoin with tabs
            return String.join("\t", fields);

        } catch (Exception e) {
            log.error("Error transforming IM2 line {}: {}", lineNumber, e.getMessage(), e);
            // Return original line if transformation fails
            return line;
        }
    }

    @Override
    public boolean requiresTransformation() {
        return true;
    }

    @Override
    public void initialize() {
        log.info("IM2Transformer initialized");
    }

    @Override
    public void cleanup() {
        log.info("IM2Transformer cleanup completed");
    }
}
