package teranet.mapdev.ingest.transformer;

/**
 * Interface for implementing custom data transformations per file type.
 * 
 * Implementations of this interface are dynamically loaded based on 
 * the transformer_class_name configured in file_validation_rules table.
 * 
 * Use cases:
 * - Date format conversions (e.g., '0000/00/00' -> NULL)
 * - Field-specific trimming beyond standard whitespace
 * - Data type conversions
 * - Complex field mappings
 * 
 * Example configuration in database:
 * INSERT INTO file_validation_rules (file_pattern, enable_data_transformation, transformer_class_name, ...)
 * VALUES ('IM2', TRUE, 'teranet.mapdev.ingest.transformer.IM2Transformer', ...);
 */
public interface DataTransformer {
    
    /**
     * Transform a single line of data from the input file.
     * 
     * This method is called for each line in the file (excluding headers if present).
     * The transformer should:
     * - Parse the line according to the file's delimiter
     * - Apply transformations to specific fields
     * - Return the transformed line
     * 
     * @param line The original line from the file
     * @param lineNumber The line number (1-based, excluding headers)
     * @return The transformed line, or null to skip this line
     */
    String transformLine(String line, long lineNumber);
    
    /**
     * Check if this transformer actually needs to modify data.
     * 
     * This is an optimization - if false, the transformation pipeline
     * can be skipped entirely for better performance.
     * 
     * @return true if transformLine() will modify data, false otherwise
     */
    default boolean requiresTransformation() {
        return true;
    }
    
    /**
     * Get the delimiter used by this file type.
     * Override if your file uses a non-tab delimiter.
     * 
     * @return The field delimiter (default: tab)
     */
    default String getDelimiter() {
        return "\t";
    }
    
    /**
     * Called once before processing starts.
     * Use this to initialize any resources.
     */
    default void initialize() {
        // Default: no initialization needed
    }
    
    /**
     * Called once after processing completes.
     * Use this to clean up resources.
     */
    default void cleanup() {
        // Default: no cleanup needed
    }
}
