package teranet.mapdev.ingest.transformer;

/**
 * No-operation transformer that passes data through unchanged.
 * 
 * Used for file patterns that don't require custom transformations.
 * This is the default transformer when:
 * - enable_data_transformation is FALSE
 * - transformer_class_name is NULL or blank
 * - transformer_class_name cannot be loaded
 */
public class NoOpTransformer implements DataTransformer {
    
    @Override
    public String transformLine(String line, long lineNumber) {
        // Pass through unchanged
        return line;
    }
    
    @Override
    public boolean requiresTransformation() {
        // Optimization: skip transformation pipeline entirely
        return false;
    }
}
