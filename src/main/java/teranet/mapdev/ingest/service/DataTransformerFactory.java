package teranet.mapdev.ingest.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import teranet.mapdev.ingest.model.FileValidationRule;
import teranet.mapdev.ingest.transformer.DataTransformer;
import teranet.mapdev.ingest.transformer.NoOpTransformer;

/**
 * Factory service for creating and caching DataTransformer instances.
 * 
 * This service:
 * - Dynamically loads transformer classes based on database configuration
 * - Caches transformer instances for performance
 * - Handles errors gracefully (falls back to NoOpTransformer)
 * - Initializes transformers with their configuration
 */
@Service
@Slf4j
public class DataTransformerFactory {
    
    // Cache for transformer instances (class name -> instance)
    private final java.util.Map<String, DataTransformer> transformerCache = 
            new java.util.concurrent.ConcurrentHashMap<>();
    
    /**
     * Get a transformer for the given validation rule.
     * 
     * The transformer is determined by:
     * 1. Check if enable_data_transformation is TRUE
     * 2. Load class specified in transformer_class_name
     * 3. Initialize with transformation_config if present
     * 4. Return NoOpTransformer if disabled or class not found
     * 
     * @param rule The file validation rule containing transformation config
     * @return DataTransformer instance (never null)
     */
    public DataTransformer getTransformer(FileValidationRule rule) {
        if (rule == null) {
            log.debug("No validation rule provided, using NoOpTransformer");
            return new NoOpTransformer();
        }
        
        // Check if transformation is enabled
        if (!Boolean.TRUE.equals(rule.getEnableDataTransformation())) {
            log.debug("Data transformation disabled for pattern: {}", rule.getFilePattern());
            return new NoOpTransformer();
        }
        
        // Check if transformer class is specified
        String className = rule.getTransformerClassName();
        if (className == null || className.trim().isEmpty()) {
            log.debug("No transformer class specified for pattern: {}, using NoOpTransformer", 
                    rule.getFilePattern());
            return new NoOpTransformer();
        }
        
        // Return cached instance if available
        if (transformerCache.containsKey(className)) {
            log.debug("Returning cached transformer: {}", className);
            return transformerCache.get(className);
        }
        
        // Load and initialize new transformer
        try {
            log.info("Loading transformer class: {} for pattern: {}", className, rule.getFilePattern());
            
            Class<?> clazz = Class.forName(className.trim());
            
            if (!DataTransformer.class.isAssignableFrom(clazz)) {
                log.error("Class {} does not implement DataTransformer interface", className);
                return new NoOpTransformer();
            }
            
            DataTransformer transformer = (DataTransformer) clazz.getDeclaredConstructor().newInstance();
            
            // Initialize transformer
            log.info("Initializing transformer: {}", className);
            transformer.initialize();
            
            // Cache the instance
            transformerCache.put(className, transformer);
            
            log.info("Successfully loaded and initialized transformer: {}", className);
            return transformer;
            
        } catch (ClassNotFoundException e) {
            log.error("Transformer class not found: {}. Using NoOpTransformer.", className, e);
        } catch (Exception e) {
            log.error("Error loading transformer class: {}. Using NoOpTransformer.", className, e);
        }
        
        return new NoOpTransformer();
    }
    
    /**
     * Clear the transformer cache.
     * Useful for hot-reloading transformers or cleaning up resources.
     */
    public void clearCache() {
        log.info("Clearing transformer cache ({} entries)", transformerCache.size());
        
        // Call cleanup on all cached transformers
        transformerCache.values().forEach(transformer -> {
            try {
                transformer.cleanup();
            } catch (Exception e) {
                log.warn("Error during transformer cleanup", e);
            }
        });
        
        transformerCache.clear();
    }
}
