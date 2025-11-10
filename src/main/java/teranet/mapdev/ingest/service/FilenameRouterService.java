package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import teranet.mapdev.ingest.config.CsvProcessingConfig;
import teranet.mapdev.ingest.config.DatabaseConfig;
import teranet.mapdev.ingest.config.IngestConfig;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service for routing files to target tables based on filename patterns
 * 
 * Supports legacy Polaris file naming conventions:
 * - PM162 -> staging_pm1 (Property Management table 1)
 * - PM262 -> staging_pm2 (Property Management table 2)
 * - IM362 -> staging_im3 (Image Management table 3)
 * 
 * The regex pattern and template are configurable via application.properties:
 * - ingest.filename-routing.regex: Pattern to extract components from filename
 * - ingest.filename-routing.template: Template to construct table name from regex groups
 * - csv.processing.staging-table-prefix: Prefix to add before table name (e.g., "staging")
 */
@Service
public class FilenameRouterService {

    private static final Logger log = LoggerFactory.getLogger(FilenameRouterService.class);
    
    private final DatabaseConfig databaseConfig;
    private final IngestConfig ingestConfig;
    private final CsvProcessingConfig csvProcessingConfig;
    private final Pattern routingPattern;
    
    public FilenameRouterService(DatabaseConfig databaseConfig, 
                                  IngestConfig ingestConfig,
                                  CsvProcessingConfig csvProcessingConfig) {
        this.databaseConfig = databaseConfig;
        this.ingestConfig = ingestConfig;
        this.csvProcessingConfig = csvProcessingConfig;
        
        // Compile the routing pattern from configuration
        String regex = ingestConfig.getFilenameRouting().getRegex();
        this.routingPattern = Pattern.compile(regex);
        
        log.info("FilenameRouter initialized with pattern: {}, template: {}, prefix: {}", 
                regex, 
                ingestConfig.getFilenameRouting().getTemplate(),
                csvProcessingConfig.getStagingTablePrefix());
    }
    
    /**
     * Check if filename routing is enabled
     */
    public boolean isEnabled() {
        return ingestConfig.getFilenameRouting().isEnabled();
    }
    
    /**
     * Resolve filename to target table name with staging prefix
     * 
     * @param filename The filename to resolve (with or without extension)
     * @return The target table name with staging prefix (e.g., "staging_pm1")
     * @throws IllegalArgumentException if filename doesn't match the routing pattern
     */
    public String resolveTableName(String filename) {
        // Strip extension from filename
        String baseFilename = stripExtension(filename);
        
        // Match against pattern
        Matcher matcher = routingPattern.matcher(baseFilename);
        
        if (!matcher.matches()) {
            String message = String.format(
                "Filename '%s' does not match routing pattern '%s'", 
                baseFilename, 
                routingPattern.pattern()
            );
            log.error(message);
            throw new IllegalArgumentException(message);
        }
        
        // Build table name from template and regex groups
        String template = ingestConfig.getFilenameRouting().getTemplate();
        String tableName = template;
        
        // Replace ${g1}, ${g2}, etc. with captured groups
        for (int i = 1; i <= matcher.groupCount(); i++) {
            String groupValue = matcher.group(i);
            tableName = tableName.replace("${g" + i + "}", groupValue);
        }
        
        // Convert table name to lowercase for PostgreSQL compatibility
        tableName = tableName.toLowerCase();
        
        // Add staging prefix from configuration (e.g., "staging_")
        String stagingPrefix = csvProcessingConfig.getStagingTablePrefix();
        if (stagingPrefix != null && !stagingPrefix.isEmpty()) {
            tableName = stagingPrefix + "_" + tableName;
        }
        
        log.debug("Resolved filename '{}' to table name '{}'", filename, tableName);
        
        return tableName;
    }
    
    /**
     * Get fully qualified table name (schema.table)
     * 
     * @param filename The filename to resolve
     * @return Fully qualified table name (e.g., "public.PM1")
     */
    public String getFullyQualifiedTableName(String filename) {
        String tableName = resolveTableName(filename);
        return tableName;
    }
    
    /**
     * Strip extension from filename
     * 
     * @param filename Filename with or without extension
     * @return Filename without extension
     */
    private String stripExtension(String filename) {
        if (filename == null) {
            return null;
        }
        
        int lastDotIndex = filename.lastIndexOf('.');
        if (lastDotIndex > 0) {
            return filename.substring(0, lastDotIndex);
        }
        
        return filename;
    }
    
    /**
     * Validate that a filename can be routed
     * 
     * @param filename The filename to validate
     * @return true if filename matches routing pattern, false otherwise
     */
    public boolean canRoute(String filename) {
        try {
            resolveTableName(filename);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
