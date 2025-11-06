package teranet.mapdev.ingest.controller;

import teranet.mapdev.ingest.service.DatabaseConnectionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Controller for staging table management
 * Handles listing and dropping staging tables based on configurable prefix
 */
@RestController
@RequestMapping("/api/v1/ingest/staging")
@CrossOrigin(origins = "*", maxAge = 3600)
@Tag(name = "Staging Management", description = "Staging table management operations")
public class StagingController {
    
    private static final Logger logger = LoggerFactory.getLogger(StagingController.class);
    
    @Autowired
    private DatabaseConnectionService databaseConnectionService;

    /**
     * Get all staging tables filtered by configurable prefix
     * Uses the staging table prefix from application.properties (default: "staging")
     * Filters tables using SQL LIKE operator: {prefix}_%
     * 
     * @return list of staging tables matching the configured prefix
     */
    @GetMapping("/tables")
    @Operation(
        summary = "List staging tables",
        description = "Retrieves all staging tables filtered by the configured table prefix (default: staging_)"
    )
    @ApiResponse(responseCode = "200", description = "Successfully retrieved staging tables")
    public ResponseEntity<Map<String, Object>> getStagingTables() {
        logger.info("Retrieving staging tables from default schema");
        
        try {
            List<String> tables = databaseConnectionService.getStagingTables();
            
            Map<String, Object> response = new HashMap<>();
            response.put("schema", "default");
            response.put("table_count", tables.size());
            response.put("tables", tables);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to retrieve staging tables", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
    
    /**
     * Drop all staging tables filtered by configurable prefix
     * Uses the staging table prefix from application.properties (default: "staging")
     * Drops all tables matching the pattern: {prefix}_%
     * 
     * @return summary of dropped tables
     */
    @DeleteMapping("/tables")
    @Operation(
        summary = "Drop all staging tables",
        description = "Drops all staging tables filtered by the configured table prefix. WARNING: This action is irreversible!"
    )
    @ApiResponse(responseCode = "200", description = "Successfully dropped staging tables")
    @ApiResponse(responseCode = "500", description = "Failed to drop staging tables")
    public ResponseEntity<Map<String, Object>> dropAllStagingTables() {
        logger.warn("Received request to drop all staging tables");
        
        try {
            Map<String, Object> result = databaseConnectionService.dropAllStagingTables();
            
            logger.info("Successfully dropped {} staging tables", result.get("tables_dropped"));
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            logger.error("Failed to drop staging tables", e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to drop staging tables: " + e.getMessage());
            errorResponse.put("tables_dropped", 0);
            
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
