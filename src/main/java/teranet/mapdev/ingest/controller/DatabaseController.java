package teranet.mapdev.ingest.controller;

import teranet.mapdev.ingest.dto.DatabaseConnectionInfoDto;
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
import java.util.Map;

/**
 * Controller for database operations
 * Handles database connection info and health checks
 */
// @RestController
@RequestMapping("/api/v1/ingest/database")
@CrossOrigin(origins = "*", maxAge = 3600)
@Tag(name = "Database Operations", description = "Database connection and health monitoring")
public class DatabaseController {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseController.class);

    @Autowired
    private DatabaseConnectionService databaseConnectionService;

    /**
     * Get database connection information
     */
    @Operation(
        summary = "Get database connection info",
        description = "Retrieve current database connection details and status"
    )
    @GetMapping("/info")
    public ResponseEntity<DatabaseConnectionInfoDto> getConnectionInfo() {
        logger.info("Retrieving database connection information");

        try {
            DatabaseConnectionInfoDto connectionInfo = databaseConnectionService.getConnectionInfo();
            return ResponseEntity.ok(connectionInfo);

        } catch (Exception e) {
            logger.error("Failed to retrieve connection information", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(null);
        }
    }

    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    @Operation(
        summary = "Database health check",
        description = "Check database connectivity and readiness status"
    )
    @ApiResponse(responseCode = "200", description = "System is healthy")
    @ApiResponse(responseCode = "503", description = "System is unhealthy")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        logger.debug("Performing database health check");

        try {
            DatabaseConnectionInfoDto connectionInfo = databaseConnectionService.getConnectionInfo();

            Map<String, Object> health = new HashMap<>();
            health.put("status", "HEALTHY");
            health.put("database_connection", connectionInfo.getConnectionStatus());
            health.put("staging_ready", connectionInfo.getStagingSchemaExists());
            health.put("timestamp", java.time.LocalDateTime.now());

            return ResponseEntity.ok(health);

        } catch (Exception e) {
            logger.error("Health check failed", e);

            Map<String, Object> health = new HashMap<>();
            health.put("status", "UNHEALTHY");
            health.put("error", e.getMessage());
            health.put("timestamp", java.time.LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(health);
        }
    }
}
