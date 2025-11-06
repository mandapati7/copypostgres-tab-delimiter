package teranet.mapdev.ingest.controller;

import teranet.mapdev.ingest.dto.CsvUploadResponseDto;
import teranet.mapdev.ingest.dto.ErrorResponseDto;
import teranet.mapdev.ingest.dto.IngestionStatusDto;
import teranet.mapdev.ingest.model.IngestionManifest;
import teranet.mapdev.ingest.service.CsvProcessingService;
import teranet.mapdev.ingest.service.IngestionManifestService;
import teranet.mapdev.ingest.util.FileValidationUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

/**
 * Controller for single CSV file operations
 * Handles individual CSV file uploads and processing to staging tables
 */
@RestController
@RequestMapping("/api/v1/ingest/csv")
@CrossOrigin(origins = "*", maxAge = 3600)
@Tag(name = "CSV Operations", description = "Single CSV file upload and processing")
public class CsvController {
    
    private static final Logger logger = LoggerFactory.getLogger(CsvController.class);
    
    @Autowired
    private CsvProcessingService csvProcessingService;
    
    @Autowired
    private IngestionManifestService manifestService;

    /**
     * Upload single CSV file to staging table
     */
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(
        summary = "Upload CSV file to staging",
        description = "Upload a single CSV file to create a staging table with automatic schema detection"
    )
    @ApiResponse(responseCode = "200", description = "File successfully uploaded and processed")
    @ApiResponse(responseCode = "400", description = "Invalid file or file format")
    @ApiResponse(responseCode = "500", description = "Processing error")
    public ResponseEntity<?> uploadCsvToStaging(
            @Parameter(
                description = "CSV file to upload",
                required = true,
                content = @Content(mediaType = MediaType.MULTIPART_FORM_DATA_VALUE)
            )
            @RequestParam("file") MultipartFile file) {
        
        logger.info("Processing CSV file to staging area: {}", file.getOriginalFilename());
        
        try {
            // Validate file using utility
            FileValidationUtil.validateFile(file, "csv");
            
            // Process to staging
            IngestionManifest manifest = csvProcessingService.processCsvToStaging(file);
            
            // Check if this was a previously processed file using the transient flag
            String message;
            String status;
            
            if (manifest.isAlreadyProcessed()) {
                // Previously processed file (idempotency)
                message = String.format(
                    "File already processed previously. Original processing completed on %s. " +
                    "Batch ID: %s. No duplicate data was inserted.",
                    manifest.getCompletedAt().toString(),
                    manifest.getBatchId()
                );
                status = "ALREADY_PROCESSED";
                logger.info("CSV file was already processed - returning existing Batch ID: {}", manifest.getBatchId());
            } else {
                // Newly processed file
                message = "CSV file processed to staging area successfully";
                status = "COMPLETED";
                logger.info("CSV file processed to staging successfully - Batch ID: {}", manifest.getBatchId());
            }
            
            CsvUploadResponseDto response = new CsvUploadResponseDto(
                manifest.getBatchId(),
                file.getOriginalFilename(),
                manifest.getTableName(),  // Include staging table name
                file.getSize(),
                status,
                message
            );
            
            return ResponseEntity.ok(response);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Validation error: {}", e.getMessage());
            ErrorResponseDto errorResponse = new ErrorResponseDto(
                "Validation Error",
                e.getMessage()
            );
            return ResponseEntity.badRequest().body(errorResponse);
            
        } catch (Exception e) {
            logger.error("Failed to process CSV file to staging: {}", file.getOriginalFilename(), e);
            ErrorResponseDto errorResponse = new ErrorResponseDto(
                "Processing Error",
                "Failed to process file: " + e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get processing status by batch ID
     */
    @GetMapping("/status/{batchId}")
    @Operation(
        summary = "Get CSV processing status",
        description = "Retrieve the processing status for a specific batch ID"
    )
    @ApiResponse(responseCode = "200", description = "Status retrieved successfully")
    @ApiResponse(responseCode = "404", description = "Batch ID not found")
    public ResponseEntity<IngestionStatusDto> getProcessingStatus(@PathVariable String batchId) {
        
        logger.info("Retrieving processing status for batch: {}", batchId);
        
        try {
            IngestionManifest manifest = manifestService.findByBatchId(java.util.UUID.fromString(batchId));
            
            if (manifest == null) {
                return ResponseEntity.notFound().build();
            }
            
            IngestionStatusDto status = new IngestionStatusDto(manifest);
            
            return ResponseEntity.ok(status);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid batch ID format: {}", batchId);
            return ResponseEntity.badRequest().build();
            
        } catch (Exception e) {
            logger.error("Failed to retrieve status for batch: {}", batchId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
