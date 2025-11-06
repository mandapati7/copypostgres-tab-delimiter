package teranet.mapdev.ingest.controller;

import teranet.mapdev.ingest.dto.BatchProcessingResultDto;
import teranet.mapdev.ingest.dto.ErrorResponseDto;
import teranet.mapdev.ingest.service.BatchProcessingService;
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

import java.util.List;

/**
 * Controller for batch processing operations
 * Handles processing multiple CSV files in a single batch operation
 */
@RestController
@RequestMapping("/api/v1/ingest/batch")
@CrossOrigin(origins = "*", maxAge = 3600)
@Tag(name = "Batch Operations", description = "Batch processing for multiple CSV files")
public class BatchController {
    
    private static final Logger logger = LoggerFactory.getLogger(BatchController.class);
    
    @Autowired
    private BatchProcessingService batchProcessingService;

    /**
     * Process multiple CSV files as a batch to staging
     */
    @PostMapping(value = "/process", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(
        summary = "Process batch of CSV files to staging",
        description = "Upload multiple CSV files and process them all to staging tables in a single batch"
    )
    @ApiResponse(responseCode = "200", description = "Batch successfully processed")
    @ApiResponse(responseCode = "400", description = "Invalid files")
    @ApiResponse(responseCode = "500", description = "Processing error")
    public ResponseEntity<?> processBatchToStaging(
            @Parameter(
                description = "Multiple CSV files to upload",
                required = true,
                content = @Content(mediaType = MediaType.MULTIPART_FORM_DATA_VALUE)
            )
            @RequestParam("files") List<MultipartFile> csvFiles) {
        
        logger.info("Processing batch of {} CSV files to staging area", csvFiles.size());
        
        try {
            // Validate files using utility
            FileValidationUtil.validateFiles(csvFiles, "csv");
            
            // Process batch to staging
            BatchProcessingResultDto result = batchProcessingService.processBatchFromFiles(csvFiles);
            
            logger.info("Batch processing completed successfully");
            
            return ResponseEntity.ok(result);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Validation error: {}", e.getMessage());
            ErrorResponseDto errorResponse = new ErrorResponseDto(
                "Validation Error",
                e.getMessage()
            );
            return ResponseEntity.badRequest().body(errorResponse);
            
        } catch (Exception e) {
            logger.error("Failed to process batch to staging", e);
            ErrorResponseDto errorResponse = new ErrorResponseDto(
                "Processing Error",
                "Failed to process batch: " + e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
