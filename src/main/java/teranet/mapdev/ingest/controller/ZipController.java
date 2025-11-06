package teranet.mapdev.ingest.controller;

import teranet.mapdev.ingest.dto.BatchProcessingResultDto;
import teranet.mapdev.ingest.dto.ErrorResponseDto;
import teranet.mapdev.ingest.dto.ZipAnalysisDto;
import teranet.mapdev.ingest.service.BatchProcessingService;
import teranet.mapdev.ingest.service.ZipProcessingService;
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
 * Controller for ZIP file operations
 * Handles ZIP file analysis and processing containing multiple CSV files
 */
@RestController
@RequestMapping("/api/v1/ingest/zip")
@CrossOrigin(origins = "*", maxAge = 3600)
@Tag(name = "ZIP Operations", description = "ZIP file analysis and processing")
public class ZipController {
    
    private static final Logger logger = LoggerFactory.getLogger(ZipController.class);
    
    @Autowired
    private ZipProcessingService zipProcessingService;
    
    @Autowired
    private BatchProcessingService batchProcessingService;

    /**
     * Analyze ZIP file contents
     */
    @Operation(
        summary = "Analyze ZIP file",
        description = "Extract and analyze contents of a ZIP file containing CSV files"
    )
    @ApiResponse(responseCode = "200", description = "ZIP file analyzed successfully")
    @ApiResponse(responseCode = "400", description = "Invalid ZIP file")
    @PostMapping(value = "/analyze", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> analyzeZipFile(
        @Parameter(
            description = "ZIP file containing CSV files",
            content = @Content(mediaType = MediaType.MULTIPART_FORM_DATA_VALUE)
        )
        @RequestParam("file") MultipartFile zipFile) {
        
        logger.info("Analyzing ZIP file: {}", zipFile.getOriginalFilename());
        
        try {
            // Validate file using utility
            FileValidationUtil.validateFile(zipFile, "zip");
            
            // Analyze ZIP contents
            ZipAnalysisDto analysis = zipProcessingService.analyzeZipFile(zipFile);
            
            logger.info("ZIP analysis completed for: {}", zipFile.getOriginalFilename());
            return ResponseEntity.ok(analysis);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Validation error: {}", e.getMessage());
            ErrorResponseDto errorResponse = new ErrorResponseDto(
                "Validation Error",
                e.getMessage()
            );
            return ResponseEntity.badRequest().body(errorResponse);
            
        } catch (Exception e) {
            logger.error("Failed to analyze ZIP file: {}", zipFile.getOriginalFilename(), e);
            ErrorResponseDto errorResponse = new ErrorResponseDto(
                "Processing Error",
                "Failed to process ZIP file: " + e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Process ZIP file to staging tables
     */
    @PostMapping(value = "/process", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(
        summary = "Process ZIP file to staging",
        description = "Upload a ZIP file containing multiple CSV files and process them all to staging tables"
    )
    @ApiResponse(responseCode = "200", description = "ZIP file successfully processed")
    @ApiResponse(responseCode = "400", description = "Invalid ZIP file")
    @ApiResponse(responseCode = "500", description = "Processing error")
    public ResponseEntity<?> processZipToStaging(
            @Parameter(
                description = "ZIP file containing CSV files",
                required = true,
                content = @Content(mediaType = MediaType.MULTIPART_FORM_DATA_VALUE)
            )
            @RequestParam("file") MultipartFile zipFile) {
        
        logger.info("Processing ZIP file to staging area: {}", zipFile.getOriginalFilename());
        
        try {
            // Validate file using utility
            FileValidationUtil.validateFile(zipFile, "zip");
            
            // Process ZIP to staging
            BatchProcessingResultDto result = batchProcessingService.processBatchFromZip(zipFile);
            
            logger.info("ZIP processing completed for: {}", zipFile.getOriginalFilename());
            
            return ResponseEntity.ok(result);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Validation error: {}", e.getMessage());
            ErrorResponseDto errorResponse = new ErrorResponseDto(
                "Validation Error",
                e.getMessage()
            );
            return ResponseEntity.badRequest().body(errorResponse);
            
        } catch (Exception e) {
            logger.error("Failed to process ZIP file to staging: {}", zipFile.getOriginalFilename(), e);
            ErrorResponseDto errorResponse = new ErrorResponseDto(
                "Processing Error",
                "Failed to process ZIP file: " + e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
