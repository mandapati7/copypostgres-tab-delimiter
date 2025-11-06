package teranet.mapdev.ingest.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Simple DTO for error responses.
 * Used for validation errors and other client errors.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponseDto {
    
    @JsonProperty("error")
    private String error;
    
    @JsonProperty("message")
    private String message;
    
    @JsonProperty("timestamp")
    private LocalDateTime timestamp;
    
    /**
     * Create error response with current timestamp
     */
    public ErrorResponseDto(String error, String message) {
        this.error = error;
        this.message = message;
        this.timestamp = LocalDateTime.now();
    }
}
